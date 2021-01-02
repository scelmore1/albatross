import logging
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoInitialization import MongoInitialization


def getShotType(row, arg_length=30, long_putt_length=12):
    if row.shotDistance == 0:
        val = 'Penalty'
    elif row.fromSurface == 'OTB' and row.par in [4, 5]:
        val = 'TEE'
    elif row.fromSurface in ['OGR', 'OCO']:
        if row.fromSurface == 'OCO' and row.startDistance > arg_length * 12:
            val = 'ARG'
        else:
            if row.startDistance > long_putt_length * 12:
                val = 'LNG_PUTT'
            else:
                val = 'SHT_PUTT'
    elif row.fromSurface in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH', 'OTB', 'OWA', 'OBR'] \
            and row.startDistance > (36 * arg_length):
        val = 'APP'
    elif row.fromSurface in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH', 'OGS', 'OWA', 'OBR']:
        val = 'ARG'
    # elif row.fromSurface in ['OUK', 'OWA']:
    #     val = 'Penalty'
    else:
        print('Unidentified from val {}'.format(row.fromSurface))
        val = 'Unknown'
    return val


def getEndLocation(row):
    if row.to in ['ELI', 'ELF', 'ELR', 'EG5', 'EG6', 'EG7']:
        direction = 'Left'
    elif row.to in ['ERI', 'ERF', 'ERR', 'EG2', 'EG1', 'EG3']:
        direction = 'Right'
    elif row.toSurface == 'Penalty':
        direction = 'Penalty'
    else:
        direction = ''

    if row.to == 'OGR':
        val = 'Green'
    elif row.to == 'hole':
        val = 'Hole'
    elif row.to in ['ELF', 'ERF', 'ERI', 'ELI', 'OFW', 'OIR', 'OCO']:
        val = 'Fairway'
    elif row.to in ['ERR', 'ELR', 'ORO', 'OCA', 'OWA', 'OBR']:
        val = 'Rough'
    elif row.to in ['OST', 'EG2', 'EG5', 'EG6', 'EG1', 'EG4', 'EG3', 'EG7', 'OGS', 'EG8']:
        val = 'Bunker'
    # elif row['to'] == 'OCA':
    #     val = 'CartPath'
    # elif row.to == 'OCO':
    #     val = 'Collar'
    elif row.to in ['ONA', 'OTH', 'OUK', 'OTB']:
        val = 'Trouble'
    elif row.to == 'OWA':
        val = 'Water'
    else:
        print('Unidentified to val {}'.format(row.to))
        val = 'Unknown'
    return direction, val


def getDateTimes(dates_str):
    dates, year = dates_str.strip().split(',')
    first_day, last_day = dates.strip().split('-')
    return datetime.strptime('{} {}'.format(first_day.strip(), year), '%A %b %d %Y'), datetime.strptime(
        '{} {}'.format(last_day.strip(), year), '%A %b %d %Y')


def getQuantiles(df, grouping='shotType', cut_on='distanceLeft'):
    shot_types = df.groupby(by=grouping)
    for name, group in shot_types:
        quantile = 20
        for i in range(20):
            if (group[cut_on].count() <= quantile) or \
                    (len(np.unique(
                        np.quantile(group[cut_on], np.linspace(0, 1, quantile, endpoint=False)))) < quantile):
                quantile -= 1
            else:
                break

        pct_labels = []
        for x in np.linspace(0, 100, quantile, endpoint=False):
            pct_labels.append('({:.2f}% to {:.2f}%]'.format(x, x + 100 / quantile))
        pct_labels.reverse()
        df['distanceLeftQuantileBin{}'.format(name)] = pd.qcut(group[cut_on],
                                                               q=quantile,
                                                               precision=0,
                                                               labels=pct_labels)
    return df


class dfHandler:
    max_hole_dist = 700
    max_green_dist = 50
    adv_pct = .5

    def __init__(self, mongo_download_obj, tournament_name_scrape, tournament_name_sg):
        self._logger = MyLogger('dfHandler', 'Analysis/logs/dfHandler.log', logging.INFO).getLogger()
        pd.set_option('display.max_columns', None)
        self._success = False
        self._year_course_hole_round = {}
        self._tournament_df = pd.DataFrame()
        self._tournament_name = tournament_name_scrape
        self._createHoleLevelDict(mongo_download_obj.getTournamentDict(tournament_name_scrape, tournament_name_sg))

    def __repr__(self):
        return 'Tournament {} DF successfully created {}\n'.format(self._tournament_name, self._success)

    def _createHoleLevelDict(self, tournament_year_dict):
        for pga_year in tournament_year_dict.keys():
            dates_str = tournament_year_dict[pga_year]['dates']
            first_dt, last_dt = getDateTimes(dates_str)

            course_dict = {}
            for course in tournament_year_dict[pga_year]['courses']:
                hole_based_dict = {}
                course_id = course['courseID']
                for course_hole in course['holes']:
                    hole_based_dict[course_hole['holeNumber']] = {}
                    for i, round_info in enumerate(course_hole['rounds']):
                        hole_based_dict[course_hole['holeNumber']][round_info['round_Id']] \
                            = {k: round_info[k] for k in round_info if k != 'round_Id'}
                        hole_based_dict[course_hole['holeNumber']][round_info['round_Id']].update(
                            {'roundDate': first_dt + timedelta(days=i),
                             'playerShots': {}})
                for player_round in tournament_year_dict[pga_year]['playerRounds']:
                    if course_id != player_round['courseId']:
                        continue
                    for player_hole in player_round['holes']:
                        hole_based_dict[player_hole['holeNumber']][player_round['roundNumber']]['playerShots'] \
                            [player_round['playerID']] = player_hole['shots']
                course_dict[course_id] = hole_based_dict
            self._year_course_hole_round[pga_year] = course_dict

    def _getBinValues(self, hole_df):
        five_yd_labels = []
        for x in range(0, self.max_hole_dist, 5):
            five_yd_labels.append('({} to {}] yds'.format(x, x + 5))
        hole_df['distanceLeft5ydBin'] = pd.cut(x=hole_df.distanceLeft,
                                               bins=np.linspace(0, self.max_hole_dist * 36,
                                                                int(self.max_hole_dist / 5) + 1),
                                               precision=0,
                                               labels=five_yd_labels,
                                               include_lowest=True,
                                               right=True)
        one_ft_labels = []
        for x in range(0, self.max_green_dist * 3, 1):
            one_ft_labels.append('({} to {}] ft'.format(x, x + 1))
        hole_df['distanceLeft1ftBin'] = pd.cut(x=hole_df.distanceLeft,
                                               bins=np.linspace(0, self.max_green_dist * 36,
                                                                (self.max_green_dist * 3) + 1),
                                               precision=0,
                                               labels=one_ft_labels,
                                               include_lowest=False,
                                               right=True)

    def _dfLogic(self, hole_df, year, course, hole_num, round_num):
        hole_df = hole_df.rename(columns={'distance': 'holeDistance'})
        hole_df['holeDistance'] = hole_df.holeDistance.astype(int) * 36
        hole_df['par'] = hole_df.par.astype(int)
        hole_df['stimp'] = hole_df.stimp.astype(np.float16)
        hole_df['roundDate'] = pd.to_datetime(hole_df.roundDate)
        hole_df['pgaYear'] = year
        hole_df['courseID'] = course
        hole_df['holeNum'] = hole_num
        hole_df['roundNum'] = round_num

        hole_df = hole_df[hole_df.playerShots.map(lambda l: len(l)) > 0]
        hole_df = hole_df.explode('playerShots')
        temp_df = pd.json_normalize(hole_df.playerShots)
        hole_df = pd.concat([hole_df.reset_index().drop(columns='playerShots'), temp_df], axis=1)
        del temp_df
        hole_df = hole_df.rename(columns={'distance': 'shotDistance',
                                          'from': 'fromSurface',
                                          'left': 'distanceLeft',
                                          'index': 'playerID'})
        hole_df['startDistance'] = np.nan
        hole_df.loc[hole_df.fromSurface == 'OTB', 'startDistance'] = hole_df.holeDistance
        hole_df['startDistance'] = hole_df.startDistance.fillna(value=hole_df.distanceLeft.shift(1))
        player_group = hole_df.groupby(by='playerID', group_keys=False)
        hole_df = hole_df[player_group.apply(lambda x: x.shot_id != x.shot_id.shift(1))]
        player_group = hole_df.groupby(by='playerID')
        hole_df['holeAvg'] = player_group.shot_id.max().mean()
        hole_df['shotsRemaining'] = player_group.cumcount(ascending=False)
        hole_df['shotType'] = hole_df.apply(getShotType, axis=1)
        hole_df['isAdvanced'] = (hole_df['shotType'] == 'APP') & \
                                (hole_df.distanceLeft > (self.adv_pct * hole_df.startDistance))
        hole_df['toSurface'] = hole_df.shotType.shift(-1)
        hole_df[['toLocation', 'toSurface']] = hole_df.apply(getEndLocation, axis=1,
                                                             result_type='expand')
        hole_df.drop(hole_df[hole_df.shotType == 'Penalty'].index, inplace=True)
        hole_df.loc[hole_df.toLocation == 'Penalty', 'distanceLeft'] = \
            hole_df.startDistance.shift(-1).fillna(0)
        self._getBinValues(hole_df)

        # self._logger.info('\nHole DF description\n{}'.
        #                   format(hole_df.describe(percentiles=[.5]).T))
        self._tournament_df = self._tournament_df.append(hole_df)

    def createTournamentDF(self):
        for year in self._year_course_hole_round.keys():
            for course in self._year_course_hole_round[year].keys():
                for hole_num in self._year_course_hole_round[year][course].keys():
                    for round_num in self._year_course_hole_round[year][course][hole_num].keys():
                        if not self._year_course_hole_round[year][course][hole_num][round_num]['playerShots']:
                            continue
                        self._logger.info(
                            'Creating hole level DF for tournament {}, year {}, course {}, hole {}, round {}\n'
                                .format(self._tournament_name, year, course, hole_num, round_num))
                        hole_df = pd.DataFrame.from_dict(
                            self._year_course_hole_round[year][course][hole_num][round_num])
                        self._dfLogic(hole_df, year, course, hole_num, round_num)

        self._success = True
        return self._tournament_df

    def getSGStats(self):
        pass

    def getHoleLevelDict(self):
        return self._year_course_hole_round

    def getTournamentDF(self):
        return self._tournament_df


if __name__ == '__main__':
    analysis_logger = MyLogger('Analysis', 'Analysis/logs/hole_df.log', logging.INFO).getLogger()
    mongo_obj = MongoInitialization()
    mongo_download = MongoDownload(mongo_obj)
    df_handler = dfHandler(mongo_download, 'waste-management-phoenix-open',
                           'Waste Management Phoenix Open')
    hole_lvl_dict = df_handler.getHoleLevelDict()
    tournament_df = df_handler.createTournamentDF()

    tee_shots = tournament_df[tournament_df.shotType == 'TEE']
    app_shots = tournament_df[tournament_df.shotType == 'APP']
    lng_putts = tournament_df[tournament_df.shotType == 'LNG_PUTT']
    sht_putts = tournament_df[tournament_df.shotType == 'SHT_PUTT']

    tee_shots_no_penalty = tee_shots[tee_shots['toLocation'] != 'Penalty']

    _ = sns.lmplot(data=tee_shots_no_penalty, x='distanceLeft', y='shotsRemaining', hue='toSurface', lowess=True)
    plt.show()
    _ = sns.lmplot(data=tee_shots_no_penalty, x='distanceLeft', y='shotsRemaining', hue='toSurface')
    plt.show()

    grouped_tee = tee_shots.groupby(['distanceLeft5ydBin', 'toSurface']).mean().reset_index()
    _ = sns.lmplot(data=grouped_tee[grouped_tee['distanceLeft'] < 10000], x='distanceLeft', y='shotsRemaining',
                   hue='toSurface', lowess=True)
    plt.show()
    _ = sns.lmplot(data=grouped_tee[grouped_tee['distanceLeft'] < 10000], x='distanceLeft', y='shotsRemaining',
                   hue='toSurface')
    plt.show()

    _ = sns.lmplot(data=app_shots, x='distanceLeft', y='shotsRemaining', hue='toSurface', lowess=True)
    plt.show()
    _ = sns.lmplot(data=app_shots, x='distanceLeft', y='shotsRemaining', hue='toSurface')
    plt.show()

    _ = sns.lmplot(data=lng_putts, x='distanceLeft', y='shotsRemaining', lowess=True)
    plt.show()
    _ = sns.lmplot(data=lng_putts, x='distanceLeft', y='shotsRemaining')
    plt.show()

    tournament_df.head()

    # for name, hole in tee_shots.groupby('holeNum'):
    #     # _ = sns.histplot(data=hole, x='shotsRemaining', hue='toSurface', kde=True,
    #     #                  kde_kws={'bw_adjust': 4}).set_title(name)
    #     # plt.show()
    #     _ = sns.lmplot(data=hole, x='distanceLeft', y='shotsRemaining', hue='toSurface', lowess=True, col='holeNum')
    #     _ = sns.lmplot(data=hole, x='distanceLeft', y='shotsRemaining', hue='toSurface', col='holeNum')
    #     plt.show()