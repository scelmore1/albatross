import logging
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoInitialization import MongoInitialization


def getShotType(row):
    if row['from'] == 'OTB':
        val = 'TEE'
    elif row['from'] in ['OGR', 'OCO']:
        if row['from'] == 'OCO' and row['startDistance'] > 20 * 12:
            val = 'ARG'
        else:
            val = 'PUTT'
    elif row['from'] in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH'] \
            and row['startDistance'] > (36 * 30):
        if row['left'] > (.5 * row['startDistance']):
            val = 'ADV'
        else:
            val = 'APP'
    elif row['from'] in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH']:
        val = 'ARG'
    elif row['from'] == 'OUK':
        val = 'Penalty'
    else:
        print('Unidentified from val {}'.format(row['from']))
        val = 'Unknown'
    return val


def getEndLocation(row):
    if row['toLocation'] == 'Penalty':
        return 'Penalty', 'Penalty'

    if row['to'] in ['ELI', 'ELF', 'ELR']:
        direction = 'Left'
    elif row['to'] in ['ERI', 'ERF', 'ERR']:
        direction = 'Right'
    else:
        direction = ''

    if row['to'] == 'OGR':
        val = 'Green'
    elif row['to'] == 'hole':
        val = 'Hole'
    elif row['to'] in ['ELF', 'ERF', 'ERI', 'ELI']:
        val = 'Fairway'
    elif row['to'] in ['ERR', 'ELR']:
        val = 'Rough'
    elif row['to'] == 'OST':
        val = 'Bunker'
    elif row['to'] == 'OCO':
        val = 'Collar'
    elif row['to'] in ['ONA', 'OTH']:
        val = 'Trouble'
    elif row['to'] == 'OTB':
        val = 'Drop'
    else:
        print('Unidentified from val {}'.format(row['to']))
        val = 'Unknown'
    return direction, val


class TournamentDF:

    def __init__(self, mongo_download_obj, tournament_name_scrape, tournament_name_sg):
        self._year_course_hole_round = {}
        self._first_dt = None
        self._last_dt = None
        self._createHoleLevelDict(mongo_download_obj.getTournamentDict(tournament_name_scrape, tournament_name_sg))

    def _getDateTimes(self, dates_str):
        dates, year = dates_str.strip().split(',')
        first_day, last_day = dates.strip().split('-')
        self._first_dt = datetime.strptime('{} {}'.format(first_day.strip(), year), '%A %b %d %Y')
        self._last_dt = datetime.strptime('{} {}'.format(last_day.strip(), year), '%A %b %d %Y')

    def _createHoleLevelDict(self, tournament_year_dict):
        for pga_year in tournament_year_dict.keys():
            dates_str = tournament_year_dict[pga_year]['dates']
            self._getDateTimes(dates_str)

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
                            {'roundDate': self._first_dt + timedelta(days=i),
                             'playerShots': {}})
                for player_round in tournament_year_dict[pga_year]['playerRounds']:
                    if course_id != player_round['courseId']:
                        continue
                    for player_hole in player_round['holes']:
                        hole_based_dict[player_hole['holeNumber']][player_round['roundNumber']]['playerShots'] \
                            [player_round['playerID']] = player_hole['shots']
                course_dict[course_id] = hole_based_dict
            self._year_course_hole_round[pga_year] = course_dict

    def createHoleDF(self, year, course, hole_num, round_num):
        hole_df = pd.DataFrame.from_dict(self._year_course_hole_round[year][course][hole_num][round_num])
        hole_df = hole_df.rename(columns={'distance': 'holeDistance'})
        hole_df['holeDistance'] = hole_df['holeDistance'].astype(int) * 36
        hole_df['par'] = hole_df['par'].astype(int)
        hole_df['stimp'] = hole_df['stimp'].astype(np.float16)
        hole_df['roundDate'] = pd.to_datetime(hole_df['roundDate'])

        hole_df = hole_df.explode('playerShots')
        temp_df = pd.json_normalize(hole_df['playerShots'])
        hole_df = pd.concat([hole_df.reset_index().drop(columns='playerShots'), temp_df], axis=1)
        del temp_df
        hole_df = hole_df.set_index('index')
        player_group = hole_df.groupby(by='index', group_keys=False)
        hole_df['shotsRemaining'] = player_group.cumcount(ascending=False)
        hole_df['startDistance'] = hole_df.apply(lambda x: x['holeDistance'] if x['from'] == 'OTB' else np.nan, axis=1)
        hole_df['startDistance'] = hole_df['startDistance'].fillna(value=hole_df['left'].shift(1))
        hole_df['drop'] = player_group.apply(lambda x: x['shot_id'] == x['shot_id'].shift(1))
        hole_df['shotType'] = hole_df.apply(getShotType, axis=1)

        hole_df['toLocation'] = hole_df['shotType'].shift(-1)
        hole_df[['toLocation', 'toSurface']] = hole_df.apply(getEndLocation, axis=1, result_type='expand')
        avg_shots = player_group['shot_id'].max().mean()

        print('\nHole DF description\n{}'.format(hole_df.describe()))
        print('\nHole averaged {} shots'.format(avg_shots))
        return hole_df

    def getHoleLevelDict(self):
        return self._year_course_hole_round


if __name__ == '__main__':
    analysis_logger = MyLogger('Analysis', 'Analysis/logs/hole_df.log', logging.INFO).getLogger()
    mongo_obj = MongoInitialization()
    mongo_download = MongoDownload(mongo_obj)
    tournament_df = TournamentDF(mongo_download, 'waste-management-phoenix-open', 'Waste Management Phoenix Open')
    hole_level_dict = tournament_df.getHoleLevelDict()

    # 2020 WMO, Round 1, Hole 1
    hole_df = tournament_df.createHoleDF('2020', '510', '1', '1')

    sg_df = tournament_df

    tee_shot = hole_df[hole_df['from'] == 'OTB']
    tee_shot['left'].describe()
    _ = sns.distplot(tee_shot['left'], hist=True, kde=True)
    plt.show()

    # app_shot_locations = ['OFW', 'ORO', 'OST', 'OIR', 'ONA']
    app_shot_locations = ['OFW', 'ORO', 'OST', 'OIR']
    app_shot = hole_df[(hole_df['from'].isin(app_shot_locations)) & (hole_df['distance'] > 36 * 30)]
    for location in app_shot_locations:
        subset = app_shot[(app_shot['from'] == location) & (app_shot['left'] < 2000)]
        _ = sns.distplot(subset['left'], hist=True, kde=True, label=location)
    plt.legend(prop={'size': 16}, title='location')
    plt.show()

    _ = sns.violinplot(y=tee_shot['left'], x=tee_shot['from'], hue=tee_shot['shotsRemaining'])
    plt.show()
    _ = sns.violinplot(y=tee_shot['shotsRemaining'], x=tee_shot['from'], hue=tee_shot['to'])
    plt.show()