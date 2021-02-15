import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoUpload import MongoUploadDF


class TournamentDFHandler:
    max_hole_dist = 700
    max_green_dist = 50
    arg_green_dist = 30
    long_putt_dist = 12
    adv_pct = .5
    pd.set_option('display.max_columns', None)

    @staticmethod
    def getNameAbbr(row):
        return '. '.join([row.firstName[0], row.lastName])

    @staticmethod
    def getShotType(row):
        if row.shotDistance == 0:
            val = 'Penalty'
        elif row.fromSurface == 'OTB' and row.par in [4, 5]:
            val = 'TEE'
        elif row.fromSurface in ['OGR', 'OCO']:
            if row.startDistance > TournamentDFHandler.long_putt_dist * 12:
                if row.fromSurface == 'OCO':
                    val = 'ARG'
                else:
                    val = 'LNG_PUTT'
            else:
                val = 'SHT_PUTT'
        elif row.fromSurface in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH', 'OTB', 'OWL', 'OBR', 'OWA'] \
                and row.startDistance > (36 * TournamentDFHandler.arg_green_dist):
            val = 'APP'
        elif row.fromSurface in ['OFW', 'ORO', 'OST', 'OIR', 'ONA', 'OTH', 'OGS', 'OWL', 'OBR', 'OWA']:
            val = 'ARG'
        else:
            print('Unidentified from val {}'.format(row.fromSurface))
            val = 'Unknown'
        return val

    @staticmethod
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
        elif row.to in ['ERR', 'ELR', 'ORO', 'OCA', 'OWL', 'OBR']:
            val = 'Rough'
        elif row.to in ['OST', 'EG2', 'EG5', 'EG6', 'EG1', 'EG4', 'EG3', 'EG7', 'OGS', 'EG8']:
            val = 'Bunker'
        elif row.to in ['ONA', 'OTH', 'OUK', 'OTB']:
            val = 'Trouble'
        elif row.to == 'OWA':
            val = 'Water'
        else:
            print('Unidentified to val {}'.format(row.to))
            val = 'Unknown'
        return direction, val

    @staticmethod
    def getDateTimes(dates_str):
        dates, year = dates_str.strip().split(',')
        first_day, last_day = dates.strip().split('-')
        return datetime.strptime('{} {}'.format(first_day.strip(), year), '%A %b %d %Y'), datetime.strptime(
            '{} {}'.format(last_day.strip(), year), '%A %b %d %Y')

    @staticmethod
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

    @staticmethod
    def getBinValues(cut_on, end_bin, interval, yds_or_feet):
        if yds_or_feet == 'ft':
            multiplier = 3
        else:
            multiplier = 1

        labels = []
        for x in range(0, end_bin * multiplier, interval):
            labels.append('({} to {}] {}'.format(x, x + interval, yds_or_feet))
        return pd.cut(x=cut_on,
                      bins=np.linspace(0, end_bin * 36,
                                       int((end_bin * multiplier) / interval) + 1),
                      precision=0,
                      labels=labels,
                      include_lowest=True,
                      right=True)

    @staticmethod
    def createHoleLevelDict(tournament_year_dict):
        year_course_hole_round = {}
        for pga_year in tournament_year_dict.keys():
            dates_str = tournament_year_dict[pga_year]['dates']
            first_dt, last_dt = TournamentDFHandler.getDateTimes(dates_str)

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
                        hole_based_dict[player_hole['holeNumber']][player_round['roundNumber']][
                            'playerShots'][player_round['playerID']] = player_hole['shots']
                course_dict[course_id] = hole_based_dict
            year_course_hole_round[pga_year] = course_dict
        return year_course_hole_round

    def __init__(self, mongo_obj, tournament_name_scrape, tournament_name_sg, force_create_sg=False,
                 force_create_tournament=False):
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'Analysis/logs/{}.log'.format(self.__class__.__name__)).getLogger()
        self._logger.info('Initializing Tournament DF Handler for tournament {}'.format(tournament_name_scrape))
        self._tournament_name = tournament_name_scrape
        self._mongo_obj = mongo_obj
        mongo_download = MongoDownload(self._mongo_obj)
        self._mongo_upload_df = MongoUploadDF(self._mongo_obj, self._tournament_name)
        self._raw_sg_df = pd.DataFrame(mongo_download.getRawSG_DF(tournament_name_scrape))
        if self._raw_sg_df.empty or force_create_sg:
            self._raw_sg_df = pd.DataFrame()
            self._createRawSG_DF(mongo_download.getSGStatsForTournament(tournament_name_scrape, tournament_name_sg),
                                 mongo_download.getPlayerNames())
            self.uploadRawSG_DF()

        self._tournament_df = pd.DataFrame(mongo_download.getTournamentDF(tournament_name_scrape))
        if self._tournament_df.empty or force_create_tournament:
            self._tournament_df = pd.DataFrame()
            self._createTournamentDF(mongo_download.consolidateTournamentInfo(tournament_name_scrape),
                                     mongo_download.getPlayerNames())
            self.uploadTournamentDF()

    def __repr__(self):
        success = True
        if self._tournament_df.empty:
            success = False
        return 'Tournament {} DF successfully created {}\n'.format(self._tournament_name, success)

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
        hole_df.drop(columns='holeDistance', inplace=True)
        hole_df['startDistance'] = hole_df.startDistance.fillna(value=hole_df.distanceLeft.shift(1))
        player_group = hole_df.groupby(by='playerID', group_keys=False)
        hole_df = hole_df[player_group.apply(lambda x: x.shot_id != x.shot_id.shift(1))]
        player_group = hole_df.groupby(by='playerID')
        hole_df['playerScore'] = player_group.shot_id.transform('max')
        hole_df['holeAvg'] = player_group.shot_id.max().mean()
        hole_df['shotsRemaining'] = player_group.cumcount(ascending=False)
        hole_df['shotType'] = hole_df.apply(TournamentDFHandler.getShotType, axis=1)
        hole_df['isAdvanced'] = (hole_df['shotType'] == 'APP') & \
                                (hole_df.distanceLeft > (self.adv_pct * hole_df.startDistance))
        hole_df['toSurface'] = hole_df.shotType.shift(-1)
        hole_df[['toLocation', 'toSurface']] = hole_df.apply(TournamentDFHandler.getEndLocation, axis=1,
                                                             result_type='expand')
        hole_df.drop(hole_df[hole_df.shotType == 'Penalty'].index, inplace=True)
        hole_df.loc[hole_df.toLocation == 'Penalty', 'distanceLeft'] = \
            hole_df.startDistance.shift(-1).fillna(0)
        hole_df['isReTee'] = hole_df.apply(
            lambda x: x['startDistance'] == x['distanceLeft'] and x['shotType'] == 'TEE', axis=1)
        hole_df = self._getDistanceBins(hole_df)

        # self._logger.info('\nHole DF description\n{}'.
        #                   format(hole_df.describe(percentiles=[.5]).T))
        return hole_df

    def _getDistanceBins(self, hole_df):
        hole_df.loc[hole_df['shotType'] == 'TEE', 'startDistance10ydBin'] = TournamentDFHandler.getBinValues(
            hole_df[hole_df['shotType'] == 'TEE'].startDistance, self.max_hole_dist, 10, 'yds')
        hole_df.loc[(hole_df['shotType'] == 'TEE') | (hole_df['shotType'] == 'APP'), 'distanceLeft5ydBin'] = \
            TournamentDFHandler.getBinValues(hole_df[(hole_df['shotType'] == 'TEE') | (hole_df['shotType'] == 'APP')].
                                             distanceLeft, self.max_hole_dist, 5, 'yds')
        hole_df.loc[hole_df['shotType'] == 'APP', 'distanceLeft1ydBin'] = \
            TournamentDFHandler.getBinValues(hole_df[hole_df['shotType'] == 'APP'].
                                             distanceLeft, self.max_green_dist, 1, 'yd')
        hole_df.loc[hole_df['shotType'] != 'TEE', 'distanceLeft1ftBin'] = TournamentDFHandler.getBinValues(
            hole_df[hole_df['shotType'] != 'TEE'].distanceLeft, self.max_green_dist, 1, 'ft')
        return hole_df

    def _createTournamentDF(self, tournament_year_dict, player_names):
        self._logger.info('Creating New Tournament DF')
        year_course_hole_round = TournamentDFHandler.createHoleLevelDict(tournament_year_dict)
        for year in year_course_hole_round.keys():
            for course in year_course_hole_round[year].keys():
                for hole_num in year_course_hole_round[year][course].keys():
                    for round_num in year_course_hole_round[year][course][hole_num].keys():
                        if not year_course_hole_round[year][course][hole_num][round_num]['playerShots']:
                            continue
                        self._logger.info(
                            'Creating hole level DF for tournament {}, pga_year {}, course {}, hole {}, round {}\n'
                                .format(self._tournament_name, year, course, hole_num, round_num))
                        hole_df = pd.DataFrame.from_dict(
                            year_course_hole_round[year][course][hole_num][round_num])
                        self._tournament_df = self._tournament_df.append(
                            self._dfLogic(hole_df, year, course, hole_num, round_num))
        player_name_df = pd.DataFrame(player_names)
        self._tournament_df = pd.merge(self._tournament_df, player_name_df, on='playerID', how='left')
        self._tournament_df.reset_index()

    def _createRawSG_DF(self, sg_dict, player_names):
        self._logger.info('Creating New Raw SG DF')
        player_name_df = pd.DataFrame(player_names)
        # noinspection PyTypeChecker
        player_name_df['playerName'] = player_name_df.apply(TournamentDFHandler.getNameAbbr, axis=1)
        for year in sg_dict.keys():
            self._raw_sg_df = self._raw_sg_df.append(sg_dict[year]['sgStats'])
        numeric_cols = ['sgPUTT', 'sgARG', 'sgAPP', 'sgOTT', 'sgT2G', 'sgTOT']
        self._raw_sg_df[numeric_cols] = self._raw_sg_df[numeric_cols].apply(pd.to_numeric, axis=1)
        self._raw_sg_df = pd.merge(self._raw_sg_df, player_name_df, on='playerName', how='left')
        self._raw_sg_df.drop(columns=['playerName', 'tournamentName'], inplace=True)

    def getTournamentDF(self):
        return self._tournament_df

    def getRawSG_DF(self):
        return self._raw_sg_df

    def uploadTournamentDF(self):
        self._logger.info('Uploading Tournament DF')
        for course, course_tournament_df in self._tournament_df.groupby('courseID'):
            for year, year_tournament_df in course_tournament_df.groupby('pgaYear'):
                for round_num, round_tournament_df in year_tournament_df.groupby('roundNum'):
                    df_dict = round_tournament_df.drop(columns='shottext').to_dict('records')
                    upload_dict = {'tournamentName': self._tournament_name, 'courseID': course,
                                   'pgaYear': year, 'roundNum': round_num, 'df': df_dict}
                    self._mongo_upload_df.uploadTournamentDF(upload_dict)

    def uploadRawSG_DF(self):
        self._logger.info('Uploading Raw SG DF')
        for year, year_tournament_df in self._raw_sg_df.groupby('pgaYear'):
            df_dict = year_tournament_df.to_dict('records')
            upload_dict = {'tournamentName': self._tournament_name,
                           'pgaYear': year, 'df': df_dict}
            self._mongo_upload_df.uploadRawSG_DF(upload_dict)
