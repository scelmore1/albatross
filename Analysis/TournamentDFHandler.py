import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoUpload import MongoUploadDF


class TournamentDFHandler:
    # ARG distance is 30 yds
    arg_green_dist = 30
    # long putt distance is 15 feet
    long_putt_dist = 15
    # if shot goes less than 50% of distance remaining it is only advanced
    adv_pct = .5

    # for debug purposes
    pd.set_option('display.max_columns', None)

    @staticmethod
    def getNameAbbr(row):
        """For deciphering names from raw SG"""
        return '. '.join([row.firstName[0], row.lastName])

    @staticmethod
    def getShotType(row):
        """Use what the data provides to create shot type and shot starting location columns"""
        if row.shotDistance == 0:
            val = ('Penalty', 'Penalty')
        elif row.fromSurface == 'OTB':
            if row.par in [4, 5]:
                val = ('TEE', 'Teebox')
            else:
                val = ('APP', 'Teebox')
        elif row.fromSurface in ['OGR', 'OCO']:
            if row.startDistance > TournamentDFHandler.long_putt_dist * 12:
                if row.fromSurface == 'OCO':
                    val = ('ARG', 'Fairway')
                else:
                    val = ('LNG_PUTT', 'Green')
            else:
                val = ('SHT_PUTT', 'Green')
        elif row.fromSurface in ['OFW', 'OIR', 'OWD']:
            if row.startDistance > (36 * TournamentDFHandler.arg_green_dist):
                val = ('APP', 'Fairway')
            else:
                val = ('ARG', 'Fairway')
        elif row.fromSurface in ['ORO', 'OBR', 'OWL']:
            if row.startDistance > (36 * TournamentDFHandler.arg_green_dist):
                val = ('APP', 'Rough')
            else:
                val = ('ARG', 'Rough')
        elif row.fromSurface in ['OST', 'OGS']:
            if row.startDistance > (36 * TournamentDFHandler.arg_green_dist):
                val = ('APP', 'Bunker')
            else:
                val = ('ARG', 'Bunker')
        elif row.fromSurface in ['ONA', 'OTH', 'OWA']:
            if row.startDistance > (36 * TournamentDFHandler.arg_green_dist):
                val = ('APP', 'Trouble')
            else:
                val = ('ARG', 'Trouble')
        else:
            print('Unidentified from val {}'.format(row.fromSurface))
            val = ('Unknown', 'Unknown')
        return val

    @staticmethod
    def getEndLocation(row):
        """Create more general end locations than what the data provides"""
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
        elif row.to in ['ERR', 'ELR', 'ORO', 'OCA', 'OWL', 'OBR', 'OTO', 'OLN']:
            val = 'Rough'
        elif row.to in ['OST', 'EG2', 'EG5', 'EG6', 'EG1', 'EG4', 'EG3', 'EG7', 'OGS', 'EG8']:
            val = 'Bunker'
        elif row.to in ['ONA', 'OTH', 'OUK', 'OTB', 'ODO']:
            val = 'Trouble'
        elif row.to == 'OWA':
            val = 'Water'
        else:
            print('Unidentified to val {}'.format(row.to))
            val = 'Unknown'
        return direction, val

    @staticmethod
    def getDateTimes(dates_str):
        """Helper method for creating datetimes"""
        dates, year = dates_str.strip().split(',')
        first_day, last_day = dates.strip().split('-')
        return datetime.strptime('{} {}'.format(first_day.strip(), year), '%A %b %d %Y'), datetime.strptime(
            '{} {}'.format(last_day.strip(), year), '%A %b %d %Y')

    # @staticmethod
    # def getQuantiles(df, grouping='shotType', cut_on='distanceLeft'):
    #     """Method for creating quantiles from certain columns and groupings"""
    #     shot_types = df.groupby(by=grouping)
    #     for name, group in shot_types:
    #         quantile = 20
    #         for i in range(20):
    #             if (group[cut_on].count() <= quantile) or \
    #                     (len(np.unique(
    #                         np.quantile(group[cut_on], np.linspace(0, 1, quantile, endpoint=False)))) < quantile):
    #                 quantile -= 1
    #             else:
    #                 break
    #
    #         pct_labels = []
    #         for x in np.linspace(0, 100, quantile, endpoint=False):
    #             pct_labels.append('({:.2f}% to {:.2f}%]'.format(x, x + 100 / quantile))
    #         pct_labels.reverse()
    #         df['distanceLeftQuantileBin{}'.format(name)] = pd.qcut(group[cut_on],
    #                                                                q=quantile,
    #                                                                precision=0,
    #                                                                labels=pct_labels)
    #     return df

    @staticmethod
    def createHoleLevelDict(tournament_year_dict):
        """Creates a hole level dictionary and also adds dates to each round."""
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

    def __init__(self, mongo_obj, tournament_name_scrape, tournament_name_sg, pga_year: list = None,
                 force_create: bool = False):
        """Get existing tournament DF from Mongo and check if there is data from a tournament scrape to add.
        Call create tournament DF if we need to create a new dataframe or append to what already exists."""
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'Analysis/logs/{}.log'.format(self.__class__.__name__)).getLogger()
        self._logger.info('Initializing Tournament DF Handler for tournament {}'.format(tournament_name_scrape))
        self._tournament_name = tournament_name_scrape
        self._mongo_obj = mongo_obj
        mongo_download = MongoDownload(self._mongo_obj)
        self._mongo_upload_df = MongoUploadDF(self._mongo_obj, self._tournament_name)
        self._tournament_df = pd.DataFrame(mongo_download.getTournamentDF(tournament_name_scrape, pga_year))
        self._raw_sg_df = pd.DataFrame(mongo_download.getTournamentRawSG_DF(tournament_name_scrape, pga_year))

        yearly_details = mongo_download.getTournamentDetailsByYear(tournament_name_scrape, pga_year)
        if self._tournament_df.empty or force_create:
            self._logger.info('Creating New Tournament DF')
            self._tournament_df = pd.DataFrame()
            self._raw_sg_df = pd.DataFrame()
        else:
            df_years = self._tournament_df['pgaYear'].unique().tolist()
            for year in df_years:
                del yearly_details[year]
            if yearly_details:
                self._logger.info('Append years {} to existing tournament DF'.format(list(yearly_details.keys())))

        if yearly_details:
            tournament_info_dict = mongo_download.consolidateTournamentInfo(yearly_details, tournament_name_scrape,
                                                                            tournament_name_sg)
            player_names = mongo_download.getPlayerNames()
            self._createTournamentDF(tournament_info_dict, player_names)
            self._createRawSG_DF(tournament_info_dict, player_names)
            self.uploadTournamentDF(yearly_details.keys())
            self.uploadRawSG_DF(yearly_details.keys())

    def __repr__(self):
        return 'Tournament {} DF looks like {}\n'.format(self._tournament_name, self._tournament_df)

    def _dfLogic(self, hole_df, year, course, hole_num, round_num):
        """Create a df for the given hole from each course, round, and year"""
        new_hole_df = hole_df.copy()
        new_hole_df.rename(columns={'distance': 'holeDistance'}, inplace=True)
        new_hole_df['holeDistance'] = new_hole_df.holeDistance.astype(int) * 36
        new_hole_df['par'] = new_hole_df.par.astype(int)
        new_hole_df['stimp'] = new_hole_df.stimp.astype(np.float16)
        new_hole_df['roundDate'] = pd.to_datetime(new_hole_df.roundDate)
        new_hole_df['pgaYear'] = year
        new_hole_df['courseID'] = course
        new_hole_df['holeNum'] = hole_num
        new_hole_df['roundNum'] = round_num

        new_hole_df = new_hole_df[new_hole_df.playerShots.map(lambda l: len(l)) > 0]
        new_hole_df = new_hole_df.explode('playerShots')
        temp_df = pd.json_normalize(new_hole_df.playerShots)
        new_hole_df = pd.concat([new_hole_df.reset_index().drop(columns='playerShots'), temp_df], axis=1)
        del temp_df
        if new_hole_df['left'].iloc[0] == 0:
            self._logger.warn('Course {} appears to be a non shot link course. Investigate.')
            return None

        new_hole_df = new_hole_df.rename(columns={'distance': 'shotDistance',
                                                  'from': 'fromSurface',
                                                  'left': 'distanceLeft',
                                                  'index': 'playerID'})
        new_hole_df['startDistance'] = np.nan
        new_hole_df.loc[new_hole_df.fromSurface == 'OTB', 'startDistance'] = new_hole_df.holeDistance
        new_hole_df.drop(columns='holeDistance', inplace=True)
        new_hole_df['startDistance'] = new_hole_df.startDistance.fillna(value=new_hole_df.distanceLeft.shift(1))
        player_group = new_hole_df.groupby(by='playerID', group_keys=False)
        new_hole_df = new_hole_df.loc[player_group.apply(lambda x: x.shot_id != x.shot_id.shift(1)), :].copy()
        player_group = new_hole_df.groupby(by='playerID')
        new_hole_df['playerScore'] = player_group.shot_id.transform('max')
        new_hole_df['holeAvg'] = player_group.shot_id.max().mean()
        new_hole_df['shotsRemaining'] = player_group.cumcount(ascending=False)
        new_hole_df[['shotType', 'fromSurface']] = new_hole_df.apply(TournamentDFHandler.getShotType, axis=1,
                                                                     result_type='expand')
        new_hole_df['isAdvanced'] = (new_hole_df['shotType'] == 'APP') & \
                                    (new_hole_df.distanceLeft > (self.adv_pct * new_hole_df.startDistance))
        new_hole_df['toSurface'] = new_hole_df.shotType.shift(-1)
        new_hole_df[['toLocation', 'toSurface']] = new_hole_df.apply(TournamentDFHandler.getEndLocation, axis=1,
                                                                     result_type='expand')
        new_hole_df.drop(new_hole_df[new_hole_df.shotType == 'Penalty'].index, inplace=True)
        new_hole_df.loc[new_hole_df.toLocation == 'Penalty', 'distanceLeft'] = \
            new_hole_df.startDistance.shift(-1).fillna(0)
        new_hole_df['distanceLeftShift'] = new_hole_df['distanceLeft'].shift(1).fillna(0)
        new_hole_df['isDrop'] = new_hole_df.apply(lambda x: x['startDistance'] != x['distanceLeftShift'] and
                                                            x['distanceLeftShift'] != 0, axis=1)
        new_hole_df.drop(columns='distanceLeftShift', inplace=True)
        new_hole_df['isReTee'] = new_hole_df.apply(
            lambda x: x['startDistance'] == x['distanceLeft'] and x['shotType'] == 'TEE', axis=1)
        new_hole_df['strokesTaken'] = -(new_hole_df['shot_id'].diff(-1))
        new_hole_df.loc[
            (new_hole_df['strokesTaken'] <= 0) | (new_hole_df['strokesTaken'] == np.nan), 'strokesTaken'] = 1
        # new_hole_df = self._getDistanceBins(new_hole_df)

        return new_hole_df

    def _createTournamentDF(self, tournament_year_dict, player_names):
        """Creates or appends a tournament DF by looking at each hole in the data for the given tournament"""
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
        """Create or appends a raw SG DF from the scraped raw SG data"""
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
        """Return the tournament DF"""
        return self._tournament_df

    def getRawSG_DF(self):
        """Return the raw SG DF"""
        return self._raw_sg_df

    def uploadTournamentDF(self, years_to_upload):
        """Upload each tournament DF for the given years"""
        self._logger.info('Uploading Tournament DF')
        for course, course_tournament_df in self._tournament_df.groupby('courseID'):
            for year, year_tournament_df in course_tournament_df.groupby('pgaYear'):
                if year in years_to_upload:
                    for round_num, round_tournament_df in year_tournament_df.groupby('roundNum'):
                        df_dict = round_tournament_df.drop(columns='shottext').to_dict('records')
                        upload_dict = {'tournamentName': self._tournament_name, 'courseID': course,
                                       'pgaYear': year, 'roundNum': round_num, 'df': df_dict}
                        self._mongo_upload_df.uploadTournamentDF(upload_dict)

    def uploadRawSG_DF(self, years_to_upload):
        """Upload each raw SG DF for the given years"""
        self._logger.info('Uploading Raw SG DF')
        for year, year_tournament_df in self._raw_sg_df.groupby('pgaYear'):
            if year in years_to_upload:
                df_dict = year_tournament_df.to_dict('records')
                upload_dict = {'tournamentName': self._tournament_name,
                               'pgaYear': year, 'df': df_dict}
                self._mongo_upload_df.uploadRawSG_DF(upload_dict)
