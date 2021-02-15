import logging
from functools import reduce

import pandas as pd

from Logging.MyLogger import MyLogger


class MongoDownload:

    def __init__(self, mongo_obj):
        """For uploading collection objects to MongoDB"""
        self._mongo_obj = mongo_obj
        self._logger = MyLogger(self.__class__.__name__, logging.INFO, 'MongoDB/logs/mongodb.log').getLogger()

    def getStrokeDistanceForGivenYear(self, pga_year, tournaments_to_include=1):
        pga_year_int = int(pga_year)
        self._logger.info('Downloading stroke distance data for pga_year {}'.format(pga_year))
        stroke_distance_df = pd.DataFrame()
        tournament_cnt = 0
        while tournament_cnt < tournaments_to_include:
            stroke_distance_cur = self._mongo_obj.stroke_distance_tournament_col.find({'pgaYear': pga_year})
            if stroke_distance_cur.count() == 0:
                self._logger.error('No stroke distance data found for pga_year {}'.format(pga_year))
                break
            for stroke_dist_doc in stroke_distance_cur:
                if tournament_cnt >= tournaments_to_include:
                    break
                tournament_cnt += 1
                self._logger.info('Appending stroke distance data for {} {}'.format(stroke_dist_doc['pgaYear'],
                                                                                    stroke_dist_doc['tournamentName']))
                tournament_stroke_distance_df = pd.DataFrame(stroke_dist_doc['df'])
                stroke_distance_df = stroke_distance_df.append(tournament_stroke_distance_df)
            pga_year_int -= 1
            pga_year = str(pga_year_int)

        return stroke_distance_df

    def getStrokeDistanceTournaments(self):
        self._logger.info('Downloading tournament names of stroke distance data')
        tournaments_downloaded = []
        for tournament_doc in self._mongo_obj.stroke_distance_tournament_col.find({}, {'tournamentName': 1}):
            tournaments_downloaded.append(tournament_doc['tournamentName'])
        return tournaments_downloaded

    def getYearsWithLowessData(self) -> list:
        self._logger.info('Downloading pga years of stroke distance lowess data')
        years_downloaded = []
        for year_doc in self._mongo_obj.stroke_distance_yearly_col.find({}, {'pgaYear': 1}):
            years_downloaded.append(year_doc['pgaYear'])
        return years_downloaded

    def getTournamentsScraped(self):
        self._logger.info('Downloading tournament scrape data')
        successfully_scraped_tournaments = []
        tournaments_scraped = self._mongo_obj.tournament_scrape_status_col.find()
        for tournament in tournaments_scraped:
            if float(tournament['percentPlayersScraped']) > .9:
                successfully_scraped_tournaments.append((tournament['tournamentName'], tournament['pgaYear']))
        return successfully_scraped_tournaments

    def getTournamentDF(self, tournament_name):
        self._logger.info('Downloading tournament df data for tournament {}'.format(tournament_name))
        tournament_df = pd.DataFrame()
        for round_based_doc in self._mongo_obj.tournament_df_col.find({'tournamentName': tournament_name}):
            self._logger.info('\tDownloading pga_year {} round {}'.format(round_based_doc['pgaYear'],
                                                                          round_based_doc['roundNum']))
            round_based_df = pd.DataFrame(round_based_doc['df'])
            tournament_df = tournament_df.append(round_based_df)
        return tournament_df

    def getRawSG_DF(self, tournament_name):
        self._logger.info('Downloading raw sg data for tournament {}'.format(tournament_name))
        raw_sg_df = pd.DataFrame()
        for round_based_doc in self._mongo_obj.raw_sg_df_col.find({'tournamentName': tournament_name}):
            round_based_df = pd.DataFrame(round_based_doc['df'])
            raw_sg_df = raw_sg_df.append(round_based_df)
        return raw_sg_df

    def getPlayerNames(self):
        self._logger.info('Downloading player names data')
        players = []
        for player in self._mongo_obj.player_meta_col.find({},
                                                           {'_id': 0, 'playerID': 1, 'firstName': 1, 'lastName': 1}):
            players.append(player)
        return players

    def getTournamentDetailsByYear(self, tournament_name):
        self._logger.info('Downloading tournament details data for tournament {}'.format(tournament_name))
        yearly_tournaments = {}
        tournament_detail_cur = self._mongo_obj.player_detail_col.find(
            {'tournamentName': tournament_name})
        if tournament_detail_cur.count() == 0:
            self._logger.error('No tournament details found for {}'.format(tournament_name))
            return None
        for tournament in tournament_detail_cur:
            yearly_tournaments[tournament['pgaYear']] = tournament
        tournament_ids = set()
        tournament_ids.add(reduce(lambda d, key: d.get(key) if d else None, 'tournamentID', yearly_tournaments))
        if len(tournament_ids) > 1:
            self._logger.error('Mismatched tournament IDs for tournament {}'.format(tournament_name))
        return yearly_tournaments

    def getPlayerRoundsForTournament(self, tournament_name, yearly_tournaments=None):
        self._logger.info('Downloading player rounds data for tournament {}'.format(tournament_name))
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        tournament_id = ''
        for year, details in yearly_tournaments.items():
            tournament_id = details['tournamentID']
            details.update({'playerRounds': []})

        player_round_cur = self._mongo_obj.player_round_col.find({'tournamentID': tournament_id})
        if player_round_cur.count() == 0:
            self._logger.error('No player rounds found for tournament ID {}'.format(tournament_id))
            return None
        for player_round in player_round_cur:
            if player_round['pgaYear'] in yearly_tournaments:
                yearly_tournaments[player_round['pgaYear']]['playerRounds'].append(player_round)
        return yearly_tournaments

    def getCourseMetaForTournament(self, tournament_name, yearly_tournaments=None):
        self._logger.info('Downloading course metadata for tournament {}'.format(tournament_name))
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        tournament_id = ''
        for year, details in yearly_tournaments.items():
            tournament_id = details['tournamentID']
            details.update({'courses': []})

        course_meta_cur = self._mongo_obj.course_meta_col.find({'tournamentID': tournament_id})
        if course_meta_cur.count() == 0:
            self._logger.error('No course meta found for tournament ID {}'.format(tournament_id))
            return None
        for course_meta in course_meta_cur:
            if course_meta['pgaYear'] in yearly_tournaments:
                yearly_tournaments[course_meta['pgaYear']]['courses'].append(course_meta)
        return yearly_tournaments

    def getSGStatsForTournament(self, tournament_name, tournament_name_sg, yearly_tournaments=None):
        self._logger.info('Downloading SG stats data for tournament {}'.format(tournament_name))
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        for year, details in yearly_tournaments.items():
            details.update({'sgStats': []})

        sg_stats_cur = self._mongo_obj.sg_stats_col.find(
            {'tournamentName': tournament_name_sg}, {'_id': 0})
        if sg_stats_cur.count() == 0:
            self._logger.error('No sg stats found for {}'.format(tournament_name_sg))
            return None
        for sg_summary in sg_stats_cur:
            if sg_summary['pgaYear'] in yearly_tournaments:
                yearly_tournaments[sg_summary['pgaYear']]['sgStats'].append(sg_summary)
        return yearly_tournaments

    def consolidateTournamentInfo(self, tournament_name):
        yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)
        yearly_tournaments = self.getPlayerRoundsForTournament(tournament_name, yearly_tournaments)
        yearly_tournaments = self.getCourseMetaForTournament(tournament_name, yearly_tournaments)
        return yearly_tournaments
