from functools import reduce

import pandas as pd


class MongoDownload:

    def __init__(self, mongo_obj):
        """For uploading collection objects to MongoDB"""
        self._tournament_db = mongo_obj.getTournamentDB()
        self._logger = mongo_obj.getLogger()

    def getTournamentsScraped(self):
        tournament_scrape_status_col = self._tournament_db['tournament_scrape_status']
        successfully_scraped_tournaments = []
        tournaments_scraped = tournament_scrape_status_col.find()
        for tournament in tournaments_scraped:
            if float(tournament['percentPlayersScraped']) > .9:
                successfully_scraped_tournaments.append((tournament['tournamentName'], tournament['pgaYear']))
        return successfully_scraped_tournaments

    def getTournamentDF(self, tournament_name):
        tournament_df_col = self._tournament_db['tournament_df']
        tournament_df = pd.DataFrame()
        for round_based_doc in tournament_df_col.find({'tournamentName': tournament_name}):
            round_based_df = pd.DataFrame(round_based_doc['df'])
            tournament_df = tournament_df.append(round_based_df)
        return tournament_df

    def getRawSG_DF(self, tournament_name):
        raw_sg_df_col = self._tournament_db['raw_sg_df']
        raw_sg_df = pd.DataFrame()
        for round_based_doc in raw_sg_df_col.find({'tournamentName': tournament_name}):
            round_based_df = pd.DataFrame(round_based_doc['df'])
            raw_sg_df = raw_sg_df.append(round_based_df)
        return raw_sg_df

    def getPlayerNames(self):
        player_meta_col = self._tournament_db['player_metadata']
        players = []
        for player in player_meta_col.find({}, {'_id': 0, 'playerID': 1, 'firstName': 1, 'lastName': 1}):
            players.append(player)
        return players

    def getTournamentDetailsByYear(self, tournament_name):
        yearly_tournaments = {}
        tournament_detail_col = self._tournament_db['tournament_detail']
        tournament_detail_cur = tournament_detail_col.find(
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
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        tournament_id = ''
        for year, details in yearly_tournaments.items():
            tournament_id = details['tournamentID']
            details.update({'playerRounds': []})

        player_round_col = self._tournament_db['player_round']
        player_round_cur = player_round_col.find({'tournamentID': tournament_id})
        if player_round_cur.count() == 0:
            self._logger.error('No player rounds found for tournament ID {}'.format(tournament_id))
            return None
        for player_round in player_round_cur:
            if player_round['pgaYear'] in yearly_tournaments:
                yearly_tournaments[player_round['pgaYear']]['playerRounds'].append(player_round)
        return yearly_tournaments

    def getCourseMetaForTournament(self, tournament_name, yearly_tournaments=None):
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        tournament_id = ''
        for year, details in yearly_tournaments.items():
            tournament_id = details['tournamentID']
            details.update({'courses': []})

        course_metadata_col = self._tournament_db['course_metadata']
        course_meta_cur = course_metadata_col.find({'tournamentID': tournament_id})
        if course_meta_cur.count() == 0:
            self._logger.error('No course meta found for tournament ID {}'.format(tournament_id))
            return None
        for course_meta in course_meta_cur:
            if course_meta['pgaYear'] in yearly_tournaments:
                yearly_tournaments[course_meta['pgaYear']]['courses'].append(course_meta)
        return yearly_tournaments

    def getSGStatsForTournament(self, tournament_name, tournament_name_sg, yearly_tournaments=None):
        if yearly_tournaments is None:
            yearly_tournaments = self.getTournamentDetailsByYear(tournament_name)

        for year, details in yearly_tournaments.items():
            details.update({'sgStats': []})

        sg_stats_col = self._tournament_db['sg_stats']
        sg_stats_cur = sg_stats_col.find(
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