class MongoDownload:

    def __init__(self, mongo_obj):
        """For uploading collection objects to MongoDB"""
        self._tournament_db = mongo_obj.getTournamentDB()
        self._logger = mongo_obj.getLogger()
        self._tournament_detail_col = self._tournament_db.tournament_detail
        self._player_metadata_col = self._tournament_db.player_metadata
        self._player_round_col = self._tournament_db.player_round
        self._course_metadata_col = self._tournament_db.course_metadata
        self._tournament_scrape_status_col = self._tournament_db.tournament_scrape_status
        self._sg_stats_col = self._tournament_db.sg_stats

    def getTournamentsScraped(self):
        successfully_scraped_tournaments = []
        tournaments_scraped = self._tournament_scrape_status_col.find()
        for tournament in tournaments_scraped:
            if float(tournament['percentPlayersScraped']) > .9:
                successfully_scraped_tournaments.append((tournament['tournamentName'], tournament['pgaYear']))
        return successfully_scraped_tournaments

    def getTournamentDict(self, tournament_name_scrape, tournament_name_sg):
        yearly_tournaments = {}
        tournament_id = None

        tournament_detail_cur = self._tournament_detail_col.find(
            {'tournamentName': tournament_name_scrape})
        if tournament_detail_cur.count() == 0:
            self._logger.error('No tournament details found for {}'.format(tournament_name_scrape))
            return None
        for tournament in tournament_detail_cur:
            tournament_id = tournament['tournamentID']
            yearly_tournaments[tournament['pgaYear']] = tournament
            yearly_tournaments[tournament['pgaYear']].update(
                {'sgStats': [],
                 'courses': [],
                 'playerRounds': []})

        sg_stats_cur = self._sg_stats_col.find(
            {'tournamentName': tournament_name_sg})
        if sg_stats_cur.count() == 0:
            self._logger.error('No sg stats found for {}'.format(tournament_name_sg))
            return None
        for sg_summary in sg_stats_cur:
            if sg_summary['pgaYear'] in yearly_tournaments:
                yearly_tournaments[sg_summary['pgaYear']]['sgStats'].append(sg_summary)

        course_meta_cur = self._course_metadata_col.find({'tournamentID': tournament_id})
        if course_meta_cur.count() == 0:
            self._logger.error('No course meta found for tournament ID {}'.format(tournament_id))
            return None
        for course_meta in course_meta_cur:
            if course_meta['pgaYear'] in yearly_tournaments:
                yearly_tournaments[course_meta['pgaYear']]['courses'].append(course_meta)

        player_round_cur = self._player_round_col.find({'tournamentID': tournament_id})
        if player_round_cur.count() == 0:
            self._logger.error('No player rounds found for tournament ID {}'.format(tournament_id))
            return None
        for player_round in player_round_cur:
            if player_round['pgaYear'] in yearly_tournaments:
                yearly_tournaments[player_round['pgaYear']]['playerRounds'].append(player_round)

        return yearly_tournaments