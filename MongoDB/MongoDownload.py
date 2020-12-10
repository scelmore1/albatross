class MongoDownload:

    def __init__(self, mongo_obj):
        """For uploading collection objects to MongoDB"""
        self._tournament_db = mongo_obj.getTournamentDB()
        self._logger = mongo_obj.getLogger()
        self._tournament_detail_col = self._tournament_db.tournament_csv
        self._player_metadata_col = self._tournament_db.player_metadata
        self._player_round_col = self._tournament_db.player_round
        self._course_metadata_col = self._tournament_db.course_metadata
        self._tournament_scrape_status_col = self._tournament_db.tournament_scrape_status

    def getTournamentsScraped(self):
        successfully_scraped_tournaments = []
        tournaments_scraped = self._tournament_scrape_status_col.find()
        for tournament in tournaments_scraped:
            if float(tournament['percentPlayersScraped']) > .9:
                successfully_scraped_tournaments.append((tournament['tournamentName'], tournament['pgaYear']))
        return successfully_scraped_tournaments