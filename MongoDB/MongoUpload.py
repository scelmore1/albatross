import logging

from Logging.MyLogger import MyLogger


class MongoUpload:

    def __init__(self, tournament_db, tournament_year, tournament_name):
        """For uploading collection objects to MongoDB"""
        self._tournament_db = tournament_db
        self._year = tournament_year
        self._name = tournament_name
        self._logger = MyLogger('MongoDB {} {}'.format(self._year, self._name),
                                'tournaments/{}_{}/logs/tournament_mongodb.log'.format(self._year, self._name),
                                logging.INFO).getLogger()
        self._tournament_detail_upload = False
        self._player_metadata_upload = 0
        self._player_metadata_overall = 0
        self._player_round_upload = 0
        self._player_round_overall = 0
        self._course_metadata_upload = 0
        self._course_metadata_overall = 0
        self._tournament_scrape_status_upload = False

    def __repr__(self):
        return 'MongoDB Upload Status: {}'.format(self._getUploadStatus())

    def uploadTournamentDetails(self, tournament_details):
        result = self._tournament_db.tournament_detail.replace_one(
            {'tournamentID': tournament_details['tournamentID'], 'pgaYear': tournament_details['pgaYear']},
            tournament_details, upsert=True)
        if result is not None:
            if result.upserted_id is not None:
                self._logger.info('Inserted tournament details into collection with id {}\n'.format(result.upserted_id))
            else:
                self._logger.info('Updated existing tournament details with key {}\n'.
                                  format(
                    {'tournamentID': tournament_details['tournamentID'], 'pgaYear': tournament_details['pgaYear']}))
            self._tournament_detail_upload = True

    def uploadPlayerMetadata(self, player_metadata):
        for player in player_metadata:
            self._player_metadata_overall += 1
            if self._tournament_db.player_metadata.find_one({"playerID": player['playerID']}) is None:
                result = self._tournament_db.player_metadata.insert_one(player)
                if result is not None:
                    self._logger.info(
                        'Inserted player metadata into collection with id {}\n'.format(result.inserted_id))
                    self._player_metadata_upload += 1

    def uploadPlayerRounds(self, player_rounds):
        for player in player_rounds:
            self._player_round_overall += 1
            result = self._tournament_db.player_round.replace_one(
                {'playerID': player['playerID'], 'tournamentID': player['tournamentID'],
                 'pgaYear': player['pgaYear'], 'roundNumber': player['roundNumber']}, player, upsert=True)
            if result is not None:
                if result.upserted_id is not None:
                    self._logger.info('Inserted player rounds into collection with id {}\n'.format(result.upserted_id))
                else:
                    self._logger.info('Updated existing player rounds with key {}\n'.format(
                        {'playerID': player['playerID'], 'tournamentID': player['tournamentID'],
                         'pgaYear': player['pgaYear'], 'roundNumber': player['roundNumber']}))
                self._player_round_upload += 1

    def uploadCourseMetadata(self, course_metadata):
        for course in course_metadata:
            self._course_metadata_overall += 1
            result = self._tournament_db.course_metadata.replace_one(
                {'courseID': course['courseID'], 'tournamentID': course['tournamentID'],
                 'pgaYear': course['pgaYear']}, course, upsert=True)
            if result is not None:
                if result.upserted_id is not None:
                    self._logger.info(
                        'Inserted course metadata into collection with id {}\n'.format(result.upserted_id))
                else:
                    self._logger.info('Updated existing course metadata with key {}\n'.format(
                        {'courseID': course['courseID'], 'tournamentID': course['tournamentID'],
                         'pgaYear': course['pgaYear']}))
                self._course_metadata_upload += 1

    def uploadTournamentScrapeStatus(self, scrape_status):
        result = self._tournament_db.tournament_scrape_status.replace_one(
            {'tournamentID': scrape_status['tournamentID'], 'pgaYear': scrape_status['pgaYear']},
            scrape_status, upsert=True)
        if result is not None:
            if result.upserted_id is not None:
                self._logger.info(
                    'Inserted tournament scrape status into collection with id {}\n'.format(result.upserted_id))
            else:
                self._logger.info('Updated existing tournament scrape status with key {}\n'.format(
                    {'tournamentID': scrape_status['tournamentID'], 'pgaYear': scrape_status['pgaYear']}))
            self._tournament_scrape_status_upload = True

    def _getUploadStatus(self):
        return '{} {}\n'.format(self._year, self._name) + \
               'Tournament Details Uploaded: {}\n'.format(self._tournament_detail_upload) + \
               'Player Metadata Uploaded: {} new players of {} total players\n'.format(self._player_metadata_upload,
                                                                                       self._player_metadata_overall) \
               + \
               'Player Rounds Uploaded: {} of {} possible\n'.format(self._player_round_upload,
                                                                    self._player_round_overall) + \
               'Course Metadata Uploaded: {} of {} possible\n'.format(self._course_metadata_upload,
                                                                      self._course_metadata_overall) + \
               'Tournament Scrape Status Uploaded: {}\n'.format(self._tournament_scrape_status_upload)