import logging

from Logging.MyLogger import MyLogger


class MongoUploadTournament:

    def __init__(self, mongo_obj, tournament_year, tournament_name):
        """For uploading tournament scrape collection objects to MongoDB"""
        self._mongo_obj = mongo_obj
        self._year = tournament_year
        self._name = tournament_name
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'tournaments/{}/{}/logs/tournament_mongodb.log'.format(self._name, self._year)
                                ).getLogger()
        self._tournament_detail_upload = False
        self._player_metadata_upload = 0
        self._player_metadata_overall = 0
        self._player_round_upload = 0
        self._player_round_overall = 0
        self._course_metadata_upload = 0
        self._course_metadata_overall = 0
        self._tournament_scrape_status_upload = False
        self._sg_stats_upload = False

    def __repr__(self):
        return 'MongoDB Tournament Upload Status: {}'.format(self._getUploadStatus())

    def uploadTournamentDetails(self, tournament_details):
        result = self._mongo_obj.tournament_detail_col.replace_one(
            {'tournamentID': tournament_details['tournamentID'], 'pgaYear': tournament_details['pgaYear']},
            tournament_details, upsert=True)
        if result is not None:
            if result.upserted_id is not None:
                self._logger.info('Inserted tournament details into collection with id {}\n'.format(result.upserted_id))
            else:
                self._logger.info('Updated existing tournament details with key {}\n'.
                                  format({'tournamentID': tournament_details['tournamentID'],
                                          'pgaYear': tournament_details['pgaYear']}))
            self._tournament_detail_upload = True

    def uploadPlayerMetadata(self, player_metadata):
        for player in player_metadata:
            self._player_metadata_overall += 1
            if self._mongo_obj.player_meta_col.find_one({"playerID": player['playerID']}) is None:
                result = self._mongo_obj.player_meta_col.insert_one(player)
                if result is not None:
                    self._logger.info(
                        'Inserted player metadata into collection with id {}\n'.format(result.inserted_id))
                    self._player_metadata_upload += 1

    def uploadPlayerRounds(self, player_rounds):
        for player in player_rounds:
            self._player_round_overall += 1
            result = self._mongo_obj.player_round_col.replace_one(
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
            result = self._mongo_obj.course_meta_col.replace_one(
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
        result = self._mongo_obj.tournament_scrape_status_col.replace_one(
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


class MongoUploadSG:

    def __init__(self, mongo_obj):
        """For uploading SG collection objects to MongoDB"""
        self._mongo_obj = mongo_obj
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'tournaments/SG/logs/sg_mongodb.log').getLogger()
        self._sg_stats_upload = 0
        self._sg_stats_overall = 0

    def __repr__(self):
        return 'MongoDB SG Upload Status: {}'.format(self._getUploadStatus())

    def uploadSGStats(self, sg_stats_list):
        for sg_stats in sg_stats_list:
            self._sg_stats_overall += 1
            result = self._mongo_obj.sg_stats_col.replace_one(
                {'playerName': sg_stats['playerName'], 'tournamentName': sg_stats['tournamentName'],
                 'pgaYear': sg_stats['pgaYear']}, sg_stats, upsert=True)
            if result is not None:
                if result.upserted_id is not None:
                    self._logger.info('Inserted SG stats into collection with id {}\n'.format(result.upserted_id))
                else:
                    self._logger.info('Updated existing sg stats with key {}\n'.format(
                        {'playerName': sg_stats['playerName'], 'tournamentName': sg_stats['tournamentName'],
                         'pgaYear': sg_stats['pgaYear']}))
                self._sg_stats_upload += 1

    def _getUploadStatus(self):
        return 'SG Stats Uploaded: {} of {} possible\n'.format(self._sg_stats_upload, self._sg_stats_overall)


class MongoUploadDF:

    def __init__(self, mongo_obj, tournament_name):
        """For uploading tournament DF to MongoDB"""
        self._mongo_obj = mongo_obj
        self._name = tournament_name
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'tournaments/DFs/{}/logs/tournament_mongodb.log'.format(self._name)).getLogger()
        self._tournament_df_upload = False
        self._raw_sg_df_upload = False

    def __repr__(self):
        return 'MongoDB DF Upload Status: {}'.format(self._getUploadStatus())

    def uploadTournamentDF(self, upload_dict):
        try:
            tournament_name = upload_dict['tournamentName']
            pga_year = upload_dict['pgaYear']
            course_id = upload_dict['courseID']
            round_num = upload_dict['roundNum']
            self._logger.info('Attempting to upload {} {}, course {}, round #{}'.
                              format(pga_year, tournament_name, course_id, round_num))
            query = {'tournamentName': tournament_name, 'courseID': course_id,
                     'pgaYear': pga_year, 'roundNum': round_num}
            values = {'$set': upload_dict}
            result = self._mongo_obj.tournament_df_col.update(query, values, upsert=True)
            if result is not None:
                if not result['updatedExisting']:
                    self._logger.info('Inserted Tournament DF into collection with id {}\n'.format(result['upserted']))
                else:
                    self._logger.info('Updated existing Tournament DF with key {}\n'.format(
                        {'tournamentName': tournament_name, 'courseID': course_id,
                         'pgaYear': pga_year, 'roundNum': round_num}))
        except Exception as e:
            self._logger.error('Problem uploading DF {}'.format(e), exc_info=True)
        else:
            self._tournament_df_upload = True

    def uploadRawSG_DF(self, upload_dict):
        try:
            tournament_name = upload_dict['tournamentName']
            pga_year = upload_dict['pgaYear']
            self._logger.info('Attempting to upload {} {}'.
                              format(pga_year, tournament_name))
            query = {'tournamentName': tournament_name, 'pgaYear': pga_year}
            values = {'$set': upload_dict}
            result = self._mongo_obj.raw_sg_df_col.update(query, values, upsert=True)
            if result is not None:
                if not result['updatedExisting']:
                    self._logger.info('Inserted Raw SG DF into collection with id {}\n'.format(result['upserted']))
                else:
                    self._logger.info('Updated existing Raw SG DF with key {}\n'.format(
                        {'tournamentName': tournament_name, 'pgaYear': pga_year}))
        except Exception as e:
            self._logger.error('Problem uploading DF {}'.format(e), exc_info=True)
        else:
            self._raw_sg_df_upload = True

    def _getUploadStatus(self):
        return 'Tournament DF Upload: {}\nRaw SG DF Upload: {}\n'.format(self._tournament_df_upload,
                                                                         self._raw_sg_df_upload)


class MongoUploadStrokeDistance:

    def __init__(self, mongo_obj):
        """For uploading stroke distance data to MongoDB"""
        self._mongo_obj = mongo_obj
        self._logger_obj = MyLogger(self.__class__.__name__, logging.INFO)
        self._logger = self._logger_obj.getLogger()
        self._tournaments_uploaded = []
        self._years_uploaded = []

    def __repr__(self):
        return 'MongoDB DF Upload Status: {}'.format(self._getUploadStatus())

    def _getUploadStatus(self):
        return 'Tournaments Uploaded: {}\nYears Uploaded: {}'.format(self._tournaments_uploaded, self._years_uploaded)

    def addGroupStrokeDistance(self, tournament_stroke_distance_dict) -> bool:
        """Create a new document for the given tournament and grouping stroke and distance data"""
        try:
            tournament_name = tournament_stroke_distance_dict['tournamentName']
            group_name = tournament_stroke_distance_dict['groupedBy']
            group_detail = tournament_stroke_distance_dict['groupDetail']
            self._logger_obj.replaceFileHandler('tournaments/stroke_distance/{}/logs/stroke_distance_mongodb.log'
                                                .format(tournament_name), 'a')
            self._logger.info('Adding {} {} from tournament {} to the stroke distance collection'.
                              format(group_name, group_detail, tournament_name))

            query = {'tournamentName': tournament_name, 'groupedBy': group_name, 'groupDetail': group_detail}
            values = {'$set': tournament_stroke_distance_dict}
            result = self._mongo_obj.stroke_distance_tournament_col.update(query, values, upsert=True)
            if result is not None:
                if not result['updatedExisting']:
                    self._logger.info('Inserted Stroke Distance document into collection with id {}\n'.
                                      format(result['upserted']))
                else:
                    self._logger.info('Updated existing Stroke Distance document with key {}\n'.format(
                        {'tournamentName': tournament_name, 'groupedBy': group_name, 'groupDetail': group_detail}))
            else:
                return False
        except Exception as e:
            self._logger.error('Problem uploading Stroke Distance doc {}'.format(e), exc_info=True)
            return False
        else:
            self._tournaments_uploaded.append((group_name, group_detail, tournament_name))
            return True

    def addYearStrokeDistance(self, year_stroke_distance_dict, pga_year) -> bool:
        """Create a new document for the a years' based stroke and distance data"""
        try:
            self._logger_obj.replaceFileHandler('tournaments/stroke_distance/{}/logs/stroke_distance_mongodb.log'
                                                .format(pga_year), 'w')
            self._logger.info('Adding {} to the stroke distance yearly collection'.
                              format(pga_year))
            query = {'pgaYear': pga_year}
            year_stroke_distance_dict.update(query)
            values = {'$set': year_stroke_distance_dict}
            result = self._mongo_obj.stroke_distance_yearly_col.update(query, values, upsert=True)
            if result is not None:
                if not result['updatedExisting']:
                    self._logger.info('Inserted Stroke Distance Yearly document into collection with id {}\n'.
                                      format(result['upserted']))
                else:
                    self._logger.info('Updated existing Stroke Distance Yearly document with key {}\n'.format(
                        {'pgaYear': pga_year}))
            else:
                return False
        except Exception as e:
            self._logger.error('Problem uploading Stroke Distance doc {}'.format(e), exc_info=True)
            return False
        else:
            self._years_uploaded.append(pga_year)
            return True
