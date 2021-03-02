import logging

import pymongo

from Logging.MyLogger import MyLogger
from config import MY_MONGO_DB_KEY


class MongoInitialization:

    def __init__(self):
        """For connecting and set up to MongoDB"""
        self.connection_str = '{}'.format(MY_MONGO_DB_KEY)
        self._logger = MyLogger(self.__class__.__name__, logging.INFO, 'MongoDB/logs/mongodb.log').getLogger()
        self._logger.info('Connecting to MongoDB...\n')
        self._client = pymongo.MongoClient(self.connection_str)
        self._logger.info('Client description {}\n'.format(self._client))

        # Tournament DB start up
        self._tournament_db = self._client.tournament_db
        self._logger.info('Tournament DB description {}\n'.format(self._tournament_db))
        self._logger.info('Tournament DB has the following collections {}\n'.
                          format(self._tournament_db.collection_names()))

        # Stroke Distance DB start up
        self._stroke_distance_db = self._client.stroke_distance_db
        self._logger.info('Stroke Distance DB description {}\n'.format(self._stroke_distance_db))
        self._logger.info('Stroke Distance DB has the following collections {}\n'.
                          format(self._stroke_distance_db.collection_names()))

        # create tournament db collections if don't exist
        self.tournament_detail_col = self._createTournamentDBCollection('tournament_detail', [('tournamentID', 1),
                                                                                              ('pgaYear', -1)])
        self.player_meta_col = self._createTournamentDBCollection('player_metadata', [('playerID', 1)])
        self.player_round_col = self._createTournamentDBCollection('player_round', [('playerID', 1),
                                                                                    ('tournamentID', 1),
                                                                                    ('pgaYear', -1),
                                                                                    ('roundNumber', 1)])
        self.course_meta_col = self._createTournamentDBCollection('course_metadata',
                                                                  [('courseID', 1), ('tournamentID', 1),
                                                                   ('pgaYear', -1)])
        self.tournament_scrape_status_col = self._createTournamentDBCollection('tournament_scrape_status',
                                                                               [('tournamentName', 1), ('pgaYear', -1)])
        self.sg_stats_col = self._createTournamentDBCollection('sg_stats', [('tournamentName', 1), ('pgaYear', -1),
                                                                            ('playerName', 1)])
        self.tournament_df_col = self._createTournamentDBCollection('tournament_df', [('tournamentName', 1),
                                                                                      ('courseID', 1),
                                                                                      ('pgaYear', -1),
                                                                                      ('roundNum', 1)])
        self.raw_sg_df_col = self._createTournamentDBCollection('raw_sg_df', [('tournamentName', 1), ('pgaYear', -1)])

        # create stroke distance collections if don't exist
        self.stroke_distance_tournament_col = self._createStrokeDistanceDBCollection('stroke_distance_tournaments',
                                                                                     [('tournamentName', 1),
                                                                                      ('groupedBy', 1),
                                                                                      ('groupDetail', 1)])
        self.stroke_distance_yearly_col = self._createStrokeDistanceDBCollection('stroke_distance_yearly',
                                                                                 [('pgaYear', -1)])

    def _createTournamentDBCollection(self, collection_name, index_dict):
        col = self._tournament_db[collection_name]
        if collection_name not in self._tournament_db.collection_names():
            idx = col.create_index(index_dict, unique=True)
            self._logger.info('Created {} Collection with index {}\n'.format(collection_name, idx))
        return col

    def _createStrokeDistanceDBCollection(self, collection_name, index_dict):
        col = self._stroke_distance_db[collection_name]
        if collection_name not in self._stroke_distance_db.collection_names():
            idx = col.create_index(index_dict, unique=True)
            self._logger.info('Created {} Collection with index {}\n'.format(collection_name, idx))
        return col

    def __repr__(self):
        return 'MongoDB Client is {}\nTournament DB is {}\nStroke Distance DB is {}\n' \
            .format(self._client, self._tournament_db, self._stroke_distance_db)

    def getLogger(self):
        return self._logger
