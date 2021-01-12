import logging

import pymongo

from Logging.MyLogger import MyLogger
from config import MY_MONGO_DB_KEY


class MongoInitialization:

    def __init__(self, called_from):
        """For connecting and set up to MongoDB"""
        self.connection_str = '{}'.format(MY_MONGO_DB_KEY)
        self._logger = MyLogger('MongoDB', 'MongoDB/logs/mongodb.log', logging.INFO).getLogger()
        self._logger.info('Connecting to MongoDB at {}\n'.format(self.connection_str))
        self._client = pymongo.MongoClient(self.connection_str)
        self._tournament_db = self._client.tournament_db
        self._logger.info('Client description {}\n'.format(self._client))
        self._logger.info('Tournament DB description {}\n'.format(self._tournament_db))
        col_names = self._tournament_db.collection_names()
        self._logger.info('TournamentDB has the following collections {}\n'.
                          format(col_names))
        if called_from == 'scraper':
            self._createCollection('tournament_detail', [('tournamentID', 1), ('pgaYear', -1)])
            self._createCollection('player_metadata', [('playerID', 1)])
            self._createCollection('player_round',
                                   [('playerID', 1), ('tournamentID', 1), ('pgaYear', -1), ('roundNumber', 1)])
            self._createCollection('course_metadata', [('courseID', 1), ('tournamentID', 1), ('pgaYear', -1)])
            self._createCollection('tournament_scrape_status', [('tournamentName', 1), ('pgaYear', -1)])
        elif called_from == 'sg':
            self._createCollection('sg_stats', [('tournamentName', 1), ('pgaYear', -1), ('playerName', 1)])
        elif called_from == 'df':
            self._createCollection('tournament_df', [('tournamentName', 1), ('courseID', 1),
                                                     ('pgaYear', -1), ('roundNum', 1)])
            self._createCollection('raw_sg_df', [('tournamentName', 1), ('pgaYear', -1)])

    def _createCollection(self, collection_name, index_dict):
        if collection_name not in self._tournament_db.collection_names():
            new_col = self._tournament_db[collection_name]
            idx = new_col.create_index(index_dict, unique=True)
            self._logger.info('Created {} Collection with index {}\n'.format(collection_name, idx))

    def __repr__(self):
        return 'MongoDB Client is {}\nTournament DB is {}\n'.format(self._client, self._tournament_db)

    def getTournamentDB(self):
        return self._tournament_db

    def getLogger(self):
        return self._logger