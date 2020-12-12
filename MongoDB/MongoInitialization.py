import logging

import pymongo

from Logging.MyLogger import MyLogger


class MongoInitialization:

    def __init__(self):
        """For connecting and set up to MongoDB"""
        self.connection_str = "mongodb+srv://scelmore1:albatross@cluster0.olrfe.mongodb.net/<dbname>?retryWrites=true" \
                              "&w" \
                              "=majority"
        self._logger = MyLogger('MongoDB', 'MongoDB/logs/mongodb.log', logging.INFO).getLogger()
        self._logger.info('Connecting to MongoDB at {}\n'.format(self.connection_str))
        self._client = pymongo.MongoClient(self.connection_str)
        self._tournament_db = self._client.tournament_db
        self._logger.info('Client description {}\n'.format(self._client))
        self._logger.info('Tournament DB description {}\n'.format(self._tournament_db))

        col_names = self._tournament_db.collection_names()
        self._logger.info('TournamentDB has the following collections {}\n'.format(col_names))
        if 'tournament_detail' not in col_names:
            idx = self._tournament_db.tournament_detail.create_index([('tournamentID', 1), ('pgaYear', -1)],
                                                                     unique=True)
            self._logger.info('Created Tournament Detail Collection with index {}\n'.format(idx))
        if 'player_metadata' not in col_names:
            idx = self._tournament_db.player_metadata.create_index([('playerID', 1)], unique=True)
            self._logger.info('Created Player Metadata Collection with index {}\n'.format(idx))
        if 'player_round' not in col_names:
            idx = self._tournament_db.player_round.create_index(
                [('playerID', 1), ('tournamentID', 1), ('pgaYear', -1), ('roundNumber', 1)], unique=True)
            self._logger.info('Created Player Round Collection with index {}\n'.format(idx))
        if 'course_metadata' not in col_names:
            idx = self._tournament_db.course_metadata.create_index(
                [('courseID', 1), ('tournamentID', 1), ('pgaYear', -1)], unique=True)
            self._logger.info('Created Course Metadata Collection with index {}\n'.format(idx))
        if 'tournament_scrape_status' not in col_names:
            idx = self._tournament_db.tournament_scrape_status.create_index([('tournamentName', 1), ('pgaYear', -1)],
                                                                            unique=True)
            self._logger.info('Created Tournament Scrape Status Collection with index {}\n'.format(idx))
        if 'sg_stats' not in col_names:
            idx = self._tournament_db.sg_stats.create_index([('tournamentName', 1), ('pgaYear', -1), ('playerName', 1)],
                                                            unique=True)
            self._logger.info('Created SG Stats Collection with index {}\n'.format(idx))

    def __repr__(self):
        return 'MongoDB Client is {}\nTournament DB is {}\n'.format(self._client, self._tournament_db)

    def getTournamentDB(self):
        return self._tournament_db

    def getLogger(self):
        return self._logger