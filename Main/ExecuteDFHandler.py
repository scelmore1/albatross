import logging

import pandas as pd

from Analysis.StrokeDistanceHandler import StrokeDistanceHandler
from Analysis.TournamentDFHandler import TournamentDFHandler
from Logging.MyLogger import MyLogger
from MongoDB.MongoInitialization import MongoInitialization

if __name__ == '__main__':
    main_logger = MyLogger('Main', logging.INFO, 'Main/logs/main.log').getLogger()
    mongo_init = MongoInitialization()

    tournaments = {}
    pd_reader = pd.read_csv('Analysis/tournament_list.csv', delimiter=',', skipinitialspace=True, header=0)
    for idx, row in pd_reader.iterrows():
        tournaments[(row.iloc[0], row.iloc[1])] = {}

    stroke_distance_handler = StrokeDistanceHandler(mongo_init)

    for tournament_tup in tournaments:
        tournament_df_handler = TournamentDFHandler(mongo_init, tournament_tup[0], tournament_tup[1])
        tournament_df = tournament_df_handler.getTournamentDF()
        tournaments[tournament_tup]['tournament_df'] = tournament_df

        sg_df = tournament_df_handler.getRawSG_DF()
        tournaments[tournament_tup]['sg_df'] = sg_df

        stroke_distance_handler.uploadTournament(tournament_df, tournament_tup[0])

    for df_dict in tournaments.values():
        for pga_year, year_df in df_dict['tournament_df'].groupby('pgaYear'):
            x = stroke_distance_handler.yearBasedPredictedStrokesFromDistance(pga_year)
        # sg_handler = TournamentSGHandler(mongo_init, df_dict['tournament_df'], df_dict['sg_df'])
    # sg_handler.applySGLogicToGroups(True)
    # sg_handler.getSGTee(False)

    # sg_df_dict = sg_handler.getSG_DF_Dict()

    # combine = pd.merge(sg_df_dict['Tee']['RawSGMatch'], sg_df, how='left', on=['playerID', 'pgaYear'])
