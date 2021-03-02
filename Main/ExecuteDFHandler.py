import logging

import pandas as pd

from Analysis.StrokeDistanceHandler import StrokeDistanceHandler
from Analysis.TournamentDFHandler import TournamentDFHandler
from Logging.MyLogger import MyLogger
from MongoDB.MongoInitialization import MongoInitialization

if __name__ == '__main__':
    main_logger = MyLogger('Main', logging.INFO, 'Main/logs/main.log').getLogger()
    mongo_init = MongoInitialization()

    # read in tournaments to do SG analysis on
    tournaments = {}
    pd_reader = pd.read_csv('Analysis/tournament_list.csv', delimiter=',', skipinitialspace=True, header=0)
    for idx, row in pd_reader.iterrows():
        tournaments[row.iloc[0]] = {}
        tournaments[row.iloc[0]]['scrape name'] = row.iloc[0]
        tournaments[row.iloc[0]]['sg name'] = row.iloc[1]

    # create the stroke distance handler for making stroke and distance lowess models for each tournament DF
    stroke_distance_handler = StrokeDistanceHandler(mongo_init)

    for tournament in tournaments.values():
        # create the tournament DF
        tournament_df_handler = TournamentDFHandler(mongo_init, tournament['scrape name'], tournament['sg name'], None,
                                                    False)
        tournament['tournament_df'] = tournament_df_handler.getTournamentDF()

        # get raw SG data
        sg_df = tournament_df_handler.getRawSG_DF()
        tournament['sg_df'] = sg_df

        # use stroke distance handler to analyze tournament shots
        tournament['stroke_distance'] = stroke_distance_handler.handleTournament(tournament['tournament_df'],
                                                                                 tournament['scrape name'])

        stroke_distance_handler.visualizeGroupedLowessModels(tournament['stroke_distance'], ['Course', 'Year'])

    # get the universal year data
    for tournament in tournaments.values():
        grouped_dict = tournament['stroke_distance']
        if 'Year' in grouped_dict:
            for pga_year in grouped_dict['Year'].keys():
                grouped_dict['UniversalYear'][pga_year] = \
                    stroke_distance_handler.downloadShotDistanceUniversalYear(pga_year)

        stroke_distance_handler.visualizeGroupedLowessModels(tournament['stroke_distance'], ['UniversalYear'])

    # sg_handler = TournamentSGHandler(mongo_init, df_dict['tournament_df'], df_dict['sg_df'])
    # sg_handler.applySGLogicToGroups(True)
    # sg_handler.getSGTee(False)

    # sg_df_dict = sg_handler.getSG_DF_Dict()

    # combine = pd.merge(sg_df_dict['Tee']['RawSGMatch'], sg_df, how='left', on=['playerID', 'pgaYear'])
