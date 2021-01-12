import logging
import pandas as pd

from Analysis.dfHandler import dfHandler
from Analysis.sgHandler import sgHandler
from Logging.MyLogger import MyLogger
from MongoDB.MongoInitialization import MongoInitialization

if __name__ == '__main__':
    analysis_logger = MyLogger('Analysis', 'Analysis/logs/hole_df.log', logging.INFO).getLogger()
    mongo_init = MongoInitialization('df')
    df_handler = dfHandler(mongo_init, 'waste-management-phoenix-open',
                           'Waste Management Phoenix Open', False, False)
    tournament_df = df_handler.getTournamentDF()
    sg_df = df_handler.getRawSG_DF()
    # df_handler.uploadTournamentDF()
    # df_handler.uploadRawSG_DF()

    sg_handler = sgHandler(mongo_init, tournament_df, sg_df)
    sg_handler.applySGLogicToGroups(True)
    # sg_handler.getSGTee(False)

    sg_df_dict = sg_handler.getSG_DF_Dict()

    # combine = pd.merge(sg_df_dict['Tee']['RawSGMatch'], sg_df, how='left', on=['playerID', 'pgaYear'])