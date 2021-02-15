import logging

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from Logging.MyLogger import MyLogger


class TournamentSGHandler:
    grouping_list = [('Course', ['courseID']),
                     ('Year', ['pgaYear']),
                     ('Round', ['roundNum']),
                     ('Hole', ['holeNum']),
                     ('YearRound', ['pgaYear', 'roundNum']),
                     ('YearHole', ['pgaYear', 'holeNum']),
                     ('YearRoundHole', ['pgaYear', 'roundNum', 'holeNum'])]
    pd.set_option('display.max_columns', None)

    @staticmethod
    def fiveYdBinExpectedRemainingShotsColumn(df, group, name):
        bin_group = group + ['distanceLeft5ydBin']
        df['5ydBinAvgExpectedShotsRemaining{}'.format(name)] = df.groupby(bin_group)[
            'shotsRemaining'].transform('mean')
        return df

    @staticmethod
    def oneFtBinExpectedRemainingShotsColumn(df, group, name):
        bin_group = group + ['distanceLeft1ftBin']
        df['1ftBinAvgExpectedShotsRemaining{}'.format(name)] = df.groupby(bin_group)[
            'shotsRemaining'].transform('mean')
        return df

    @staticmethod
    def getGroupAveragesAndSGOverAvg(df, name, group, column_to_avg, sg_type, column_to_subtract):
        df['{}{}'.format(name, column_to_avg)] = \
            df.groupby(group).transform('mean')[column_to_avg]
        df['SG{}Over_{}'.format(sg_type, column_to_avg)] = df['{}{}'.format(name, column_to_avg)] - \
                                                           df[column_to_subtract]
        return df

    @staticmethod
    def getGroupSTDofSGOverAvg(df, name, group_by_cols, column_to_avg, sg_type):
        df['{}STDofSG{}'.format(name, sg_type)] = \
            df.groupby(group_by_cols)['SG{}Over_{}'.format(sg_type, column_to_avg)].transform('std')
        df['NumSTDFromSG{}Over_{}'.format(sg_type, column_to_avg)] = \
            abs(df['SG{}Over_{}'.format(sg_type, column_to_avg)] / df['{}STDofSG{}'.format(name, sg_type)])
        return df

    # @staticmethod
    # def createSGTeeColumns(df, name):
    #     df['SGTeeOverLM{}'.format(name)] = df['lmExpectedShotsRemaining{}'.format(name)] - df['shotsRemaining']
    #     df['SGTeeOverLowess{}'.format(name)] = df['lowessExpectedShotsRemaining{}'.format(name)] - df[
    #     'shotsRemaining']
    #     df['SGTeeOverBinAvg{}'.format(name)] = df['5ydBinAvgExpectedShotsRemaining{}'.format(name)] - \
    #                                            df['shotsRemaining']
    #     return df

    # @staticmethod
    # def getSGMeasure(df, sg_measure, starting_col, shots_remain_col, add_stroke):
    #     df['SG{}Over{}'.format(sg_measure, starting_col)] = df[starting_col] - df[shots_remain_col] - add_stroke
    #     return df

    # @staticmethod
    # def getSGReTee(df, sg_measure, starting_col):
    #     df['SG{}Over{}'.format(sg_measure, starting_col)] = -1
    #     return df

    def __init__(self, mongo_obj, tournament_df, raw_sg_df, distances_df=None):
        self._sg_df_dict = {}
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'Analysis/logs/{}.log'.format(self.__class__.__name__)).getLogger()
        self._tournament_df = tournament_df
        self._raw_sg_df = raw_sg_df
        self._mongo_obj = mongo_obj
        self._distances_df = distances_df

    def __repr__(self):
        return 'SG DF Dictionary has keys {}\n'.format(self._sg_df_dict.keys())

    def applySGLogicToGroups(self, visualize=False):
        for name, group in TournamentSGHandler.grouping_list:
            self._logger.info('Creating SG Stats for group {}'.format(name))
            self._sg_df_dict[name] = {}
            if 'Hole' in name:
                self._sg_df_dict[name]['Total'] = self.getSGOverall(name, group, visualize)
            self._sg_df_dict[name]['Total'] = self.getSGOverall(name, group, visualize)

    def getSGOverall(self, name, group_by_cols, visualize):
        self._logger.info('Getting SG Overall {} Stats'.format(name))
        relevant_cols = ['pgaYear', 'courseID', 'holeNum', 'roundNum', 'holeAvg', 'playerScore']

        sg_tot_df = self._tournament_df.loc[self._tournament_df['shot_id'] == 1, relevant_cols].copy()
        # sg_tot_df['SGTotOverHoleAvg'] = sg_tot_df['holeAvg'] - sg_tot_df['playerScore']
        self._logger.info('Getting SG Tot For Grouping by {}'.format(group_by_cols))
        sg_tot_df = TournamentSGHandler.getGroupAveragesAndSGOverAvg(sg_tot_df, name, group_by_cols, 'holeAvg', 'Tot',
                                                                     'playerScore')
        sg_tot_df = TournamentSGHandler.getGroupSTDofSGOverAvg(sg_tot_df, name, group_by_cols, 'holeAvg', 'Tot')
        if visualize:
            _ = sns.histplot(data=sg_tot_df, x='SGTotOver_holeAvg'.format(name), kde=True, hue='holeNum', binwidth=.25,
                             kde_kws={'bw_adjust': 4})
            plt.show()
            _ = sns.histplot(data=sg_tot_df, x='NumSTDFromSGTotOver_holeAvg'.format(name), kde=True, hue='holeNum',
                             binwidth=.25, kde_kws={'bw_adjust': 4})
            plt.show()

        return sg_tot_df

    def sumSGTotalDFs(self, sg_tot_df):
        self._sg_df_dict['Total']['SumByRound'] = sg_tot_df.groupby(['playerID', 'pgaYear', 'roundNum']).sum(). \
            reset_index()
        sg_cols = [col for col in sg_tot_df if 'SG' in col]
        self._sg_df_dict['Total']['RawSGMatch'] = sg_tot_df.groupby(['playerID', 'pgaYear']). \
            apply(lambda x: x[sg_cols].sum() / x['roundNum'].nunique()).reset_index()

    # def getSGTee(self, visualize=False):
    #     relevant_cols = ['playerID', 'firstName', 'lastName', 'pgaYear', 'courseID', 'holeNum', 'roundNum',
    #                      'par', 'startDistance', 'startDistance10ydBin', 'distanceLeft', 'distanceLeft5ydBin',
    #                      'distanceLeft1ydBin', 'distanceLeft1ftBin', 'toSurface', 'shotsRemaining', 'isReTee']
    #     tee_shots_df = self._tournament_df[(self._tournament_df['shotType'] == 'TEE')][relevant_cols].copy()
    #     tee_shots_df = TournamentSGHandler.getStartingExpectedShots(tee_shots_df, visualize)
    #     tee_shots_df = TournamentSGHandler.getRemainingExpectedShots(tee_shots_df, visualize)
    #     tee_shots_df.drop(columns='distance/shots', inplace=True)
    #     tee_shots_df['SGTeeByLowess'] = tee_shots_df['lowessExpectedShotsStartingGrouped'] - \
    #                                     tee_shots_df['lowessExpectedShotsRemainingBySurface'] - 1
    #     tee_shots_df['SGTeeByLowess'].fillna(-2, inplace=True)
    #     if visualize:
    #         _ = sns.histplot(data=tee_shots_df, x='SGTeeByLowess', kde=True, hue='holeNum', binwidth=.25,
    #                          kde_kws={'bw_adjust': 4})
    #         plt.show()
    #     self._sg_df_dict['Tee'] = {}
    #     tee_shots_df['NumSTDFromSGTeeByLowess'] = abs(tee_shots_df['SGTeeByLowess'] /
    #                                                   tee_shots_df.groupby(['pgaYear', 'roundNum'])
    #                                                   ['SGTeeByLowess'].transform('std'))
    #     tee_shots_df['AvgSGTeeByLowess'] = tee_shots_df.groupby(['pgaYear', 'roundNum'])['SGTeeByLowess']. \
    #         transform('mean')
    #     self._sg_df_dict['Tee']['RoundBased'] = tee_shots_df
    #     self._sg_df_dict['Tee']['SumByRound'] = tee_shots_df.groupby(['playerID', 'pgaYear', 'roundNum']).sum(). \
    #         reset_index()
    #     sg_cols = [col for col in tee_shots_df if 'SG' in col]
    #     self._sg_df_dict['Tee']['RawSGMatch'] = tee_shots_df.groupby(['playerID', 'pgaYear']). \
    #         apply(lambda x: x[sg_cols].sum() / x['roundNum'].nunique()).reset_index()

    def getSG_DF_Dict(self):
        return self._sg_df_dict
