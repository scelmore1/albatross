import copy
import logging
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from statsmodels.nonparametric.smoothers_lowess import lowess

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoUpload import MongoUploadStrokeDistance


class StrokeDistanceHandler:
    pd.set_option('display.max_columns', None)
    max_hole_dist = 700
    max_green_dist = 50
    max_quantile_num = 40
    shot_types = ['TEE', 'APP', 'ARG', 'SHT_PUTT', 'LNG_PUTT']
    stroke_distance_cols = ['pgaYear', 'courseID', 'holeNum', 'roundNum', 'shotType', 'fromSurface',
                            'startDistance', 'shotsRemaining']
    # necessary_cols = ['pgaYear', 'courseID', 'holeNum', 'roundNum', 'par', 'shotType', 'fromSurface',
    #                   'startDistance', 'shotsRemaining', 'isDrop', 'isReTee']
    grouping_list = [
        ('Course', ['courseID']),
        ('Year', ['pgaYear']),
        ('Round', ['roundNum']),
        ('Hole', ['holeNum']),
        ('YearRound', ['pgaYear', 'roundNum']),
        ('YearHole', ['pgaYear', 'holeNum'])]

    # ('YearRoundHole', ['pgaYear', 'roundNum', 'holeNum'])]

    # distance_bins = {'10yd': 'distance10ydBin',
    #                  '5yd': 'distance5ydBin',
    #                  '1yd': 'distance1ydBin',
    #                  '1ft': 'distance1ftBin',
    #                  '4in': 'distance4inBin'}

    @staticmethod
    def iterdict(d):
        for k, v in d.items():
            if isinstance(v, dict):
                StrokeDistanceHandler.iterdict(v)
            else:
                return k, v

    @staticmethod
    def visualizeLowess(shot_df, shot_type, group_desc):
        try:
            viz_df = shot_df.reset_index().copy()
            fig, ax = plt.subplots()
            ax.set_title('Expected Shots For {} Distance Grouped By {}\n and Surface Lowess Model'.
                         format(shot_type, group_desc), fontsize=8)
            fig.subplots_adjust(top=.9)
            _ = sns.scatterplot(data=viz_df, x='distance', y='shotsRemaining', hue='surface', ax=ax, legend=False)
            _ = sns.lineplot(data=viz_df, x='distance', y='lowessPredictedShots', hue='surface', ax=ax)
            plt.show()
        except Exception as e:
            print('Trouble Plotting Lowess due to {}'.format(e))
            plt.clf()

    # @staticmethod
    # def lmExpectedShotsByDistance(distance_shots):
    #     new_df = distance_shots.str.split('/', expand=True)
    #     lm = LinearRegression()
    #     lm.fit(new_df[[0]], new_df[[1]])
    #     return lm.predict(new_df[[0]]).flatten()
    #
    # @staticmethod
    # def lmExpectedRemainingShotsGroup(df, group, name):
    #     df['lmExpectedShotsRemaining{}'.format(name)] = df.groupby(group)['distance/shots']. \
    #         transform(sgHandler.lmExpectedShotsByDistance)
    #     return df

    @staticmethod
    def getBinValues(cut_on, end_bin, interval, unit_measurement):
        if unit_measurement == 'ft':
            multiplier = 3
        elif unit_measurement == 'in':
            multiplier = 36
        else:
            multiplier = 1

        labels = []
        for x in range(0, end_bin * multiplier, interval):
            labels.append('({} to {}] {}'.format(x, x + interval, unit_measurement))
        return pd.cut(x=cut_on,
                      bins=np.linspace(0, end_bin * 36,
                                       int((end_bin * multiplier) / interval) + 1),
                      precision=0,
                      labels=labels,
                      include_lowest=True,
                      right=True)

    @staticmethod
    def getQuantiles(distance_series):
        """Method for creating quantiles from certain columns and groupings"""
        quantiles = StrokeDistanceHandler.max_quantile_num
        if len(np.unique(distance_series)) == 1:
            quantiles = 1
        elif len(np.unique(distance_series)) <= (quantiles * 2):
            quantiles = int(len(np.unique(np.quantile(distance_series, np.linspace(0, 1, quantiles, endpoint=False),
                                                      interpolation='nearest'))) * .75)
        return pd.qcut(distance_series,
                       q=quantiles,
                       precision=0,
                       duplicates='drop')

    # @staticmethod
    # def _getDistanceBins(str_dis_df):
    #     str_dis_df.loc[str_dis_df['shotType'] == 'TEE', StrokeDistanceHandler.distance_bins['10yd']] = \
    #         StrokeDistanceHandler.getBinValues(str_dis_df[str_dis_df['shotType'] == 'TEE'].distance,
    #                                            StrokeDistanceHandler.max_hole_dist, 10, 'yds')
    #     str_dis_df.loc[str_dis_df['shotType'] == 'APP', StrokeDistanceHandler.distance_bins['5yd']] = \
    #         StrokeDistanceHandler.getBinValues(str_dis_df[str_dis_df['shotType'] == 'APP'].distance,
    #                                            StrokeDistanceHandler.max_hole_dist, 5, 'yds')
    #     str_dis_df.loc[str_dis_df['shotType'] == 'ARG', StrokeDistanceHandler.distance_bins['1yd']] = \
    #         StrokeDistanceHandler.getBinValues(str_dis_df[str_dis_df['shotType'] == 'ARG'].distance,
    #                                            TournamentDFHandler.arg_green_dist, 1, 'yd')
    #     str_dis_df.loc[str_dis_df['shotType'] == 'LNG_PUTT', StrokeDistanceHandler.distance_bins['1ft']] = \
    #         StrokeDistanceHandler.getBinValues(str_dis_df[str_dis_df['shotType'] == 'LNG_PUTT'].distance,
    #                                            StrokeDistanceHandler.max_green_dist, 1, 'ft')
    #     str_dis_df.loc[str_dis_df['shotType'] == 'SHT_PUTT', StrokeDistanceHandler.distance_bins['4in']] = \
    #         StrokeDistanceHandler.getBinValues(str_dis_df[str_dis_df['shotType'] == 'SHT_PUTT'].distance,
    #                                            StrokeDistanceHandler.max_green_dist, 4, 'in')
    #     return str_dis_df

    @staticmethod
    def lowessExpectedShotsByDistance(distance_shots):
        new_df = distance_shots.str.split('/', expand=True)
        if len(new_df) > 1:
            endog = new_df[[1]].values.ravel()
            exog = new_df[[0]].values.ravel()
            dynamic_frac = .9 - ((len(endog) / StrokeDistanceHandler.max_quantile_num) * .3)
            # print('\t\t\tDynamic frac in lowess used: {:.2f}'.format(dynamic_frac))
            lowess_series = lowess(endog=endog, exog=exog, return_sorted=False, frac=round(dynamic_frac, 2),
                                   delta=.01 * np.ptp(exog.astype(float)))
            lowess_series[lowess_series < 1] = 1
        else:
            lowess_series = new_df[[1]].values.ravel()
        pd_series = pd.Series(index=new_df.index, data=lowess_series)
        pd_series.interpolate(inplace=True)
        return pd_series

    @staticmethod
    def lowessExpectedRemainingShotsColumn(df, group, name):
        df['lowessExpectedShotsRemaining{}'.format(name)] = df.groupby(group)['distance/shots']. \
            transform(StrokeDistanceHandler.lowessExpectedShotsByDistance)
        return df

    @staticmethod
    def remainingExpectedShots(shot_df):
        shot_df['distance/shots'] = shot_df.apply(lambda x: str(x['distance']) + '/' +
                                                            str(x['shotsRemaining']), axis=1)
        shot_df['lowessPredictedShots'] = shot_df.groupby('surface')['distance/shots']. \
            transform(StrokeDistanceHandler.lowessExpectedShotsByDistance)
        shot_df.drop(columns='distance/shots', inplace=True)
        return shot_df

    def __init__(self, mongo_obj):
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'Analysis/logs/{}.log'.format(self.__class__.__name__)).getLogger()
        self._mongo_download = MongoDownload(mongo_obj)
        self._mongo_upload = MongoUploadStrokeDistance(mongo_obj)
        self._tournaments_uploaded = defaultdict(list)
        self._already_added_stroke_distance_years = self._mongo_download.getYearsWithLowessData()

    def __repr__(self):
        return 'Stroke Distance Handler has uploaded tournaments: {}'.format(dict(self._tournaments_uploaded))

    def _uploadStrokeDistanceGroup(self, tournament_name, group_name, group_desc, shot_type_dict):
        """Add tournament to mongoDB stroke distance collection if doesn't exist"""
        shot_type_dict_cp = copy.deepcopy(shot_type_dict)
        self._logger.info('Uploading {} {} for tournament {}'.format(group_name, group_desc, tournament_name))
        for shot_type, df in shot_type_dict_cp.items():
            shot_type_dict_cp[shot_type] = df.to_dict('records')
        for missing_shot in set(self.shot_types).difference(set(shot_type_dict_cp.keys())):
            shot_type_dict_cp[missing_shot] = {}
        stroke_dist_dict = {'tournamentName': tournament_name, 'groupedBy': group_name,
                            'groupDetail': group_desc, 'TEE': shot_type_dict_cp['TEE'], 'APP': shot_type_dict_cp['APP'],
                            'ARG': shot_type_dict_cp['ARG'], 'LNG_PUTT': shot_type_dict_cp['LNG_PUTT'],
                            'SHT_PUTT': shot_type_dict_cp['SHT_PUTT']}
        if self._mongo_upload.addGroupStrokeDistance(stroke_dist_dict) and group_name == 'Year':
            self._tournaments_uploaded[tournament_name].append(group_desc)

    def handleTournament(self, tournament_df, tournament_name, force_upload=False):
        if force_upload:
            stroke_distance_grouped_df_dict = defaultdict(dict)
        else:
            stroke_distance_grouped_df_dict = self._mongo_download. \
                getStrokeDistanceGroupingsForTournament(tournament_name)

        self._logger.info('Creating shot distance dataframes by shot type and surface for tournament {}'.
                          format(tournament_name))

        stroke_distance_df = tournament_df.loc[tournament_df['startDistance'] > 0, self.stroke_distance_cols].copy()
        stroke_distance_df.rename(columns={'startDistance': 'distance', 'fromSurface': 'surface'}, inplace=True)
        stroke_distance_df['shotsRemaining'] = stroke_distance_df['shotsRemaining'] + 1

        for group_name, group_list in self.grouping_list:
            self._logger.info('Grouping by {}'.format(group_name))
            grouped_dfs = stroke_distance_df.groupby(group_list)
            for group_desc, group_df in grouped_dfs:
                if type(group_desc) is tuple:
                    group_desc = ' '.join(group_desc)
                    if stroke_distance_grouped_df_dict.get(group_name, {}).get(group_desc):
                        self._logger.info('\tStroke Distance data for {} {} already uploaded'.format(group_name,
                                                                                                     group_desc))
                        continue
                stroke_distance_grouped_df_dict[group_name][group_desc] = {}
                self._logger.info('\tCalculating stroke and distance lowess regression for {} {}'.
                                  format(group_name, group_desc))
                for shot_type, shot_type_df in group_df.groupby('shotType'):
                    self._logger.info('\t\tCreate new model broken down by shot type {}'.format(shot_type))
                    shot_type_df['distance_range'] = shot_type_df.groupby('surface')['distance']. \
                        transform(self.getQuantiles)
                    shot_type_df.sort_values(by='distance', inplace=True)
                    qbin_df = shot_type_df.groupby(['surface', 'distance_range']).mean()
                    qbin_df = self.remainingExpectedShots(qbin_df.reset_index())
                    qbin_df['distance_range'] = qbin_df['distance_range'].astype(str)
                    stroke_distance_grouped_df_dict[group_name][group_desc][shot_type] = qbin_df.copy()

                self._uploadStrokeDistanceGroup(tournament_name, group_name, group_desc,
                                                stroke_distance_grouped_df_dict[group_name][group_desc])

        return stroke_distance_grouped_df_dict

    def visualizeGroupedLowessModels(self, stroke_distance_grouped_df_dict, group_filter: list = None):
        for group_name in stroke_distance_grouped_df_dict:
            if group_filter and group_name in group_filter:
                for group_desc in stroke_distance_grouped_df_dict[group_name]:
                    for shot_type, shot_df in stroke_distance_grouped_df_dict[group_name][group_desc].items():
                        self.visualizeLowess(shot_df, shot_type, f'{group_name} {group_desc}')

    def downloadShotDistanceUniversalYear(self, pga_year):
        """Given a pga_year create the expected strokes from each distance and location and append to the grouped shot
         distance dictionary a universal year group containing this dataframe"""
        self._logger.info('Getting Universal Year stroke and distance data for {}'.format(pga_year))
        if self._already_added_stroke_distance_years.get(pga_year):
            self._logger.info('\tAlready Added')
            universal_year_shot_type_dict = self._already_added_stroke_distance_years[pga_year]
        else:
            self._logger.info('\tCreating yearly DF')
            universal_year_shot_type_dict = self._mongo_download.getStrokeDistanceForGivenYear(pga_year)
            for shot_t in universal_year_shot_type_dict:
                universal_year_shot_type_dict[shot_t] = universal_year_shot_type_dict[shot_t].to_dict('records')
            self._mongo_upload.addYearStrokeDistance(universal_year_shot_type_dict, pga_year)
            self._already_added_stroke_distance_years[pga_year] = universal_year_shot_type_dict

        return universal_year_shot_type_dict
