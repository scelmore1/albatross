import logging

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from statsmodels.nonparametric.smoothers_lowess import lowess

from Logging.MyLogger import MyLogger
from MongoDB.MongoDownload import MongoDownload
from MongoDB.MongoUpload import MongoUploadStrokeDistance


class StrokeDistanceHandler:

    # @staticmethod
    # def visualizeDistanceLeft(df, title):
    #     _ = sns.lmplot(data=df, x='distanceLeft', y='shotsRemaining', hue='toSurface')
    #     plt.title(title + ' LM')
    #     plt.show()
    #     _ = sns.lmplot(data=df, x='distanceLeft', y='shotsRemaining', hue='toSurface', lowess=True)
    #     plt.title(title + ' Lowess')
    #     plt.show()
    #     distance_grouped = df.groupby(['distanceLeft5ydBin', 'toSurface']).mean().reset_index()
    #     _ = sns.scatterplot(data=distance_grouped, x='distanceLeft', y='shotsRemaining', hue='toSurface')
    #     plt.title(title + ' 5ydBin')
    #     plt.show()

    # @staticmethod
    # def visualizeStartDistance(df, group, title):
    #     _ = sns.lmplot(data=df, x='startDistance', y='shotsTaken', hue=group)
    #     plt.title(title + ' LM')
    #     plt.show()
    #     _ = sns.lmplot(data=df, x='startDistance', y='shotsTaken', hue=group, lowess=True)
    #     plt.title(title + ' Lowess')
    #     plt.show()

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
    def lowessExpectedShotsByDistance(distance_shots):
        new_df = distance_shots.str.split('/', expand=True)
        endog = new_df[[1]].values.ravel()
        exog = new_df[[0]].values.ravel()
        return pd.Series(index=new_df.index, data=lowess(endog=endog, exog=exog, return_sorted=False,
                                                         delta=.01 * len(exog)))

    @staticmethod
    def lowessExpectedRemainingShotsColumn(df, group, name):
        df['lowessExpectedShotsRemaining{}'.format(name)] = df.groupby(group)['distance/shots']. \
            transform(StrokeDistanceHandler.lowessExpectedShotsByDistance)
        return df

    @staticmethod
    def getStartingExpectedShots(tee_shots_df, visualize):
        tee_shots_df['shotsTaken'] = tee_shots_df['shotsRemaining'] + 1
        tee_shots_df['distance/shots'] = tee_shots_df.apply(lambda x: str(x['startDistance']) + '/' +
                                                                      str(x['shotsTaken']), axis=1)
        tee_shots_df['lowessPredictedShots'] = tee_shots_df['distance/shots']. \
            transform(StrokeDistanceHandler.lowessExpectedShotsByDistance)
        if visualize:
            _ = sns.lmplot(data=tee_shots_df, x='startDistance', y='shotsTaken', lowess=True)
            plt.title('Expected Shots From Start Distance Lowess Model')
            plt.show()
        return tee_shots_df

    @staticmethod
    def getRemainingExpectedShots(tee_shots_df, visualize=False):
        tee_shots_df['distance/shots'] = tee_shots_df.apply(lambda x: str(x['distanceLeft']) + '/' +
                                                                      str(x['shotsRemaining']), axis=1)

        no_retee_df = tee_shots_df[~tee_shots_df['isReTee']].copy()
        tee_shots_df['lmExpectedShotsRemainingBySurface'] = no_retee_df.groupby('toSurface')['distance/shots']. \
            transform(StrokeDistanceHandler.lowessExpectedShotsByDistance)
        tee_shots_df = tee_shots_df.drop(columns='distance/shots')
        if visualize:
            _ = sns.lmplot(data=tee_shots_df, x='distanceLeft', y='shotsRemaining', hue='toSurface', lowess=True)
            plt.title('Expected Shots For Distance Left Grouped By Surface Lowess Model')
            plt.show()
        return tee_shots_df

    @staticmethod
    def getTeeShotStrokeDistanceValues(tee_shots):
        start_distance_grouped = tee_shots.groupby('startDistance10ydBin').mean()
        start_distance_grouped = StrokeDistanceHandler.getStartingExpectedShots(start_distance_grouped, True)
        start_distance_grouped.drop(columns='distance/shots', inplace=True)
        return start_distance_grouped

    def __init__(self, mongo_obj):
        self._tournaments_uploaded = []
        self._tournaments_downloaded = []
        self._logger = MyLogger(self.__class__.__name__, logging.INFO,
                                'Analysis/logs/{}.log'.format(self.__class__.__name__)).getLogger()
        self._mongo_download = MongoDownload(mongo_obj)
        self._already_added_tournaments = self._mongo_download.getStrokeDistanceTournaments()
        self._already_added_stroke_distance_years = self._mongo_download.getYearsWithLowessData()
        self._mongo_upload = MongoUploadStrokeDistance(mongo_obj)

    def __repr__(self):
        return 'Stroke Distance Handler has:\nUploaded tournaments: {}\nDownloaded tournaments: {}'. \
            format(self._tournaments_uploaded, self._tournaments_downloaded)

    def uploadTournament(self, tournament_df, tournament_name, force_upload=False):
        """Add tournament to mongoDB stroke distance collection if doesn't exist"""
        if tournament_name not in self._already_added_tournaments or force_upload:
            self._logger.info('Upload tournament {}'.format(tournament_name))
            for year, year_df in tournament_df.groupby('pgaYear'):
                necessary_cols = ['par', 'fromSurface', 'startDistance', 'startDistance10ydBin', 'distanceLeft',
                                  'distanceLeft5ydBin', 'distanceLeft1ydBin', 'distanceLeft1ftBin', 'toSurface',
                                  'shotsRemaining']
                df_dict = year_df.loc[:, necessary_cols].to_dict('records')
                stroke_dist_dict = {'tournamentName': tournament_name, 'pgaYear': year, 'df': df_dict}
                if self._mongo_upload.addTournament(stroke_dist_dict):
                    self._tournaments_uploaded.append(tournament_name)

    def yearBasedPredictedStrokesFromDistance(self, pga_year, force_upload=False):
        """Given a PGA pga_year create the expected strokes from each distance and location"""
        if pga_year not in self._already_added_stroke_distance_years or force_upload:
            lowess_upload_dict = dict.fromkeys(['StartDistance10yd', 'DistanceLeft5yd', 'DistanceLeft1yd',
                                                'DistanceLeft1ft'])
            stroke_distance_df = self._mongo_download.getStrokeDistanceForGivenYear(pga_year)
            tee_shots = stroke_distance_df.loc[stroke_distance_df['fromSurface'] == 'OTB', :].copy()
            tee_shot_stroke_distance_df = self.getTeeShotStrokeDistanceValues(tee_shots). \
                drop(columns=['distanceLeft', 'shotsRemaining'])
            lowess_upload_dict['StartDistance10yd'] = tee_shot_stroke_distance_df

            return lowess_upload_dict
