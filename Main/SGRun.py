from DataScraping.SGScraper import SGScraper
from MongoDB.MongoUpload import MongoUploadSG


class SGRun:

    def __init__(self, mongo_client, logger):
        self._mongo_client = mongo_client
        self._logger = logger
        self._success = False

    def __repr__(self):
        return self.__class__.__name__ + \
               f'\nScraped SG and Uploaded to MongoDB: {self._success}\n'

    def runSG(self):
        self._logger.info('Scraping SG Stats')
        mongo_collection = self.__getMongoDBCollectionsFromScrape()
        if mongo_collection:
            self._logger.info('Uploading SG Stats To MongoDB\n')
            result = self.__uploadMongoDBCollections(mongo_collection)
            self._logger.info('Result of MongoDB upload: \n{}\n'.format(result))
            self._success = True
        return self.__repr__()

    def __getMongoDBCollectionsFromScrape(self):
        sg_scraper = SGScraper()
        mongo_collection = None
        if sg_scraper.runScrape():
            self._logger.info('{}\n'.format(sg_scraper))
            mongo_collection = sg_scraper.getSGCollection()

        sg_scraper.web_driver.closeDriver()
        sg_scraper.web_driver.proxy_list.pop()

        return mongo_collection

    def __uploadMongoDBCollections(self, sg_stats_col):
        mongo_upload = MongoUploadSG(self._mongo_client.getTournamentDB())
        mongo_upload.uploadSGStats(sg_stats_col)
        return mongo_upload