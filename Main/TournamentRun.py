from DataScraping.TournamentScraper import TournamentScraper
from MongoDB.MongoUpload import MongoUploadTournament


class TournamentRun:
    failed_scrape_list = []

    def __init__(self, name, year, mongo_client, logger):
        """Get tournament name and pga_year from .csv file to initialize class"""
        self.name = name
        self.year = str(year)
        self._mongo_client = mongo_client
        self._logger = logger
        self._webdriver = None
        self._success = False

    def __repr__(self):
        return self.__class__.__name__ + ' ' + str(self.year) + ' ' + self.name + \
               f'\nScraped and Uploaded Tournament to MongoDB: {self._success}\n'

    def runTournament(self, driver, remove_driver):
        self._logger.info('Scraping Tournament {} -- PGA Year {} \n'.format(self.name, self.year))
        mongo_collection = self.__getMongoDBCollectionsFromScrape(driver, remove_driver)
        if mongo_collection:
            self._logger.info('Uploading Tournament {} -- PGA Year {} To MongoDB\n'.format(self.name, self.year))
            result = self.__uploadMongoDBCollections(mongo_collection)
            self._logger.info('Result of MongoDB upload: \n{}\n'.format(result))
            self._success = True
        return self.__repr__()

    def getDriverObj(self):
        return self._webdriver

    def __getMongoDBCollectionsFromScrape(self, driver, remove_driver):
        """Get MongoDB collections from the Tournament Scraper,
        pass in a driver if one exists"""
        scraped_tournament = TournamentScraper(self.name, self.year, driver)
        mongo_collection = None
        for i in range(3):
            if scraped_tournament.runScrape():
                self._logger.info('Attempt {} successful at scraping {}\n'.format(str(i + 1), scraped_tournament))
                scraped_collection = scraped_tournament.convertDictsToMongoDBCollection()
                mongo_collection = {'Tournament Scrape Status': {'tournamentName': self.name,
                                                                 'pgaYear': self.year,
                                                                 'tournamentID': scraped_tournament.tournament_id,
                                                                 'percentPlayersScraped': '{:.2f}'.format(
                                                                     scraped_tournament.successfully_scraped)},
                                    'Player Rounds': scraped_collection[0],
                                    'Player Metadata': scraped_collection[1],
                                    'Course Metadata': scraped_collection[2],
                                    'Tournament Details': scraped_collection[3]}
                break
        else:
            self._logger.error('Scraping for -- {} -- failed. Adding to failure list.\n'.format(scraped_tournament))
            self.failed_scrape_list.append({'Name': self.name, 'Year': self.year})

        if remove_driver:
            scraped_tournament.web_driver.closeDriver()
            scraped_tournament.web_driver.proxy_list.pop()
        else:
            self._webdriver = scraped_tournament.web_driver.getDriver()

        return mongo_collection

    def __uploadMongoDBCollections(self, collection_dict):
        mongo_upload = MongoUploadTournament(self._mongo_client, self.year, self.name)
        for key, value in collection_dict.items():
            if key == 'Tournament Scrape Status':
                mongo_upload.uploadTournamentScrapeStatus(value)
            elif key == 'Player Rounds':
                mongo_upload.uploadPlayerRounds(value)
            elif key == 'Player Metadata':
                mongo_upload.uploadPlayerMetadata(value)
            elif key == 'Course Metadata':
                mongo_upload.uploadCourseMetadata(value)
            elif key == 'Tournament Details':
                mongo_upload.uploadTournamentDetails(value)
        return mongo_upload
