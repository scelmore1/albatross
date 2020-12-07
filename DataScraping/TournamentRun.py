import csv

from DataScraping.TournamentScraper import TournamentScraper


class TournamentRun:

    def __init__(self, name, year, logger):
        """Get tournament name and year from .csv file to initialize class"""
        self.name = name
        self.year = str(year)
        self.scraped_tournament = None
        self.mongo_collection = None
        self._success = False
        self._logger = logger

    def __repr__(self):
        return self.__class__.__name__ + ' ' + str(self.year) + ' ' + self.name + \
               f'\nScraped and Uploaded to MongoDB: {self._success}\n'

    def runTournament(self, driver, remove_driver):
        self.__getMongoDBCollectionsFromScrape(driver)
        if remove_driver:
            self.scraped_tournament.web_driver.closeDriver()
        self.__uploadMongoDBCollections()
        return self.__repr__()

    def getDriverObj(self):
        return self.scraped_tournament.web_driver

    def __getMongoDBCollectionsFromScrape(self, driver):
        """Get MongoDB collections from the Tournament Scraper,
        pass in a driver if one exists"""
        self.scraped_tournament = TournamentScraper(self.name, self.year, driver)
        if self.scraped_tournament.runScrape():
            scraped_collection = self.scraped_tournament.convertDictsToMongoDBCollection()
            self.mongo_collection = {'Tournament Name': self.name,
                                     'Year': self.year,
                                     'Player Rounds': scraped_collection[0],
                                     'Player Metadata': scraped_collection[1],
                                     'Course Metadata': scraped_collection[2],
                                     'Tournament Details': scraped_collection[3]}
            self._success = len(scraped_collection) > 0
            self._logger.info('{}\n'.format(self.scraped_tournament))
        else:
            self._logger.error('Scraping for {} failed. Adding to failure list.\n'.format(self.scraped_tournament))
            with open('tournaments/FailedTournamentList.csv', 'a') as failed_list:
                failed_list = csv.writer(failed_list)
                failed_list.writerow([self.name, self.year])

    def __uploadMongoDBCollections(self):
        pass