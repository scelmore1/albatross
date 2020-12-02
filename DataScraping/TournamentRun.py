from DataScraping.TournamentScraper import TournamentScraper


class TournamentRun:

    def __init__(self, name, year):
        """Get tournament name and year from .csv file to initialize class"""
        self.name = name
        self.year = str(year)
        self.scraped_tournament = None
        self.mongo_collections = None
        self.scraped = False

    def __repr__(self):
        return self.__class__.__name__ + ' ' + str(self.year) + ' ' + self.name + \
               f'\nCreated Mongo DB Collection: {self.scraped}'

    def createMongoDBCollectionsFromScrape(self, remove_driver=True, driver=None):
        """Create Mongo DB collections from the Tournament Scraper,
        pass in a driver if one exists"""
        self.scraped_tournament = TournamentScraper(self.name, self.year, driver)
        self.scraped_tournament.initializeDriver()
        self.scraped_tournament.runScrape()
        if remove_driver:
            self.scraped_tournament.driver.close()
        self.mongo_collections = self.scraped_tournament.convertDictsToMongoDBCollection()
        self.scraped = len(self.mongo_collections) > 0
        return self.__repr__()

    def checkJSON(self):
        """Help debug by uploading JSON files from the dictionaries in the Tournament Scraper"""
        self.scraped_tournament.uploadDictsToJSON()