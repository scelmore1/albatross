import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from Logging.MyLogger import MyLogger
from SeleniumDriver.WebDriver import WebDriver
from SeleniumDriver.WebDriver import wait_for_text_to_match


class SGScraper:
    """Given a tournament and year, this scrapes pgatour.com tournament result
     page to create json files containing data on tournament info and player course_hole by course_hole shots"""

    def __init__(self):
        """Initialize SG Scraper"""
        self._sg_url = 'https://datagolf.com/historic-event-data'

        # create place holder dictionaries for data once scraped
        self._tournament_sg_col = []

        # all I/O done in tournaments/'pga_year'_'tournament_name' directory
        self._file_handler = 'tournaments/SG/logs/sg_scape.log'

        # initialize logger
        self._logger = MyLogger(self.__class__.__name__, self._file_handler, logging.INFO, 'w').getLogger()

        # initialize driver
        self.web_driver = WebDriver(self._logger)
        self.year_options = None

    def __repr__(self):
        """Print Scraper Class with scraped status"""
        return self.__class__.__name__

    def _sgStatsToDict(self, year_name, tournament_name, sg_stats):
        self._logger.info('Getting SG stats for {} during {} {}'.format(sg_stats[0], year_name, tournament_name))
        self._tournament_sg_col.append({
            'pgaYear': year_name,
            'tournamentName': tournament_name,
            'playerName': sg_stats[0],
            'sgPUTT': sg_stats[1],
            'sgARG': sg_stats[2],
            'sgAPP': sg_stats[3],
            'sgOTT': sg_stats[4],
            'sgT2G': sg_stats[5],
            'sgTOT': sg_stats[6]
        })

    def runScrape(self):
        """"""
        self._logger.info(
            'Go to SG Scrape url {}\n'.format(self._sg_url))
        self.web_driver.goToURL(self._sg_url)
        driver = self.web_driver.getDriver()

        try:
            tournament_selector = Select(driver.find_element_by_id('dropdown'))
            num_options = len(tournament_selector.options)
            for idx in range(num_options):
                tournament_selector.select_by_index(idx)
                tournament_name = tournament_selector.first_selected_option.text
                _ = self.web_driver.webDriverWait(driver,
                                                  wait_for_text_to_match(
                                                      (By.CLASS_NAME, 'subtitle'),
                                                      r'\d+ {}'.format(tournament_name)),
                                                  'Error waiting for tournament to load\n{}')
                self.year_options = driver.find_elements_by_class_name('yearoptions')
                for year in reversed(self.year_options):
                    year_name = year.text
                    if int(year_name) < 2018:
                        break

                    self._logger.info('\nRunning SG Scrape for {} {}'.format(year_name, tournament_name))
                    year.click()
                    sg_table = driver.find_element_by_class_name('table')
                    data_rows = sg_table.find_elements_by_class_name('datarow')
                    for row in data_rows:
                        sg_stats = row.text.split('\n')
                        if sg_stats[3] == '--':
                            self._logger.info('No SG stats for {} {}'.format(year_name, tournament_name))
                            break
                        self._sgStatsToDict(year_name, tournament_name, [sg_stats[i] for i in (1, 3, 4, 5, 6, 7, 8)])
            return True
        except Exception as e:
            self._logger.error('Failed running SG scrape due to {}'.format(e), exc_info=True)
            return False

    def getSGCollection(self):
        return self._tournament_sg_col