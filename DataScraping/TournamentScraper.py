import json
import logging
import os
import re

from nested_lookup import nested_lookup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager


# from selenium.webdriver.remote.remote_connection import LOGGER


def findKeyInJSON(json_body, key):
    return nested_lookup(key, json_body)[0]


class TournamentScraper:
    """Given a tournament and year, this scrapes pgatour.com tournament result
     page to create json files containing data on tournament info and player hole by hole shots"""

    # use this default dictionary to read in html wires for necessary data
    default_wire_html_dict = {
        'tournament_id': r'https://statdata.pgatour.com/r/\d+',
        'tournament_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/leaderboard.json',
        'course_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/courseC_ID',
        'round_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/drawer/rROUND_NUM-mMAIN_PLAYER_ID'
    }

    timeout_length = 30

    def __init__(self, pga_tournament, pga_year, driver=None, wire_requests_dict=None):
        """Initialize scraper with tournament, year, optional logger name, wire requests dict, web driver"""
        self.pga_tournament = pga_tournament
        self.pga_year = pga_year
        # all I/O done in tournaments/'pga_year'_'tournament_name' directory
        self.dir = 'tournaments/' + self.pga_year + '_' + self.pga_tournament + '/'
        if not os.path.exists(self.dir):
            os.makedirs(os.path.dirname(self.dir + 'logs/tournament_scape.log'), exist_ok=True)
        if wire_requests_dict is None:
            self.wire_requests_dict = self.default_wire_html_dict
        else:
            self.wire_requests_dict = wire_requests_dict
        self.driver = driver
        # initialize logger
        self.__loggerInitialize()
        self.tournament_url = 'https://www.pgatour.com/competition/' + pga_year + '/' + pga_tournament + \
                              '/leaderboard.html'
        self.tournament_id = None
        self.successfully_scraped = False
        self.__course_ids = set()
        self.__tournament_info_dict = {}
        self.__player_meta_dict = {}
        self.__course_meta_dict = {}
        self.__player_round_dict = {}

    def __repr__(self):
        """Print Scraper Class with year, tournament and scraped status"""
        return self.__class__.__name__ + ' ' + self.pga_year + ' ' + self.pga_tournament + \
               '\nScrape Status: ' + str(self.successfully_scraped)

    def __loggerInitialize(self):
        self.__logger = logging.getLogger(self.__class__.__name__ + ' ' + self.pga_year + ' ' + self.pga_tournament)
        if self.__logger.hasHandlers():
            self.__logger.handlers.clear()
        self.__logger.setLevel(logging.INFO)
        # selenium log kept separate from class log
        selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
        selenium_logger.setLevel(logging.WARNING)
        # create handlers
        file_handler = logging.FileHandler(self.dir + 'logs/tournament_scape.log', mode='w+')
        console_handler = logging.StreamHandler()
        selenium_logger.addHandler(file_handler)
        # define custom formatter
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:\t%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        # assign handlers
        self.__logger.addHandler(file_handler)
        self.__logger.addHandler(console_handler)

    def __wireRequestToJSON(self, request_str):
        """Take request string and return json object from html wire"""
        request = self.driver.wait_for_request(request_str, timeout=self.timeout_length)
        return json.loads(request.response.body.decode('utf-8'))

    def __scrapeTournamentInfo(self):
        """Scrape data related to the tournament itself"""
        request_str = self.wire_requests_dict['tournament_detail'] \
            .replace('PGA_YEAR', self.pga_year).replace('TOURNAMENT_ID', self.tournament_id)
        tournament_detail_json = self.__wireRequestToJSON(request_str)

        # make sure pga years match
        if self.pga_year != findKeyInJSON(tournament_detail_json, 'year'):
            self.__logger.warning('Error: Non-matching PGA years. User Input {}; JSON {}'
                                  .format(self.pga_year, findKeyInJSON(tournament_detail_json, 'year')))

        # cutline data
        cut_line_info = findKeyInJSON(tournament_detail_json, 'cutLines')
        cut_dict = {'cuts': []}
        for i, cut in enumerate(cut_line_info, start=1):
            cut_dict['cuts'].append({
                'cutNumber': i,
                'cutCount': cut['cut_count'],
                'cutScore': cut['cut_line_score'],
                'cutPaidCount': cut['paid_players_making_cut']
            })
        self.__tournament_info_dict.update(cut_dict)

        # all other tournament data
        self.__tournament_info_dict.update({
            'tournamentId': self.tournament_id,
            'format': findKeyInJSON(tournament_detail_json, 'format'),
            'pgaYear': findKeyInJSON(tournament_detail_json, 'year'),
            'status': findKeyInJSON(tournament_detail_json, 'roundState'),
            'playoff': findKeyInJSON(tournament_detail_json, 'playoffPresent'),
            'dates': self.driver.find_elements_by_xpath('.//span[@class = "dates"]')[0].text,
            'location': self.driver.find_elements_by_xpath('.//span[@class = "name"]')[0].text
        })

        # create player name dictionary
        player_rows = findKeyInJSON(tournament_detail_json, 'rows')
        for row in player_rows:
            self.__player_meta_dict[row['playerId']] = {}
            self.__player_meta_dict[row['playerId']]['firstName'] = row['playerNames']['firstName']
            self.__player_meta_dict[row['playerId']]['lastName'] = row['playerNames']['lastName']
            self.__course_ids.add(row['courseId'])

    def __scrapeCourseDetail(self):
        """Scrape data related to the course itself"""
        # only scrape if course hasn't been added to dictionary yet
        for c_id in self.__course_ids:
            if c_id in self.__course_meta_dict:
                continue

            request_str = self.wire_requests_dict['course_detail']. \
                replace('PGA_YEAR', self.pga_year).replace('TOURNAMENT_ID', self.tournament_id).replace('C_ID', c_id)
            course_detail_json = self.__wireRequestToJSON(request_str)
            hole_detail_dict = {}

            # hole by hole data
            for hole in findKeyInJSON(course_detail_json, 'holes'):
                round_info = {'round #': []}
                for round_details in hole['rounds']:
                    round_detail = {
                        'distance': round_details['distance'],
                        'par': round_details['par'],
                        'stimp': round_details['stimp']
                    }
                    round_info[round_details['roundId']] = round_detail
                hole_detail_dict[hole['holeId']] = round_info

            # add metadata
            self.__course_meta_dict[c_id] = {
                'courseCode': findKeyInJSON(course_detail_json, 'courseCode'),
                'parIn': findKeyInJSON(course_detail_json, 'parIn'),
                'parOut': findKeyInJSON(course_detail_json, 'parOut'),
                'parTotal': findKeyInJSON(course_detail_json, 'parTotal'),
                'holes': hole_detail_dict
            }

    def __scrapePlayerDetail(self, main_player_id, round_num):
        """Scrape data related to a player's round"""
        request_str = self.wire_requests_dict['round_detail'] \
            .replace('PGA_YEAR', self.pga_year) \
            .replace('TOURNAMENT_ID', self.tournament_id).replace('ROUND_NUM', round_num) \
            .replace('MAIN_PLAYER_ID', main_player_id)
        round_detail_json = self.__wireRequestToJSON(request_str)
        play_by_play = findKeyInJSON(round_detail_json, 'playersHoles')
        player_hole_dict = {}

        # get shot level data
        for hole in play_by_play:
            hole_id = hole['holeId']
            for player in hole['players']:
                player_id = player['playerId']
                if player_id not in player_hole_dict:
                    player_hole_dict[player_id] = {}
                player_hole_dict[player_id][hole_id] = player['shots']

        # check to see if main player id is indeed contained in json data
        if main_player_id not in player_hole_dict:
            self.__logger.warning('Main Player ID is {}, player IDs in JSON File {}'.format(
                main_player_id, player_hole_dict.keys()))

        # assign shot data and create metadata for round
        for player_id in player_hole_dict.keys():
            if player_id not in self.__player_round_dict:
                self.__player_round_dict[player_id] = {}
            if round_num not in self.__player_round_dict[player_id]:
                self.__player_round_dict[player_id][round_num] = {}
            self.__player_round_dict[player_id][round_num]['play-by-play'] = player_hole_dict[player_id]
            self.__player_round_dict[player_id][round_num]['metadata'] = {
                'completedRound': findKeyInJSON(round_detail_json, 'roundComplete'),
                'groupId': findKeyInJSON(round_detail_json, 'groupId'),
                'startingHoleId': findKeyInJSON(round_detail_json, 'startingHoleId'),
                'courseId': findKeyInJSON(round_detail_json, 'courseId'),
                'playedWith': [other_id for other_id in player_hole_dict.keys() if other_id != player_id]
            }

    def initializeDriver(self):
        """Get driver if none exists, get tournament url"""
        if self.driver is None:
            self.__logger.info('Initializing New Driver...')
            self.driver = webdriver.Chrome(ChromeDriverManager().install())
        try:
            self.driver.get(self.tournament_url)
            self.driver.minimize_window()
        except Exception as e:
            self.__logger.error('Error loading url {}\n{}'.format(self.tournament_url, e))

    def runScrape(self):
        """Call all scraping methods to populate all dictionaries with necessary data"""
        self.__logger.info('\nRunning Scrape for {} {}\n'.format(self.pga_year, self.pga_tournament))
        try:
            request_str = self.default_wire_html_dict['tournament_id']
            self.tournament_id = findKeyInJSON(self.__wireRequestToJSON(request_str), 'tid')
        except Exception as e:
            self.__logger.error('Error getting tournament ID from ' + self.default_wire_html_dict['tournament_id']
                                + '; Trying xpath method\n{}\n'.format(e), exc_info=True)
            try:
                self.tournament_id = re.findall(r'\d+', self.driver.find_element_by_xpath(
                    "//meta[@name='branch:deeplink:tournament_id']").get_attribute('content'))[0]
            except Exception as e:
                self.__logger.error('Error getting tournament ID from xpath\n{}\n'.format(e),
                                    exc_info=True)
                return False
        finally:
            self.__logger.info('Tournament ID is {}'.format(self.tournament_id))

        try:
            self.__scrapeTournamentInfo()
        except Exception as e:
            self.__logger.error('Error getting tournament details\n{}\n'.format(e), exc_info=True)
            return False

        try:
            row_lines = WebDriverWait(self.driver, 30).until(
                EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'tr.line-row.line-row')))
        except Exception as e:
            self.__logger.error('Error locating player elements on page\n{}\n'.format(e), exc_info=True)
            return False
        else:
            # iterate through the players contained on the url page
            for i, row in enumerate(row_lines):
                if i > 5:
                    continue
                # get player's shot information chart open on url
                _ = row.location_once_scrolled_into_view
                main_player_id = re.findall(r'\d+', row.get_attribute('class'))[0]
                self.__logger.info('Scraping row {}, player ID {}'.format(i, main_player_id))
                WebDriverWait(row, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, 'player-name-col'))).click()
                try:
                    self.__scrapeCourseDetail()
                except Exception as e:
                    self.__logger.error('Error getting course detail\n{}\n'.format(e), exc_info=True)
                    continue

                WebDriverWait(row.parent, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, 'tab-button.play-by-play-button'))).click()
                play_tab = row.parent.find_element_by_class_name('tab-component.play-by-play-tab')
                round_buttons = play_tab.find_elements_by_class_name('round')

                # go round by round to scrape data
                for round_button in round_buttons:
                    round_num = round_button.text
                    self.__logger.info('Getting data for round {}'.format(round_num))
                    if main_player_id in self.__player_round_dict and round_num in self.__player_round_dict[
                        main_player_id]:
                        continue
                    WebDriverWait(play_tab, 10).until(EC.element_to_be_clickable(
                        (By.XPATH, './div/div[1]/div[1]/span[' + str(int(round_num) + 1) + ']'))).click()
                    try:
                        self.__scrapePlayerDetail(main_player_id, round_num)
                    except Exception as e:
                        self.__logger.error('Error scraping player detail for {}, round number {}\n{}\n'.
                                            format(main_player_id, round_num, e), exc_info=True)
                        continue

                # this closes the player's shot information chart
                WebDriverWait(row, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, 'player-name-col'))).click()

        finally:
            if len(self.__player_round_dict) == len(self.__player_meta_dict):
                self.successfully_scraped = True
                self.__logger.info('Successfully scraped data for all players in tournament {} {}'.format(self.pga_year,
                                                                                                          self.pga_tournament))
            elif len(self.__player_round_dict) == 0:
                self.successfully_scraped = False
                self.__logger.info(
                    'Unsuccessfully scraped data for tournament {} {}'.format(self.pga_year, self.pga_tournament))
                return False
            elif len(self.__player_round_dict) < len(self.__player_meta_dict):
                self.successfully_scraped = False
                self.__logger.info('Only scraped data for {:.2f}% of players in tournament {} {}'.
                                   format((len(self.__player_round_dict) / len(self.__player_meta_dict)) * 100,
                                          self.pga_year,
                                          self.pga_tournament))

            return True

    def __convertPlayerRoundToMongoDBCollection(self):
        player_round_collection = []
        for player_id, round_num in self.__player_round_dict.items():
            for round_key, round_values in round_num.items():
                player_round_level = {'playerID': player_id, 'roundNumber': round_key}
                player_round_level.update(round_values['metadata'])
                player_round_level['holes'] = []
                for hole_key, hole_values in round_values['play-by-play'].items():
                    hole_level = {'holeNumber': hole_key, 'shots': []}
                    for shot in hole_values:
                        hole_level['shots'].append(shot)
                    player_round_level['holes'].append(hole_level)
                player_round_collection.append(player_round_level)
        return player_round_collection

    def __convertPlayerMetaToMongoDBCollection(self):
        player_meta_collection = []
        for player_id, meta_values in self.__player_meta_dict.items():
            player_meta = {'playerID': player_id}
            player_meta.update(meta_values)
            player_meta_collection.append(player_meta)
        return player_meta_collection

    def __convertCourseMetaToMongoDBCollection(self):
        course_meta_collection = []
        for course_id, course_details in self.__course_meta_dict.items():
            course_meta = {'courseID': course_id}
            course_meta.update(course_details)
            hole_level_list = []
            for hole_key, round_info in course_meta['holes'].items():
                hole_level = {'holeNumber': hole_key, 'rounds': []}
                for round_num, round_details in round_info.items():
                    round_level = {'roundNumber': round_num}
                    round_level.update(round_details)
                    hole_level['rounds'].append(round_level)
                hole_level_list.append(hole_level)
            course_meta['holes'] = hole_level_list
            course_meta_collection.append(course_meta)
        return course_meta_collection

    def convertDictsToMongoDBCollection(self):
        """General method for converting all class dictionaries to Mongo DB Collections"""
        mongoDB_collections = [self.__convertPlayerRoundToMongoDBCollection(),
                               self.__convertPlayerMetaToMongoDBCollection(),
                               self.__convertCourseMetaToMongoDBCollection(), self.__tournament_info_dict]
        return mongoDB_collections

    def uploadDictsToJSON(self):
        """Upload the dictionaries to json files for debugging purposes"""
        with open(self.dir + 'player_round.json', 'w') as f:
            json.dump(self.__player_round_dict, f)
        with open(self.dir + 'player_meta.json', 'w') as f:
            json.dump(self.__player_meta_dict, f)
        with open(self.dir + 'tournament_info.json', 'w') as f:
            json.dump(self.__tournament_info_dict, f)
        with open(self.dir + 'course_meta.json', 'w') as f:
            json.dump(self.__course_meta_dict, f)

    def downloadDictsFromJSON(self):
        """Download the JSON files to dictionaries for debugging purposes"""
        with open(self.dir + 'player_round.json', 'r') as f:
            self.__player_round_dict = json.load(f)
        with open(self.dir + 'player_meta.json', 'r') as f:
            self.__player_meta_dict = json.load(f)
        with open(self.dir + 'tournament_info.json', 'r') as f:
            self.__tournament_info_dict = json.load(f)
        with open(self.dir + 'course_meta.json', 'r') as f:
            self.__course_meta_dict = json.load(f)