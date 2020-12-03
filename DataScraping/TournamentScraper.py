import json
import logging
import os
import re

from nested_lookup import nested_lookup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from Logging.MyLogger import MyLogger
from SeleniumDriver.WebDriver import WebDriver


def findKeyInJSON(json_body, key):
    return nested_lookup(key, json_body)[0]


class TournamentScraper:
    """Given a tournament and year, this scrapes pgatour.com tournament result
     page to create json files containing data on tournament info and player hole by hole shots"""

    # use this default dictionary to read in html wires for necessary data
    default_wire_html_dict = {
        'tournament_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/leaderboard.json',
        'course_general': 'https://statdata.pgatour.com/r/TOURNAMENT_ID/course.json',
        'course_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/courseC_ID',
        'round_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/drawer/rROUND_NUM-mMAIN_PLAYER_ID'
    }

    def __init__(self, pga_tournament, pga_year, driver=None, wire_requests_dict=None):
        """Initialize scraper with tournament, year, optional logger name, wire requests dict, web driver"""
        self.pga_tournament = pga_tournament
        self.pga_year = pga_year
        self.tournament_url = 'https://www.pgatour.com/competition/' + pga_year + '/' + pga_tournament + \
                              '/leaderboard.html'
        self.successfully_scraped = False
        self._tournament_id = None
        self._course_ids = set()
        self._tournament_info_dict = {}
        self._player_meta_dict = {}
        self._course_general_dict = {}
        self._course_meta_dict = {}
        self._player_round_dict = {}

        # all I/O done in tournaments/'pga_year'_'tournament_name' directory
        self.dir = 'tournaments/' + self.pga_year + '_' + self.pga_tournament + '/'
        if not os.path.exists(self.dir):
            os.makedirs(os.path.dirname(self.dir + 'logs/tournament_scape.log'), exist_ok=True)
        self._file_handler = self.dir + 'logs/tournament_scape.log'
        # os.remove(self._file_handler)

        # change the urls to make wire requests from if provided
        if wire_requests_dict is None:
            self._wire_requests_dict = self.default_wire_html_dict
        else:
            self._wire_requests_dict = wire_requests_dict

        # initialize logger
        self._logger = MyLogger(self.__class__.__name__ + ' ' + self.pga_year + ' ' + self.pga_tournament,
                                self._file_handler, logging.INFO, 'a').getLogger()

        # initialize driver
        if driver is None:
            self.web_driver = WebDriver(self._logger)
        else:
            self.web_driver = driver
        self.web_driver.updateLogLocations(' ' + self.pga_year + ' ' + self.pga_tournament, self._file_handler)

    def __repr__(self):
        """Print Scraper Class with year, tournament and scraped status"""
        return (self.__class__.__name__ + ' ' + self.pga_year + ' ' + self.pga_tournament
                + '\nScrape Status: ' + str(self.successfully_scraped))

    def __scrapeTournamentInfo(self):
        """Scrape data from the detailed tournament info JSON"""
        request_str = self._wire_requests_dict['tournament_detail'] \
            .replace('PGA_YEAR', self.pga_year).replace('TOURNAMENT_ID', self._tournament_id)
        tournament_detail_json = self.web_driver.wireRequestToJSON(request_str)
        if not tournament_detail_json:
            raise RuntimeError('Error scraping tournament detail data')

        # make sure pga years match
        if self.pga_year != findKeyInJSON(tournament_detail_json, 'year'):
            self._logger.warning('Error: Non-matching PGA years. User Input {}; JSON {}'
                                 .format(self.pga_year, findKeyInJSON(tournament_detail_json, 'year')))

        # cut line data
        cut_line_info = findKeyInJSON(tournament_detail_json, 'cutLines')
        cut_dict = {'cuts': []}
        for i, cut in enumerate(cut_line_info, start=1):
            cut_dict['cuts'].append({
                'cutNumber': i,
                'cutCount': cut['cut_count'],
                'cutScore': cut['cut_line_score'],
                'cutPaidCount': cut['paid_players_making_cut']
            })
        self._tournament_info_dict.update(cut_dict)

        # all other tournament data
        self._tournament_info_dict.update({
            'tournamentId': self._tournament_id,
            'format': findKeyInJSON(tournament_detail_json, 'format'),
            'pgaYear': findKeyInJSON(tournament_detail_json, 'year'),
            'status': findKeyInJSON(tournament_detail_json, 'roundState'),
            'playoff': findKeyInJSON(tournament_detail_json, 'playoffPresent'),
            'dates': self.web_driver.findElementByXPath('.//span[@class = "dates"]'),
            'location': self.web_driver.findElementByXPath('.//span[@class = "name"]')
        })

        # create player name dictionary
        player_rows = findKeyInJSON(tournament_detail_json, 'rows')
        for row in player_rows:
            self._player_meta_dict[row['playerId']] = {}
            self._player_meta_dict[row['playerId']]['firstName'] = row['playerNames']['firstName']
            self._player_meta_dict[row['playerId']]['lastName'] = row['playerNames']['lastName']
            self._course_ids.add(row['courseId'])

    def __scrapeCourseGeneral(self):
        """Scrape data from the general course information JSON"""
        request_str = self._wire_requests_dict['course_general'].replace('TOURNAMENT_ID', self._tournament_id)
        course_general_json = self.web_driver.wireRequestToJSON(request_str, 10)
        if not course_general_json:
            raise RuntimeError('Error scraping course general data')

        for course_desc in course_general_json['courses']:
            course_id = findKeyInJSON(course_desc, 'number')
            self._course_ids.add(course_id)
            self._course_general_dict[course_id] = {
                'description': findKeyInJSON(course_desc, 'body'),
                'name': findKeyInJSON(course_desc, 'name'),
                'totalYards': findKeyInJSON(course_desc, 'yards')
            }

    def __scrapeCourseDetail(self, c_id):
        """Scrape data from the course detail JSON"""
        request_str = self._wire_requests_dict['course_detail']. \
            replace('PGA_YEAR', self.pga_year).replace('TOURNAMENT_ID', self._tournament_id).replace('C_ID', c_id)
        course_detail_json = self.web_driver.wireRequestToJSON(request_str)
        if not course_detail_json:
            raise RuntimeError('Error scraping course detail data')

        course_id = findKeyInJSON(course_detail_json, 'courseId')
        # check if this is a mismatch from c_id
        if c_id != course_id:
            self._logger.warning(
                'Course ID {} from course detail JSON mismatches the player round course ID {}'.format(course_id, c_id))

        # check if course exists from earlier general json scrape
        if len(self._course_ids) > 0 and course_id not in self._course_ids:
            self._logger.warning(
                'Course ID {} came through the wire but did not exist in the general course JSON'.format(course_id))

        self._course_ids.add(course_id)
        hole_detail_dict = {}

        # hole by hole data
        for hole in findKeyInJSON(course_detail_json, 'holes'):
            round_info = {'rounds': []}
            for round_details in hole['rounds']:
                round_detail = {
                    'round_Id': round_details['roundId'],
                    'distance': round_details['distance'],
                    'par': round_details['par'],
                    'stimp': round_details.get('stimp')
                }
                round_info['rounds'].append(round_detail)
            hole_detail_dict[hole['holeId']] = round_info

        # add metadata
        self._course_meta_dict[course_id] = {
            'courseCode': findKeyInJSON(course_detail_json, 'courseCode'),
            'parIn': findKeyInJSON(course_detail_json, 'parIn'),
            'parOut': findKeyInJSON(course_detail_json, 'parOut'),
            'parTotal': findKeyInJSON(course_detail_json, 'parTotal'),
            'holes': hole_detail_dict
        }
        # add data from course general dict if exists
        if course_id in self._course_general_dict:
            self._course_meta_dict[course_id].update(self._course_general_dict[course_id])

    def __scrapePlayerDetail(self, main_player_id, round_num):
        """Scrape data from the player round specific JSON"""
        request_str = self._wire_requests_dict['round_detail'] \
            .replace('PGA_YEAR', self.pga_year) \
            .replace('TOURNAMENT_ID', self._tournament_id).replace('ROUND_NUM', round_num) \
            .replace('MAIN_PLAYER_ID', main_player_id)
        round_detail_json = self.web_driver.wireRequestToJSON(request_str)
        if not round_detail_json:
            raise RuntimeError(
                'Error scraping player detail for {}, round number {}\n'.format(main_player_id, round_num))

        course_id = findKeyInJSON(round_detail_json, 'courseId')
        # only scrape if course hasn't been added to dictionary yet
        if course_id not in self._course_meta_dict:
            # scrape course detail
            try:
                self.__scrapeCourseDetail(course_id)
            except Exception as e:
                self._logger.warning('{}\n'.format(e), exc_info=True)

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
            self._logger.warning('Main Player ID is {}, player IDs in JSON File {}'.format(
                main_player_id, player_hole_dict.keys()))

        # assign shot data and create metadata for round
        for player_id in player_hole_dict.keys():
            if player_id not in self._player_round_dict:
                self._player_round_dict[player_id] = {}
            if round_num not in self._player_round_dict[player_id]:
                self._player_round_dict[player_id][round_num] = {}
            self._player_round_dict[player_id][round_num]['play-by-play'] = player_hole_dict[player_id]
            self._player_round_dict[player_id][round_num]['metadata'] = {
                'completedRound': findKeyInJSON(round_detail_json, 'roundComplete'),
                'groupId': findKeyInJSON(round_detail_json, 'groupId'),
                'startingHoleId': findKeyInJSON(round_detail_json, 'startingHoleId'),
                'courseId': findKeyInJSON(round_detail_json, 'courseId'),
                'playedWith': [other_id for other_id in player_hole_dict.keys() if other_id != player_id]
            }

    def runScrape(self):
        """Call all scraping methods to populate all dictionaries with necessary data"""
        self._logger.info(
            '\nRunning Scrape for {} {}\nURL is {}\n'.format(self.pga_year, self.pga_tournament, self.tournament_url))
        self.web_driver.goToURL(self.tournament_url)
        tournament_xpath = self.web_driver.webDriverWait(self.web_driver.getDriver(),
                                                         EC.presence_of_element_located(
                                                             (By.XPATH,
                                                              "//meta[@name='branch:deeplink:tournament_id']")),
                                                         'Error getting tournament_id\n{}').get_attribute('content')
        self._tournament_id = re.findall(r'\d+', tournament_xpath)[0]
        if not self._tournament_id:
            self._logger.error('Could not get a tournament ID out of {}\n'.format(tournament_xpath))
            return False

        self._logger.info('Tournament ID is {}'.format(self._tournament_id))

        # scrape tournament data
        try:
            self.__scrapeTournamentInfo()
        except Exception as e:
            self._logger.warning('{}\n'.format(e), exc_info=True)

        # scrape course general data
        try:
            self.__scrapeCourseGeneral()
        except Exception as e:
            self._logger.warning('{}\n'.format(e), exc_info=True)

        row_lines = self.web_driver.webDriverWait(self.web_driver.getDriver(),
                                                  EC.visibility_of_all_elements_located(
                                                      (By.CSS_SELECTOR, 'tr.line-row.line-row')),
                                                  'Error locating player elements on page\n{}')
        if row_lines is None:
            return False

        # iterate through the players contained on the url page
        for i, row in enumerate(row_lines):
            if i > 1:
                continue
            # get player's shot information chart open on url
            _ = row.location_once_scrolled_into_view
            main_player_id = re.findall(r'\d+', row.get_attribute('class'))[0]
            self._logger.info('Scraping row {}, player ID {}'.format(i, main_player_id))
            player_name_col_button = self.web_driver.webDriverWait(row,
                                                                   EC.element_to_be_clickable(
                                                                       (By.CLASS_NAME, 'player-name-col')),
                                                                   'Error getting player column to click\n{}')
            if player_name_col_button is None:
                continue
            player_name_col_button.click()

            # get round by round data by clicking player round buttons
            play_by_play_button = self.web_driver.webDriverWait(row.parent, EC.element_to_be_clickable(
                (By.CLASS_NAME, 'tab-button.play-by-play-button')), 'Error getting play by play button to click\n{}')
            if play_by_play_button is None:
                continue
            play_by_play_button.click()
            play_tab = row.parent.find_element_by_class_name('tab-component.play-by-play-tab')
            round_buttons = play_tab.find_elements_by_class_name('round')

            # go round by round to scrape data
            for round_button in round_buttons:
                round_num = round_button.text
                self._logger.info('Getting data for round {}, player ID {}'.format(round_num, main_player_id))
                if main_player_id in self._player_round_dict and round_num in self._player_round_dict[main_player_id]:
                    continue
                round_num_button = self.web_driver.webDriverWait(play_tab, EC.element_to_be_clickable(
                    (By.XPATH, './div/div[1]/div[1]/span[' + str(int(round_num) + 1) + ']')),
                                                                 'Error getting round number button to click\n{}')
                if round_num_button is None:
                    continue
                round_num_button.click()
                try:
                    self.__scrapePlayerDetail(main_player_id, round_num)
                except Exception as e:
                    self._logger.warning('{}\n'.format(e), exc_info=True)
                    continue

            # this closes the player's shot information chart
            player_name_col_button = self.web_driver.webDriverWait(row,
                                                                   EC.element_to_be_clickable(
                                                                       (By.CLASS_NAME, 'player-name-col')),
                                                                   'Error getting player column to click\n{}')
            if player_name_col_button is None:
                continue
            player_name_col_button.click()

        # after iterating over all rows, check to see how well it scraped
        if len(self._player_round_dict) == len(self._player_meta_dict):
            self.successfully_scraped = True
            self._logger.info('Successfully scraped data for all players in tournament {} {}'.format(self.pga_year,
                                                                                                     self.pga_tournament))
        elif len(self._player_round_dict) == 0:
            self.successfully_scraped = False
            self._logger.info(
                'Unsuccessfully scraped data for tournament {} {}'.format(self.pga_year, self.pga_tournament))
            return False
        elif len(self._player_round_dict) < len(self._player_meta_dict):
            self.successfully_scraped = False
            self._logger.info('Only scraped data for {:.2f}% of players in tournament {} {}'.
                              format((len(self._player_round_dict) / len(self._player_meta_dict)) * 100,
                                     self.pga_year,
                                     self.pga_tournament))
        return True

    def __convertPlayerRoundToMongoDBCollection(self):
        player_round_collection = []
        for player_id, round_num in self._player_round_dict.items():
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
        for player_id, meta_values in self._player_meta_dict.items():
            player_meta = {'playerID': player_id}
            player_meta.update(meta_values)
            player_meta_collection.append(player_meta)
        return player_meta_collection

    def __convertCourseMetaToMongoDBCollection(self):
        course_meta_collection = []
        for course_id, course_details in self._course_meta_dict.items():
            course_meta = {'courseID': course_id}
            course_meta.update(course_details)
            hole_level_list = []
            for hole_key, round_info in course_meta['holes'].items():
                hole_level = {'holeNumber': hole_key}
                hole_level.update(round_info)
                hole_level_list.append(hole_level)
            course_meta['holes'] = hole_level_list
            course_meta_collection.append(course_meta)
        return course_meta_collection

    def convertDictsToMongoDBCollection(self):
        """General method for converting all class dictionaries to Mongo DB Collections"""
        mongoDB_collections = [self.__convertPlayerRoundToMongoDBCollection(),
                               self.__convertPlayerMetaToMongoDBCollection(),
                               self.__convertCourseMetaToMongoDBCollection(), self._tournament_info_dict]
        return mongoDB_collections

    def uploadDictsToJSON(self):
        """Upload the dictionaries to json files for debugging purposes"""
        with open(self.dir + 'player_round.json', 'w') as f:
            json.dump(self._player_round_dict, f)
        with open(self.dir + 'player_meta.json', 'w') as f:
            json.dump(self._player_meta_dict, f)
        with open(self.dir + 'tournament_info.json', 'w') as f:
            json.dump(self._tournament_info_dict, f)
        with open(self.dir + 'course_meta.json', 'w') as f:
            json.dump(self._course_meta_dict, f)

    def downloadDictsFromJSON(self):
        """Download the JSON files to dictionaries for debugging purposes"""
        with open(self.dir + 'player_round.json', 'r') as f:
            self._player_round_dict = json.load(f)
        with open(self.dir + 'player_meta.json', 'r') as f:
            self._player_meta_dict = json.load(f)
        with open(self.dir + 'tournament_info.json', 'r') as f:
            self._tournament_info_dict = json.load(f)
        with open(self.dir + 'course_meta.json', 'r') as f:
            self._course_meta_dict = json.load(f)