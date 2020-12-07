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

    def __init__(self, pga_tournament, pga_year, driver=None):
        """Initialize scraper with tournament, year, optional logger name, wire requests dict, web driver"""
        self.pga_tournament = pga_tournament
        self.pga_year = pga_year
        self.tournament_url = 'https://www.pgatour.com/competition/' + pga_year + '/' + pga_tournament + \
                              '/leaderboard.html'
        self.successfully_scraped = 0
        self._tournament_id = None
        self._run_thrus = 0

        # create place holder dictionaries for data once scraped
        self._course_ids = set()
        self._tournament_info_dict = {}
        self._player_meta_dict = {}
        self._course_general_dict = {}
        self._course_meta_dict = {}
        self._player_round_dict = {}
        self._unsuccessful_player_scrape = {}
        self._course_requests = {}

        # use this default dictionary as template for wire requests
        self.template_wire_html_dict = {
            'tournament_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/leaderboard.json',
            'course_general': 'https://statdata.pgatour.com/r/TOURNAMENT_ID/course.json',
            'course_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/courseC_ID',
            'round_detail': 'https://lbdata.pgatour.com/PGA_YEAR/r/TOURNAMENT_ID/drawer/rROUND_NUM-mMAIN_PLAYER_ID'
        }

        # all I/O done in tournaments/'pga_year'_'tournament_name' directory
        self.dir = 'tournaments/' + self.pga_year + '_' + self.pga_tournament + '/'
        if not os.path.exists(self.dir):
            os.makedirs(os.path.dirname(self.dir + 'logs/tournament_scape.log'), exist_ok=True)
        self._file_handler = self.dir + 'logs/tournament_scape.log'

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
                + '\nScrape Status: Scraped {:.2f}% of potential data'.format(self.successfully_scraped))

    def _scrapeTournamentJSON(self, tournament_detail_json):
        """Insert into dictionaries from the detailed tournament info JSON"""

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
            'multiCourse': findKeyInJSON(tournament_detail_json, 'multiCourse'),
            'totalRounds': findKeyInJSON(tournament_detail_json, 'totalRounds'),
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

    def _scrapeCourseGeneral(self, course_general_json):
        """Insert into dictionaries from the general course information JSON"""

        for course_desc in course_general_json['courses']:
            course_id = findKeyInJSON(course_desc, 'number')
            self._course_general_dict[course_id] = {
                'description': findKeyInJSON(course_desc, 'body'),
                'name': findKeyInJSON(course_desc, 'name'),
                'totalYards': findKeyInJSON(course_desc, 'yards')
            }

    def _scrapePlayerDetail(self, player_key, round_detail_json):
        """Insert into dictionaries the data from the player round detail JSON"""
        main_player_id = player_key.split()[0]
        round_num = player_key.split()[1]

        """Scrape data from the player round specific JSON"""
        if main_player_id in self._player_round_dict and round_num in self._player_round_dict[main_player_id]:
            self._logger.info(
                'Previously downloaded JSON for round {} from player ID {}'.format(round_num, main_player_id))
            return

        self._logger.info('Downloading JSON from round {} for player ID {}'.format(round_num, main_player_id))
        course_id = findKeyInJSON(round_detail_json, 'courseId')
        # only add if course hasn't been added to course ids yet
        if course_id not in self._course_ids:
            # add course to wire requests
            self._course_requests[course_id] = self.template_wire_html_dict['course_detail'] \
                .replace('PGA_YEAR', self.pga_year) \
                .replace('TOURNAMENT_ID', self._tournament_id) \
                .replace('C_ID', course_id)
            self._course_ids.add(course_id)

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
        self._unsuccessful_player_scrape.pop(player_key, None)

    def _scrapeCourseDetail(self, c_id, course_detail_json):
        """Insert into dictionaries from the course detail JSON"""
        self._logger.info('Downloading JSON for course {}'.format(c_id))

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

    def _getTournamentJSON(self, req_str):
        """Get tournament details from the JSON request string, rerun scrape if this isn't working"""
        tournament_detail_json = self.web_driver.wireRequestToJSON(req_str)
        if tournament_detail_json:
            self._scrapeTournamentJSON(tournament_detail_json)
            return True
        else:
            if self._run_thrus < 3:
                self._logger.warn('Failed getting tournament details, will retry this scrape again.')
                self.runScrape()
            return False

    def _getCourseGeneralJSON(self, req_str):
        """Get course general details from the JSON request string"""
        course_general_json = self.web_driver.wireRequestToJSON(req_str)
        if course_general_json:
            self._scrapeCourseGeneral(course_general_json)

    def _getPlayerLevelJSON(self, player_key, req_str):
        """Get player level details from the JSON request string"""
        round_detail_json = self.web_driver.wireRequestToJSON(req_str)
        if round_detail_json:
            self._scrapePlayerDetail(player_key, round_detail_json)
            return True
        else:
            return False

    def _getCourseDetailJSON(self):
        """Get course details from the JSON request string"""
        for c_id, req_str in self._course_requests.items():
            course_detail_json = self.web_driver.wireRequestToJSON(req_str)
            if course_detail_json:
                self._scrapeCourseDetail(c_id, course_detail_json)

    def _getTournamentID(self):
        """Get tournament ID from Xpath"""
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
        return True

    def _scrapeThroughPlayerRow(self, row):
        """Each player row will need to be clicked and then each round will need to show play by play data"""

        # get player's shot information chart open on url
        _ = row.location_once_scrolled_into_view
        main_player_id = re.findall(r'\d+', row.get_attribute('class'))[0]
        player_name_col_button = self.web_driver.webDriverWait(row,
                                                               EC.element_to_be_clickable(
                                                                   (By.CLASS_NAME, 'player-name-col')),
                                                               'Error getting player column to click\n{}')
        if player_name_col_button is None:
            return None
        _ = player_name_col_button.location_once_scrolled_into_view
        player_name_col_button.click()

        # get round by round data by clicking player round buttons
        round_selector = self.web_driver.webDriverWait(row.parent,
                                                       EC.visibility_of_element_located(
                                                           (By.CLASS_NAME, 'round-selector')),
                                                       'Error getting round selector\n{}')
        if round_selector is None:
            return None
        round_buttons = round_selector.find_elements_by_class_name('round')

        # go round by round to scrape data
        player_requests = {}
        for round_button in round_buttons:
            round_num = round_button.text
            if main_player_id in self._player_round_dict and round_num in self._player_round_dict[main_player_id]:
                self._logger.info(
                    'Previously scraped data for round {} from player ID {}'.format(round_num, main_player_id))
                continue

            self._logger.info('Getting JSON wire for round {} from player ID {}'.format(round_num, main_player_id))
            round_button.click()
            self.web_driver.getDriver().implicitly_wait(.1)
            player_key = main_player_id + ' ' + round_num

            player_requests[player_key] = self.template_wire_html_dict[
                'round_detail'] \
                .replace('PGA_YEAR', self.pga_year) \
                .replace('TOURNAMENT_ID', self._tournament_id) \
                .replace('ROUND_NUM', round_num) \
                .replace('MAIN_PLAYER_ID', main_player_id)

        # this closes the player's shot information chart
        player_name_col_button.click()
        return player_requests

    def _checkScrapeResults(self):
        """After getting all JSON and converting to dictionaries, check to see how we did"""
        if len(self._player_round_dict) == len(self._player_meta_dict):
            self.successfully_scraped = 100
            self._logger.info('Successfully scraped data for all players in tournament {} {}'.format(self.pga_year,
                                                                                                     self.pga_tournament))
        elif len(self._player_round_dict) == 0:
            self._logger.info(
                'Unsuccessfully scraped data for tournament {} {}'.format(self.pga_year, self.pga_tournament))
        elif len(self._player_round_dict) < len(self._player_meta_dict):
            self.successfully_scraped = (len(self._player_round_dict) / len(self._player_meta_dict)) * 100
            self._logger.info('Only scraped data for {:.2f}% of players in tournament {} {}'.
                              format((len(self._player_round_dict) / len(self._player_meta_dict)) * 100,
                                     self.pga_year,
                                     self.pga_tournament))
            self._logger.info(
                'Player rows unsuccessfully scraped are:\n{}'.format(self._unsuccessful_player_scrape.keys()))

    def runScrape(self):
        """Main function for running the scrape, get all necessary info from the page, iterate through
        players shot charts, try to scrape as much as possible from the JSON requests."""
        self._logger.info(
            '\nRunning Scrape for {} {}\nURL is {}\n'.format(self.pga_year, self.pga_tournament, self.tournament_url))
        self._run_thrus += 1
        self.web_driver.goToURL(self.tournament_url)
        if self._getTournamentID():
            # request string for tournament detail
            tournament_req_str = self.template_wire_html_dict['tournament_detail'].replace(
                'PGA_YEAR', self.pga_year).replace('TOURNAMENT_ID', self._tournament_id)
            # scrape JSON of tournament detail
            if not self._getTournamentJSON(tournament_req_str):
                return False

            # request string for course general info
            course_gen_req_str = self.template_wire_html_dict['course_general'].replace(
                'TOURNAMENT_ID', self._tournament_id)
            # scrape JSON of course general
            self._getCourseGeneralJSON(course_gen_req_str)

            row_lines = self.web_driver.webDriverWait(self.web_driver.getDriver(),
                                                      EC.visibility_of_all_elements_located(
                                                          (By.CSS_SELECTOR, 'tr.line-row.line-row')),
                                                      'Error locating player elements on page\n{}')
            if row_lines:
                unsuccessful_rows = set()
                successive_failures = 0
                # split up player JSON requests because some data overlaps in the play by play JSON
                for i in range(3):
                    # run first time through and keep track of unsuccessful scrapes
                    for row_num, row in enumerate(row_lines[i::3]):
                        row_num = i + (row_num * 3)
                        player_requests = self._scrapeThroughPlayerRow(row)
                        if player_requests is not None:
                            result = 'Successfully'
                            for player_key, req_str in player_requests.items():
                                if not self._getPlayerLevelJSON(player_key, req_str):
                                    self._unsuccessful_player_scrape[player_key] = req_str
                                    self._logger.warning(
                                        'Unsuccessfully retrieved JSON for row number {}, player ID {} -- round '
                                        'number {}. Will retry all rounds later.\n'
                                            .format(row_num, player_key.split()[0], player_key.split()[1]))
                                    unsuccessful_rows.add(row_num)
                                    result = 'Unsuccessfully'
                                    successive_failures += 1
                                    break
                                else:
                                    successive_failures = 0
                        else:
                            unsuccessful_rows.add(row_num)
                            successive_failures += 1
                            result = 'Unsuccessfully'

                        self._logger.info('{} iterated over row {} the first time'.format(result, str(row_num)))
                        # Something's wrong
                        if successive_failures > 5:
                            self._logger.warn(
                                'Had 5 successive failures while getting player round JSON, exiting scrape')
                            return False

                # can get course detail data once all players have been added with the courses they played
                self._getCourseDetailJSON()

                # run through a second time with all the rows that were unsuccessful at first
                for row_num in unsuccessful_rows:
                    player_requests = self._scrapeThroughPlayerRow(row_lines[row_num])
                    if player_requests is not None:
                        result = 'Successfully'
                        for player_key, req_str in player_requests.items():
                            if not self._getPlayerLevelJSON(player_key, req_str):
                                self._unsuccessful_player_scrape[player_key] = req_str
                                self._logger.warning(
                                    'Unsuccessfully retrieved JSON for row number {}, player ID {} -- round number {'
                                    '}.\n'
                                        .format(row_num, player_key.split()[0], player_key.split()[1]))
                                result = 'Unsuccessfully'
                    else:
                        result = 'Unsuccessfully'
                    self._logger.info('{} iterated over row {} the second time'.format(result, str(row_num)))

                self._checkScrapeResults()
                return True
        return False

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
        """General method for converting all class dictionaries to MongoDB Collections"""
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