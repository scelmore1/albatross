import json
import logging
import re

from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from Logging.MyLogger import MyLogger


def getProxies():
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get("https://free-proxy-list.net/")

    PROXIES = []
    table_len = Select(driver.find_element_by_name('proxylisttable_length'))
    table_len.select_by_value('80')
    proxies = driver.find_elements_by_css_selector("tr[role='row']")
    for p in proxies:
        result = p.text.split(" ")

        if result[-4] == "yes":
            PROXIES.append(result[0] + ":" + result[1])

    driver.close()
    return PROXIES


def proxyDriver(proxies):
    if len(proxies) < 1:
        print("--- Proxies used up (%s)" % len(proxies))
        proxies = getProxies()

    pxy = proxies[-1]
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--proxy-server=https://{}'.format(pxy))
    return chrome_options


class WebDriver:
    """Initialize a Selenium Web Driver and make all calls via this class"""
    proxy_list = getProxies()

    def __init__(self, called_from_logger, wait_time=10, wire_time=3):
        """Initialize new web driver using selenium"""
        self._wait_time = wait_time
        self._wire_time = wire_time
        self._class_logger = called_from_logger
        self._selenium_logger = MyLogger('selenium.webdriver.remote.remote_connection', None,
                                         logging.INFO).getLogger()
        self._class_logger.info('Initializing New Driver...')
        chrome_options = proxyDriver(self.proxy_list)
        prefs = {"profile.default_content_setting_values.notifications": 2,
                 "profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        self._driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)
        self._driver.create_options()

    def updateLogLocations(self, tournament_name, file_handler):
        # selenium log kept separate from class log
        self._selenium_logger = MyLogger('selenium.webdriver.remote.remote_connection', file_handler,
                                         logging.WARNING).getLogger()
        self._class_logger = MyLogger(self.__class__.__name__ + tournament_name, file_handler, logging.INFO,
                                      'a').getLogger()

    def goToURL(self, url_string):
        """Pass url for driver to get"""
        try:
            self._driver.get(url_string)
            # self._driver.minimize_window()
        except Exception as e:
            self._class_logger.error('Error loading url {}\n{}'.format(url_string, e))

    def wireRequestToJSON(self, request_str, timeout=None):
        """Take request string and return json object from html wire"""
        try:
            if timeout is None:
                request = self._driver.wait_for_request(request_str, timeout=self._wire_time)
            else:
                request = self._driver.wait_for_request(request_str, timeout=timeout)
            return json.loads(request.response.body.decode('utf-8'))
        except Exception as e:
            self._class_logger.error('Error making request {}\n{}'.format(request_str, e))
            return ''

    def findElementByXPath(self, xpath, meta=False):
        try:
            if meta:
                return self._driver.find_element_by_xpath(xpath).get_attribute('content')
            else:
                return self._driver.find_element_by_xpath(xpath).text
        except Exception as e:
            self._class_logger.error('Error pulling text from xPath {}\n{}'.format(xpath, e))
            return ''

    def webDriverWait(self, element, EC_method, error_message):
        try:
            return WebDriverWait(element, self._wait_time).until(EC_method)
        except Exception as e:
            self._class_logger.error(error_message.format(e), exc_info=True)
            return None

    def getDriver(self):
        """Return driver object"""
        return self._driver

    def closeDriver(self):
        """Close driver"""
        self._driver.close()


class wait_for_text_to_match(object):
    def __init__(self, locator, pattern):
        self.locator = locator
        self.pattern = re.compile(pattern)

    def __call__(self, driver):
        try:
            element_text = EC._find_element(driver, self.locator).text
            return self.pattern.search(element_text)
        except StaleElementReferenceException:
            return False