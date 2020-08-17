import pandas as pd
import os
import subprocess
import time
import argparse

import selenium.webdriver as webdriver

import request_video
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, ElementNotVisibleException, StaleElementReferenceException, \
    NoSuchElementException
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException
from selenium.webdriver.chrome.options import Options

# create necessary arguments to run the analysis
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--id',
                    type=str,
                    required=True,
                    help='channel id for which video is to be scraped!!')

parser.add_argument('-y', '--year',
                    choices=[2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
                             2015, 2016, 2017, 2018],
                    type=int,
                    help='The years for which analysis is to be performed.')

parser.add_argument('--output_path',
                    type=str,
                    default="/media/rohit/2TB WD/videos",
                    help="the path where scraped videos are to be stored.")

parser.add_argument('--data_path',
                    type=str,
                    default='./data',
                    help="the path where transcript links are stored")


class VideoScraper:
    def __init__(self, save_path, data_path):
        self.save_path = save_path
        self.data_path = data_path

        self.PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        chrome_options = Options()
        chrome_options.add_argument("user-data-dir=" + self.PROJECT_ROOT + "/Profile 1")
        chrome_options.add_argument('--headless')
        DRIVER_BIN = os.path.join(self.PROJECT_ROOT, "chromedriver")
        self.browser = webdriver.Chrome(executable_path=DRIVER_BIN, chrome_options=chrome_options)
        self.login()

    def clickThroughToNewPage(self, link_xpath, time_wait=10, time_wait_stale=5, additional=None):

        try:
            WebDriverWait(self.browser, time_wait).until(EC.presence_of_element_located((By.XPATH, link_xpath)))
        except TimeoutException:
            raise NoSuchElementException("Could not find the next page button.")
        try:
            WebDriverWait(self.browser, time_wait).until(EC.visibility_of_element_located((By.XPATH, link_xpath)))
        except TimeoutException:
            raise ElementNotVisibleException("The next page button is not visible.")

        try:
            WebDriverWait(self.browser, time_wait).until(EC.element_to_be_clickable((By.XPATH, link_xpath)))
        except TimeoutException:
            raise ElementNotVisibleException("The next page button is not clickable.")

        link = self.browser.find_element_by_xpath(link_xpath)
        link.click()

        def waitFor(condition_function):
            start_time = time.time()
            while time.time() < start_time + time_wait_stale:
                if condition_function():
                    return True
                else:
                    time.sleep(0.1)

            return False

        def linkHasGoneStale():
            try:
                # poll the link with an arbitrary call
                link.find_elements_by_tag_name('h2')
                return False
            except StaleElementReferenceException:
                return True

        value = waitFor(linkHasGoneStale)

    def login(self):
        url = "https://learningonscreen.ac.uk/ondemand/"
        self.browser.get(url)

        try:
            self.clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", self.browser)
        except Exception:
            self.browser.quit()
            print(">>> Did not find the next page button. Failed to Log In.")

        try:
            WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
            self.browser.find_element_by_id('institution-field').send_keys('University of Warwick')
            self.browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
            self.browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
        except NoSuchElementException:
            self.browser.quit()
            return

        try:
            self.browser.find_element_by_id('userName').send_keys('u1664202')
            self.browser.find_element_by_id('password').send_keys('mk011211')
            self.browser.find_element_by_xpath("//button[@id='signinbutton']").click()
        except NoSuchElementException:
            pass

        time.sleep(99999999)

    def scrap_video(self, bbc_id, year):
        path = os.path.join(self.data_path, '{}/{}/no_transcripts'.format(bbc_id, year))
        sources = os.listdir(path)

        for source in sources:
            source_path = os.path.join(path, source)
            source_df = pd.read_csv(source_path, index_col=0)

            for index, row in source_df.iterrows():
                source_name = row['Source']
                program = row['Program Name']
                date = row['Date']
                unavailable_link = row['Unavailable link']
                reason = row['Unavailable reason']

                output_option = '-o'
                output_name = '{}/videos/{}/{}/{}-{}-{}.mp4'.format(output_path, bbc_id, year, source_name, program,
                                                                    date)
                video_link = row['video_link']

                if os.path.isfile(output_name):
                    print('video from {} already downloaded'.format(video_link))
                    continue

                print('getting video from {}'.format(video_link))
                cmd = ['youtube-dl', output_option, output_name, video_link]
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                o, e = proc.communicate()

                if e is not None:
                    print('reason: ', reason)
                    print('Error: ' + e.decode('ascii'))
                    if 'to be requested' in reason:
                        pass

                # delay between videos
                time.sleep(10 * 60)


if __name__ == '__main__':
    args = parser.parse_args()
    year = args.year
    bbc_id = args.id
    output_path = args.output_path
    data_path = args.data_path
    scraper = VideoScraper(output_path, data_path)
    scraper.scrap_video(bbc_id, year)
