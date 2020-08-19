import pandas as pd
import os
import subprocess
import time
import argparse

import selenium.webdriver as webdriver
import queue
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
                    default="./data",
                    help="the path where scraped videos are to be stored.")

parser.add_argument('--data_path',
                    type=str,
                    default='./data/bbc',
                    help="the path where transcript links are stored")


def clickThroughToNewPage(browser, link_xpath, time_wait=10, time_wait_stale=5, additional=None):
    try:
        WebDriverWait(browser, time_wait).until(EC.presence_of_element_located((By.XPATH, link_xpath)))
    except TimeoutException:
        raise NoSuchElementException("Could not find the next page button.")
    try:
        WebDriverWait(browser, time_wait).until(EC.visibility_of_element_located((By.XPATH, link_xpath)))
    except TimeoutException:
        raise ElementNotVisibleException("The next page button is not visible.")

    try:
        WebDriverWait(browser, time_wait).until(EC.element_to_be_clickable((By.XPATH, link_xpath)))
    except TimeoutException:
        raise ElementNotVisibleException("The next page button is not clickable.")

    link = browser.find_element_by_xpath(link_xpath)
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


def login(browser):
    url = "https://learningonscreen.ac.uk/ondemand/"
    browser.get(url)

    try:
        clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']")
    except Exception as e:
        print(str(e))
        browser.quit()
        print(">>> Did not find the next page button. Failed to Log In.")

    try:
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
        browser.find_element_by_id('institution-field').send_keys('University of Warwick')
        browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
        browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
    except NoSuchElementException:
        browser.quit()
        return

    try:
        browser.find_element_by_id('userName').send_keys('u1664202')
        browser.find_element_by_id('password').send_keys('mk011211')
        browser.find_element_by_xpath("//button[@id='signinbutton']").click()
    except NoSuchElementException:
        pass


class VideoInfo:
    def __init__(self, video_link, bcast_link, bbc_id, year, source_name, program, date, reason):
        self.video_link = video_link
        self.bcast_link = bcast_link
        self.bbc_id = bbc_id
        self.year = year
        self.source_name = source_name
        self.program = program
        self.date = date
        self.reason = reason

    def is_ready_to_download(self):
        pass

    def __str__(self):
        return '{} {} {} {} {}'.format(str(self.bbc_id), str(self.year), self.source_name, self.program, self.date)


class VideoScraper:
    def __init__(self, save_path, data_path):
        self.save_path = save_path
        self.data_path = data_path

        self.to_be_requested = queue.Queue()
        self.requested = queue.Queue()
        self.last_request_made = None
        self.request_video_every = 6*60*60  # interval between requesting video 6hrs
        self.n_videos_every_request = 5  # 5 videos can be request within 6hrs

        self.PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        chrome_options = Options()
        chrome_options.add_argument("user-data-dir=" + self.PROJECT_ROOT + "/Profile 17")
        # chrome_options.add_argument('--headless')
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
            self.clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']")
        except Exception as e:
            print(str(e))
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

    def request_videos(self):
        if self.last_request_made is None:
            self.last_request_made = time.time()
        else:
            diff = time.time() - self.last_request_made
            if diff <= 6*60*60:
                print('can only make 5 video request every 6 hour')
                return False

        for i in range(self.n_videos_every_request):
            video = self.to_be_requested.get()
            print('requesting {}'.format(video))
            link = video.bcast_link
            request_link = '{}&request=1'.format(link)
            self.browser.get(request_link)

    def download_videos(self):
        wait_time = time.time() - self.last_request_made
        while wait_time < self.request_video_every:
            if self.requested.empty():
                break
            video = self.requested.get()

            print('getting video from {}'.format(video.video_link))
            print('program link for video {}'.format(video.bcast_link))
            print('reason for unavailability {}'.format(video.reason))
            output_option = '-o'
            video_link = video.video_link

            output_name = '{}/videos/{}/{}/{}-{}-{}.mp4'.format(self.save_path, video.bbc_id, video.year,
                                                                video.source_name, video.program, video.date)

            cmd = ['youtube-dl', output_option, output_name, video_link]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            o, e = proc.communicate()
            if e is not None:
                print('error downloading the video')
                self.requested.put(video)
            else:
                time.sleep(10*60)
            wait_time = time.time() - self.last_request_made

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

                video = VideoInfo(video_link, unavailable_link, bbc_id, year, source_name, program, date, reason)

                if 'to be requested' in reason:
                    self.to_be_requested.put(video)
                else:
                    self.requested.put(video)

        while not self.to_be_requested.empty():
            # first request 5 videos and for the next 6 hours keep trying to download videos since we can only make
            # 5 video request every 6 hour. If there are no more videos to be downloaded, wait before the 6 hour wait
            # is over before making requests.
            self.request_videos()
            self.download_videos()  # download videos for next 6 hours
            # wait to make another request
            diff = self.request_video_every - time.time() + self.last_request_made
            if diff > 0:
                time.sleep(diff)


if __name__ == '__main__':
    args = parser.parse_args()
    y = args.year
    _id = args.id
    output_path = args.output_path
    path = args.data_path
    scraper = VideoScraper(output_path, path)
    scraper.scrap_video(_id, y)
