 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  1 14:05:26 2018

"""

import selenium.webdriver as webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import Select
import time
import pandas as pd
import multiprocessing
import queue
import threading
import datetime
from bidict import bidict
from datetime import timedelta, date
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, ElementNotVisibleException
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException
from selenium.webdriver.chrome.options import Options
import os
from collections import OrderedDict
import pickle
import sys
import glob, shutil
from urllib3.exceptions import MaxRetryError
from preprocessing import getFiles
from db.update_table import updateRequestedFix
from db.select_from_table import selectUnavailableLinks


global info_dict
info_dict = {}

global used_profiles
used_profiles = []

class myThread (threading.Thread):
    def __init__(self, threadID, function, workQueue, queueLocks, exitFlag, params, data):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.workQueue = workQueue
        self.function = function
        self.exitFlag = exitFlag
        self.queueLocks = queueLocks
        self.params = params
        self.data = data
        
    def run(self):
        self.function(self.threadID, self.exitFlag, self.queueLocks, self.workQueue, self.params, self.data)
            


class processParallel():

    def __init__(self, no_threads, workQueue, function, params, data):
        self.exitFlag = [0]
        self.function = function
        self.workQueue = workQueue
        self.threads = []
        queueLock1 = threading.Lock()
        queueLock2 = threading.Lock()
        queueLock3 = threading.Lock()
        self.locks_queue = [queueLock1, queueLock2, queueLock3]
        
        num_cores = multiprocessing.cpu_count()
        if(no_threads == 0 or no_threads > num_cores):
            no_threads = num_cores
        
        self.run_event = threading.Event()
        self.run_event.set()
        
        threadID = 1
        for i in range(no_threads):
            self.threads.append(myThread(threadID, self.function, self.workQueue, self.locks_queue, self.exitFlag, params, data))
            threadID += 1
            global used_profiles
            used_profiles.append(threadID)

    def process(self):
        for thread in self.threads:
            thread.start()

        for thread in self.threads:
            thread.join()
    
    def join(self):
        self.run_event.clear()
        for thread in self.threads:
            thread.join()

#6742 - NYT
#400546 - TVEyes London

class Scraper():
    
    def __init__(self, webpage_function, login_function = None, no_cores=12, headless_chrome=False, verbose = 2, use_profiles = False, ocbrowser = True, extract_first_paragraph=False):
        self.webpage_function = webpage_function
        self.login_function = login_function
        self.column_names = []
        self.data = []
        self.extract_first_paragraph = extract_first_paragraph
        self.no_cores = no_cores
        self.headless_chrome = headless_chrome
        self.verbose = verbose
        self.PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        self.use_profiles = use_profiles
        self.processed_empty_months_list = []
        self.ocbrowser = ocbrowser
         
    @staticmethod
    def convertDate(date):
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                              '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                              '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)
        
        date = date.split("-")
        if(date[0][0]=='0'):
            day = date[0][1]
        else:
            day = date[0]
        
        month = months.inv[date[1]]
        
        year = date[2]    
        return [day, month, year]
    
    @staticmethod
    def getDays(start_date, end_date):
    
        def dateRange(date1, date2):
            for n in range(int ((date2 - date1).days)+1):
                yield date1 + timedelta(n)
    
        start_date_conv = Scraper.convertDate(start_date)
        end_date_conv = Scraper.convertDate(end_date)
                    
        try:
            datetime.datetime(year=int(start_date_conv[2]),month=int(start_date_conv[1]),day=int(start_date_conv[0]))
            datetime.datetime(year=int(end_date_conv[2]),month=int(end_date_conv[1]),day=int(end_date_conv[0]))
        except ValueError:
            raise ValueError("Incorrect date given")
        
        
        start_dt = date(int(start_date_conv[2]), int(start_date_conv[1]), int(start_date_conv[0]))
        end_dt = date(int(end_date_conv[2]), int(end_date_conv[1]), int(end_date_conv[0]))
        
        days_list = []
        
        for dt in dateRange(start_dt, end_dt):
            days_list.append(dt.strftime("%d-%b-%Y").lower())
        
        return days_list



    @staticmethod
    def clickThroughToNewPage(link_xpath, browser, time_wait = 10, time_wait_stale = 5, additional = None):

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




    @staticmethod
    def clickButton(link_xpath, browser, time_wait = 10):
        
        try:
            WebDriverWait(browser, time_wait).until(EC.presence_of_element_located((By.XPATH, link_xpath)))
        except TimeoutException:
            raise NoSuchElementException("Could not find the button.")
        try:    
            WebDriverWait(browser, time_wait).until(EC.visibility_of_element_located((By.XPATH, link_xpath)))
        except TimeoutException:
            raise ElementNotVisibleException("The button is not visible.")

        try:    
            WebDriverWait(browser, time_wait).until(EC.element_to_be_clickable((By.XPATH, link_xpath)))
        except TimeoutException:
            raise ElementNotVisibleException("The button is not clickable.")
            
        link = browser.find_element_by_xpath(link_xpath) 
        link.click()


    @staticmethod
    def waitForStaleLink(browser, time_wait = 10):
        
        def waitFor(condition_function):
            start_time = time.time() 
            while time.time() < start_time + time_wait: 
                if condition_function(): 
                    return True 
                else: 
                    time.sleep(0.1) 
                
        def linkHasGoneStale():
            try:
                # poll the link with an arbitrary call
                browser.find_elements_by_tag_name('h2')
                return False
            except StaleElementReferenceException:
                return True

        waitFor(linkHasGoneStale)
        
    @staticmethod    
    def ensureDir(file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            
    

    def login(self):

        DRIVER_BIN = os.path.join(self.PROJECT_ROOT, "chromedriver")
        chrome_options = Options()  
        chrome_options.add_argument("user-data-dir=" + self.PROJECT_ROOT + "/Profile 1")

        if(self.headless_chrome == True):
            chrome_options.add_argument("--headless") 

        browser = webdriver.Chrome(executable_path=DRIVER_BIN,   chrome_options=chrome_options)
        
        try:
            self.login_function(browser, self.clickThroughToNewPage)
            browser.quit()
        except TypeError:
            pass
            browser.quit()



    def scrape(self, search_term, start_date, end_date, source, save_and_empty_data = False):

        days_list = Scraper.getDays(start_date, end_date)
    
        params = [search_term, start_date, end_date, source]
        
        days_queue = queue.Queue()
        days_queue.queue = queue.deque(days_list)
        
        
        
        

        def getDay(threadID, exitFlag, locks_queue, days_queue, params, data):
            
            
            DRIVER_BIN = os.path.join(self.PROJECT_ROOT, "chromedriver")
            chrome_options = Options() 

            if(self.use_profiles == True):
                chrome_options.add_argument("user-data-dir=" + self.PROJECT_ROOT + "/Profile " + str(threadID))
                    
            if(self.headless_chrome == True):
                chrome_options.add_argument("--headless") 
            
            if(self.ocbrowser == False):
                browser = webdriver.Chrome(executable_path=DRIVER_BIN,   options=chrome_options)
            
            while not exitFlag[0]:
                locks_queue[0].acquire()
                print('lock accquired')
                if days_queue.empty():
                    exitFlag[0] = 1
                
                
                if not days_queue.empty():
                    day = days_queue.get()

                    locks_queue[0].release()
                    
                    if(self.verbose >= 2):
                        locks_queue[2].acquire()
                        print("Thread %s processing %s" % (threading.current_thread(), day), flush=True)
                        locks_queue[2].release()



                    try:
                        if(self.ocbrowser == True):
                            global info_dict
                            try:
                                info_dict[day] += 1
                            except:
                                info_dict[day] = 0
                                
                            #### Sometimes empty transcript fields occur for some programs. 
                            #### One one profile you may infinitely (or at least many times) refresh the page and it will not appear but on other it appears right away. Magic. 
                            #### We randomly choose profile here so different threads may choose the same profile resulting in WebDriverException.
                            #### If that happes we just rerun the while loop.
                            if(info_dict[day] >= 2):
                                import numpy as np
                                new_threadID = np.random.randint(low=12, high=20)
                                chrome_options.add_argument("user-data-dir=" + self.PROJECT_ROOT + "/Profile " + str(new_threadID))
                                
                            browser = webdriver.Chrome(executable_path=DRIVER_BIN,   options=chrome_options)
                    except (SessionNotCreatedException, WebDriverException) as e:
                        locks_queue[0].acquire()
                        days_queue.put(day)
                        exitFlag[0] = 0
                        locks_queue[0].release()
                        locks_queue[2].acquire()
                        if(self.verbose >= 2):
                            print("\n", flush=True)
                            print(">>> Session could not be created. " + day + " will be reacquired", flush=True)
                            print(e.__class__.__name__)
                            print(str(e))
                            print("\n", flush=True)
                        if(self.verbose >=1):
                            print("Thread %s finished processing %s" % (threading.current_thread(), day), flush=True)
                        locks_queue[2].release()
                        browser.quit()
                        continue
                    try:
                        print('calling webpage function')
                        self.webpage_function(day, days_queue, data, params, locks_queue, browser, self.clickThroughToNewPage, self.convertDate, self.verbose, self.extract_first_paragraph, self.column_names, exitFlag, self.ocbrowser, self.saveAndEmptyData)
                        print('get webpage function')
                    except (TimeoutException, ConnectionRefusedError) as e:
                        locks_queue[0].acquire()
                        days_queue.put(day)
                        exitFlag[0] = 0
                        locks_queue[0].release()
                        if(self.verbose >= 2):
                            if(str(e.__class__.__name__) == "TimeoutException"):
                                locks_queue[2].acquire()
                                print("\n", flush=True)
                                print(">>> Timed out. " + day + " will be reacquired", flush=True)
                                print(e.__class__.__name__)
                                print(str(e))
                                print("\n", flush=True)
                                locks_queue[2].release()
                            else:
                                locks_queue[2].acquire()
                                print("\n", flush=True)
                                print(">>> Connection refused. " + day + " will be reacquired", flush=True)
                                print(e.__class__.__name__)
                                print(str(e))
                                print("\n", flush=True)
                                locks_queue[2].release()   
                    except Exception as e:
                        locks_queue[0].acquire()
                        days_queue.put(day)
                        exitFlag[0] = 0
                        locks_queue[0].release()
                        if(self.verbose >= 2):
                            locks_queue[2].acquire()
                            print("\n", flush=True)
                            print(">>> Some other exception occured. " + day + " will be reacquired", flush=True)
                            print(e.__class__.__name__)
                            print(str(e))
                            print("\n", flush=True)
                            locks_queue[2].release()   
                    finally:
                        browser.quit()

                            
                        
                    if(self.ocbrowser == True):    
                        browser.quit()
                    
                    if(self.verbose >=1):
                        locks_queue[2].acquire()
                        print("Thread %s finished processing %s" % (threading.current_thread(), day), flush=True)
                        locks_queue[2].release()
                    
                    locks_queue[0].acquire()                   
                    if(days_queue.empty()):
                        exitFlag[0] = 1
                        
                    locks_queue[0].release()
                    
                    
                else:
                    locks_queue[0].release()
            
                time.sleep(1)
    
            if(self.ocbrowser == False):    
                browser.quit()        
        
        startTime = time.time()
        self.parallel_processor = processParallel(self.no_cores, days_queue, getDay, params, self.data)
        self.parallel_processor.process()
        duration = time.time() - startTime
        print('get days done')
        # This function has an upper limit on max secounds of sth like 30 days
        def convertTime(seconds):
            d = datetime.datetime(1,1,1) + timedelta(seconds = seconds)
            return [d.day-1, d.hour, d.minute, d.second]



        duration = convertTime(duration)
        
        if(save_and_empty_data == True):
            PATH = os.getcwd() + "/Data/" + source + "/" 
            self.saveAndEmptyData(PATH, search_term + " " + start_date  + "-" + end_date + " " + source + ".csv")
        
        print("\n")
        print("Scraping took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")

        try:
            res_scraped = len(self.data[0])
            print("Scraped " + str(res_scraped) + " results.")
        except Exception:
            print("Scraped no results.")
            
        print("\n")

        
    def saveAndEmptyData(self, file_path, file_name):

        transcripts = pd.DataFrame.from_dict(OrderedDict(zip(self.column_names, self.data)))
#        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#            print(transcripts)
        if(transcripts.empty == True):
            print("There were no search results for " +  file_name)
            print("\n")
            if(file_name not in self.processed_empty_months_list):
                Scraper.ensureDir(file_path[:-5])
                no_results_file = open(file_path[:-5] + "empty_months_list.pkl", 'wb')
                self.processed_empty_months_list.append(file_name)
                pickle.dump(self.processed_empty_months_list, no_results_file)
                no_results_file.close()
            return
        if("Has Transcript" in self.column_names):
            transcript_with = transcripts[transcripts["Has Transcript"] == "True"]
            transcript_without = transcripts[transcripts["Has Transcript"] == "False"]
            if(transcript_with.shape[0] != 0):
                Scraper.ensureDir(file_path + "/transcripts/" + file_name)
                transcript_with.to_csv(file_path + "/transcripts/" + file_name, encoding="UTF-8")
            if(transcript_without.shape[0] != 0):
                Scraper.ensureDir(file_path + "/no_transcripts/" + file_name)
                transcript_without.to_csv(file_path + "/no_transcripts/" + file_name[:-4] + " no transcripts.csv", encoding="UTF-8")
        else:
            Scraper.ensureDir(file_path)
            transcripts.to_csv(file_path + file_name, encoding="UTF-8")
        for i in range(len(self.data)):
            self.data[i] = []

    def scrapeMonth(self, search_term, month, year, source, save_and_empty_data = False):
        
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", '6': "jun", \
                              '7': "jul", '8': "aug", '9': "sep", '10': "oct", '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)
        month_no = months.inv[month]

        for i in range(31, 27, -1):
            try:
                datetime.datetime(year=int(year),month=int(month_no),day=int(i))
                final_day = i
                break
            except ValueError:
                continue
        
        start_date = "1-" + month + "-" + year
        end_date = str(final_day) + "-" + month + "-" + year
        
        self.scrape(search_term, start_date, end_date, source)
        if(save_and_empty_data == True):
            PATH = os.getcwd() + "/Data/" + source + "/" + year + "/"
            self.saveAndEmptyData(PATH, search_term + " " + month + "-" + year + " " + source + ".csv")
        

    def scrapeYear(self, search_term, year, source):
        
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", '6': "jun", \
                              '7': "jul", '8': "aug", '9': "sep", '10': "oct", '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)

        startTime = time.time()
        for i in range(12):
            month_name = months[str(i+1)]
            
            PATH = os.getcwd() + "/Data/" + source + "/" + year + "/"
            
            exists1 = os.path.isfile(PATH + "/transcripts/" + search_term + " " + month_name + "-" + year + " " + source + ".csv")
            exists2 = os.path.isfile(PATH + "/no_transcripts/" + search_term + " " + month_name + "-" + year + " " + source + " no transcripts.csv")
            exists3 = os.path.isfile(PATH + search_term + " " + month_name + "-" + year + " " + source + ".csv")
            exists = (exists1 or exists2) or exists3
            
            try:
                self.processed_empty_months_list = []
                no_results_file = open(PATH[:-5] + "empty_months_list.pkl", 'rb')
                self.processed_empty_months_list = pickle.load(no_results_file)
            except FileNotFoundError:
                print("Could not load empty_months_list.pkl for " + source, flush=True)

            
            if (exists==True or (search_term + " " + month_name + "-" + year + " " + source + ".csv" in self.processed_empty_months_list)):
                print("File: " + search_term + " " + month_name + "-" + year + " " + source + ".csv" + " has been found or has already been processed." + " Moving on to the next month.")
                continue
            else:
                self.scrapeMonth(search_term, month_name, year, source)
                self.saveAndEmptyData(PATH, search_term + " " + month_name + "-" + year + " " + source + ".csv")
        duration = time.time() - startTime

        # This function has an upper limit on max secounds of sth like 30 days
        def convertTime(seconds):
            d = datetime.datetime(1,1,1) + timedelta(seconds = seconds)
            return [d.day-1, d.hour, d.minute, d.second]

        duration = convertTime(duration)

        print("\n")
        print("Scraping of the whole year took: " + str(duration[0]) + " days " + str(duration[1]) + " hours " + str(duration[2]) + " minutes " + str(duration[3])  + " seconds")
        print("\n")
        
    def readSources(self, file_name, sheet_name):
        file_path = os.path.join(self.PROJECT_ROOT, file_name)
        file = pd.read_excel(io=file_path, sheet_name=sheet_name)
        return file["csi"]

def clickButton(link_xpath, browser, time_wait = 10):
    
    try:
        WebDriverWait(browser, time_wait).until(EC.presence_of_element_located((By.XPATH, link_xpath)))
    except TimeoutException:
        raise NoSuchElementException("Could not find the button.")
    try:    
        WebDriverWait(browser, time_wait).until(EC.visibility_of_element_located((By.XPATH, link_xpath)))
    except TimeoutException:
        raise ElementNotVisibleException("The button is not visible.")

    try:    
        WebDriverWait(browser, time_wait).until(EC.element_to_be_clickable((By.XPATH, link_xpath)))
    except TimeoutException:
        raise ElementNotVisibleException("The button is not clickable.")
        
    link = browser.find_element_by_xpath(link_xpath) 
    link.click()

def NexisScrape(day, days_queue, data, params, locks_queue, browser, clickThroughToNewPage, convertDate, verbose, extract_first_paragraph, column_names, exitFlag, ocbrowser, saveAndEmptyData):
  
    
    
    
    search_term = params[0]
    source_csi = params[3]
    
    if(search_term != ""):
        search_term = "((" + search_term + "))+and+"

    url="https://www.nexis.com/api/version1/sr?sr=" + search_term + "date+geq+(" \
    + day + ")+and+date+leq+(" + day + ")&csi=" + source_csi

    if(day[-7:] == " first "):   
            day_name = day[:-7]
            url="https://www.nexis.com/api/version1/sr?sr=" + search_term + "date+geq+(" \
    + day_name + ")+and+date+leq+(" + day_name + ")+and+length<=100&csi=" + source_csi
            
    if(day[-7:] == " second"):
            day_name = day[:-7]
            url="https://www.nexis.com/api/version1/sr?sr=" + search_term + "date+geq+(" \
    + day_name + ")+and+date+leq+(" + day_name + ")+and+length>100+and+length<=200&csi=" + source_csi
            
    if(day[-7:] == " third "):
            day_name = day[:-7]
            url="https://www.nexis.com/api/version1/sr?sr=" + search_term + "date+geq+(" \
    + day_name + ")+and+date+leq+(" + day_name + ")+and+length>200&csi=" + source_csi

    browser.get(url)

    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='pageFooter']/ul/li[7]/a")))
        clickThroughToNewPage("/html/body/table/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr[4]/td[2]/table/tbody/tr/td/table/tbody/tr[1]/td/a[1]", browser, 0.5)
    except Exception as e:
        if(ocbrowser == True):
            locks_queue[0].acquire()
            days_queue.put(day)
            exitFlag[0] = 0
            locks_queue[0].release()
            browser.quit()
            if(verbose >= 2):
                locks_queue[2].acquire()
                print("\n", flush=True)
                print(">>> Did not find the next page button. " + day + " will be reacquired", flush=True)
                print(str(e), flush=True)
                print("\n", flush=True)
                locks_queue[2].release()
            return


    try:
        no_doc_found = browser.find_element_by_xpath("/html/body/table/tbody/tr[2]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[2]/td/table/tbody/tr[1]/td/table/tbody/tr/td[2]").text

        if(no_doc_found == "Source Not Available"):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Do not have subscription for this source. Source no: " + source_csi, flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
            return
    except Exception:
        pass        
    
    try:
        no_doc_found = browser.find_element_by_xpath("//*[@id='results']/h1").text
        if(no_doc_found == "No documents found"):
            return
    except Exception:
        pass
    
    
    day_links = []
    day_sources = []
    data.append([])
    day_dates = []
    data.append([])
    day_program_names = []
    data.append([])
    if(extract_first_paragraph==True):
        day_program_transcripts_first_paragraph = []
        data.append([])
        if(len(column_names) == 0):
            locks_queue[1].acquire()
            column_names.clear()
            column_names += ["Source", "Date", "Program Name", "First Paragraph Transcript", "Transcript"]
            locks_queue[1].release()
    else:
        if(len(column_names) == 0):
            locks_queue[1].acquire()
            column_names.clear()
            column_names += ["Source", "Date", "Program Name", "Transcript"]
            locks_queue[1].release()
    day_program_transcripts = []
    data.append([])
    



    def get_link(link):
    
        browser.get(link)
        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='navbar']/ul/li[8]/a")))
        except:
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> Load last element for a link failed on " + day, flush=True)
                locks_queue[2].release()
        
        try:
            source = browser.find_element_by_xpath("//*[@id='document']/div[2]/div[1]/span/span/span[1]/center").text
        except NoSuchElementException:
            source = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had the empty source name on " + day, flush=True)
                locks_queue[2].release()
        try:
            date = browser.find_element_by_xpath("//*[@id='document']/div[2]/div[1]/span/span/center[1]").text
        except NoSuchElementException:
            date = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had the empty date on " + day, flush=True)
                locks_queue[2].release()
        try:
            program_name = browser.find_element_by_xpath("//*[@id='document']/div[2]/div[1]/span/span/span[2]").text
        except NoSuchElementException:
            program_name = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had the empty program name on " + day, flush=True)
                locks_queue[2].release()
        try:
            program_transcript_list = browser.find_elements_by_xpath("//*[@id='document']/div[2]/div[1]/span/span/p")
        except NoSuchElementException:
            program_transcript_list = []
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had no paragraphs content on " + day, flush=True)
                locks_queue[2].release()
            
           
        program_transcript = ""
        
        for p in program_transcript_list[1:]:
            if(p.text[:10] != "LANGUAGE:" and p.text[:4] != "URL:"):
                program_transcript += p.text + " "
            
        day_sources.append(source)
        day_dates.append(date)
        day_program_names.append(program_name)
        if(extract_first_paragraph==True):
            if(len(program_transcript_list)==0):
                day_program_transcripts_first_paragraph.append("")  
            else:
                day_program_transcripts_first_paragraph.append(program_transcript_list[0].text)      
        day_program_transcripts.append(program_transcript)
        
#                    queueLocks[2].acquire()
#                    print(day_sources[-1], flush=True)
#                    print(day_dates[-1], flush=True)
#                    print(day_program_names[-1], flush=True)
#                    print(day_program_transcripts[-1], flush=True)
#                    print('\n', flush=True)
#                    queueLocks[2].release()


    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='updateCountDiv']")))
        no_results = browser.find_element_by_xpath("//*[@id='updateCountDiv']").text
    except Exception:
        try:
            no_results = browser.find_element_by_xpath("//*[@id='docnav']/div[2]/ul/li[2]").text
            get_link(browser.current_url)
            locks_queue[1].acquire()  
            data[0] = data[0] + day_sources
            data[1] = data[1] + day_dates
            data[2] = data[2] + day_program_names
            if(extract_first_paragraph==True):
                data[3] = data[3] + day_program_transcripts_first_paragraph
                data[4] = data[4] + day_program_transcripts
            else:
                data[3] = data[3] + day_program_transcripts
            locks_queue[1].release()
            return
        except Exception:
            try:
                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='container']/table/tbody/tr/td/table/tbody/tr[3]/td[2]/span")))
                tt_results = browser.find_element_by_xpath("//*[@id='container']/table/tbody/tr/td/table/tbody/tr[3]/td[2]/span").text
                if(tt_results == "More than 3000 Results"):
                    print("\n", flush=True)
                    print(">>> More than 3000 results for " + day + " It will be reacquired.", flush=True)
                    print("\n", flush=True)
                    locks_queue[0].acquire()
                    days_queue.put(day + " first ")
                    days_queue.put(day + " second")
                    days_queue.put(day + " third ")
                    locks_queue[0].release()
                    return
            except Exception as e: 
                locks_queue[2].acquire()
                print("\n", flush=True)
                print(">>> Could not find no of search results for " + day + " It will be reacquired.", flush=True)
                print(str(e), flush=True)
                print("\n", flush=True)
                locks_queue[2].release()
                locks_queue[0].acquire()
                days_queue.put(day)
                exitFlag[0] = 0
                locks_queue[0].release()
                browser.quit()
                return

        
    table = str.maketrans(dict.fromkeys('()'))
    no_results = no_results.translate(table)

    try:
        
        
        while(1):
#            try:
#                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='navbar']/ul/li[8]/a")))
#            except:
#                if(verbose >= 3):
#                    locks_queue[2].acquire()
#                    print(">>> Links main page load last element failed " + day, flush=True)
#                    locks_queue[2].release()
                    
            WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//ol[contains(@class,'nexisresult')]")))
            results = browser.find_elements_by_xpath("//ol[contains(@class,'nexisresult')]")
          
            for result in results:          
                day_links.append(result.find_element_by_tag_name('a').get_attribute("href"))
            
            
            try:

                clickThroughToNewPage("//a[contains(@class, 'la-TriangleRight')]", browser)
                
            except  NoSuchElementException as e:
#                if(verbose >= 3):
#                    locks_queue[2].acquire()
#                    print("For day: " + day, flush=True)
#                    print(str(e), flush=True)
#                    locks_queue[2].release()
                break
            
            
            
    except  Exception as e:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Some Exception occured while obtaining links. " + day + " will be reacquired.", flush=True)
            print(str(e), flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        return
        

    if(len(day_links) != int(no_results)):
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Wrong no. of links for the day: " + str(day), flush=True)
            print(">>> Ought to get " + no_results + " links. Got " + str(len(day_links)) + " links.", flush=True)
            print(">>> " + day + " will be reacquired.", flush=True)
            print("\n", flush=True)
            for link in day_links:
                print(link, flush = True)
            
            locks_queue[2].release()
            
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        return
    
    day_links_queue = queue.Queue()
    day_links_queue.queue = queue.deque(day_links)
    
    while(not day_links_queue.empty()):
        link = day_links_queue.get()
        get_link(link)

    locks_queue[1].acquire()  
    data[0] = data[0] + day_sources
    data[1] = data[1] + day_dates
    data[2] = data[2] + day_program_names
    if(extract_first_paragraph==True):
        data[3] = data[3] + day_program_transcripts_first_paragraph
        data[4] = data[4] + day_program_transcripts
    else:
        data[3] = data[3] + day_program_transcripts
    locks_queue[1].release()





def BoBLogin(browser, clickThroughToNewPage):
    
    url="https://learningonscreen.ac.uk/ondemand/"
    browser.get(url)

    try:
        clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", browser)
    except Exception:
        browser.quit()
        print(">>> Did not find the next page button. Failed to Log In.")

    browser.find_element_by_id('institution-field').send_keys('University of Warwick')
    browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
    browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
    
    browser.find_element_by_id('userName').send_keys('u1664202')
    browser.find_element_by_id('password').send_keys('mk011211')
    browser.find_element_by_xpath("//button[@id='signinbutton']").click()


def BoBScrape(day, days_queue, data, params, locks_queue, browser, clickThroughToNewPage, convertDate, verbose, extract_first_paragraph, column_names, exitFlag, ocbrowser, saveAndEmptyData):

 
    day_page = convertDate(day)

    if(len(day_page[0])==1):
        day_page[0] = "0" + day_page[0]
        
    if(len(day_page[1])==1):
        day_page[1] = "0" + day_page[1]
    day_page = str(day_page[2]) + "-" + str(day_page[1]) + "-" + str(day_page[0]) 

    
    search_term = str(params[0])
    channel = str(params[3])
    channels = {"5 USA":"71400", "5*": "71441", "BBC News 24":"16", "BBC Parliament":"18", "BBC Radio 1": "19", "BBC Radio 2": "20", \
                "BBC Radio 3": "21", "BBC Radio 4": "22", "BBC Radio 4 Extra": "71394", "BBC Radio 5": "24", "BBC World Service Radio": "52", \
                "BBC 1 London": "54", "BBC 1 Scotland": "61", "BBC 1 Wales": "65",  "BBC2 England": "68", "BBC2 Scotland": "75", "BBC2 Wales": "79", \
                "BBC4": "82", "Channel 4": "106", "Channel 5": "107", "E4": "133", "FilmFour": "137", "ITV London": "175", "ITV2": "176", "ITV3": "71301", \
                "ITV4": "71324", "More4": "71300", "S4C": "248", "Sky News": "279", "ZDF": "71479", "CNNI": "115", "France24": "71411", "Tagesschau24": "71488", \
                "RaiNews24": "71492", "RussiaToday": "71413"}
    

    url="https://learningonscreen.ac.uk/ondemand/"
    
    
    try:
        browser.get(url)
    except Exception:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not get first url.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        return    
    
    try:
        clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", browser)
    except Exception:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Did not find the Sign In button. Failed to Log In.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        return

    
    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
        browser.find_element_by_id('institution-field').send_keys('University of Warwick')
        browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
        browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
    except NoSuchElementException:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Did not find the Sign In button. Failed to Log In.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        return
    
#    time.sleep(99999999)
    
    try:
        browser.find_element_by_id('userName').send_keys('u1664202')
        browser.find_element_by_id('password').send_keys('mk011211')
        browser.find_element_by_xpath("//button[@id='signinbutton']").click()  
    except NoSuchElementException:
        pass  

    time.sleep(99999999)

    url="https://learningonscreen.ac.uk/ondemand/search.php/prog?q%5B0%5D%5Bv%5D=" + search_term + "&date_start=" \
    + day_page + "-00-00&date_end=" + day_page + "-23-59&date_type=1&search_type=1&q%5B0%5D%5Bindex%5D=title&page=1&source=&channel[]=" + channel + "&sort=date_asc"
    


    browser.get(url)


    try:
        no_doc_found = browser.find_element_by_xpath("//*[@id='main-content']/div/h4").text
        if(no_doc_found=="No results found"):
            return
    except Exception:
        pass



#    time.sleep(99999999)
    
    day_links = []
    
    
    day_sources = []
    data.append([])
    
    day_program_names = []
    data.append([])  
    
    day_dates = []
    data.append([])
    
    day_times = []
    data.append([])
    
    day_running_times = []
    data.append([])
    
    day_no_transcript_texts = []
    data.append([])
    
    day_program_transcripts = []
    data.append([])
    
    day_unavailable_transcript_links = []
    data.append([])
    
    day_unavailable_transcript_reason = []
    data.append([])

    day_video_links = []
    data.append([])
    
    if(len(column_names) == 0):
            locks_queue[1].acquire()
            column_names.clear()
            column_names += ["Source", "Date", "Program Name", "Time", "Duration", "Has Transcript", "Transcript", "Video link", "Unavailable link", "Unavailable reason"]
            locks_queue[1].release()


    
    try:
        while(1):

            WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'h4')))
            results = browser.find_elements_by_tag_name('h4')
            
            
            
            for result in results:
                if(result.text != ""):
                    WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
                    day_links.append(result.find_element_by_tag_name('a').get_attribute("href"))

            try:
                clickThroughToNewPage("//a[contains(@class, 'btn btn-standard float-right')]", browser)
            except  NoSuchElementException:
                break
    except  Exception:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Some Exception occured while obtaining links. " + day + " will be reacquired.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        return
                                                     

    
    def get_link(link):
    
        browser.get(link)
        
        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'broadcaster')]")))
            source = browser.find_element_by_xpath("//p[contains(@class, 'broadcaster')]").text
        except NoSuchElementException:
            source = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had the empty source field on " + day, flush=True)
                locks_queue[2].release()
                
        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'broadcast-date')]")))
            date = browser.find_element_by_xpath("//p[contains(@class, 'broadcast-date')]").text
        except NoSuchElementException:
            date = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the articles had the empty date on " + day, flush=True)
                locks_queue[2].release()            
    
        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='programme-details']/h2")))
            program_name = browser.find_element_by_xpath("//*[@id='programme-details']/h2").text
        except NoSuchElementException:
            program_name = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the programs had the empty name field on " + day, flush=True)
                locks_queue[2].release()
         

        date=date.split(",")
        date[1] = date[1][1:].lower().split(" ")
        date[2] = date[2][1:].split(" ")
        date[1] = "-".join(date[1])
        date[2] = date[2][0]
        program_time = date[2]
        date = date[1]

        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//span[contains(@class, 'prog-running-time')]")))
            running_time = browser.find_element_by_xpath("//span[contains(@class, 'prog-running-time')]").text
        except NoSuchElementException:
            running_time = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the programs had the empty duration field on " + day, flush=True)
                locks_queue[2].release()

        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//source[contains(@type, 'application/x-mpegurl')]")))
            video_link = browser.find_element_by_xpath("//source[contains(@type, 'application/x-mpegurl')]").src
            print(video_link)
        except NoSuchElementException:
            video_link = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the programs had the empty video field on " + day, flush=True)
                locks_queue[2].release()
                
        unavailable_link = ""
        unavailable_reason = ""
        program_transcript = ""

        try:
            no_transcript_text = browser.find_element_by_xpath("//p[contains(@class, 'intro state-message')]").text
            
            if(no_transcript_text == "Sorry!, there was a problem processing this broadcast"):
#                try:
#                    clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
#                    browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
#                    browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
#                    clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
#                except Exception as e:
#                    if(verbose >= 2):
#                        locks_queue[2].acquire()
#                        print("\n", flush=True)
#                        print(">>> Did not manage to send the message. " + day + " will be reacquired.", flush=True)
#                        print(str(e), flush=True)
#                        print("\n", flush=True)
#                        locks_queue[2].release()
#                    raise StaleElementReferenceException("Did not manage to send the message.")   
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Problem processing the broadcast."
            elif(no_transcript_text == "Request to watch this programme" or "Sorry, this programme hasn't been broadcast yet"):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Program has to be requested."
                
            elif(no_transcript_text == "Sorry but this programme is not available."):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "The program is not available."                
                
            elif(no_transcript_text == "Sorry, this programme hasn't been broadcast yet"):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Program has not yet been broadcasted."                
            else:
                locks_queue[2].acquire()
                print(">>> Program is not sure if transctipt exists on " + day, flush=True)
                locks_queue[2].release()                
        except NoSuchElementException:
            try:

                # 300 secounds for more confidence
                clickThroughToNewPage("//a[contains(@class, 'btn btn-standard float-right')]", browser, time_wait = 20, time_wait_stale = 5)
                
                WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'transcript-body')]")))

                def textLocator(webdriver):
                    return bool(len(webdriver.find_element_by_xpath("//div[contains(@class, 'transcript-body')]").text) != 0)

                try:
                    # 600 secounds for more confidence
                    WebDriverWait(browser, 60).until(textLocator)
                except TimeoutException:

                    program_transcript_body = browser.find_element_by_xpath("//div[contains(@class, 'transcript-body')]")
                    if(program_transcript_body.text == ""):
#                        try:
#                            clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
#                            browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
#                            browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
#                            clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
#                        except Exception as e:
#                            if(verbose >= 2):
#                                locks_queue[2].acquire()
#                                print("\n", flush=True)
#                                print(">>> Did not manage to send the message. " + day + " will be reacquired.", flush=True)
#                                print(str(e), flush=True)
#                                print("\n", flush=True)
#                                locks_queue[2].release()
#                            raise StaleElementReferenceException("Did not manage to send the message.")
                        unavailable_link = link
                        unavailable_reason = "Program has an empty transcript field."
                        global info_dict
                        try:
                            c = info_dict[link]
                        except Exception:
                            c = 0
                            info_dict[link] = 0
                        
                        if(c <= 6):
                            info_dict[link] += 1
                            browser.delete_all_cookies()
                            raise Exception("No empty transcripts.")
                        else:
                            raise NoSuchElementException("Transcript body is empty.")

                WebDriverWait(browser, 60).until(EC.presence_of_all_elements_located((By.XPATH, "//span[contains(@class, 'transcript-text')]")))        

                program_transcript_list = browser.find_elements_by_xpath("//span[contains(@class, 'transcript-text')]")

                for p in program_transcript_list:
                    program_transcript += p.text + " "   


            except (NoSuchElementException, ElementNotVisibleException) as e:
#                try:
#                    clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
#                    browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
#                    browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
#                    clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
#                except Exception as e:
#                    if(verbose >= 2):
#                        locks_queue[2].acquire()
#                        print("\n", flush=True)
#                        print(">>> Did not manage to send the message. " + day + " will be reacquired.", flush=True)
#                        print(str(e), flush=True)
#                        print("\n", flush=True)
#                        locks_queue[2].release()
#                    raise StaleElementReferenceException("Did not manage to send the message.")
                    
                if(verbose >= 3):
                    locks_queue[2].acquire()
                    print(">>> Transcript text has not been found on " + day, flush=True)
                    print(str(e), flush = True)
                    locks_queue[2].release()
                if(str(e) == "Message: Transcript body is empty.\n"):
                    unavailable_link = link
                    unavailable_reason = "Program has an empty transcript field."
                else:    
                    unavailable_link = link
                    unavailable_reason = "Program has no transcript button."

            if(program_transcript != ""):
                transcript_text = "True"
            else:
                transcript_text = "False"


        day_sources.append(source)
        day_dates.append(date)
        day_program_names.append(program_name)
        day_times.append(program_time)
        day_running_times.append(running_time)
        day_video_links.append(video_link)
        day_no_transcript_texts.append(transcript_text)
        day_program_transcripts.append(program_transcript)
        day_unavailable_transcript_links.append(unavailable_link)
        day_unavailable_transcript_reason.append(unavailable_reason)


    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'h2')))
        no_results = browser.find_element_by_xpath("//*[@id='main-content']/div/div[1]/h2").text
    except Exception as e:
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not find the total number of links. " + day + " will be reacquired.", flush=True)
            print(str(e), flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        return


    if(len(no_results.split(" ")) == 2):
        no_results = str(no_results.split(" ")[0])
    else:
        no_results = no_results.split(" ")
        no_results = no_results[2]

    if(len(day_links) != int(no_results)):
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Wrong no. of links for the day: " + str(day), flush=True)
            print(">>> Ought to get " + no_results + " links. Got " + str(len(day_links)) + " links.", flush=True)
            print(">>> " + day + " will be reacquired.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        return



    day_links_queue = queue.Queue()
    day_links_queue.queue = queue.deque(day_links)

    count = 0

    while(not day_links_queue.empty()):

        link = day_links_queue.get()
        try:
            get_link(link)
        except StaleElementReferenceException as e:
            if(verbose >= 2):
                locks_queue[2].acquire()
                print("\n", flush=True)
                print(">>> Stale element reference. " + day + " will be reacquired.", flush=True)
                print(str(e), flush=True)
                print("\n", flush=True)
                locks_queue[2].release()
            locks_queue[0].acquire()
            days_queue.put(day)
            exitFlag[0] = 0
            locks_queue[0].release()
            browser.quit()
            return
        except Exception as e:
            if(verbose >= 2):
                locks_queue[2].acquire()
                print("\n", flush=True)
                print(">>> Some error has occured. " + day + " will be reacquired.", flush=True)
                print(str(e), flush=True)
                print("\n", flush=True)
                locks_queue[2].release()
            locks_queue[0].acquire()
            days_queue.put(day)
            exitFlag[0] = 0
            locks_queue[0].release()
            browser.quit()
            return
            
            
            

    locks_queue[1].acquire()
    data[0] += day_sources
    data[1] += day_dates
    data[2] += day_program_names
    data[3] += day_times
    data[4] += day_running_times
    data[5] += day_no_transcript_texts
    data[6] += day_program_transcripts
    data[7] += day_video_links
    data[8] += day_unavailable_transcript_links
    data[9] += day_unavailable_transcript_reason
    locks_queue[1].release()    


def RTSScrape(day, days_queue, data, params, locks_queue, browser, clickThroughToNewPage, convertDate, verbose, extract_first_paragraph, column_names, exitFlag, ocbrowser, saveAndEmptyData):
   
    
    
    data = []
    
    def saveAndEmptyData(file_path, file_name, encoding = "ISO-8859-1"):

        transcripts = pd.DataFrame.from_dict(OrderedDict(zip(column_names, data)))

        if(transcripts.empty == True):
            print("There were no search results for " +  file_name)
            print("\n")
            return
        if("Has Transcript" in column_names):
            transcript_with = transcripts[transcripts["Has Transcript"] == "True"]
            transcript_without = transcripts[transcripts["Has Transcript"] == "False"]
            if(transcript_with.shape[0] != 0):
                Scraper.ensureDir(file_path + "/transcripts/" + file_name)
                transcripts_1 = transcripts[transcripts["Transcript 1"] != ""]
                transcripts_1 = transcripts_1.drop(["Transcript 2"], axis=1)
                transcripts_2 = transcripts[transcripts["Transcript 2"] != ""]
                transcripts_2 = transcripts_2.drop(["Transcript 1"], axis=1)
#                with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#                    print(transcripts_2)
                
                
                if(transcripts_1.empty == False):
                    transcripts_1.to_csv(file_path + "/transcripts/" + file_name[:-4] + " latin1.csv", encoding="ISO-8859-1")
                if(transcripts_2.empty == False):
                    transcripts_2.to_csv(file_path + "/transcripts/" + file_name[:-4] + " UTF-8.csv", encoding="UTF-8")
            if(transcript_without.shape[0] != 0):
                Scraper.ensureDir(file_path + "/no_transcripts/" + file_name)
                transcript_without.to_csv(file_path + "/no_transcripts/" + file_name[:-4] + " no transcripts.csv", encoding="UTF-8")
        else:
            Scraper.ensureDir(file_path)
            transcripts.to_csv(file_path + file_name, encoding=encoding)
        for i in range(len(data)):
            data[i] = []
    
    
    def waitForStaleLink(browser, time_wait = 10):
        
        def waitFor(condition_function):
            start_time = time.time() 
            while time.time() < start_time + time_wait: 
                if condition_function(): 
                    return True 
                else: 
                    time.sleep(0.1) 
                
        def linkHasGoneStale():
            try:
                # poll the link with an arbitrary call
                browser.find_elements_by_tag_name('h2')
                return False
            except StaleElementReferenceException:
                return True

        waitFor(linkHasGoneStale)
    
    def getTranscriptFromFile(file_path, file_name):
        if(len(file_name) == 12):
            transc = ""
            f=open(file_path + file_name, 'r', encoding="ISO-8859-1").readlines()
            beg = 0
            for w in f:
                if(len(w)>0 and w != "\n"):
                    if(w[:8] == "Locuteur"):
                        beg = 1
                    if(beg != 0):
                        if(w[:8] == "Locuteur"):
                            pass
                        else:
                            transc += w
    
            transc = transc.replace("\n", " ").replace("\r", " ")
            return transc
    
                        
        elif(len(file_name)==21):
            f=open(file_path + file_name, 'r', encoding="UTF-8").readlines()
            transc = ""
            for w in f:
                if(len(w)>0):
                    w = w.strip()
                    if(len(w) == 24):
                        if(w[2] != ":" or w[5] != ":" or w[8] != ":"):
                            transc += w + " "
                    elif(len(w) == 25):
                        if(w[3] != ":" or w[6] != ":" or w[9] != ":"):
                            transc += w + " "
                    else:
                        if(w.strip()[:12] == "Sous-titrage" or w.strip()[:12] == "SOUS-TITRAGE"):
                            break
                        w = w.strip()
                        if(w[:3] != "..."):
                            transc += w + " "
            return transc
            
        else:
            raise("Check length of file name.")
 
    titles_all = []
    data.append([])
    dates_all = []
    data.append([])
    durations_all = []
    data.append([])
    has_transcript_all = []
    data.append([])
    transcripts_one_all = []
    data.append([])
    transcripts_two_all = []
    data.append([])
    
    def getPrograms():
        WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//*[@id="results"]//li[contains(@class, "document asset")]')))
        
        programs = browser.find_elements_by_xpath('//*[@id="results"]//li[contains(@class, "document asset")]')
        

        
        for program in programs:

            transcript_one_text = ""
            transcript_two_text = ""
            title = program.find_element_by_xpath(".//div[contains(@class, 'collection data')]//dd").text
            date = program.find_element_by_xpath(".//div[contains(@class, 'half data')]//dd[contains(@class, 'value')]").text
            duration = program.find_element_by_xpath(".//div[contains(@class, 'half data')]//dd[contains(@data-field, 'Duree')]").text
            date = date[:10]
            
            has_transcript = "False"
            try:            
                transcript_file_links = program.find_elements_by_xpath(".//dd[contains(@data-field, 'Documents')]//a")
   
                for tr in transcript_file_links:
         
                    file_name = tr.get_attribute("textContent")
                    if(file_name[-3:] == "txt"):
                        browser.get(tr.get_attribute("href"))
                        while not os.path.exists(data_path + file_name):
                            time.sleep(0.1)
                            
                        if os.path.isfile(data_path + file_name):
                            if(len(file_name) == 12):
                                transcript_one_text = getTranscriptFromFile(data_path, file_name)
                                has_transcript = "True"
                            elif(len(file_name) == 21):
                                transcript_two_text = getTranscriptFromFile(data_path, file_name)
#                                try:
#                                    transcript_two_text.find(u'\u24B8')
#                                    transcript_two_text = getTranscriptFromFile(data_path, file_name, "UTF-8")
#                                except:
#                                    pass
                                
                                has_transcript = "True"
                        else:
                            ff = data_path + file_name
                            raise ValueError("%s isn't a file!" % ff) 

#                        os.remove(data_path + file_name)
                        
                
                transcripts_one_all.append(transcript_one_text)
                transcripts_two_all.append(transcript_two_text)
                has_transcript_all.append(has_transcript)
                
            except NoSuchElementException:
                print("Transcript not found.")

            titles_all.append(title)
            dates_all.append(date)
            durations_all.append(duration)
            
#            print(title)
#            print(date)
#            print(duration)
#            print(transcript_one_text)
#            print(transcript_two_text)
            
#        for i in range(len(titles_all)):
#            print(titles_all[i])
#            print(dates_all[i])
#            print(durations_all[i])
#            print(transcripts_one_all[i])
#            print(transcripts_two_all[i])
        

        

        
        
    data_path = "/Users/lvardges/Desktop/Scraping/Data/RTS/"
    url = "https://rtsarchives.portal.srgssr.ch"
    browser.get(url)
    
    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='form-holder']/div/form/div[1]/input")))
        browser.find_element_by_xpath('//*[@id="form-holder"]/div/form/div[1]/input').send_keys('LevonyVa')
        browser.find_element_by_xpath('//*[@id="form-holder"]/div/form/div[2]/input').send_keys('1MaxFrisch20!8')
        clickThroughToNewPage("//*[@id='form-actions']/button[1]", browser)
    except NoSuchElementException:
        pass

    time.sleep(90)
    
    for i in range(0,19):
        
        titles_all = []
        dates_all = []
        durations_all = []
        has_transcript_all = []
        transcripts_one_all = []
        transcripts_two_all = []
        
        waitForStaleLink(browser)
#        period=1.10.2018-31.10.2018
#        https://rtsarchives.portal.srgssr.ch/#tri=recents&facetDatePublication=2009-2010&facetDureeSec=300-10000&emissions=19h30&q=*
        if(len(str(i)) == 1 and i != 9):
            url = "https://rtsarchives.portal.srgssr.ch/#tri=recents&facetDatePublication=200" + str(i) + "-200" + str(i+1) + "&facetDureeSec=300-10000&emissions=19h30&q=*"
        elif(i==9):
            url = "https://rtsarchives.portal.srgssr.ch/#tri=recents&facetDatePublication=200" + str(i) + "-20" + str(i+1) + "&facetDureeSec=300-10000&emissions=19h30&q=*"
        else:
            url = "https://rtsarchives.portal.srgssr.ch/#tri=recents&facetDatePublication=20" + str(i) + "-20" + str(i+1) + "&facetDureeSec=300-10000&emissions=19h30&q=*"
        
        browser.get(url)
        waitForStaleLink(browser)
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='emissions_chosen']/ul/li/input")))
        prog = browser.find_element_by_xpath('//*[@id="emissions_chosen"]/ul/li/input')
        prog.send_keys(Keys.RETURN)
        time.sleep(2)
        prog.send_keys('19h30')
        time.sleep(2)
        prog.send_keys(Keys.RETURN)

    
        try:
            while(1):
                
                    try:
                        getPrograms()
                    except Exception as e:
                        print("Failed to get programs.")
                        print(str(e))
                    
                    time.sleep(5)
                    try:
                        try:
                            browser.find_element_by_xpath("//*[@id='pagination_bottom']/a[contains(@class, 'next reached')]")
                            break
                        except Exception as e:
                            pass

                        
                        try:
                            clickThroughToNewPage("//*[@id='pagination_bottom']/a[contains(@class, 'next')]", browser, 20)
                        except (ConnectionRefusedError, TimeoutException) as e:
                            time.sleep(5)
                            print("Retrying to get new page.")
                            print(str(e))
                            clickThroughToNewPage("//*[@id='pagination_bottom']/a[contains(@class, 'next')]", browser, 20)
                        
                    except  (NoSuchElementException, ElementNotVisibleException) as e:
                        print("Exception occured: ")
                        print(str(e))
                        break
                    
                
                
                
            
        except  Exception as e:
            browser.quit()
            if(verbose >= 2):
                locks_queue[2].acquire()
                print("\n", flush=True)
                print("Failed to obtain data.", flush=True)
                print(str(e))
                print("\n", flush=True)
                locks_queue[2].release()
            return
                        
        if(len(column_names) == 0):
                locks_queue[1].acquire()
                column_names.clear()
                column_names += ["Date", "Program Name", "Duration", "Has Transcript", "Transcript 1", "Transcript 2"]
                locks_queue[1].release()            
    
        program_name = "19h30 and 12h45"

        data[0] += dates_all
        data[1] += titles_all
        data[2] += durations_all
        data[3] += has_transcript_all
        data[4] += transcripts_one_all
        data[5] += transcripts_two_all
        
        
        if(len(str(i)) == 1):
            year = "200" + str(i)
        if(len(str(i)) == 2):
            year = "20" + str(i)            
            
        PATH = os.getcwd() + "/Data/RTS/" + year + "/"
                
        exists1 = os.path.isfile(PATH + "/transcripts/" + program_name + " "  + year + ".csv")
        exists2 = os.path.isfile(PATH + "/no_transcripts/" + program_name + " " + year + " no transcripts.csv")
        exists = exists1 or exists2

        if (exists==True):
            print("File: " + program_name + " "  + year + ".csv" + " has been found or has already been processed." + " Moving on to the next month.")
        else:
            saveAndEmptyData(PATH, program_name + " "  + year + ".csv", "UTF-8")

        
        source_dir = os.getcwd() + "/Data/RTS/"  #Path where your files are at the moment
        dst = PATH #Path you want to move your files to
        files = glob.iglob(os.path.join(source_dir, "*.txt"))
        for file in files:
            if os.path.isfile(file):
                shutil.copy2(file, dst)
                os.remove(file)
    
    
    time.sleep(999999)
    
    

def BBCMakeRequests(search_term, channel, years = None, start_date = None, end_date = None, use_database = True, report_transcripts = False, headless_chrome = 1, verbose = 3):
    
    PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
    DRIVER_BIN = os.path.join(PROJECT_ROOT, "chromedriver")
    chrome_options = Options() 
    chrome_options.add_argument("user-data-dir=" + PROJECT_ROOT + "/Profile")
            
    if(headless_chrome == True):
        chrome_options.add_argument("--headless") 

    browser = webdriver.Chrome(executable_path=DRIVER_BIN,   options=chrome_options)
    

    request_links = []
    
    def convertDate(date):
        months_num_to_name = {'1': "jan", '2': "feb",'3': "mar", '4': 'apr', '5': "may", \
                              '6': "jun", '7': "jul", '8': "aug", '9': "sep", '10': "oct", \
                              '11': "nov", '12': "dec"}
        months = bidict(months_num_to_name)
        
        date = date.split("-")
        if(date[0][0]=='0'):
            day = date[0][1]
        else:
            day = date[0]
        
        month = months.inv[date[1]]
        
        year = date[2]    
        return [day, month, year]
    
    if(start_date is not None and end_date is not None):
        start_dateo = start_date
        start_date = convertDate(start_date)
    
        if(len(start_date[0])==1):
            start_date[0] = "0" + start_date[0]
            
        if(len(start_date[1])==1):
            start_date[1] = "0" + start_date[1]
        start_date = str(start_date[2]) + "-" + str(start_date[1]) + "-" + str(start_date[0]) 
    
    
        end_dateo = end_date
        end_date = convertDate(end_date)
    
        if(len(end_date[0])==1):
            end_date[0] = "0" + end_date[0]
            
        if(len(end_date[1])==1):
            end_date[1] = "0" + end_date[1]
        end_date = str(end_date[2]) + "-" + str(end_date[1]) + "-" + str(end_date[0]) 



    def clickThroughToNewPage(link_xpath, browser, time_wait = 10, time_wait_stale = 5, additional = None):

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




    # 1980-01-01-23-59
    def getStartEndDays(month_no, year):
        for i in range(31, 27, -1):
            try:
                datetime.datetime(year=int(year),month=int(month_no),day=int(i))
                final_day = i
                break
            except ValueError:
                continue
            
        if(len(str(month_no)) == 1):
            month = "0" + str(month_no)
        else:
            month = str(month_no)
        
        start_date = year + "-" + month + "-" + "01"
        end_date = year + "-" + month + "-" + str(final_day)
        return [start_date, end_date]
    
    
    

    url="https://learningonscreen.ac.uk/ondemand/"
    browser.get(url)

    try:
        clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", browser)
    except Exception:
        browser.quit()
        if(verbose >= 2):
            print("\n", flush=True)
            print(">>> Did not find the Sign In button. Failed to Log In.", flush=True)
            print("\n", flush=True)
        return


    try:
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
        browser.find_element_by_id('institution-field').send_keys('University of Warwick')
        browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
        browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
    except NoSuchElementException:
        browser.quit()
        if(verbose >= 2):
            print("\n", flush=True)
            print(">>> Did not find the Sign In button. Failed to Log In.", flush=True)
            print("\n", flush=True)
        return
    
    try:
        browser.find_element_by_id('userName').send_keys('u1664202')
        browser.find_element_by_id('password').send_keys('mk011211')
        browser.find_element_by_xpath('//*[@id="sessionDuration"]/option[text() = "Indefinitely until I sign out"]').click()
        browser.find_element_by_xpath("//button[@id='signinbutton']").click()  
    except NoSuchElementException:
        pass  

    
    
    if years is not None:
        for year in years:
            for month_no in range(12, 0, -1):

                start_date, end_date = getStartEndDays(month_no, year)
                
                url="https://learningonscreen.ac.uk/ondemand/search.php/prog?q%5B0%5D%5Bv%5D=" + search_term + "&date_start=" \
                + start_date + "-00-00&date_end=" + end_date + "-23-59&date_type=1&search_type=1&q%5B0%5D%5Bindex%5D=title&page=1&source=&channel[]=" + channel + "&sort=date_desc"
                
            
            
                browser.get(url)
        #        time.sleep(999999)
            
                try:
                    no_doc_found = browser.find_element_by_xpath("//*[@id='main-content']/div/h4").text
                    if(no_doc_found=="No results found"):
                        continue
                except Exception:
                    pass
                
                
                try:
                    while(1):
            
                        WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
                        results = browser.find_elements_by_xpath("//a[contains(@class, 'avail avail-request-this')]")
                        
                        
                        
                        
                        for result in results:
                            if(result.text != ""):
                                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
                                request_links.append(result.get_attribute("href"))
            
                        try:
                            clickThroughToNewPage("//a[contains(@class, 'btn btn-standard float-right')]", browser)
                        except  NoSuchElementException:
                            break
                except  Exception as e:
                    browser.quit()
                    if(verbose >= 2):
                        print("\n", flush=True)
                        print(">>> Some Exception occured while obtaining links. " + year + " has to be reacquired.", flush=True)
                        print(str(e))
                        print("\n", flush=True)
                    return
    else:


        url="https://learningonscreen.ac.uk/ondemand/search.php/prog?q%5B0%5D%5Bv%5D=" + search_term + "&date_start=" \
        + start_date + "-00-00&date_end=" + end_date + "-23-59&date_type=1&search_type=1&q%5B0%5D%5Bindex%5D=title&page=1&source=&channel[]=" + channel + "&sort=date_desc"
        
    
    
        browser.get(url)
#        time.sleep(999999)

        

        try:
            no_doc_found = browser.find_element_by_xpath("//*[@id='main-content']/div/h4").text
            if(no_doc_found=="No results found"):
                return
        except Exception:
            pass
        
        if(use_database == False):
            try:
                while(1):
                    
                    if(report_transcripts == True):
                                
                        WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'h4')))
                        results = browser.find_elements_by_tag_name('h4')
                        
                        
                        
                        for result in results:
                            if(result.text != ""):
                                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
                                request_links.append(result.find_element_by_tag_name('a').get_attribute("href"))
                    else:
                    
                        WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
                        results = browser.find_elements_by_xpath("//a[contains(@class, 'avail avail-request-this')]")
                        
                        
                        
                        
                        for result in results:
                            if(result.text != ""):
                                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
                                request_links.append(result.get_attribute("href"))
        
                    try:
                        clickThroughToNewPage("//a[contains(@class, 'btn btn-standard float-right')]", browser)
                    except  NoSuchElementException:
                        break
            except  Exception as e:
                browser.quit()
                if(verbose >= 2):
                    print("\n", flush=True)
                    print(">>> Some Exception occured while obtaining links. " + start_date + " to " + end_date + " has to be reacquired.", flush=True)
                    print(str(e))
                    print("\n", flush=True)
                return        
        else:
            
            
            request_links = selectUnavailableLinks(start_dateo, end_dateo, channel) 
            
            if(report_transcripts == True):
                request_links = [x for x in request_links if x[2] != "Program has to be requested."]
            else:
                request_links = [x for x in request_links if x[2] == "Program has to be requested."]
        
        
    def get_link(link):
    
        
        browser.get(link)
        


        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//span[contains(@class, 'prog-running-time')]")))
            running_time = browser.find_element_by_xpath("//span[contains(@class, 'prog-running-time')]").text
        except NoSuchElementException:
            running_time = ""
            if(verbose >= 3):
                print(">>> One of the programs had the empty duration field on " + link, flush=True)


        unavailable_link = ""
        unavailable_reason = ""
        program_transcript = ""

        try:
            no_transcript_text = browser.find_element_by_xpath("//p[contains(@class, 'intro state-message')]").text
            
            if(no_transcript_text == "Sorry!, there was a problem processing this broadcast"):
                try:
                    clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
                    browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
                    browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
                    clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
                except Exception as e:
                    if(verbose >= 2):
                        print("\n", flush=True)
                        print(">>> Did not manage to send the message. " + link + " will be reacquired.", flush=True)
                        print(str(e), flush=True)
                        print("\n", flush=True)
                    return -1 
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Problem processing the broadcast."
            elif(no_transcript_text == "Request to watch this programme" or "Sorry, this programme hasn't been broadcast yet"):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Program has to be requested."
                
            elif(no_transcript_text == "Sorry but this programme is not available."):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "The program is not available."                
                
            elif(no_transcript_text == "Sorry, this programme hasn't been broadcast yet"):
                transcript_text = "False"
                unavailable_link = link
                unavailable_reason = "Program has not yet been broadcasted."                
            else:
                print(">>> Program is not sure if transctipt exists on " + link, flush=True)               
        except NoSuchElementException:
            try:

                # 300 secounds for more confidence
                clickThroughToNewPage("//a[contains(@class, 'btn btn-standard float-right')]", browser, time_wait = 10, time_wait_stale = 5)
                
                WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'transcript-body')]")))

                def textLocator(webdriver):
                    return bool(len(webdriver.find_element_by_xpath("//div[contains(@class, 'transcript-body')]").text) != 0)

                try:
                    # 600 secounds for more confidence

                    WebDriverWait(browser, 600).until(textLocator)

                except TimeoutException:
                    ### case there is no transcript although there is a button
                    program_transcript_body = browser.find_element_by_xpath("//div[contains(@class, 'transcript-body')]")
                    if(program_transcript_body.text == ""):
                        try:
                            clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
                            browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
                            browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
                            clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
                        except Exception as e:
                            if(verbose >= 2):
                                print("\n", flush=True)
                                print(">>> Did not manage to send the message. " + link + " will be reacquired.", flush=True)
                                print(str(e), flush=True)
                                print("\n", flush=True)
                            raise StaleElementReferenceException("Did not manage to send the message.")
                        unavailable_link = link
                        unavailable_reason = "Program has an empty transcript field."


            except (NoSuchElementException, ElementNotVisibleException) as e:
                ### cases there is no button or the button is not visible
                try:
                    clickThroughToNewPage("//*[@id='link-report-problems']", browser, 5)
                    browser.find_element_by_xpath("//*[@id='report-type']/option[3]").click()
                    browser.find_element_by_xpath("//*[@id='report-description']").send_keys('Transcript text is missing.')
                    clickThroughToNewPage("//*[@id='btn-report-send']", browser, 5)
                except Exception as e:
                    if(verbose >= 2):
                        print("\n", flush=True)
                        print(">>> Did not manage to send the message. " + link + " will be reacquired.", flush=True)
                        print(str(e), flush=True)
                        print("\n", flush=True)
                    raise StaleElementReferenceException("Did not manage to send the message.")
                    
                if(verbose >= 3):
                    print(">>> Transcript text has not been found on " + link, flush=True)
                    print(str(e), flush = True)
                if(str(e) == "Message: Transcript body is empty.\n"):
                    unavailable_link = link
                    unavailable_reason = "Program has an empty transcript field."
                else:    
                    unavailable_link = link
                    unavailable_reason = "Program has no transcript button."


                

            return (True, False)[unavailable_reason in ["Program has an empty transcript field.", "Program has no transcript button."]]
    

    count = 0
    count_total = 0


    for bundle in request_links:

        
        if(use_database == True):
            transcriptid, link, reason = bundle
        else:
            link = bundle
        
        print(link)
        try:
            clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", browser)
        except Exception:
            pass

        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
            browser.find_element_by_id('institution-field').send_keys('University of Warwick')
            browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
            browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
        except TimeoutException:
            pass
 

    
        if(report_transcripts == True):

            found_transcript = get_link(link)

            if(found_transcript == True):
                if(use_database == True):
                    updateRequestedFix(transcriptid, 2)
                continue
            elif(found_transcript == False):
                if(use_database == True):
                    updateRequestedFix(transcriptid, 1)
                print("Count = " + str(count))
            else:
                if(use_database == True):
                    updateRequestedFix(transcriptid, -1)  
                continue
        else:
            browser.get(link)


        time.sleep(5)
        count += 1
        count_total += 1
        print("Processed " + str(count_total) + " of " + str(len(request_links)))
        if(count == 5):
            count = 0
            time.sleep(21600)
            
            browser.get(link)
            
            try:
                clickThroughToNewPage("//a[@class='btn btn-primary' and text()='Sign In']", browser)
            except Exception:
                pass
        
            try:
                WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@class='btn btn-primary btn-select']")))
                browser.find_element_by_id('institution-field').send_keys('University of Warwick')
                browser.find_element_by_xpath('//li[@data-idp="https://idp.warwick.ac.uk/idp/shibboleth"]').click()
                browser.find_element_by_xpath("//input[@class='btn btn-primary btn-select']").click()
            except TimeoutException:
                pass






def FactivaScrape(day, days_queue, data, params, locks_queue, browser, clickThroughToNewPage, convertDate, verbose, extract_first_paragraph, column_names, exitFlag, ocbrowser, saveAndEmptyData):


    
    day_page = convertDate(day)
    if(len(day_page[0])==1):
        day_page[0] = "0" + day_page[0]
    if(len(day_page[1])==1):
        day_page[1] = "0" + day_page[1]
    day_page = str(day_page[2]) + str(day_page[1]) + str(day_page[0]) 
    
    

    search_term = str(params[0])
    channel = str(params[3])


    url="https://global.factiva.com/en/sess/login.asp?XSID=S004cVk3HVpZXJyMTZyMTMtM96mMTImMtmm5Ff9R9apRsJpWVFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB"

    try:
        browser.get(url)
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="navmbm12"]/a')))
    except Exception as e:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not perform the search.", flush=True)
            print(str(e.__class__.__name__))
            print(str(e))
            print("\n", flush=True)
            locks_queue[2].release()
        return  
    
    
    
    url="https://global.factiva.com/sb/default.aspx?NAPC=S"
    
    try:
        browser.get(url)
        
    except Exception as e:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not perform the search.", flush=True)
            print(str(e.__class__.__name__))
            print(str(e))
            print("\n", flush=True)
            locks_queue[2].release()
        return  
    
    
    try:
    
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ftxCont"]')))

        if(browser.find_element_by_xpath('//*[@id="ftxCont"]').get_attribute('style')[-5:-1] == 'none'):
            browser.find_element_by_xpath('//*[@id="switchbutton"]/div').click()

        
        string = 'la=en and sn=' + str(source) + ' and date ' + str(day_page)
        
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ftx"]')))
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="ftx"]')))


        



        window = browser.find_element_by_xpath('//*[@id="ftx"]')
        window_id = window.get_attribute("id")

        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="ftx"]')))
        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.ID, window_id)))
        WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.ID, window_id)))
        window.click()
        window.send_keys(string)
        
        select = Select(browser.find_element_by_xpath('//*[@id="dr"]'))
        select.select_by_visible_text("All Dates")

        WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="btnSBSearch"]/div/span')))
        browser.find_element_by_xpath('//*[@id="btnSBSearch"]/div/span').click()

    except Exception as e:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not perform the search.", flush=True)
            print(str(e.__class__.__name__))
            print(str(e))
            print("\n", flush=True)
            locks_queue[2].release()
        return
    

#    time.sleep(9999)

    try:
        no_doc_found = browser.find_element_by_xpath('//*[@id="headlines"]').text
        if(no_doc_found==" No search results. This may be due to content restrictions on your academic account."):
            return
    except Exception:
        pass
    
    
    
    
    
    
    day_links = []
    
    
    day_sources = []
    data.append([])
    
    day_program_names = []
    data.append([])  
    
    day_dates = []
    data.append([])
    
    day_no_transcript_texts = []
    data.append([])
    
    day_program_transcripts = []
    data.append([])
    
    if(len(column_names) == 0):
            locks_queue[1].acquire()
            column_names.clear()
            column_names += ["Source", "Date", "Program Name", "Has Transcript", "Transcript"]
            locks_queue[1].release()


    
    try:
        while(1):

            WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
            results = browser.find_elements_by_class_name('enHeadline')
            
            time.sleep(30)
            
            for result in results:
                day_links.append(result.get_attribute("href"))


            try:
                clickThroughToNewPage("//a[contains(@class, 'nextItem')]", browser)
            except  NoSuchElementException:
                break
    except  Exception as e:
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Some Exception occured while obtaining links. " + day + " will be reacquired.", flush=True)
            print(e.__class__.__name__)
            print("\n", flush=True)
            locks_queue[2].release()
        return
                                                     

    
    def get_link(link, source):
    
        browser.get(link)
        source = source        
        date = day
        
#        try:
#            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "article enArticle")]')))
#            source = browser.find_element_by_xpath('//div[contains(@class, "article enArticle")]/div[6]').text
#        except NoSuchElementException:
#            source = ""
#            if(verbose >= 3):
#                locks_queue[2].acquire()
#                print(">>> One of the articles had the empty source field on " + day, flush=True)
#                locks_queue[2].release()
                

#        try:
#            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "article enArticle")]')))
#            date = browser.find_element_by_xpath('//div[contains(@class, "article enArticle")]/div[5]').text    
#        except NoSuchElementException:
#            date = ""
#            if(verbose >= 3):
#                locks_queue[2].acquire()
#                print(">>> One of the articles had the empty date on " + day, flush=True)
#                locks_queue[2].release()            

        try:
            WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="hd"]/span')))
            program_name = browser.find_element_by_xpath('//*[@id="hd"]/span').text
        except NoSuchElementException:
            program_name = ""
            if(verbose >= 3):
                locks_queue[2].acquire()
                print(">>> One of the programs had the empty name field on " + day, flush=True)
                locks_queue[2].release()



        program_transcript = ""

        try:
            WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//span[contains(@class, 'enHeadline')]")))
            if(browser.find_element_by_xpath("//span[contains(@class, 'enHeadline')]").text == "This is a deletion"):
                program_transcript = ""
            else:
                WebDriverWait(browser, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//p[contains(@class, 'articleParagraph enarticleParagraph')]")))
                program_transcript_list = browser.find_elements_by_xpath("//p[contains(@class, 'articleParagraph enarticleParagraph')]")
    
                for p in program_transcript_list:
                    program_transcript += p.text + " "    

        except Exception as e:
            if(verbose >= 2):
                locks_queue[2].acquire()
                print("\n", flush=True)
                print(">>> Stale element reference. " + day + " will be reacquired.", flush=True)
                print(str(e.__class__.__name__), flush=True)
                print(str(e), flush=True)
                print("\n", flush=True)
                locks_queue[2].release()
            locks_queue[0].acquire()
            days_queue.put(day)
            exitFlag[0] = 0
            locks_queue[0].release()
            browser.quit()
            return
        
        if(program_transcript != ""):
            transcript_text = "True"
        else:
            transcript_text = "False"


        day_sources.append(source)
        day_dates.append(date)
        day_program_names.append(program_name)
        day_no_transcript_texts.append(transcript_text)
        day_program_transcripts.append(program_transcript)

#        locks_queue[2].acquire()
#        print(day_sources[-1], flush=True)
#        print(day_dates[-1], flush=True)
#        print(day_program_names[-1], flush=True)
#        print(day_no_transcript_texts[-1], flush=True)
#        print(day_program_transcripts[-1], flush=True)
#        print('\n', flush=True)
#        locks_queue[2].release()
#        time.sleep(3)


    try:
        no_results = browser.find_element_by_xpath('//*[@id="headlineTabs"]/table[1]/tbody/tr/td/span[2]/a/span').text.replace("(", "").replace(")","").replace(",","")
    except Exception as e:
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Could not find the total number of links. " + day + " will be reacquired.", flush=True)
            print(str(e), flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        return

    
    
    if(len(day_links) != int(no_results)):
        if(verbose >= 2):
            locks_queue[2].acquire()
            print("\n", flush=True)
            print(">>> Wrong no. of links for the day: " + str(day), flush=True)
            print(">>> Ought to get " + no_results + " links. Got " + str(len(day_links)) + " links.", flush=True)
            print(">>> " + day + " will be reacquired.", flush=True)
            print("\n", flush=True)
            locks_queue[2].release()
        locks_queue[0].acquire()
        days_queue.put(day)
        exitFlag[0] = 0
        locks_queue[0].release()
        browser.quit()
        return



    day_links_queue = queue.Queue()
    day_links_queue.queue = queue.deque(day_links)

    while(not day_links_queue.empty()):
        link = day_links_queue.get()
        try:
            get_link(link, source)
            time.sleep(30)
        except (ConnectionRefusedError, MaxRetryError, TimeoutException) as e:
            day_links_queue.put(link)
            continue

    locks_queue[1].acquire()
    data[0] += day_sources
    data[1] += day_dates
    data[2] += day_program_names
    data[3] += day_no_transcript_texts
    data[4] += day_program_transcripts
    locks_queue[1].release()    






#BBCMakeRequests("BBC News", "54", ["2015"], 1, 2)

### headless_chrome = 1 causes some timeouterror -> set to 0
#BBCMakeRequests("BBC+News", "54", years = None, start_date = "1-jan-2015", end_date = "31-dec-2017", use_database = True, report_transcripts = True, headless_chrome = 0, verbose = 2)

    
##################### Use profiles for Bob don't use for Nexis /// 6 cores do not produce refusals of connection
##################### Nexis Scrape
#crawler = Scraper(NexisScrape, None, 1, headless_chrome=1, verbose=3, use_profiles = False, ocbrowser=True, extract_first_paragraph=False) 
#crawler.scrape(search_term = "Russia", start_date = "09-mar-2013", end_date = "09-mar-2013", source = "408506")
#
### Russia and ruble scrape
#crawler.scrape(search_term = "Russia", start_date = "15-feb-2014", end_date = "15-feb-2014", source = "6742")
#
#for year in ["2014", "2015"]:
#    crawler.scrapeYear(search_term = "ruble", year = year, source = "6742")    
#    crawler.scrapeYear(search_term = "Russia", year = year, source = "6742")
#    
#
#### Big Scrape
#sources = crawler.readSources("UK Newspaper list.xlsx", "Sheet1")
#
#for source in sources:    
#    for year in ["2013", "2014", "2015", "2016"]:
#        crawler.scrapeYear(search_term = "", year = year, source = str(source))


#################### BoB Scrape
crawler = Scraper(BoBScrape, BoBLogin, 1, headless_chrome=0, verbose=3, use_profiles = True, ocbrowser=True, extract_first_paragraph=False) 
#crawler.login()
#crawler.scrape(search_term = "BBC+News", start_date = "2-jun-2010", end_date = "2-jun-2010", source = "54")
#crawler.scrapeMonth(search_term = "BBC+News", month = "jul", year = "2016", source = "54")
sources = crawler.readSources("BoB List.xlsx", "Sheet1")


year_start = 2015
year_end = 2017
years = [str(year) for year in range(year_start, year_end)]
#crawler.scrapeYear(search_term = "BBC+News", year = "2020", source = str(54))
#crawler.scrapeMonth(search_term = "BBC+News", month = "may", year = "2016", source = str(54))
#crawler.saveAndEmptyData(os.getcwd() + "/Data/", "BBC+News" + " " + "may" + "-" + "2016" + " " + "54"+ ".csv")



#titles = ["Look+north+(north+east)", "North+west+tonight", "Look+north+(north)", "Bbc+london+news", "Newsline",
#"Southeast+today", "Wales+today", "Reporting+scotland", "Spotlight", "Points+west", "South+today",
#"East+midlands+today", "Midlands+today", "Look+east"]
#
#
#sources = ["54", "61", "65", "71557", "71558", "53", "71559", "71556", "59", "63", "66", "71560", "71519"]
#
#ss = "54"
#
#for s in sources[1:]:
#    ss += "&channel[]=" + s



for source in sources:
    for year in years:
        crawler.scrapeYear(search_term = "BBC+News", year = year, source = str(54))


#crawler.scrape(search_term = "BBC+News", start_date = "26-jan-2015", end_date = "26-jan-2015", source = "54", save_and_empty_data = True)

#april 1 to july 1 request news channel 4 channel 5 itv london

#for title in titles:
#        crawler.scrape(search_term = title, start_date = "1-jan-2015", end_date = "31-dec-2017", source = ss, save_and_empty_data = True)

##################### RTS Scrape
#crawler = Scraper(RTSScrape, None, 1, headless_chrome=False, verbose=3, use_profiles = True, ocbrowser=False, extract_first_paragraph=False) 
#
#
#for year in ["2017", "2018"]:
#        crawler.scrapeYear(search_term = "BBC+News", year = year, source = str("RTS"))

            
    
    
    
#################### Factiva Scrape
#crawler = Scraper(FactivaScrape, None, 1, headless_chrome=0, verbose=3, use_profiles = True, ocbrowser=True, extract_first_paragraph=False) 
#sources = crawler.readSources("Factiva List.xlsx", "Sheet1")
#
#year_start = 2006
#year_end = 2008
#years = [str(year) for year in range(year_start, year_end)]
#
#
##crawler.scrape(search_term = "", start_date = "23-sep-2006", end_date = "23-sep-2006", source = str(sources[0]))
#
#
#for source in sources:
#    for year in years:
#        crawler.scrapeYear(search_term = "", year = year, source = str(source))
    
    
 
    
    
