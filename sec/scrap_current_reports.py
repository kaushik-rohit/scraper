import requests
from bs4 import BeautifulSoup
import pandas as pd
from dateutil.parser import parse
import time
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from tqdm import tqdm
import pathlib
import os

BASE_URL = 'https://www.sec.gov/edgar/search/'
curr_dir = os.getcwd()
data_dir = os.path.join(curr_dir, 'data')
report_dir = '/media/rohit/ext_HD/current_reports'

profile = webdriver.FirefoxProfile()
profile.set_preference("browser.download.folderList", 2)
profile.set_preference("browser.download.manager.showWhenStarting", False)
profile.set_preference("browser.download.dir", data_dir)
profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
driver = webdriver.Firefox(firefox_profile=profile)

sec_comps = pd.read_csv('sec_left_comps.csv')
sec_comps = sec_comps.dropna(subset=['cik'])
sec_comps['cik'] = sec_comps['cik'].astype(int)
sec_comps = sec_comps.sort_values(by=['cik'])

def check_exists_by_xpath(xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        pass
    
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    
    return True

def check_if_clickable(xpath):
    element = driver.find_element_by_xpath(xpath)

    try:
        element.click()
    except WebDriverException:
        return False
    
    return True

def search_companies():
    rows = []
    # df = pd.read_csv('sec_comps_left_links.csv')

    # for index, row in df.iterrows():
    #     rows.append([row['cik'], row['conml'], row['year'], row['href']])
    
    for index in tqdm(range(1, len(sec_comps))):
        row = sec_comps.iloc[index]
        dt = row['datadate']
        if 'dec' in dt:
            year = row['year'] + 1
        else:
            year = row['year']

        try:
            driver.get(BASE_URL)
            driver.find_element_by_id('show-full-search-form').click()
            WebDriverWait(driver, 100).until(EC.visibility_of_element_located((By.XPATH, "//input[@id='keywords']")))
            driver.find_element_by_id('keywords').send_keys('"Merger Agreement"')
            driver.find_element_by_id('entity-full-form').send_keys('CIK {}'.format(str(row['cik']).zfill(10)))
            driver.find_element_by_id('show-filing-types').click()
            driver.find_element_by_id('category-filter-btn').click()
            driver.find_element_by_xpath('/html/body/div[5]/div/div/div[1]/div[2]/ul/li[2]').click()
            # WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//div[@id='fcbd-8-K']")))
            checkbox = driver.find_element_by_css_selector("label[for='fcb97']")
            ActionChains(driver).move_to_element_with_offset(checkbox, 1, 1).click().perform()
            driver.find_element_by_id('custom_forms_set').click()
            date_from = driver.find_element_by_id('date-from')
            ActionChains(driver).move_to_element(date_from).click().send_keys(Keys.BACKSPACE * 9).send_keys(Keys.DELETE * 2).send_keys('{}-01-01'.format(year)).send_keys(Keys.ENTER).perform()
            date_to = driver.find_element_by_id('date-to')
            ActionChains(driver).move_to_element(date_to).click().send_keys(Keys.BACKSPACE * 9).send_keys(Keys.DELETE * 2).send_keys('{}-12-31'.format(year)).send_keys(Keys.ENTER).perform()
            WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//a[@class='ui-state-default']")))
            WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='justify-content-center align-items-center searching-overlay']")))
            WebDriverWait(driver, 100).until(EC.element_to_be_clickable((By.XPATH, '//button[@id="search"]')))
            driver.find_element_by_id('search').click()

            anchors = driver.find_elements_by_xpath('/html/body/div[3]/div[2]/div[2]/table/tbody/tr/td[1]/a')
            # anchors = driver.find_elements_by_xpath('//a[contains(text(), "8-K (Current report)")]')
            nanchors = len(anchors)
            links = []
            
            print(row['conml'], nanchors)
            for i in range(1, nanchors):
                WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='justify-content-center align-items-center searching-overlay']")))
                anchor = driver.find_element_by_xpath('/html/body/div[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[1]/a')
                # anchor = anchors[i]

                txt = anchor.text
                if '8-K' not in txt:
                    continue

                anchor.click()
                link = driver.find_element_by_id('open-file').get_attribute('href')
                links.append(link)
                WebDriverWait(driver, 100).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="close"]')))
                driver.find_element_by_class_name('close').click()
                time.sleep(5)
    
            for link in links:
                print(link)
                rows.append([row['cik'], row['conml'], row['year'], link])
            
            if len(rows) != 0:
                print('saving')
                df = pd.DataFrame(rows, columns=['cik', 'conml', 'year', 'href'])
                df.to_csv('sec_comps_left_links.csv', index=False)
        except:
            for link in links:
                print(link)
                rows.append([row['cik'], row['conml'], row['year'], link])
            print('saving')
            df = pd.DataFrame(rows, columns=['cik', 'conml', 'year', 'href'])
            df = df.drop_duplicates()
            df.to_csv('sec_comps_left_links.csv', index=False)
        
        time.sleep(20)

def download_reports():
    df = pd.read_csv('sec_comps_left_links2.csv')

    for index in tqdm(range(len(df))):
        row = df.iloc[index]
        save_path = '/media/rohit/ext_HD/current_reports/{}/{}'
        if os.path.isdir(save_path.format(row['conml'], row['year'])):
            continue

        pathlib.Path(save_path.format(row['conml'], row['year'])).mkdir(parents=True, exist_ok=True)
        n = len(os.listdir(save_path.format(row['conml'], row['year'])))
        save_file = '/media/rohit/ext_HD/current_reports/{}/{}/{}.htm'.format(row['conml'], row['year'], n+1)

        driver.get(row['href'])
        text = driver.page_source
        with open(save_file, 'w') as f:
            f.write(text)
        time.sleep(1)

download_reports()
driver.close()
