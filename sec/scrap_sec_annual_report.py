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

BASE_URL = 'https://www.sec.gov/edgar/browse/?CIK={}'
curr_dir = os.getcwd()
data_dir = os.path.join(curr_dir, 'data')
report_dir = '/media/rohit/ext_HD/annual_reports'

profile = webdriver.FirefoxProfile()
profile.set_preference("browser.download.folderList", 2)
profile.set_preference("browser.download.manager.showWhenStarting", False)
profile.set_preference("browser.download.dir", data_dir)
profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
driver = webdriver.Firefox(firefox_profile=profile)

sec_comps = pd.read_csv('sec_comps.csv')
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

def scrap_filings_list():

    for index in tqdm(range(len(sec_comps))):
        row = sec_comps.iloc[index]
        
        if row['cik'] in [1044906, 39341, 773753, 1280728]:
            continue
        # if the filing is already downloaded skip this cik
        if os.path.isfile(os.path.join(data_dir, '{}.csv'.format(row['cik']))):
            print('{} already downloaded'.format(row['cik']))
            continue

        url = BASE_URL.format(row['cik'])
        date = parse(row['datadate'])
        print(url)

        year = date.year
        if date.month == 12:
            year = date.year + 1
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="dt-button buttons-csv buttons-html5 btn btn-sm btn-primary"]')))
            driver.find_element_by_xpath('//button[@class="dt-button buttons-csv buttons-html5 btn btn-sm btn-primary"]').click()
            time.sleep(10)
            df = pd.read_csv(os.path.join(data_dir, 'EDGAR Entity Landing Page.csv'))
            df.to_csv(os.path.join(data_dir, '{}.csv'.format(row['cik'])), index=False)
            os.remove(os.path.join(data_dir, 'EDGAR Entity Landing Page.csv'))
        except:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'investor-toolkit-text')))
            toolkit_btn = driver.find_element_by_id('investor-toolkit-text')
            print(toolkit_btn.text)
            if toolkit_btn.text == 'Investor Toolkit: On':
                hover = ActionChains(driver).move_to_element(toolkit_btn)
                hover.perform()
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'investor-toolkit-toggle')))
                driver.find_element_by_id('investor-toolkit-toggle').click()
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="dt-button buttons-csv buttons-html5 btn btn-sm btn-primary"]')))

            driver.find_element_by_xpath('//button[@class="dt-button buttons-csv buttons-html5 btn btn-sm btn-primary"]').click()
            time.sleep(10)
            df = pd.read_csv(os.path.join(data_dir, 'EDGAR Entity Landing Page.csv'))
            df.to_csv(os.path.join(data_dir, '{}.csv'.format(row['cik'])), index=False)
            os.remove(os.path.join(data_dir, 'EDGAR Entity Landing Page.csv'))

paper_format = [
    737572, 
    913059, 
    49906, 
    717826, 
    765245, 
    3124, 
    57083, 
    109821, 
    715153,
    753762, 
    756620, 
    756894, 
    805260, 
    807198,
    813810,
    813811,
    824046,
    842294,
    842519,
    854088,
    857471,
    877799,
    882505,
    885012,
    887028,
    887153,
    889132,
    842021,
    887225,
    892450,
    898427,
    888746,
    906338,
    906522,
    908732,
    910631,
    912505,
    912958,
    913785,
    917333,
    926864,
    900268, 
    904851,
    929058,
    929987,
    930309,
    932470,
    939187,
    946770,
    1004155,
    1000177,
    1001290,
    1001474,
    1001576,
    1002242,
    1003470,
    1016118,
    1016837,
    1038583,
    1041799,
    1046649,
    1012477,
    1013626,
    1015650,
    1026291,
    1037333,
    1038143,
    1047716,
    1048098,
    1048515,
    1054487,
    1060964,
    1066119,
    1069336,
    1061736,
    1066117,
    1070304,
    1071321,
    1072397,
    1073404,
    1080259,
    1080259,
    1089642,
    1094517,
    1096061,
    1096200,
    1110646,
    1113866,
    1114700,
    1117399,
    1122135,
    1123658,
    1123661,
    1126113,
    1127051,
    1135951,
    1144967,
    1158967,
    1159510,
    1159512
]

ciks_to_redownload = [
    1711269,
    1725526,
    1730168,
    1734107,
    1739940,
    1744489,
    1748790,
    1792044
]

def scrap_annual_reports():
    ciks = sec_comps['cik'].unique()
    for cik in tqdm(ciks):
        years = sec_comps.loc[sec_comps['cik'] == cik]['year'].unique()
        min_year = min(years)
        max_year = max(years)

        path = os.path.join(os.path.join(data_dir, '{}.csv'.format(cik)))

        if cik not in ciks_to_redownload:
            continue

        if not os.path.isfile(path):
            continue
        
        filings = pd.read_csv(path)
        filings = filings.loc[filings['Form description'].str.contains('Annual')]
        filings = filings.loc[(filings['Form type'] == '10-K') | (filings['Form type'] == '20-F')]

        print('getting filings from {}'.format(cik))
        
        def download(url, expand=False):
            driver.get(url)
            if expand:
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'btnViewAllFilings')))
                driver.find_element_by_id('btnViewAllFilings').click()
            
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'filingDateFrom')))
            date_from = driver.find_element_by_id('filingDateFrom')
            date_from.clear()
            date_from.send_keys('{}-{:02d}-{:02d}'.format(dt.year, dt.month, dt.day))
            date_to = driver.find_element_by_id('filingDateTo')
            date_to.clear()
            date_to.send_keys('{}-{:02d}-{:02d}'.format(dt.year, dt.month, dt.day))
            date_to.send_keys(Keys.ENTER)
            search = driver.find_element_by_id('searchbox')
            search.clear()
            search.send_keys('Annual')
            search.send_keys(Keys.ENTER)
            href = driver.find_element_by_xpath("/html/body/main/div[5]/div/div[3]/div[3]/div[2]/table/tbody/tr/td[2]/div/a[1]").get_attribute('href')
            if href.endswith('.paper'):
                return ''
            print(href)
            driver.get(href)
            time.sleep(30)
            try:
                driver.find_element_by_id('menu-dropdown-link').click()
                driver.find_element_by_id('form-information-html').click()
                windhd = driver.window_handles
                current = driver.current_window_handle
                for w in windhd:
                    if w != current:
                        driver.switch_to(w)
                        break
            except:
                print('menu not found')
                pass
            text = driver.execute_script("return document.documentElement.outerHTML;")
            return text
        
        for index, row in filings.iterrows():
            url = BASE_URL.format(cik)
            dt = parse(row['Filing date'])
            dt2 = parse(row['Reporting date'])
            
            save_path = os.path.join(report_dir, '{}'.format(cik))
            pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)
            save_file = os.path.join(save_path, '{}.html'.format(dt2.year))

            # check if file was already downloaded and if yes skip
            if os.path.isfile(save_file):
                print('already downloaded {}'.format(save_file))
                continue
            
            try:
                text = download(url, expand=True)
            except:
                time.sleep(10)
                print('error getting filing from {}'.format(url))
                text = download(url, expand=False)

            if text == '':
                time.sleep(10)
                continue
            with open(save_file, 'w') as f:
                f.write(text) 

            time.sleep(10)

scrap_annual_reports()
