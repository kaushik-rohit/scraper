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
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from tqdm import tqdm
import pathlib
import os

BASE_URL = 'https://www.sec.gov/edgar/search/'
curr_dir = os.getcwd()
data_dir = os.path.join(curr_dir, 'unmatched_companies')

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
ciks = sec_comps['cik'].unique()
sec_comps = sec_comps.loc[sec_comps['matched'] == 0]
unmatched_names = sec_comps['conml'].unique()

def parse_table():
    rws = driver.find_elements_by_xpath("/html/body/div[3]/div[2]/div[2]/table/tbody/tr")
    print(len(rws))
    cols = driver.find_elements_by_xpath("/html/body/div[3]/div[2]/div[2]/table/tbody/tr[1]")
    print(len(cols))
    rows = []
    for i in range(1, len(rws)):
        row = [0,0,0,0]
        flag = 1
        for j in range(1, 5):
            d = driver.find_element_by_xpath("//tr["+str(i)+"]/td["+str(j)+"]").text.strip()
            if d == '':
                flag=0
                break
            row[j-1] = d
        if flag:
            rows.append(row)
    
    if len(rows) == 0:
        return
    df = pd.DataFrame(rows, columns=['form', 'filed', 'reporting for', 'filing entity'])
    print(df)


def search_companies():
    df = pd.read_csv('unmatched_companies_links.csv')
    print(df.columns)
    # df = df.drop(['Unnamed: 0'], axis=1)

    rows = df.values.tolist()
    # rows = []
    for index in tqdm(range(len(sec_comps))):
        row = sec_comps.iloc[index]
        name = '"{}" "Annual report"'.format(row['conml'])
        year = row['year'] + 1

        if len(df.loc[(df['conml'] == row['conml']) & (df['year'] == year)]) > 0:
            continue
        
        try:
            driver.get(BASE_URL)
            driver.find_element_by_id('show-full-search-form').click()
            WebDriverWait(driver, 100).until(EC.visibility_of_element_located((By.XPATH, "//input[@id='keywords']")))
            driver.find_element_by_id('keywords').send_keys(name)
            date_from = driver.find_element_by_id('date-from')
            ActionChains(driver).move_to_element(date_from).click().send_keys(Keys.BACKSPACE * 8).send_keys(Keys.DELETE * 2).send_keys('{}-01-01'.format(year)).send_keys(Keys.ENTER).perform()
            date_to = driver.find_element_by_id('date-to')
            ActionChains(driver).move_to_element(date_to).click().send_keys(Keys.BACKSPACE * 9).send_keys(Keys.DELETE * 2).send_keys('{}-12-31'.format(year)).send_keys(Keys.ENTER).perform()
            WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//a[@class='ui-state-default']")))
            WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='justify-content-center align-items-center searching-overlay']")))
            WebDriverWait(driver, 100).until(EC.element_to_be_clickable((By.XPATH, '//button[@id="search"]')))
            driver.find_element_by_id('search').click()

            driver.implicitly_wait(60)

            anchors = driver.find_elements_by_xpath('/html/body/div[3]/div[2]/div[2]/table/tbody/tr/td[1]/a')
            nanchors = len(anchors)
            links = []

            print(nanchors)
            for i in range(1, nanchors):
                print(i)
                WebDriverWait(driver, 100).until(EC.invisibility_of_element_located((By.XPATH, "//div[@class='justify-content-center align-items-center searching-overlay']")))
                anchor = driver.find_element_by_xpath('/html/body/div[3]/div[2]/div[2]/table/tbody/tr['+str(i)+']/td[1]/a')
                txt = anchor.text
                if not('10-K' in txt or '20-F' in txt):
                    continue
                anchor.click()
                link = driver.find_element_by_id('open-file').get_attribute('href')
                links.append(link)
                WebDriverWait(driver, 100).until(EC.element_to_be_clickable((By.XPATH, '//button[@class="close"]')))
                driver.find_element_by_class_name('close').click()
            for link in links:
                rows.append([row['conml'], row['year'], link])
            #parse_table()
            time.sleep(10)
            if(len(rows) == 0):
                continue
        except:
            continue
        df = pd.DataFrame(rows, columns=['conml', 'year', 'href'])
        df.to_csv('unmatched_companies_links.csv', index=False)

def download_reports():
    df = pd.read_csv('unmatched_companies_links.csv')

    for index in tqdm(range(len(df))):
        row = df.iloc[index]
        save_path = './unmatched_companies/{}/{}'
        pathlib.Path(save_path.format(row['conml'], row['year'])).mkdir(parents=True, exist_ok=True)
        n = len(os.listdir(save_path.format(row['conml'], row['year'])))
        save_file = './unmatched_companies/{}/{}/{}.htm'.format(row['conml'], row['year'], n+1)

        driver.get(row['href'])
        text = driver.page_source
        with open(save_file, 'w') as f:
            f.write(text)

search_companies()
driver.close()
