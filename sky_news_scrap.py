
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import multiprocessing as mp
import calendar
import pandas as pd
import time
import sys

max_rec = 0x100000
sys.setrecursionlimit(max_rec)

class Story:

    def __init__(self, title, href, transcript=None, date=None):
        self.title = title
        self.href = href
        self.transcript = transcript
        self.date = date

BASE_URL = 'https://web.archive.org/'

def get_story_text(url):
    try:
        page = requests.get(url)
    except:
        print('error getting page {}'.format(url))
        return None

    try:
        soup = BeautifulSoup(page.content, "html.parser")
        main = soup.find('div', {'id': 'main'})
        pagetitle = main.find('div', {'class': 'pageTitle'})

        title = pagetitle.find('h1').text

        articleText = main.find('div', {'id': 'articleText'})

        paras = articleText.find_all('p')
    except:
        return None

    text = ''
    for para in paras:
        text = text + ' ' + para.text
    
    s = Story(title, BASE_URL + url, text.strip())

    return s

def get_left_stories(left):
    hrefs = []

    latest_stories = left.find('section', {'id': 'latest_stories'})
    analysis = left.find('section', {'id': 'analysis'})
    more_top_stories = left.find('section', {'id': 'more_top_stories'})
    latest_secondary = latest_stories.find('div', {'id': 'latest_secondary'})
    latest_tertiary = latest_stories.find('div', {'id': 'latest_tertiary'})

    def get_articles(section):
        articles = section.find_all('article')

        for article in articles:
            href = article.find('a').get('href')
            href = BASE_URL + href
            hrefs.append(href)


    sections = [latest_stories, analysis, more_top_stories, latest_secondary, latest_tertiary]
    
    for section in sections:
        get_articles(section)

    return hrefs


def get_right_stories(right):
    hrefs = []
    trending = right.find('section', {'id': 'trending'})

    articles = trending.find_all('article')

    for article in articles:
        href = article.find('a').get('href')
        href = BASE_URL + href
        hrefs.append(href)

    return hrefs

def get_all_stories(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    left = soup.find('div', {'class': 'left'})
    right = soup.find('div', {'class': 'right'})
    hrefs = []

    hrefs.extend(get_left_stories(left))
    
    pool = mp.Pool(8)
    ret = pool.map(get_story_text, hrefs)
    pool.close()

    for r in ret:
        if r is None:
            continue
        results.append(r)

    return results


def get_story_text2(url):
    if 'video' in url or 'gallery' in url:
        return None
    print(url)
    try:
        page = requests.get(url)
    except:
        print('error getting page {}'.format(url))
        return None

    try:
        soup = BeautifulSoup(page.content, "html.parser")
        main = soup.find('div', {'class': 'main'})
        section = main.find('section', {'class': 'story__content'})

        title = section.find('p', {'class': 'story__intro'}).text

        paras = section.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text

        s = Story(title, BASE_URL + url, text.strip())
        return s
    except:
        pass
    
    try:
        soup = BeautifulSoup(page.content, "html.parser")
        main = soup.find('div', {'class': 'main'})
        section = main.find('div', {'class': 'story__content'})

        title = section.find('p', {'class': 'story__intro'}).text

        paras = section.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
        s = Story(title, BASE_URL + url, text.strip())
        return s
    except:
        pass

    try:
        soup = BeautifulSoup(page.content, "html.parser")
        section = soup.find('div', {'class': 'content-column'})

        title = section.find('p', {'class': 'story__intro'}).text

        paras = section.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
        s = Story(title, BASE_URL + url, text.strip())
        return s
    except:
        pass

    try:
        soup = BeautifulSoup(page.content, "html.parser")
        section = soup.find('div', {'class': 'sky-component-story-article__body'})

        title = soup.find('p', {'class': 'sky-component-story-article__intro'}).text

        paras = section.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
        s = Story(title, BASE_URL + url, text.strip())
        return s
    except:
        pass

    try:
        soup = BeautifulSoup(page.content, "html.parser")
        section = soup.find('div', {'class': 'sdc-news-story-article__body'})

        title = soup.find('p', {'class': 'sdc-news-story-article__intro'}).text

        paras = section.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
        s = Story(title, BASE_URL + url, text.strip())
        return s
    except:
        return None


def get_all_stories2(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    
    hrefs = []

    articles = soup.find_all('article', {'class': 'section-top-stories'})

    for article in articles:
        lists = article.find_all('li')

        for _list in lists:
            href = _list.find('a').get('href')
            href = BASE_URL + href
            hrefs.append(href)

    classes = ['section-highlights', 'section-analysis', 'section-top-stories-more', 'section-trending-stories']

    for clss in classes:
        print(clss)
        try:
            article = soup.find('article', {'class': clss})
            lists = article.find_all('li')
        except:
            article = soup.find('div', {'class': clss})
            lists = article.find_all('li')

        for lis in lists:
            href = lis.find('a').get('href')
            href = BASE_URL + href
            hrefs.append(href)
    
    hrefs = list(set(hrefs))
    pool = mp.Pool(8)
    ret = pool.map(get_story_text2, hrefs)
    pool.close()

    for r in ret:
        if r is None:
            continue
        results.append(r)

    return results

def get_all_stories3(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    
    hrefs = []

    article = soup.find('div', {'id': 'component-top-stories'})

    lists = article.find_all('a')

    for _list in lists:
        href = _list.get('href')
        href = BASE_URL + href
        hrefs.append(href)

    classes = ['component-highlights-secondary', 'component-analysis', 'component-top-stories-secondary', 'component-trending']

    for clss in classes:
        print(clss)
        try:
            article = soup.find('div', {'id': clss})
            lists = article.find_all('a')
        except:
            article = soup.find('div', {'class': clss})
            lists = article.find_all('a')

        for lis in lists:
            href = lis.get('href')
            href = BASE_URL + href
            hrefs.append(href)
    print(len(hrefs))
    hrefs = list(set(hrefs))
    pool = mp.Pool(8)
    ret = pool.map(get_story_text2, hrefs)
    pool.close()

    for r in ret:
        if r is None:
            continue
        results.append(r)

    return results

def get_all_stories4(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    
    hrefs = []

    articles = soup.find_all('div', {'class': 'sdc-site-tiles'})

    for article in articles:
        lists = article.find_all('a')

        for _list in lists:
            href = _list.get('href')
            href = BASE_URL + href
            hrefs.append(href)

    classes = ['site-commercial-features-box', 'sdc-site-tiles sdc-site-tiles--alt4', 'sdc-site-trending', 'sdc-site-load-more']

    for clss in classes:
        print(clss)
        try:
            article = soup.find('div', {'id': clss})
            lists = article.find_all('a')
        except:
            article = soup.find('div', {'class': clss})
            lists = article.find_all('a')

        for lis in lists:
            href = lis.get('href')
            href = BASE_URL + href
            hrefs.append(href)
    print(len(hrefs))
    hrefs = list(set(hrefs))
    pool = mp.Pool(8)
    ret = pool.map(get_story_text2, hrefs)
    pool.close()

    for r in ret:
        if r is None:
            continue
        results.append(r)

    return results
def iterate_wayback_machine(start_date, end_date, savename=None):
    curr_date = start_date
    delta = timedelta(days=1)

    if savename is None:
        print('provide a savename for csv file')
    
    rows = []
    stories = []
    endpoint = 'http://archive.org/wayback/available?url=news.sky.com/uk&timestamp={}{:02d}{:02d}'
    while(curr_date <= end_date):
        if not ((curr_date.month ==12 and curr_date.day in [3,5,6,7,9,11,13,15,16,19,20,22,23,25,26,27,30]) or 
        (curr_date.month == 11 and curr_date.day in [4,5,12,14,18,20,25])):
            curr_date += delta
            continue

        print('downloading for {}/{}/{}'.format(curr_date.year, curr_date.month, curr_date.day))
        print(endpoint.format(curr_date.year, curr_date.month, curr_date.day))
        r = requests.get(endpoint.format(curr_date.year, curr_date.month, curr_date.day))
        if r.status_code != 200:
            print('not archived')
            curr_date += delta
            continue

        json_response = r.json()
        try:
            url = json_response['archived_snapshots']['closest']['url']
            print(url)
        except:
            time.sleep(120)
            r = requests.get(endpoint.format(curr_date.year, curr_date.month, curr_date.day))
            json_response = r.json()
            if 'closest' not in json_response['archived_snapshots']:
                curr_date += delta
                continue

            url = json_response['archived_snapshots']['closest']['url']
        
        try:
            stories.extend(get_all_stories2(url))
        except:
            stories.extend(get_all_stories4(url))
        for story in stories:
            rows.append([curr_date, story.title, story.href, story.transcript])
        # print(len(rows))
        curr_date += delta
        time.sleep(10)

    res_df = pd.DataFrame(rows, columns=['date', 'program', 'link', 'transcript'])
    res_df = res_df.drop_duplicates(subset=['program', 'link'])
    res_df.to_csv(savename, index=False)


years = [2018]

for year in years:
    for month in range(12, 13):
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
        savename = 'SKYNews+Website {}-{} 58.csv'.format(calendar.month_abbr[month].lower(), year)
        iterate_wayback_machine(start_date, end_date, savename)
