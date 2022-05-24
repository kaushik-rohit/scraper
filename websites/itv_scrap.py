import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import multiprocessing as mp
import calendar
import pandas as pd
import time

class Story:

    def __init__(self, title, href, transcript=None, date=None):
        self.title = title
        self.href = href
        self.transcript = transcript
        self.date = date

BASE_URL = 'https://web.archive.org/'

def get_main_stories(soup):
    
    main_contents = soup.find(id='stream')
    # print(main_contents)
    all_stories = main_contents.find_all('article', {'class': 'update text-update'})

    if all_stories is None:
        all_stories = main_contents.find_all('article', {'class': 'update'})

    # print(all_stories)
    results = []

    for story in all_stories:
        header_anchor = story.find('header').find('a')
        title = header_anchor.text
        href = header_anchor.get('href')
        time = story.find('time', {'class': 'time-ago'})
        dt = time.get('datetime')
        paras = story.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
        
        s = Story(title, BASE_URL + href, text.strip(), dt)
        results.append(s)

    return results

def get_side_stories(href):
    page = requests.get(href)
    soup = BeautifulSoup(page.content, "html.parser")
    try:
        res = get_main_stories(soup)
    except:
        return []
    # time.sleep(10)
    return res

def get_all_stories(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    results.extend(get_main_stories(soup))

    side_stories = soup.find_all('li', {'class': 'story'})
    hrefs = []
    for s in side_stories:
        href = s.find('a').get('href')
        href = BASE_URL + href
        hrefs.append(href)
    
    pool = mp.Pool(8)
    ret = pool.map(get_side_stories, hrefs)
    # for side_story in side_stories:
    #     href = side_story.find('a').get('href')
    #     href = BASE_URL + href
    #     page = requests.get(href)
    #     soup = BeautifulSoup(page.content, "html.parser")
    #     results.extend(get_main_stories(soup))
    #     time.sleep(10)
    pool.close()

    for r in ret:
        results.extend(r)

    return results

def get_story_text(url):
    page = requests.get(url)
    try:
        soup = BeautifulSoup(page.content, "html.parser")
        article = soup.find('article', {'class': 'update'})

        # print(all_stories)
        results = []

        title = article.find('header').find('h1').text
        paras = article.find_all('p')

        text = ''
        for para in paras:
            text = text + ' ' + para.text
    except:
        return []
    
    s = Story(title, BASE_URL + url, text.strip())
    results.append(s)

    return results

def get_all_stories2(url):
    results = []
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    top_articles = soup.find_all('article', {'class': 'top-articles__item-content'})
    hrefs = []
    for article in top_articles:
        href = article.find('a').get('href')
        href = BASE_URL + href
        hrefs.append(href)
    
    all_stories = soup.find(id='stream')

    articles = all_stories.find_all('article', {'class': 'update'})

    for a in articles:
        header = a.find('header')
        if header is None:
            continue
        h2 = header.find('h2')
        href = h2.find('a').get('href')
        href = BASE_URL + href
        hrefs.append(href)
    
    pool = mp.Pool(8)
    ret = pool.map(get_story_text, hrefs)
    pool.close()

    for r in ret:
        results.extend(r)

    return results


def iterate_wayback_machine(start_date, end_date, savename=None):
    curr_date = start_date
    delta = timedelta(days=1)

    if savename is None:
        print('provide a savename for csv file')
    
    rows = []
    stories = []
    endpoint = 'http://archive.org/wayback/available?url=itv.com/news&timestamp={}{:02d}{:02d}'
    while(curr_date <= end_date):
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
        
        stories.extend(get_all_stories2(url))
        
        for story in stories:
            rows.append([curr_date, story.title, story.href, story.transcript])

        curr_date += delta
        time.sleep(30)

    res_df = pd.DataFrame(rows, columns=['date', 'program', 'link', 'transcript'])
    res_df = res_df.drop_duplicates(subset=['program', 'link'])
    res_df.to_csv(savename, index=False)


years = [2016]

for year in years:
    for month in range(1, 10):
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, calendar.monthrange(year, month)[1])
        savename = 'ITV+Website {}-{} 57.csv'.format(calendar.month_abbr[month].lower(), year)
        iterate_wayback_machine(start_date, end_date, savename)
