import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import calendar
import pandas as pd
import time

class Story:

    def __init__(self, title, href):
        self.title = title
        self.href = href
        self.transcript = None
        self.date = None

BASE_URL = 'https://web.archive.org/'


# def get_all_stories(url):
#     page = requests.get(url)

#     soup = BeautifulSoup(page.content, "html.parser")

#     main_contents = soup.find(id='container-top-stories-without-splash')

#     all_stories = main_contents.find_all('a', {'class': 'story'})

#     results = []

#     for story in all_stories:
#         s = Story(story.text.strip(), BASE_URL + story.get('href'))
#         results.append(s)

#     return results

def get_all_stories(url):
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    main_contents = soup.find(id='main-content')

    all_stories = main_contents.find_all('a', {'class': 'story'})

    results = []

    for story in all_stories:
        s = Story(story.text.strip(), BASE_URL + story.get('href'))
        results.append(s)

    return results

def get_all_stories2(url):
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    main_contents = soup.find('div', {'class': 'container', 'role': 'main'})

    if main_contents is None:
        main_contents = soup.find('div', {'role': 'main'})
    
    all_stories = main_contents.find_all('a', {'class': 'title-link'})

    results = []

    for story in all_stories:
        s = Story(story.text.strip(), BASE_URL + story.get('href'))
        results.append(s)

    return results

def test_scraping_stories():
    url = 'http://web.archive.org/web/20150109235604/http://www.bbc.com/news/uk/'
    stories = get_all_stories(url)
    for story in stories:
        print(story.title, story.href)

def scrap_story_transcript(stories):
    for story in stories:
        print(story.href)
        try:
            page = requests.get(story.href)
            soup = BeautifulSoup(page.content, 'html.parser')
        except:
            print('error getting story')

        try:
            story_body = soup.find('div', {'class':'story-body'})
            paragraphs = story_body.find_all('p')
        except:
            continue

        transcript = ''

        for para in paragraphs:
            transcript = transcript + para.text

        story.transcript = transcript


def iterate_wayback_machine(start_date, end_date, savename=None):
    curr_date = start_date
    delta = timedelta(days=1)

    if savename is None:
        print('provide a savename for csv file')
    
    rows = []
    endpoint = 'http://archive.org/wayback/available?url=bbc.com/news/politics&timestamp={}{:02d}{:02d}'
    while(curr_date <= end_date):
        print(curr_date.year, curr_date.month, curr_date.day)
        r = requests.get(endpoint.format(curr_date.year, curr_date.month, curr_date.day))
        if r.status_code != 200:
            print('not archived')
            continue

        json_response = r.json()
        try:
            url = json_response['archived_snapshots']['closest']['url']
            print(url)
        except:
            time.sleep(10)
            r = requests.get(endpoint.format(curr_date.year, curr_date.month, curr_date.day))
            json_response = r.json()
            url = json_response['archived_snapshots']['closest']['url']
        try:
            stories = get_all_stories(url)
        except:
            stories = get_all_stories2(url)
        
        # scrap_story_transcript(stories)

        rows.append([curr_date.year, curr_date.month, curr_date.day, len(stories)])

        curr_date += delta
        time.sleep(1)
        res_df = pd.DataFrame(rows, columns=['year', 'month', 'day', 'n_links'])
        res_df.to_csv(savename, index=False)

# test_scraping_stories()

start_date = datetime(2014, 1, 1)
end_date = datetime(2018, 12, 31)
savename = 'links_count.csv'
iterate_wayback_machine(start_date, end_date, savename)
