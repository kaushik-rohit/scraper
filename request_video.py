import requests


def request_video(link):
    request_link = '{}&request=1'.format(link)
    resp = requests.get(request_link)
    print(resp.text)
