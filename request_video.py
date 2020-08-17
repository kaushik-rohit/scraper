import requests
import queue


class VideoInfo:
    def __init__(self, link):
        self.link


class VideoRequest:

    def __init__(self):
        self.queue = queue.Queue()  # the queue to hold video request
        self.request_every = 7*60
        self.request_made = False  # at the setup we have not request any video

    def login(self):
        pass

    def can_make_request(self):
        pass

    def request_videos(self):
        link = self.queue.get()
        request_link = '{}&request=1'.format(link)
        resp = requests.get(request_link)
        print(resp.text)
