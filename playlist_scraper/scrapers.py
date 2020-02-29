from concurrent.futures.thread import ThreadPoolExecutor
from http import HTTPStatus
from queue import Queue, Empty
from threading import Thread
from csv import DictWriter

from urllib.error import HTTPError

from playlist_scraper.data_utils import extract
from playlist_scraper.playlist_scraping import scrape_playlistnet_playlist
from settings import PLAYLIST_NET_ID, TIMEOUT_IN_SECONDS

import requests
from collections import namedtuple


class LinkScraper:
    def __init__(self, queue):
        self.queue = queue
        self.url = PLAYLIST_NET_ID
        self.page_url_pair = namedtuple('PagePlaylist', ('page', 'url'))

    def run(self, start_page=1):
        def extract_url(playlist):
            return self.url + playlist['slug']

        page = start_page
        more = True
        while more:
            response = self._get_playlists_at_page(page)
            if response.status_code == HTTPStatus.OK:
                content = response.json()
                more = content['more']
                playlists = content['playlists']
                self._put_in_queue(map(extract_url, playlists), page)
            else:
                more = False
            page += 1

    def _get_playlists_at_page(self, page):
        url = self.url + 'ajax/playlists/loadMore'
        params = {'page': page, 'filter': 'most-played'}
        return requests.get(url, params=params)

    def _put_in_queue(self, playlists, page):
        for playlist_url in playlists:
            pair = self.page_url_pair(page, playlist_url)
            self.queue.put(pair)


class CSVWriter:
    def __init__(self, filename, queue):
        self.filename = filename
        self.queue = queue
        self.fieldnames = ['playlist_id', 'owner_id', 'playlist_name', 'track_id',
                           'track_name', 'artist_id', 'artist_name']

    def run(self):
        file = open(self.filename, 'w')
        writer = DictWriter(file, self.fieldnames)
        writer.writeheader()

        while True:
            try:
                track = self.queue.get(timeout=TIMEOUT_IN_SECONDS)
                writer.writerow(track)
            except Empty:
                break
            except Exception as e:
                print(e)

        file .close()


class PlaylistScraper:
    def __init__(self, filename, workers):
        self.playlist_queue = Queue()
        self.write_to_csv_queue = Queue()
        self.pool = ThreadPoolExecutor(max_workers=workers)
        self.link_scraper = LinkScraper(self.playlist_queue)
        self.csv_writer = CSVWriter(filename, self.write_to_csv_queue)

    def run_scraper(self):
        self._start_thread(self.link_scraper)
        self._start_thread(self.csv_writer)

        while True:
            try:
                url_to_scrape = self.playlist_queue.get(timeout=TIMEOUT_IN_SECONDS)
                job = self.pool.submit(self._scrape_playlist, url_to_scrape)
                job.add_done_callback(self._post_scrape)
            except Empty:
                break
            except Exception as e:
                print(e)
                continue

        self.playlist_queue.join()

    def _post_scrape(self, playlist_future):
        playlist = playlist_future.result()
        if playlist is None:
            return

        for track in playlist:
            if track is not None:
                self.write_to_csv_queue.put(track)

    @staticmethod
    def _start_thread(worker):
        worker_thread = Thread(target=worker.run)
        worker_thread.setDaemon(True)
        worker_thread.start()

    def _scrape_playlist(self, page_playlist_pair):
        playlist_url = page_playlist_pair.url
        page = page_playlist_pair.page

        if page % 5 == 0:
            print(page)

        try:
            spotify_dict = scrape_playlistnet_playlist(playlist_url)
            return extract(spotify_dict)
        except HTTPError as e:
            if e.code in (HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.BAD_GATEWAY):
                self.playlist_queue.put(page_playlist_pair)
                print(f'Putting {e.url} back')
            else:
                print(e)
            return None
