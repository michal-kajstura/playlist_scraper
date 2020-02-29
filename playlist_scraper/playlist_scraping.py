import json
from http import HTTPStatus

import requests
from bs4 import BeautifulSoup
from urllib.error import HTTPError


def scrape_playlistnet_playlist(playlist_url):
    soup = _get_soup(playlist_url)
    spotify_playlist_url = soup.find('iframe')['src']
    return _scrape_spotify_playlist(spotify_playlist_url)


def _scrape_spotify_playlist(spotify_url):
    soup = _get_soup(spotify_url)
    resource_node = soup.find('script', attrs={'id': 'resource'})
    spotify_resource = json.loads(resource_node.text)
    return spotify_resource


def _get_soup(url):
    response = requests.get(url)
    if response.status_code == HTTPStatus.OK:
        return BeautifulSoup(response.content, features='lxml')
    else:
        error = HTTPError(
            response.url,
            response.status_code,
            f'{response.status_code} {response.url}',
            response.headers,
            None
        )
        raise error
