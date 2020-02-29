def extract(spotify_resource):
    data = _extract_playlist_data(spotify_resource)
    for track in data['tracks']:
        if track is None:
            return None
        yield {
            'playlist_id': data['playlist_id'],
            'playlist_name': data['playlist_name'],
            'owner_id': data['owner_id'],
            'track_id': track['track_id'],
            'track_name': track['track_name'],
            'artist_id': track['artists'][0]['artist_id'],
            'artist_name': track['artists'][0]['artist_name']
        }


def _extract_playlist_data(spotify_resource):
    return {
        'playlist_id': spotify_resource['id'],
        'playlist_name': spotify_resource['name'],
        'owner_name': spotify_resource['owner']['display_name'],
        'owner_id': spotify_resource['owner']['id'],
        'tracks': (_extract_track_data(track)
                   for track in spotify_resource['tracks']['items'])
    }


def _extract_track_data(track_dict):
    track_dict = track_dict['track']

    # Handle removed tracks
    if track_dict is None:
        return None
    return {
        'track_id': track_dict['id'],
        'track_name': track_dict['name'],
        'track_popularity': track_dict['popularity'],
        'artists': [_extract_artist_data(artist)
                    for artist in track_dict['artists']],
        'album_id': track_dict['album']['id'],
        'album_name': track_dict['album']['name'],
        'album_release_date': track_dict['album']['release_date'],
        'track_duration': track_dict['duration_ms']
    }


def _extract_artist_data(artist_dict):
    return {
        'artist_id': artist_dict['id'],
        'artist_name': artist_dict['name'],
    }
