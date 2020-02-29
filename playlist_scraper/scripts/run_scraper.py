from playlist_scraper.scrapers import PlaylistScraper

scraper = PlaylistScraper('playlists.csv', workers=7)
scraper.run_scraper()
