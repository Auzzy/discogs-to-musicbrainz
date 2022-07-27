import argparse
import json
import os.path
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup


USER_AGENT = "discogs-export/1.0"

URL_BASE = "https://www.discogs.com"
API_BASE = "https://api.discogs.com"
RATINGS_URL_FMT = f"{URL_BASE}/users/ratings/{{username}}?page={{pagenum}}&limit={{per_page}}"
RELEASE_URL_FMT = f"{API_BASE}/releases/{{release_id}}?token={{token}}"
IDENTITY_URL_FMT = f"{API_BASE}/oauth/identity?token={{token}}"
WANTLIST_URL_FMT = f"{API_BASE}/users/{{username}}/wants?page={{pagenum}}&per_page={{per_page}}&token={{token}}"
COLLECTION_URL_FMT = f"{API_BASE}/users/{{username}}/collection/folders/0/releases?page={{pagenum}}&per_page={{per_page}}&token={{token}}"


class _DiscogsApiBase:
    def __init__(self, user_agent):
        self.headers = {"User-Agent": user_agent}

    def _get(self, url):
        resp = requests.get(url, headers=self.headers)
        if resp.status_code == 429:
            print("Making requests too quickly. Taking a break, then continuing...")
            time.sleep(60)
            print("Continuing...")
            return self._get(url)
        return resp

class DiscogsHtmlApi(_DiscogsApiBase):
    def __init__(self, user_agent, cookie):
        super().__init__(user_agent)

        self.headers["cookie"] = cookie
        
    def ratings(self, username, pagenum, per_page=500):
        url = RATINGS_URL_FMT.format(username=username, pagenum=pagenum, per_page=per_page)
        return self._get(url).text

class DiscogsRestApi(_DiscogsApiBase):
    def __init__(self, user_agent, token):
        super().__init__(user_agent)

        self.token = token

    def release(self, release_id):
        url = RELEASE_URL_FMT.format(release_id=release_id, token=self.token)
        return self._get(url).json()
    
    def identity(self):
        url = IDENTITY_URL_FMT.format(token=self.token)
        return self._get(url).json()

    def list_collection(self, username, pagenum, per_page=500):
        url = COLLECTION_URL_FMT.format(username=username, token=self.token, pagenum=pagenum, per_page=per_page)
        return self._get(url).json()

    def list_wantlist(self, username, pagenum, per_page=500):
        url = WANTLIST_URL_FMT.format(username=username, token=self.token, pagenum=pagenum, per_page=per_page)
        return self._get(url).json()


class DiscogsHtmlClient:
    def __init__(self, user_agent, cookie):
        self._api = DiscogsHtmlApi(user_agent, cookie)

        self.username = DiscogsHtmlClient._extract_username_from_cookie(cookie)

    @staticmethod
    def _extract_username_from_cookie(cookie):
        cookie_dict = dict([argstr.strip().strip(";").split("=", 1) for argstr in cookie.split(" ")])
        return cookie_dict.get("ck_username")

    # The href entry on the activity page includes the name of the release. The API version just includes the ID.
    @staticmethod
    def url_short_form(url_path):
        pieces = url_path.split("/")
        pieces[-1] = pieces[-1].split("-", 1)[0]
        return "/".join(pieces)

    @staticmethod
    def _parse_release_info(row):
        release_anchor = row.select('span.release_title a[href^="/release"]').pop()
        release_path = DiscogsHtmlClient.url_short_form(release_anchor.get('href'))
        return {
            "name": release_anchor.get_text(),
            "url": f"{URL_BASE}{release_path}"
        }

    @staticmethod
    def _parse_artists_info(row):
        artists_info = []
        for artist_anchor in row.select('span.release_title a[href^="/artist"]'):
            artists_info.append({
                "name": artist_anchor.get_text(),
                "url": f"{URL_BASE}{DiscogsHtmlClient.url_short_form(artist_anchor.get('href'))}"
            })
        return artists_info

    @staticmethod
    def _parse_release_ratings_html(page_html):
        page = BeautifulSoup(page_html, 'html.parser')
        
        info = []
        for row in page.select("table.release_list_table tbody tr"):
            rating_tag = row.select("span.rating").pop()

            entry = {
                "artists": DiscogsHtmlClient._parse_artists_info(row),
                "release": DiscogsHtmlClient._parse_release_info(row),
                "rating": rating_tag["data-value"]
            }
            info.append(entry)

        return info

    def _iter_pages(self, api_func, process_page_func):
        all_items = []
        pagenum = 1
        while True:
            page_text = api_func(self.username, pagenum)

            page_items = process_page_func(page_text)
            if not page_items:
                break

            all_items.extend(page_items)
            pagenum += 1

        return all_items

    def export_release_ratings(self, export_dir):
        rating_info = self._iter_pages(self._api.ratings, self._parse_release_ratings_html)

        print("Writing result...")
        os.makedirs(export_dir, exist_ok=True)
        dest_filepath = os.path.join(export_dir, "release-ratings.json")
        with open(dest_filepath, 'w') as discogs_ratings_file:
            json.dump(rating_info, discogs_ratings_file)

        return dest_filepath

class DiscogsRestClient:
    def __init__(self, user_agent, token):
        self._api = DiscogsRestApi(user_agent, token)
        
        self.username = self._api.identity()["username"]

    def release_master_url(self, release_id):
        release_info = self._api.release(release_id)
        return release_info.get("master_url")
    
    def _iter_pages(self, api_func, root_key):
        pagenum = 1
        while True:
            resp_json = api_func(self.username, pagenum)
            for entry in resp_json[root_key]:
                yield entry

            if pagenum == resp_json["pagination"]["pages"]:
                break

            pagenum += 1

    def _collection(self):
        return self._iter_pages(self._api.list_collection, "releases")

    def _wantlist(self):
        return self._iter_pages(self._api.list_wantlist, "wants")

    @staticmethod
    def _extract_release_info(release_json):
        return {
            "name": release_json["title"],
            "url": release_json["resource_url"],
            "master_url": release_json["master_url"]
        }

    @staticmethod
    def _extract_artist_info(artist_json):
        return {
            "name": artist_json["name"],
            "name_variation": artist_json["anv"],
            "url": artist_json["resource_url"]
        }

    @staticmethod
    def _collect_release_info(release_list):
        release_info = []
        for item in release_list:
            release_info.append({
                "artists": [DiscogsRestClient._extract_artist_info(artist_json) for artist_json in item["basic_information"]["artists"]],
                "release": DiscogsRestClient._extract_release_info(item["basic_information"])
            })
        return release_info

    def export_collection(self, export_dir):
        collection_info = DiscogsRestClient._collect_release_info(self._collection())

        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, "collection.json")
        with open(filepath, 'w') as collection_file:
            json.dump(collection_info, collection_file)

        return filepath

    def export_wantlist(self, export_dir):
        wantlist_info = DiscogsRestClient._collect_release_info(self._wantlist())

        os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, "wantlist.json")
        with open(filepath, 'w') as wantlist_file:
            json.dump(wantlist_info, wantlist_file)

        return filepath


def _get_master_url(release_info):
    release_url = release_info["release"]["url"]
    release_path = urllib.parse.urlparse(release_url).path
    release_id = release_path.split('/')[-1]
    master_url = rest_client.release_master_url(release_id)
    return {**relase_info, "master_url": master_url}

def export_release_ratings(html_client, rest_client, export_dir, include_master):
    ratings_filepath = html_client.export_release_ratings(export_dir)

    if include_master:
        with open(ratings_filepath) as ratings_file:
            ratings_json = json.load(ratings_file)

        updated_ratings_json = []
        for entry in ratings_json:
            updated_ratings_json.append(_get_master_url(entry))

            # Throttle the requests, since we'll likely be making a lot.
            time.sleep(0.75)
        
        with open(ratings_filepath, 'w') as ratings_file:
            ratings_json = json.dump(updated_ratings_json, ratings_file)

def export(user_agent, cookie, token, include_master, export_dir):
    html_client = DiscogsHtmlClient(user_agent, cookie)
    rest_client = DiscogsRestClient(user_agent, token)

    export_release_ratings(html_client, rest_client, export_dir, include_master)
    rest_client.export_collection(export_dir)
    rest_client.export_wantlist(export_dir)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("cookie", help="Your Discogs cookie, retrieved by logging in via the web and grabbing it from your Network tools.")
    parser.add_argument("token", help="Your Discogs user token.")
    parser.add_argument("--include-ratings-master", action="store_true",
            help="Get the master of each rated release. This requires an extra rate-limited request, but will make import into Musicbrainz faster.")
    parser.add_argument("--export-dir", default=os.path.join(os.getcwd(), "discogs-export"),
            help="The directory to place the exported data. Default: %(default)s")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    export(USER_AGENT, args.cookie, args.token, args.include_ratings_master, args.export_dir)
