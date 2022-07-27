import argparse
import json
import os
import urllib.parse
from getpass import getpass

import musicbrainzngs
import musicbrainzngs.musicbrainz
import requests
from bs4 import BeautifulSoup
from musicbrainzngs.musicbrainz import _do_mb_put


WEB_LOGIN_URL = "https://musicbrainz.org/login"
NEW_COLLECTION_URL = "https://musicbrainz.org/collection/create"


# musicbrainzngs only supports adding releases to collections for some reason.
# So to add release-groups, we have to monkey-patch in this method. Maybe I'll
# make a PR.
def add_release_groups_to_collection(collection, release_groups=[]):
    """Add release groups to a collection.
    Collection and release groups should be identified by their MBIDs
    """
    # XXX: Maximum URI length of 16kb means we can only submit ~400 release groups at once.
    chunk_size = 200
    for index in range(0, len(release_groups), chunk_size):
        chunk = release_groups[index:index + chunk_size + 1]
        print(chunk[-1])
        release_group_list = ";".join(chunk)
        _do_mb_put(f"collection/{collection}/release-groups/{release_group_list}")

def discog_api_url_to_www(api_url):
    parsed = urllib.parse.urlparse(api_url)
    fixed = parsed._replace(netloc=parsed.netloc.replace("api", "www"))
    if parsed.path.startswith("/releases"):
        fixed = fixed._replace(path=parsed.path.replace("releases", "release", 1))
    elif parsed.path.startswith("/masters"):
        fixed = fixed._replace(path=parsed.path.replace("masters", "master", 1))
    elif parsed.path.startswith("/artists"):
        fixed = fixed._replace(path=parsed.path.replace("artists", "artist", 1))
    return urllib.parse.urlunparse(fixed)

def search_release_group_by_artists(discog_entry, artist_mbids):
    def release_group_info(release_group):
        return {
            "name": release_group['title'],
            "artist": release_group['artist-credit-phrase'],
            "id": release_group['id']
        }

    rg_results = []
    for artist_mbid in artist_mbids:
        search_results = musicbrainzngs.search_release_groups(discog_entry["release"]["name"], arid=artist_mbid)
        release_group_list = search_results["release-group-list"]
        if int(release_group_list[0]["ext:score"]) > 95:
            return [release_group_info(release_group_list[0])]
        
        rg_results.extend([release_group_info(rg) for rg in release_group_list if int(rg["ext:score"]) >= 75])
    
    return sorted(rg_results, key=lambda rg: int(rg["ext:score"]), reverse=True)

def lookup_mbid_by_discog_url(url, type_name):
    uri = discog_api_url_to_www(url)
    includes = [f"{type_name}-rels"]
    try:
        result = musicbrainzngs.browse_urls(uri, includes=includes)
    except musicbrainzngs.musicbrainz.ResponseError:
        return None

    relation_list = result["url"][f"{type_name.replace('-', '_')}-relation-list"]
    connection = next((relation for relation in relation_list if relation["type"] == "discogs"), None)
    return connection[type_name]["id"] if connection else None

def lookup_artist_mbids(discog_entry):
    return [lookup_mbid_by_discog_url(artist["url"], "artist") for artist in discog_entry["artists"]]

def lookup_release_mbid(discog_entry):
    return lookup_mbid_by_discog_url(discog_entry["release"]["url"], "release")

def _get_master_mbid(master_url):
    return lookup_mbid_by_discog_url(master_url, "release-group")

def lookup_master_mbid(discog_entry):
    master_id = None
    master_url = discog_entry["release"].get("master_url")
    if master_url:
        master_id = _get_master_mbid(master_url)
    if not master_id:
        release_id = lookup_release_mbid(discog_entry)
        if release_id:
            result = musicbrainzngs.get_release_by_id(release_id, includes=["release-groups"])
            master_id = result["release"]["release-group"]["id"]
    if not master_id:
        artist_ids = lookup_artist_mbids(discog_entry)
        results = search_release_group_by_artists(discog_entry, artist_ids)
        if len(results) == 1:
            master_id = results[0]["id"]
        else:
            print(f"Candidates for {discog_entry['release']['name']} by {discog_entry['artist']['name']}:")
            for info in results:
                print(f"{info['name']} by {info['artist']}: {info['id']}")
    return master_id

def load_discogs_releases(import_dir, filename):
    with open(os.path.join(import_dir, filename)) as releases_file:
        return json.load(releases_file)

def _get_collection_types(session):
    get_resp = session.get("https://musicbrainz.org/collection/create")

    page = BeautifulSoup(get_resp.text, 'html.parser')
    dropdown = page.find(id="id-edit-list.type_id")
    last_parent = None
    collection_type_dict = {}
    for option in dropdown.find_all("option"):
        name = option.text
        if name.startswith(u"\u00A0"):
            name = f"{last_parent} - {name.strip()}"
        else:
            last_parent = name
        collection_type_dict[name.lower()] = option.attrs["value"]
    return collection_type_dict

def _new_collection(session, name, collection_type):
    collection_type_dict = _get_collection_types(session)
    payload = {
        "edit-list.name": name,
        "edit-list.type_id": collection_type_dict[collection_type.lower()],
        "edit-list.description": ""
    }

    session.post("https://musicbrainz.org/collection/create", data=payload)

def _web_login(session):
    get_resp = session.get(WEB_LOGIN_URL)

    page = BeautifulSoup(get_resp.text, 'html.parser')

    payload = {
        "csrf_session_key": page.find("input", attrs={"name": "csrf_session_key"}).attrs["value"],
        "csrf_token": page.find("input", attrs={"name": "csrf_token"}).attrs["value"],
        "username": musicbrainzngs.musicbrainz.user,
        "password": musicbrainzngs.musicbrainz.password
    }

    session.post(WEB_LOGIN_URL, data=payload)

def create_collection(name, collection_type):
    session = requests.Session()
    _web_login(session)
    _new_collection(session, name, collection_type)

def mb_collection(name):
    collections = musicbrainzngs.get_collections()["collection-list"]
    return next((collection for collection in collections if collection["name"] == name), None)

def import_to_collection(collection_name, releases):
    collection = mb_collection(collection_name)
    if not collection:
        create_collection(collection_name, "release group collection")
        collection = mb_collection(collection_name)
        if not collection:
            print(f"There was an issue creating {collection_name}. Skipping...")
            return
    release_groups = [master_mbid for entry in releases if (master_mbid := lookup_master_mbid(entry))]
    add_release_groups_to_collection(collection["id"], release_groups)

def import_ratings(import_dir):
    discogs_ratings = load_discogs_releases(import_dir, "release-ratings.json")
    release_group_ratings = {master_mbid: (int(entry["rating"]) * 20) for entry in discogs_ratings if (master_mbid := lookup_master_mbid(entry))}
    musicbrainzngs.submit_ratings(release_group_ratings=release_group_ratings)

def import_to_wishlist(import_dir, wishlist_name):
    releases = load_discogs_releases(import_dir, "wantlist.json")
    import_to_collection(wishlist_name, releases)

def import_to_owned(import_dir, owned_name):
    releases = load_discogs_releases(import_dir, "collection.json")
    import_to_collection(owned_name, releases)

def init_client(username, password, email):
    if not password:
        password = getpass("Musicbrainz password: ")
    musicbrainzngs.auth(username, password)
    musicbrainzngs.set_useragent("import-from-discogs", "0.1", email)

def import_to_musicbrainz(import_dir, owned_name, wishlist_name, load_ratings, load_owned, load_wishlist):
    if load_ratings:
        import_ratings(import_dir)
    if load_owned:
        import_to_owned(import_dir, owned_name)
    if load_wishlist:
        import_to_wishlist(import_dir, wishlist_name)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("import_dir", help="The directory containing your Discogs export files.")
    parser.add_argument("username", help="Your Musicbrainz username.")
    parser.add_argument("--password",
                        help="Your Musicbrainz password. If omitted, you'll be prompted for it.")
    parser.add_argument("--email",
                        help="To be included in the user agent for identification purposes.")
    parser.add_argument("--owned-name", default="Owned",
                        help=("Name for the collection of release groups you own. If it does not "
                              "exist, it will be created. Default: %(default)s"))
    parser.add_argument("--wishlist-name", default="Wishlist",
                        help=("Name for the collection of release groups you wish to own. If it "
                              "does not exist, it will be created. Default: %(default)s"))
    parser.add_argument("--no-ratings", action="store_false", dest="load_ratings",
                        help="Disable importing your release group ratings.")
    parser.add_argument("--no-owned", action="store_false", dest="load_owned",
                        help="Disable importing your owned list.")
    parser.add_argument("--no-wishlist", action="store_false", dest="load_wishlist",
                        help="Disable importing your wishlist.")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    init_client(args.username, args.password, args.email)
    import_to_musicbrainz(args.import_dir, args.owned_name, args.wishlist_name, args.load_ratings, args.load_owned, args.load_wishlist)
