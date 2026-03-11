import concurrent.futures
import json
import os
import time

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}

API_URL_DISCOVER = "https://api.themoviedb.org/3/discover/tv"
API_URL_DETAILS = "https://api.themoviedb.org/3/tv/{show_id}"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

DISCOVERED_FILE = os.path.join(DATA_DIR, "01_tmdb_discovered_raw.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "01_tmdb_details_fetched.jsonl")
FAILED_FILE = os.path.join(DATA_DIR, "01_tmdb_failed_ids.json")

MAX_PAGES_TO_DISCOVER = 500
DISCOVERY_SLEEP_TIME = 0.25
MAX_WORKERS = 12


def get_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=MAX_WORKERS + 2,
        pool_maxsize=MAX_WORKERS + 2,
    )
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def load_discovered_data():
    discovered_dict = {}
    if not os.path.exists(DISCOVERED_FILE):
        return discovered_dict

    with open(DISCOVERED_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                discovered_dict[item["id"]] = item
            except json.JSONDecodeError:
                continue
    return discovered_dict


def load_processed_ids():
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if "id" in data:
                        processed.add(data["id"])
                except json.JSONDecodeError:
                    continue
    return processed


def discover_all_tv_shows(session):
    print("Starting Discovery...")

    discovered_dict = load_discovered_data()
    initial_count = len(discovered_dict)
    print(f"Loaded {initial_count} existing shows from cache.")

    # params = {"sort_by": "vote_count.desc"}
    params = {"sort_by": "popularity.desc"}
    # params = {"sort_by": "vote_count.asc", "vote_count.gte": 10}

    for page in range(1, MAX_PAGES_TO_DISCOVER + 1):
        params["page"] = page
        try:
            response = session.get(API_URL_DISCOVER, params=params)
            response.raise_for_status()

            results = response.json().get("results", [])
            if not results:
                print(f"No more results at page {page}. Stopping.")
                break

            for show in results:
                discovered_dict[show["id"]] = show

            if page % 10 == 0:
                print(f"Discovered page {page}/{MAX_PAGES_TO_DISCOVER}")

            time.sleep(DISCOVERY_SLEEP_TIME)

        except Exception as e:
            print(f"Error discovering page {page}: {e}")

    all_shows = list(discovered_dict.values())
    with open(DISCOVERED_FILE, "w", encoding="utf-8") as f:
        for show in all_shows:
            f.write(json.dumps(show, ensure_ascii=False) + "\n")

    new_count = len(all_shows)
    print(
        f"Discovery Complete. Total Shows: {new_count} (New: {new_count - initial_count})"
    )


def fetch_single_show(session, show_id):
    url = API_URL_DETAILS.format(show_id=show_id)
    try:
        response = session.get(url)

        if response.status_code == 404:
            return show_id, None, False, "404 Not Found"

        if response.status_code == 200:
            return show_id, response.json(), True, None

        return show_id, None, False, f"Status {response.status_code}"

    except requests.RequestException as e:
        return show_id, None, False, str(e)


def fetch_all_details(session):
    discovered_dict = load_discovered_data()
    if not discovered_dict:
        print("No discovered data found. Run discovery first.")
        return

    processed_ids = load_processed_ids()

    all_ids = set(discovered_dict.keys())
    to_fetch_ids = list(all_ids - processed_ids)

    print(f"Total Discovered: {len(all_ids)}")
    print(f"Already Processed: {len(processed_ids)}")
    print(f"Remaining to fetch: {len(to_fetch_ids)}")

    if not to_fetch_ids:
        print("Nothing new to fetch.")
        return

    failed_ids = []

    print(f"Starting fetch with {MAX_WORKERS} threads...")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f_out:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {
                executor.submit(fetch_single_show, session, sid): sid
                for sid in to_fetch_ids
            }

            for i, future in enumerate(
                concurrent.futures.as_completed(future_to_id), 1
            ):
                show_id, data, success, error_msg = future.result()

                if success:
                    f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                else:
                    print(f"  [!] Failed ID {show_id}: {error_msg}")
                    failed_ids.append(show_id)

                if i % 50 == 0 or i == len(to_fetch_ids):
                    print(f"Progress: {i}/{len(to_fetch_ids)} fetched.")
                    f_out.flush()

    if failed_ids:
        print(f"Run complete. {len(failed_ids)} IDs failed.")
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            json.dump(failed_ids, f, indent=4)
    else:
        print("Run complete. No failures.")
        if os.path.exists(FAILED_FILE):
            os.remove(FAILED_FILE)


if __name__ == "__main__":
    sess = get_session()
    discover_all_tv_shows(sess)
    fetch_all_details(sess)
