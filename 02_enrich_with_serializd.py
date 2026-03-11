import concurrent.futures
import json
import os
import random
import time

from bs4 import BeautifulSoup
from curl_cffi import requests
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

INPUT_FILE = os.path.join(DATA_DIR, "01_tmdb_details_fetched.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "02_data_enriched.jsonl")
FAILED_FILE = os.path.join(DATA_DIR, "02_serializd_failed_ids.json")

SERIALIZD_BASE = "https://www.serializd.com/show/"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.serializd.com/",
}

MAX_WORKERS = 4


def get_session():
    session = requests.Session(impersonate="chrome")
    session.headers.update(HEADERS)
    return session


def load_input_data():
    if not os.path.exists(INPUT_FILE):
        return {}
    data_map = {}
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if "id" in item:
                    data_map[item["id"]] = item
            except json.JSONDecodeError:
                continue
    return data_map


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


def fetch_single_serializd(session, tmdb_data):
    show_id = tmdb_data.get("id")
    url = f"{SERIALIZD_BASE}{show_id}"

    defaults = {
        "serializd_average_rating_5_scale": None,
        "serializd_average_rating_10_scale": None,
        "serializd_rating_count": None,
        "serializd_nanogenres": [],
    }

    for attempt in range(3):
        try:
            time.sleep(random.uniform(0.5, 1.5))

            response = session.get(url, timeout=15)

            if response.status_code == 404:
                tmdb_data.update(defaults)
                return show_id, tmdb_data, True, "404 Not Found (Skipped)"

            if response.status_code == 403:
                time.sleep(2 * (attempt + 1))
                continue

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if not next_data_script:
                if attempt < 2:
                    continue
                return show_id, None, False, "Missing __NEXT_DATA__ (Poss. Captcha)"

            next_json = json.loads(next_data_script.string)
            page_props_data = (
                next_json.get("props", {}).get("pageProps", {}).get("data", {})
            )

            schema_script = soup.find("script", type="application/ld+json")
            aggregate_rating_data = {}
            if schema_script:
                try:
                    aggregate_rating_data = json.loads(schema_script.string).get(
                        "aggregateRating", {}
                    )
                except json.JSONDecodeError:
                    pass

            def safe_float(v):
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return 0.0

            def safe_int(v):
                try:
                    return int(v)
                except (TypeError, ValueError):
                    return 0

            extracted_data = {
                "serializd_average_rating_5_scale": safe_float(
                    aggregate_rating_data.get("ratingValue")
                ),
                "serializd_average_rating_10_scale": page_props_data.get(
                    "averageRating", 0
                ),
                "serializd_rating_count": safe_int(
                    aggregate_rating_data.get("ratingCount")
                ),
                "serializd_nanogenres": [
                    {"id": item.get("id"), "name": item.get("name")}
                    for item in page_props_data.get("nanogenres", [])
                ],
            }

            defaults.update(extracted_data)
            tmdb_data.update(defaults)
            return show_id, tmdb_data, True, None

        except Exception as e:
            if attempt == 2:
                return show_id, None, False, str(e)
            time.sleep(1)

    return show_id, None, False, "Max Retries (403/Network)"


def enrich_all_shows():
    session = get_session()

    print("Loading input data...")
    input_data_map = load_input_data()
    if not input_data_map:
        print("No input data found.")
        return

    processed_ids = load_processed_ids()
    all_ids = set(input_data_map.keys())
    to_fetch_ids = list(all_ids - processed_ids)

    print(
        f"Total Input: {len(all_ids)} | Already Done: {len(processed_ids)} | Remaining: {len(to_fetch_ids)}"
    )

    if not to_fetch_ids:
        print("Nothing new to enrich.")
        return

    failed_ids = []

    count = 0
    total = len(to_fetch_ids)

    print(f"Starting enrichment with {MAX_WORKERS} threads (using curl_cffi)...")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f_out:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {
                executor.submit(
                    fetch_single_serializd, session, input_data_map[sid]
                ): sid
                for sid in to_fetch_ids
            }

            for future in concurrent.futures.as_completed(future_to_id):
                count += 1
                show_id = future_to_id[future]

                try:
                    sid, data, success, error_msg = future.result()

                    if success:
                        f_out.write(json.dumps(data, ensure_ascii=False) + "\n")
                    else:
                        print(f"  [!] Failed ID {show_id}: {error_msg}")
                        failed_ids.append(show_id)

                except Exception as exc:
                    print(f"  [!] Unhandled exception ID {show_id}: {exc}")
                    failed_ids.append(show_id)

                if count % 10 == 0 or count == total:
                    print(f"Progress: {count}/{total} processed.")
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
    enrich_all_shows()
