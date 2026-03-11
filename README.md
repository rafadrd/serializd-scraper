


# TV show ranking pipeline

This project contains Python scripts that download TV show metadata from the TMDB API, extract user ratings from Serializd, and output a sorted JSON Lines file.

## Requirements

* Python 3.12
* A TMDB API key
* `uv` for dependency management

## Setup

1. Create a `.env` file in the project root directory.
2. Add your TMDB API key to the file:
   ```text
   TMDB_API_KEY=your_api_key
   ```
3. Install dependencies using `uv`:
   ```bash
   uv sync
   ```

## Usage

The pipeline consists of three scripts that must be executed sequentially. All output files are saved to a local `data` directory.

### 1. Fetch TMDB data

```bash
uv run 01_fetch_tmdb_data.py
```
This script queries the TMDB API for TV shows sorted by popularity. It saves the raw discovery data to `01_tmdb_discovered_raw.jsonl` and the detailed show data to `01_tmdb_details_fetched.jsonl`. Failed IDs are logged to `01_tmdb_failed_ids.json`.

### 2. Enrich with Serializd data

```bash
uv run 02_enrich_with_serializd.py
```
This script reads the fetched TMDB details and requests the corresponding show pages on Serializd. It extracts the 5-point scale rating, 10-point scale rating, total rating count, and nanogenres from the page's JSON data. The output is saved to `02_data_enriched.jsonl`. The script uses `curl_cffi` to manage TLS fingerprinting.

### 3. Build ranked list

```bash
uv run 03_build_ranked_list.py --min-ratings 100
```
This script filters the enriched dataset based on a minimum rating count threshold. It formats the data and sorts the shows by their Serializd average rating in descending order. The final output is written to `03_shows_ranked_final.jsonl`.

Command line arguments:
* `--input`: Path to the enriched JSONL file (default: `data/02_data_enriched.jsonl`).
* `--output`: Path for the final sorted JSONL file (default: `data/03_shows_ranked_final.jsonl`).
* `--min-ratings`: The minimum number of ratings required for a show to be included in the final list (default: 0).
