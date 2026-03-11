import argparse
import json
import os

def main(input_path, output_path, min_rating_count):
    shows = []
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    shows.append(json.loads(line))
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_path}'.")
        return
    except json.JSONDecodeError:
        print(
            f"Error: Could not decode JSONL from '{input_path}'. Check if it's valid JSON Lines."
        )
        return

    print(
        f"Processing shows from '{input_path}' with a minimum of {min_rating_count} ratings..."
    )

    processed_shows = []
    for show in shows:
        rating_count = show.get("serializd_rating_count") or 0
        if rating_count >= min_rating_count and show.get("id"):
            processed_shows.append(
                {
                    "name": show.get("name"),
                    "link": f"https://www.serializd.com/show/{show['id']}",
                    "serializd_average_rating": show.get(
                        "serializd_average_rating_5_scale"
                    ),
                    "serializd_rating_count": show.get("serializd_rating_count"),
                    "status": show.get("status"),
                    "type": show.get("type"),
                    "first_air_date": show.get("first_air_date"),
                    "last_air_date": show.get("last_air_date"),
                    "number_of_episodes": show.get("number_of_episodes"),
                    "number_of_seasons": show.get("number_of_seasons"),
                    "languages": [
                        lang.get("english_name")
                        for lang in show.get("spoken_languages", [])
                        if lang.get("english_name")
                    ],
                    "genres": [
                        g.get("name") for g in show.get("genres", []) if g.get("name")
                    ],
                    "nanogenres": [
                        g.get("name")
                        for g in show.get("serializd_nanogenres", [])
                        if g.get("name")
                    ],
                }
            )

    sorted_list = sorted(
        processed_shows,
        key=lambda x: x.get("serializd_average_rating") or 0,
        reverse=True,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        for show in sorted_list:
            f.write(json.dumps(show, ensure_ascii=False) + "\n")

    print(f"Successfully processed and sorted {len(sorted_list)} show(s).")
    print(f"Final ranked list saved to '{output_path}'.")


if __name__ == "__main__":
    DATA_DIR = "data"
    
    parser = argparse.ArgumentParser(
        description="Filter, format, and sort enriched TV show data to produce a final ranked JSONL list."
    )
    parser.add_argument(
        "--input",
        default=os.path.join(DATA_DIR, "02_data_enriched.jsonl"),
        help=f"Input JSONL file (default: {os.path.join(DATA_DIR, '02_data_enriched.jsonl')})",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(DATA_DIR, "03_shows_ranked_final.jsonl"),
        help=f"Output JSONL file (default: {os.path.join(DATA_DIR, '03_shows_ranked_final.jsonl')})",
    )
    parser.add_argument(
        "--min-ratings",
        type=int,
        default=0,
        help="The minimum number of Serializd ratings a show must have to be included.",
    )

    args = parser.parse_args()
    main(
        input_path=args.input,
        output_path=args.output,
        min_rating_count=args.min_ratings,
    )
