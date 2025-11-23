from datasets import load_dataset
from datetime import datetime, timezone
from tqdm import tqdm
import json
import sys

def main():
    # ---- 1. Define time window: 2018-01-01 to 2019-01-01 (UTC) ----
    start_dt = datetime(2018, 1, 1, tzinfo=timezone.utc)
    end_dt   = datetime(2019, 1, 1, tzinfo=timezone.utc)

    start_ts = int(start_dt.timestamp())
    end_ts   = int(end_dt.timestamp())

    print(f"Target Window: {start_dt} to {end_dt}")

    # ---- 2. Load Only 2018 Files (Recursive Search) ----
    print("Connecting to Hugging Face (searching recursively for 2018 files)...")
    
    try:
        # The "**/...parquet" pattern searches ALL subfolders for the files
        ds = load_dataset(
            "fddemarco/pushshift-reddit",
            split="train",
            streaming=True,
            data_files="**/*2018*.parquet" 
        )
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        print("Could not find 2018 files even with recursive search.")
        sys.exit(1)

    out_path = "r_news_2018.jsonl"
    count_news = 0
    scanned_count = 0

    print(f"Streaming data to {out_path}...")

    # ---- 3. Stream, Filter, and Flush ----
    with open(out_path, "w", encoding="utf-8", buffering=1) as f_out:
        
        for ex in tqdm(ds, desc="Posts Scanned", unit=" posts"):
            scanned_count += 1
            
            # 1. Check Timestamp 
            created_raw = ex.get("created_utc")
            if created_raw is None:
                continue

            try:
                created_ts = int(created_raw)
            except (ValueError, TypeError):
                continue
            
            # Filter logic
            if created_ts < start_ts:
                continue
            if created_ts >= end_ts:
                continue

            # 2. Check Subreddit (Case insensitive)
            subreddit = (ex.get("subreddit") or "").lower()
            if subreddit != "news":
                continue

            # 3. Save Data
            record = {
                "created_utc": created_ts,
                "subreddit": subreddit,
                "score": int(ex.get("score", 0)),
                "id": ex.get("id", ""),
                "title": ex.get("title", ""),
                "url": ex.get("url", ""),
                "selftext": ex.get("selftext", "")[:1000] 
            }

            f_out.write(json.dumps(record) + "\n")
            count_news += 1

            # Explicit flush
            if count_news % 50 == 0:
                f_out.flush()

    print(f"\nDone! Saved {count_news} posts to {out_path}")

if __name__ == "__main__":
    main()