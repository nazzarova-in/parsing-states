import os
import time
import json
import copy
import requests
from datetime import datetime
from dotenv import load_dotenv

from config import HEADERS, JSON_DATA_TEMPLATE
from cookies import get_fresh_cookies
from intervals import intervals
from states import states
from progress import save_progress, load_progress, clear_progress
from utils import upload_ndjson_to_sftp, move_file_to_production

load_dotenv()

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
AUTH = os.getenv("AUTHORIZATION")

headers = HEADERS.copy()
headers["authorization"] = AUTH

filename = f"all_data_{datetime.now():%Y-%m-%d_%H-%M-%S}.ndjson"
MAX_LINES = 10000
line_counter = 0

while True:
    try:
        progress = load_progress() or {}
        state_start = progress.get("state_index", 0)
        interval_start = progress.get("interval_index", 0)
        filename = progress.get("filename_only", filename)
        line_counter = progress.get("line_counter", 0)

        for s_idx, state in enumerate(states[state_start:], start=state_start):
            cookies = get_fresh_cookies(LOGIN, PASSWORD)
            json_data = copy.deepcopy(JSON_DATA_TEMPLATE)
            json_data["STATE"] = state

            for i_idx, (start, end) in enumerate(intervals[interval_start:], start=interval_start):
                json_data["FILING_DATE"]["start"] = start
                json_data["FILING_DATE"]["end"] = end

                try:
                    r = requests.post(
                        "https://biz.sosmt.gov/api/Records/businesssearch",
                        cookies=cookies,
                        headers=headers,
                        json=json_data
                    )
                    print(f"[{state}] {start[:10]} — {end[:10]} | Status {r.status_code}")
                except Exception as e:
                    print(f"Network error: {e}")
                    raise

                if r.status_code == 200:
                    rows = r.json().get("rows", {})
                    if not rows:
                        print("No data")
                    else:
                        for item in rows.values():
                            item["STATE"] = state
                            mode = "w" if line_counter == 0 else "a"
                            upload_ndjson_to_sftp(json.dumps(item, ensure_ascii=False) + "\n", filename, mode=mode)
                            line_counter += 1

                            if line_counter >= MAX_LINES:
                                move_file_to_production(filename)
                                filename = f"all_data_{datetime.now():%Y-%m-%d_%H-%M-%S}.ndjson"
                                line_counter = 0

                        print(f"Uploaded {len(rows)} rows")

                    save_progress(s_idx, i_idx + 1, filename, line_counter)
                    time.sleep(25)

                elif r.status_code == 429:
                    print("429 Too Many Requests — sleeping 45 minutes...")
                    save_progress(s_idx, i_idx, filename, line_counter)
                    time.sleep(2700)
                    raise Exception("Retry after 429")

                elif r.status_code == 524:
                    print("524 Timeout — refreshing cookies and retrying...")
                    cookies = get_fresh_cookies(LOGIN, PASSWORD)
                    time.sleep(10)
                    continue

                else:
                    print(f"Unexpected error {r.status_code}, retrying in 15 sec...")
                    save_progress(s_idx, i_idx, filename, line_counter)
                    time.sleep(15)
                    raise Exception("Retry after unexpected error")

            interval_start = 0
            print("Waiting 2 min before next state...")
            time.sleep(120)

        if line_counter:
            move_file_to_production(filename)

        clear_progress()
        print("Done — all data processed.")
        break

    except Exception as e:
        print(f"Runtime error: {e}")
        time.sleep(120)
