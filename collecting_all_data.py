import json
import time
import requests
from dotenv import load_dotenv
import os
import copy
from datetime import datetime
from config import HEADERS, JSON_DATA_TEMPLATE

from progress import save_progress, load_progress, clear_progress
from utils import upload_ndjson_to_sftp, move_file_to_production
from cookies import get_fresh_cookies
from intervals import intervals
from states import states


load_dotenv()

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
AUTHORIZATION = os.getenv("AUTHORIZATION")

headers = HEADERS.copy()
headers['authorization'] = AUTHORIZATION

json_data = copy.deepcopy(JSON_DATA_TEMPLATE)


timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename_only = f'all_data_{timestamp}.ndjson'

MAX_LINES_PER_FILE = 10000
line_counter = 0
has_written_data = False


while True:
    try:
        progress = load_progress()
        if progress:
            state_start = progress["state_index"]
            interval_start = progress["interval_index"]
            filename_only = progress["filename_only"]
            line_counter = progress["line_counter"]
            print(f"Resuming from state index {state_start}, interval index {interval_start}")
        else:
            state_start = 0
            interval_start = 0

        for s_idx, state in enumerate(states):
            if s_idx < state_start:
                continue

            cookies = get_fresh_cookies(LOGIN, PASSWORD)
            json_data['STATE'] = state

            interval_start_idx = interval_start if s_idx == state_start else 0

            for i_idx, (start, end) in enumerate(intervals):
                if i_idx < interval_start_idx:
                    continue

                json_data["FILING_DATE"]["start"] = start
                json_data["FILING_DATE"]["end"] = end

                response = requests.post(
                    'https://biz.sosmt.gov/api/Records/businesssearch',
                    cookies=cookies,
                    headers=headers,
                    json=json_data
                )

                print(f"Request period: {start[:10]} to {end[:10]} — status {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    rows = data.get("rows", {})

                    if rows:
                        rows_list = list(rows.values())
                        total_rows = len(rows_list)
                        current_index = 0

                        while current_index < total_rows:
                            remaining_space = MAX_LINES_PER_FILE - line_counter
                            batch = rows_list[current_index:current_index + remaining_space]

                            ndjson_buffer = ""
                            for item in batch:
                                item["STATE"] = state
                                ndjson_buffer += json.dumps(item, ensure_ascii=False) + "\n"

                            mode = 'w' if line_counter == 0 else 'a'
                            upload_ndjson_to_sftp(ndjson_buffer, filename_only, mode=mode)
                            print(f"Uploaded batch of {len(batch)} rows to {filename_only}")

                            line_counter += len(batch)
                            has_written_data = True
                            current_index += remaining_space

                            if line_counter >= MAX_LINES_PER_FILE:
                                print("File limit reached, moving to production...")
                                move_file_to_production(filename_only)

                                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                                filename_only = f'all_data_{timestamp}.ndjson'
                                line_counter = 0
                                has_written_data = False


                    else:
                        print(f"No data for period {start[:10]} to {end[:10]}")

                    if i_idx + 1 < len(intervals):
                        save_progress(s_idx, i_idx + 1, filename_only, line_counter)
                    else:
                        save_progress(s_idx + 1, 0, filename_only, line_counter)
                    print(f"Progress saved: state {s_idx}, interval {i_idx}")

                    print("Pausing 20 seconds ...")
                    time.sleep(20)

                else:
                    print(f"Request error {response.status_code} for period {start[:10]} to {end[:10]}")
                    if response.status_code == 429:
                        print("Got 429 — sleeping for 30 minutes...")
                        time.sleep(1800)
                        save_progress(s_idx, i_idx, filename_only, line_counter)
                        break

                    elif response.status_code == 524:
                        print("Got 524 — server timeout, refreshing cookies and retrying this interval...")
                        cookies = get_fresh_cookies(LOGIN, PASSWORD)
                        time.sleep(10)
                        continue

                    else:
                        print("Other error — sleeping 15 seconds and retrying this interval...")
                        save_progress(s_idx, i_idx, filename_only, line_counter)
                        time.sleep(15)
                        break

            print(f"Finished state {state}. Sleeping for 1 minutes before next state...")
            time.sleep(60)

        if line_counter > 0:
            print("Final file is not empty. Moving to production...")
            move_file_to_production(filename_only)
        else:
            print("Final file is empty. Nothing to move.")

        clear_progress()
        print("Progress cleared — all data processed successfully.")
        break

    except Exception as e:
        print(f"Runtime error: {e}")
        print("Wait 1 minutes before trying again....")
        time.sleep(60)

