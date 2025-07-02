import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

from config import HEADERS, JSON_DATA_TEMPLATE
from cookies import get_fresh_cookies
from intervals import intervals
from states import states
from progress import save_progress, load_progress, clear_progress
from sftp_utils import SFTPBufferedUploader, move_file_to_production
from request_utils import fetch_data, handle_error
from save_data_utils import process_rows

load_dotenv()

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
AUTH = os.getenv("AUTHORIZATION")

headers = HEADERS.copy()
headers["authorization"] = AUTH

MAX_LINES = 10000


def create_uploader_with_resume(filename):
    uploader = SFTPBufferedUploader(filename)
    uploader.connect()
    line_count = uploader.count_lines_remote()
    return uploader, line_count

def create_new_uploader():
    filename = f"all_data_{datetime.now():%Y-%m-%d_%H-%M-%S}.ndjson"
    uploader = SFTPBufferedUploader(filename)
    uploader.connect()
    return filename, uploader, 0


def process_intervals(state, s_idx, interval_start, filename, line_counter, headers, uploader, cookies):
    for i_idx, (start, end) in enumerate(intervals[interval_start:], start=interval_start):
        try:
            r = fetch_data(state, start, end, cookies, headers, JSON_DATA_TEMPLATE)
        except requests.RequestException as err:
            code = getattr(err.response, "status_code", None)
            handle_error(code, s_idx, i_idx, filename, line_counter)
            raise

        if r.status_code == 200:
            rows = r.json().get("rows", {})
            filename, line_counter, uploader = process_rows(
                rows, state, filename, line_counter, uploader, max_lines=MAX_LINES
            )
            save_progress(s_idx, i_idx + 1, filename, line_counter)
            time.sleep(25)

    return filename, line_counter, uploader


def process_states(state_start, interval_start, filename, line_counter, headers, uploader):
    for s_idx, state in enumerate(states[state_start:], start=state_start):
        cookies = get_fresh_cookies(LOGIN, PASSWORD)
        print(f"Processing state {state} (index {s_idx})")

        filename, line_counter, uploader = process_intervals(
            state, s_idx, interval_start, filename, line_counter, headers, uploader, cookies
        )

        interval_start = 0
        print("Waiting 2 min before next state...")
        time.sleep(120)

    return filename, line_counter, uploader


def main():
    progress = load_progress() or {}
    state_start = progress.get("state_index", 0)
    interval_start = progress.get("interval_index", 0)
    filename = progress.get("filename_only")
    line_counter = progress.get("line_counter", 0)

    if filename:
        uploader, line_counter = create_uploader_with_resume(filename)
        if line_counter >= MAX_LINES:
            uploader.close()
            move_file_to_production(filename)
            filename = None
    else:
        uploader = None

    if not filename:
        filename, uploader, line_counter = create_new_uploader()

    filename, line_counter, uploader = process_states(
        state_start, interval_start, filename, line_counter, headers, uploader
    )

    if line_counter:
        uploader.close()
        move_file_to_production(filename)

    clear_progress()
    print("Done — all data processed.")


if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except Exception as e:
            print(f"Runtime error: {e}")
            time.sleep(120)
