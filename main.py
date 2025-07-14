import os
import time
from dotenv import load_dotenv

from config import HEADERS, JSON_DATA_TEMPLATE
from uploader import create_uploader_with_resume, create_new_uploader
from state_processor import process_states
from sftp_utils import move_file_to_production
from progress import load_progress, clear_progress

load_dotenv()

LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")

headers = HEADERS.copy()

MAX_LINES = 10000

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
        state_start, interval_start, filename, line_counter, headers, uploader,
        LOGIN, PASSWORD, JSON_DATA_TEMPLATE
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
