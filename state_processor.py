import time
from cookies import get_fresh_cookies
from intervals import intervals
from states import states
from request_utils import fetch_data, handle_error
from save_data_utils import process_rows
from progress import save_progress

MAX_LINES = 10000

def process_intervals(state, s_idx, interval_start, filename, line_counter, headers, uploader, cookies, json_template):
    for i_idx, (start, end) in enumerate(intervals[interval_start:], start=interval_start):
        try:
            r = fetch_data(state, start, end, cookies, headers, json_template)
        except Exception as err:
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

def process_states(state_start, interval_start, filename, line_counter, headers, uploader, login, password, json_template):
    for s_idx, state in enumerate(states[state_start:], start=state_start):
        cookies = get_fresh_cookies(login, password)
        print(f"Processing state {state} (index {s_idx})")

        filename, line_counter, uploader = process_intervals(
            state, s_idx, interval_start, filename, line_counter, headers, uploader, cookies, json_template
        )

        interval_start = 0
        print("Waiting 2 min before next state...")
        time.sleep(120)

    return filename, line_counter, uploader
