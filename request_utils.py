import copy
import time
import requests
from progress import save_progress

MAX_LINES = 10000

too_many = requests.status_codes.codes.too_many_requests
forbidden = requests.status_codes.codes.forbidden
timeout_524 = 524

def fetch_data(state, start, end, cookies, headers, json_template):
    json_data = copy.deepcopy(json_template)
    json_data["STATE"] = state
    json_data["FILING_DATE"]["start"] = start
    json_data["FILING_DATE"]["end"] = end

    r = requests.post(
        "https://biz.sosmt.gov/api/Records/businesssearch",
        cookies=cookies,
        headers=headers,
        json=json_data,
        timeout=60
    )
    r.raise_for_status()
    print(f"[{state}] {start[:10]} — {end[:10]} | Status {r.status_code}")
    return r


def handle_error(code, s_idx, i_idx, filename, line_counter):
    print(f"Error code: {code}")
    save_progress(s_idx, i_idx, filename, line_counter)

    if code == too_many:
        print("429 Too Many Requests — sleeping 1 hour...")
        time.sleep(3600)

    elif code == forbidden:
        print("403 Forbidden — refreshing cookies and retrying in 10 seconds...")
        time.sleep(10)

    elif code == timeout_524:
        print("524 Timeout — retrying in 30 seconds...")
        time.sleep(30)

    else:
        print("Unexpected error — sleeping 15 seconds...")
        time.sleep(15)
