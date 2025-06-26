import json
import os

PROGRESS_FILE = "progress.json"

def save_progress(state_idx, interval_idx, filename, line_count):
    progress = {
        "state_index": state_idx,
        "interval_index": interval_idx,
        "filename_only": filename,
        "line_counter": line_count
    }
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
