import json
import redis
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

PROGRESS_KEY = "biz_parsing_progress"

def save_progress(state_idx, interval_idx, filename, line_count):
    progress = {
        "state_index": state_idx,
        "interval_index": interval_idx,
        "filename_only": filename,
        "line_counter": line_count
    }
    client.set(PROGRESS_KEY, json.dumps(progress))

def load_progress():
    value = client.get(PROGRESS_KEY)
    if value:
        return json.loads(value)
    return None

def clear_progress():
    client.delete(PROGRESS_KEY)
