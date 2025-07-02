import json
from datetime import datetime
from sftp_utils import move_file_to_production, SFTPBufferedUploader


def process_rows(rows, state, filename, line_counter, uploader, max_lines=10000):
    if not rows:
        print("No data")
        return filename, line_counter, uploader

    for item in rows.values():
        item["STATE"] = state
        uploader.write(json.dumps(item, ensure_ascii=False) + "\n")
        line_counter += 1

        if line_counter >= max_lines:
            uploader.flush()
            uploader.close()
            move_file_to_production(filename)

            filename = f"all_data_{datetime.now():%Y-%m-%d_%H-%M-%S}.ndjson"
            uploader = SFTPBufferedUploader(filename)
            uploader.connect()

            line_counter = 0

    print(f"Uploaded {len(rows)} rows")
    return filename, line_counter, uploader

