from datetime import datetime
from sftp_utils import SFTPBufferedUploader, move_file_to_production

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
