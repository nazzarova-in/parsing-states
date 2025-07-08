import paramiko
from dotenv import load_dotenv
import os

load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")

class SFTPBufferedUploader:
    def __init__(self, filename):
        self.filename = filename
        self.remote_path = f"temporary_data/{filename}"
        self.transport = None
        self.sftp = None
        self.remote_file = None

    def connect(self):
        self.transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        self.transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        self.remote_file = self.sftp.open(self.remote_path, "a")

    def write(self, content: str):
        if self.remote_file:
            self.remote_file.write(content)


    def close(self):
        if self.remote_file:
            self.remote_file.close()
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()


    def count_lines_remote(self):
        try:
            with self.sftp.open(self.remote_path, "r") as f:
                return sum(1 for _ in f)
        except IOError:
            return 0


def move_file_to_production(filename):
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        sftp.rename(f"temporary_data/{filename}", f"production/{filename}")
        print(f"Successfully moved {filename} to the 'production/' folder.")

    except Exception as e:
        print(f"Error while moving file to 'production': {e}")

    finally:
        sftp.close()
        transport.close()
