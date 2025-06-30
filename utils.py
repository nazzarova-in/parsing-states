import paramiko

from dotenv import load_dotenv
import os


load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")


def upload_ndjson_to_sftp(content: str, filename: str, mode='a'):
  transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
  transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
  sftp = paramiko.SFTPClient.from_transport(transport)

  remote_path = f"temporary_data/{filename}"

  try:
    with sftp.open(remote_path, mode) as remote_file:
      remote_file.write(content)
  finally:
    sftp.close()
    transport.close()


def move_file_to_production(filename):
  transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
  transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
  sftp = paramiko.SFTPClient.from_transport(transport)

  try:

    try:
      sftp.stat("production")
      print(" 'production' folder already exists.")
    except FileNotFoundError:
      print(" 'production' folder not found. Creating it...")
      sftp.mkdir("production")

    sftp.rename(f"temporary_data/{filename}", f"production/{filename}")
    print(f"Successfully moved {filename} to the 'production/' folder.")
  except Exception as e:
    print(f"Error while moving file to 'production': {e}")
  finally:
    sftp.close()
    transport.close()
