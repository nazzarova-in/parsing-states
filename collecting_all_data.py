import json
import time
import requests
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import os
from datetime import datetime
import paramiko


load_dotenv()
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")
AUTHORIZATION = os.getenv("AUTHORIZATION")
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")


output_dir = './sftp/data'
os.makedirs(output_dir, exist_ok=True)


def get_fresh_cookies():
    options = uc.ChromeOptions()
    driver = uc.Chrome(options=options)

    driver.get("https://biz.sosmt.gov/search/business")

    wait = WebDriverWait(driver, 30)
    print("Waiting for the login button...")
    login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.login-link")))
    login_button.click()

    print("Waiting for the login form...")
    wait.until(EC.visibility_of_element_located((By.ID, "username")))

    print("Enter your login and password...")
    driver.find_element(By.ID, "username").send_keys(LOGIN)
    driver.find_element(By.ID, "password").send_keys(PASSWORD)

    print("Click the login button...")
    driver.find_element(By.CSS_SELECTOR, "button.submit").click()

    time.sleep(7)

    print("Let's go to the API URL to get the final cookies...")
    driver.get("https://biz.sosmt.gov/api/Records/businesssearch")
    time.sleep(10)

    selenium_cookies = driver.get_cookies()
    driver.quit()

    cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
    return cookies_dict


headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': 'd4882ecc-c552-4384-81a1-982cd5092976',
    'content-type': 'application/json',
    'origin': 'https://biz.sosmt.gov',
    'priority': 'u=1, i',
    'referer': 'https://biz.sosmt.gov/search/business',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.50"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.50", "Google Chrome";v="138.0.7204.50"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

json_data = {
    'SEARCH_VALUE': '',
    'QUERY_TYPE_ID': 1010,
    'FILING_TYPE_ID': '0',
    'FILING_SUBTYPE_ID': '0',
    'STATUS_ID': '0',
    'STATE': 'Alabama',
    'COUNTY': '',
    'CRA_SEARCH_YN': False,
    'FILING_DATE': {
        'start': '',
        'end': '',
    },
    'EXPIRATION_DATE': {
        'start': None,
        'end': None,
    },
}

intervals = [
    ('1995-01-01T00:00:00', '1999-12-31T23:59:59'),
    ('2000-01-01T00:00:00', '2004-12-31T23:59:59'),
    ('2005-01-01T00:00:00', '2009-12-31T23:59:59'),
    ('2010-01-01T00:00:00', '2014-12-31T23:59:59'),
    ('2015-01-01T00:00:00', '2019-12-31T23:59:59'),
    ('2020-01-01T00:00:00', '2025-06-17T23:59:59'),
]

states = [
    "Arizona", "Alaska"
]

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename_only = f'all_data_{timestamp}.ndjson'

MAX_LINES_PER_FILE = 30
line_counter = 0
has_written_data = False


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


for state in states:
    print(f"\n============================\nProcessing state: {state}\n============================")
    cookies = get_fresh_cookies()
    json_data['STATE'] = state

    for start, end in intervals:
        json_data['FILING_DATE']['start'] = start
        json_data['FILING_DATE']['end'] = end

        response = requests.post(
            'https://biz.sosmt.gov/api/Records/businesssearch',
            cookies=cookies,
            headers=headers,
            json=json_data
        )

        print(f"Request period: {start[:10]} to {end[:10]} — status {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            rows = data.get("rows", {})

            if rows:
                rows_list = list(rows.values())
                total_rows = len(rows_list)
                current_index = 0

                while current_index < total_rows:
                    remaining_space = MAX_LINES_PER_FILE - line_counter
                    batch = rows_list[current_index:current_index + remaining_space]

                    ndjson_buffer = ""
                    for item in batch:
                        item["STATE"] = state
                        ndjson_buffer += json.dumps(item, ensure_ascii=False) + "\n"

                    mode = 'w' if line_counter == 0 else 'a'
                    upload_ndjson_to_sftp(ndjson_buffer, filename_only, mode=mode)
                    print(f"Uploaded batch of {len(batch)} rows to {filename_only}")

                    line_counter += len(batch)
                    has_written_data = True
                    current_index += remaining_space

                    if line_counter >= MAX_LINES_PER_FILE:
                        print("File limit reached, moving to production...")
                        move_file_to_production(filename_only)

                        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        filename_only = f'all_data_{timestamp}.ndjson'
                        line_counter = 0
                        has_written_data = False


                else:
                        print("No data written. File will not be moved.")
                        print("Stopping script.")



            else:
                print(f"No data for period {start[:10]} to {end[:10]}")


            print("Pausing 20 seconds ...")
            time.sleep(20)

        else:
            print(f"Request error {response.status_code} for period {start[:10]} to {end[:10]}")
            if response.status_code == 429:
                print("Got 429 — sleeping for 30 minutes...")
                time.sleep(1800)
            else:
                time.sleep(15)

    print(f"Finished state {state}. Sleeping for 5 minutes before next state...")
    time.sleep(300)

