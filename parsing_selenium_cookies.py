import json
import time
import requests
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
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
    driver.find_element(By.ID, "username").send_keys('lena22222')
    driver.find_element(By.ID, "password").send_keys('3132607476lenaL!')

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

cookies = get_fresh_cookies()

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': 'bf8282b6-c47b-4d03-b1e1-64c9b685aa02',
    'content-type': 'application/json',
    'origin': 'https://biz.sosmt.gov',
    'priority': 'u=1, i',
    'referer': 'https://biz.sosmt.gov/search/business',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"137.0.7151.104"',
    'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.104", "Chromium";v="137.0.7151.104", "Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
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
            filename = os.path.join(output_dir, f'Alabama_{start[:10]}_{end[:10]}.ndjson')
            with open(filename, 'w', encoding='utf-8') as f:
                for item in rows.values():
                    json.dump(item, f, ensure_ascii=False)
                    f.write('\n')
            print(f"Saved NDJSON: {filename}")
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

