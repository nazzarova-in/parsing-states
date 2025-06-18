import requests
import os
import json
import time

output_dir = './sftp/data'
os.makedirs(output_dir, exist_ok=True)

cookies = {
    "ASP.NET_SessionId": "f2ab5fwyj2nqmqsqmkgzmhbd",
    "_ga": "GA1.1.458948427.1750135397",
    "intercom-id-wj2gx489": "e56fc704-0a04-477f-8eda-41941ea4aa21",
    "intercom-session-wj2gx489": "",
    "intercom-device-id-wj2gx489": "4ecac14c-eab6-4a41-8b25-ef6d560a62a2",
    "_ga_T1KT8E06RP": "GS2.1.s1750139018$o2$g1$t1750139060$j18$l0$h0",
    "__cf_bm": "tRU6Exq3SktTJE1eWSQntb1NyheafQASLi1X7errsTA-1750223492-1.0.1.1-Kx3gCSlzFparYDBDFpraaBoPTjn9AexhCWHTmhclZpOcL_cKbA_pbu3hQE2ValOBKOIX.3RHQGhXs4vEBkgvI1_5M0LLCFK8GKuHFpPiAbU",
    "cf_clearance": "eI5N3BtABbtAjHFjY3UoHoRqDe7tVRzKxnLYG3NMjcE-1750223573-1.2.1.1-xdd8191XTDUikc2HwDTA0b3GIGsCfm7CaQzsEudAiSZy.oaXIaz5H01pnZgqoWUqwzcAlDwNWkh8Bkj0BtTI3uR1pmC2uSKfUtyIcIAbacXVcdWgTWGhEdhSmSIpLgoQ6YKgVSuBTKSUAcLsxyvAV636cNRBW0lhmXok76MKdDHuyl7WPkvCu8DgdNwgp15n0zC8pjN5ZYTxhSvWsOguUqz7gaALVu.DI3fgvAz4ZgIJTsWxQna9xMoOSR3pGr8ZUjR9GYilWOyWCKpPiVTJ2wrLnaWSVERyoMf4z2fmhNNYAjW5oWmQbIq7oh7CXWvvkWs578Wjb4QiLeQlgSTZ7dNoDqA6vS2cSCZyjRLGYn8"
}


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