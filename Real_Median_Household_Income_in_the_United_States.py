# Real Median Household Income in the United States

import requests
import json
import datetime as dt

url = "https://api.stlouisfed.org/fred/series/observations?series_id=MEHOINUSA672N&api_key=7ef44306675240d156b2b8786339b867&file_type=json"

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    observations = data['observations']
    result = {}
    for obs in observations:
        date = dt.datetime.strptime(obs['date'], '%Y-%m-%d')
        formatted_date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
        try:
            value = float(obs['value'])
            result[formatted_date] = value
        except ValueError:
            continue
    print(result)
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")