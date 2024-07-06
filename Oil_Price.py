# WTI Crude Oil Daily Prices
import requests
import json
import datetime as dt

url = "https://api.eia.gov//v2//seriesid//PET.RWTC.D?api_key=wxFRLAoaTMQ9Ra7NvakhNKSxxstutZsG28nuerWR"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['response']['data']
    result = {}

    # Sort the data by period (date) in ascending order
    sorted_data = sorted(time_series, key=lambda x: x['period'])

    for item in sorted_data:
        period = item['period']
        formatted_date = dt.datetime.strptime(period + ' 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
        formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        value = item['value']
        if value is not None:
            try:
                price = float(value)
                result[formatted_date_str] = price
            except ValueError:
                continue

    print(result)
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")