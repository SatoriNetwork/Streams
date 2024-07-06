# USD Daily Opening Prices

import requests
import json
import datetime as dt

url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=USD&outputsize=full&apikey=YOUR_API_KEY"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['Time Series (Daily)']
    result = {}

    sorted_data = sorted(time_series.items(), key=lambda x: dt.datetime.strptime(x[0], '%Y-%m-%d'))

    for date, values in sorted_data:
        formatted_date = dt.datetime.strptime(date + ' 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
        formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        try:
            open_price = float(values['1. open'])
            result[formatted_date_str] = open_price
        except ValueError:
            continue
    print(result)
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")