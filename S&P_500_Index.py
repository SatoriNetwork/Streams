# S&P 500 Index

import requests
import json
import datetime as dt

url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&outputsize=full&apikey=YOUR_API_KEY"

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['Time Series (Daily)']
    result = {}
    for obs in time_series:
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