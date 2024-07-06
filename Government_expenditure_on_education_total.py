# Government expenditure on education, total (% of GDP)
import requests
import json
import datetime as dt

url = "https://api.worldbank.org/v2/country/WLD/indicator/SE.XPD.TOTL.GD.ZS?format=json"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data[1]  # The actual data is in the second element of the list
    result = {}

    # Sort the data by date in ascending order
    sorted_data = sorted(time_series, key=lambda x: x['date'])

    for item in sorted_data:
        date = item['date']
        formatted_date = dt.datetime.strptime(date + '-01-01 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
        formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        value = item['value']
        if value is not None:
            try:
                population = float(value)
                result[formatted_date_str] = population
            except ValueError:
                continue

    print(result)
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")

