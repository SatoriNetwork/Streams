# S&P 500 Index

# import requests
# import json
# import datetime as dt
#
# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&outputsize=full&apikey=YOUR_API_KEY"
#
# response = requests.get(url)
# if response.status_code == 200:
#     data = json.loads(response.text)
#     time_series = data['Time Series (Daily)']
#     result = {}
#     for obs in time_series:
#         date = dt.datetime.strptime(obs['date'], '%Y-%m-%d')
#         formatted_date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
#         try:
#             value = float(obs['value'])
#             result[formatted_date] = value
#         except ValueError:
#             continue
#     print(result)
# else:
#     print(f"Failed to retrieve data. Status code: {response.status_code}")

import requests
import json
import datetime as dt
import csv

url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&outputsize=full&apikey=YOUR_API_KEY"

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['Time Series (Daily)']

    # Convert the time series data into a list of tuples
    sorted_data = []
    for date, values in time_series.items():
        formatted_date = dt.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S.%f')
        try:
            value = float(values['4. close'])  # Assuming you want the closing price
            sorted_data.append((formatted_date, value))
        except ValueError:
            continue

    # Sort the data by date, oldest first
    sorted_data.sort(key=lambda x: x[0])

    # Open a CSV file for writing
    with open('output.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        csvwriter = csv.writer(csvfile)

        # Write the header
        csvwriter.writerow(['index', 'value'])

        # Write the sorted data
        for date, value in sorted_data:
            csvwriter.writerow([date, value])

    print("Data has been saved to output.csv with oldest values first")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")