# Russell 2000 ETF

import requests
import json
import datetime as dt
import csv

url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IWM&outputsize=full&apikey=YOUR_API_KEY"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['Time Series (Daily)']
    result = {}

    sorted_data = sorted(time_series.items(), key=lambda x: dt.datetime.strptime(x[0], '%Y-%m-%d'))

    # Open a CSV file for writing
    with open('Russell_2000_ETF.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write the header
        csvwriter.writerow(['index', 'value'])

        for date, values in reversed(sorted_data):
            formatted_date = dt.datetime.strptime(date + ' 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
            formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
            try:
                open_price = float(values['4. close'])
                # Write each row to the CSV file
                csvwriter.writerow([formatted_date_str, open_price])
            except ValueError:
                continue

    print("Data has been saved to Russell_2000_ETF.csv")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")

# import requests
# import json
# import datetime as dt

# url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IWM&outputsize=full&apikey=YOUR_API_KEY"
# response = requests.get(url)

# if response.status_code == 200:
#     data = json.loads(response.text)
#     time_series = data['Time Series (Daily)']
#     result = {}

#     # Convert to list of tuples and sort by date
#     sorted_data = sorted(time_series.items(), key=lambda x: dt.datetime.strptime(x[0], '%Y-%m-%d'))

#     for date, values in sorted_data:
#         formatted_date = dt.datetime.strptime(date + ' 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
#         formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
#         try:
#             open_price = float(values['1. open'])
#             result[formatted_date_str] = open_price
#         except ValueError:
#             continue

#     print(result)
# else:
#     print(f"Failed to retrieve data. Status code: {response.status_code}")
