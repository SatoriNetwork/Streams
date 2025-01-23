import requests
import csv
from datetime import datetime
import pytz
from collections import OrderedDict

url = "https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=KRW&apikey=YOUR_API_KEY"

response = requests.get(url)
data = response.json()

processed_data = {}
for date, values in data['Time Series FX (Daily)'].items():
    open_value = float(values['1. open'])
    close_value = float(values['4. close'])
    average_value = (open_value + close_value) / 2

    date_obj = datetime.strptime(date, '%Y-%m-%d')

    date_obj = date_obj.replace(hour=7, minute=13, second=1, microsecond=17905, tzinfo=pytz.UTC)

    formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')

    processed_data[formatted_date] = f"{average_value:.2f}"

sorted_data = OrderedDict(sorted(processed_data.items()))

with open('forex_data.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['index', 'value'])  # Write header

    for date, value in sorted_data.items():
        csvwriter.writerow([date, value])

print("Data has been saved to forex_data.csv")




# def postRequestHook(response: 'requests.Response'):
#     import requests
#     import csv
#     from datetime import datetime
#     import pytz
#     from collections import OrderedDict
#     try:
#         data = response.json()
#
#         processed_data = {}
#         for date, values in data['Time Series FX (Daily)'].items():
#             open_value = float(values['1. open'])
#             close_value = float(values['4. close'])
#             average_value = (open_value + close_value) / 2
#
#             date_obj = datetime.strptime(date, '%Y-%m-%d')
#             date_obj = date_obj.replace(hour=7, minute=13, second=1, microsecond=17905, tzinfo=pytz.UTC)
#             formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')
#
#             processed_data[formatted_date] = f"{average_value:.2f}"
#
#         sorted_data = OrderedDict(sorted(processed_data.items(), reverse=True))
#
#         # Return the latest value (first item in the sorted dictionary)
#         return next(iter(sorted_data.values()), None)
#
#     except Exception as e:
#         # If any error occurs during processing, return None
#         return None