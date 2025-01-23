# 10-Year Treasury Constant Maturity Rate

# import requests
# import json
# import datetime as dt
#
# url = "https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=7ef44306675240d156b2b8786339b867&file_type=json"
#
# response = requests.get(url)
# if response.status_code == 200:
#     data = json.loads(response.text)
#     observations = data['observations']
#     result = {}
#     for obs in observations:
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

url = "https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=7ef44306675240d156b2b8786339b867&file_type=json"

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    observations = data['observations']

    # Open a CSV file for writing
    with open('treasury_yield_data.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        # Write the header
        csv_writer.writerow(['index', 'value'])

        # Write the data
        for obs in observations:
            date = dt.datetime.strptime(obs['date'], '%Y-%m-%d')
            formatted_date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
            try:
                value = float(obs['value'])
                csv_writer.writerow([formatted_date, value])
            except ValueError:
                continue

    print("Data has been saved to treasury_yield_data.csv")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")

