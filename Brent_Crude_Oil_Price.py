# Brent Crude Oil Price
import requests
import json
import datetime as dt
import csv

url = "https://api.eia.gov//v2//seriesid//PET.RBRTE.D?api_key=wxFRLAoaTMQ9Ra7NvakhNKSxxstutZsG28nuerWR"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['response']['data']
    result = []

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
                result.append([formatted_date_str, price])
            except ValueError:
                continue

    # Save the data to a CSV file
    with open('Brent_Crude_Oil_Price.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['index', 'value'])  # Write the header
        csvwriter.writerows(result)

    print("Data has been saved to Brent_Crude_Oil_Price.csv")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")