# Russell 2000 ETF
# Generate CSV
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
                open_price = float(values['1. open'])
                # Write each row to the CSV file
                csvwriter.writerow([formatted_date_str, open_price])
            except ValueError:
                continue

    print("Data has been saved to Russell_2000_ETF.csv")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")

# Generate latest value
def postRequestHook(response: 'requests.Response'):
    ''' Returns the latest value from the FRED API response '''
    import json
    if response.status_code != 200:
        return None

    try:
        data = json.loads(response.text)
        time_series = data['Time Series (Daily)']
        sorted_data = sorted(time_series.items(), key=lambda x: dt.datetime.strptime(x[0], '%Y-%m-%d'))
        # Find the latest non-empty value
        for date,item in reversed(sorted_data):
            try:
                if item['1. open'] is not None:
                    return float(item['1. open'])
            except ValueError:
                continue

        return None
    except Exception as e:
        return None
    
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IWM&outputsize=full&apikey=YOUR_API_KEY"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)
