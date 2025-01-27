# S&P 500 Index
# Generate CSV
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
            value = float(values['1. open'])  # Assuming you want the closing price
            sorted_data.append((formatted_date, value))
        except ValueError:
            continue

    # Sort the data by date, oldest first
    sorted_data.sort(key=lambda x: x[0])

    # Open a CSV file for writing
    with open('S&P_500_Index.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        csvwriter = csv.writer(csvfile)

        # Write the header
        csvwriter.writerow(['index', 'value'])

        # Write the sorted data
        for date, value in sorted_data:
            csvwriter.writerow([date, value])

    print("Data has been saved to S&P_500_Index.csv with oldest values first")
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
    
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&outputsize=full&apikey=YOUR_API_KEY"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)
