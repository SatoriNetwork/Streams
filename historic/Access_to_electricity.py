# Access to electricity (% of population)
# Generate CSV
import requests
import json
import datetime as dt
import csv

url = "https://api.worldbank.org/v2/country/WLD/indicator/EG.ELC.ACCS.ZS?format=json"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data[1]  # The actual data is in the second element of the list
    # result = {}

    # Sort the data by date in ascending order
    sorted_data = sorted(time_series, key=lambda x: x['date'])
    # Open a CSV file for writing
    with open('Access_to_electricity.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        csvwriter = csv.writer(csvfile)
        # Write the header
        csvwriter.writerow(['index', 'value'])

        for item in sorted_data:
            date = item['date']
            formatted_date = dt.datetime.strptime(date + '-01-01 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
            formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
            value = item['value']
            if value is not None:
                try:
                    population = float(value)
                    # result[formatted_date_str] = population
                    csvwriter.writerow([formatted_date_str, value])
                except ValueError:
                    continue

    # print(result)
    print("Data has been saved to 'Access_to_electricity.csv'")
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
        time_series = data[1]
        sorted_data = sorted(time_series, key=lambda x: x['date'])
        # Find the latest non-empty value
        for item in reversed(sorted_data):
            try:
                if item['value'] is not None:
                    return float(item['value'])
            except ValueError:
                continue

        return None
    except Exception as e:
        return None
    
url = "https://api.worldbank.org/v2/country/WLD/indicator/EG.ELC.ACCS.ZS?format=json"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)