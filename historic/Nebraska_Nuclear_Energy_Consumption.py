# Nebraska Nuclear Energy Consumption
# Generate CSV
import requests
import json
import datetime as dt
import csv

url = "https://api.eia.gov//v2//seriesid//SEDS.NUETB.NE.A?api_key=wxFRLAoaTMQ9Ra7NvakhNKSxxstutZsG28nuerWR"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['response']['data']
    result = []

    # Sort the data by period (date) in ascending order
    sorted_data = sorted(time_series, key=lambda x: x['period'])

    for item in sorted_data:
        period = str(item['period'])
        formatted_date = dt.datetime.strptime(period + '-01-01 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
        formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        value = item['value']
        if value is not None:
            try:
                price = float(value)
                result.append([formatted_date_str, price])
            except ValueError:
                continue

    # Save the data to a CSV file
    with open('Nebraska_Nuclear_Energy_Consumption.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['index', 'value'])  # Write the header
        csvwriter.writerows(result)

    print("Data has been saved to Nebraska_Nuclear_Energy_Consumption.csv")
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
        time_series = data['response']['data']
        sorted_data = sorted(time_series, key=lambda x: x['period'])
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
    
url = "https://api.eia.gov//v2//seriesid//SEDS.NUETB.NE.A?api_key=wxFRLAoaTMQ9Ra7NvakhNKSxxstutZsG28nuerWR"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)