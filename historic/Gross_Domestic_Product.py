# Gross Domestic Product
# Generate CSV
import requests
import json
import datetime as dt
import csv

url = "https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key=7ef44306675240d156b2b8786339b867&file_type=json"

response = requests.get(url)
if response.status_code == 200:
    data = json.loads(response.text)
    observations = data['observations']
    # result = {}
    # Open a CSV file for writing
    with open('Gross_Domestic_Product.csv', 'w', newline='') as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)
        # Write the header
        csv_writer.writerow(['index', 'value'])

        for obs in observations:
            date = dt.datetime.strptime(obs['date'], '%Y-%m-%d')
            formatted_date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
            try:
                value = float(obs['value'])
                # result[formatted_date] = value
                csv_writer.writerow([formatted_date, value])
            except ValueError:
                continue
    # print(result)
    print("Data has been saved to Gross_Domestic_Product.csv")
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
        observations = data['observations']

        # Find the latest non-empty value
        for obs in reversed(observations):
            try:
                value = float(obs['value'])
                return value
            except ValueError:
                continue

        return None
    except Exception as e:
        return None
    
url = "https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key=7ef44306675240d156b2b8786339b867&file_type=json"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)
