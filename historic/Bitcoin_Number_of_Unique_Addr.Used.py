# Bitcoin Number of Unique Addr. Used		
# Generate CSV
import requests
import json
import datetime as dt
import csv

url = "https://data.nasdaq.com/api/v3/datatables/QDL/BCHAIN?code=NADDU&api_key=zjsgHD-jbXKBVezYzyqT"
response = requests.get(url)

if response.status_code == 200:
    data = json.loads(response.text)
    time_series = data['datatable']['data']
    result = {}

    # Convert to list of tuples and sort by date
    sorted_data = sorted(time_series, key=lambda x: dt.datetime.strptime(x[1], '%Y-%m-%d'))
    # Open a CSV file for writing
    with open('Bitcoin_Number_of_Unique_Addr.Used.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Write the header
        csvwriter.writerow(['index', 'value'])

        for items in sorted_data:
            formatted_date = dt.datetime.strptime(items[1] + ' 00:00:00.000000', '%Y-%m-%d %H:%M:%S.%f')
            formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')
            try:
                value= float(items[2])
                # result[formatted_date_str] = open_price
                csvwriter.writerow([formatted_date_str, value])
            except ValueError:
                continue

    print("Data has been saved to Bitcoin_Number_of_Unique_Addr.Used.csv")
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
        time_series = data['datatable']['data']
        sorted_data = sorted(time_series, key=lambda x: dt.datetime.strptime(x[1], '%Y-%m-%d'))
        # Find the latest non-empty value
        for items in reversed(sorted_data):
            # print(item)
            try:
                if items[2] is not None:
                    return float(items[2])
            except ValueError:
                continue

        return None
    except Exception as e:
        return None
    
url = "https://data.nasdaq.com/api/v3/datatables/QDL/BCHAIN?code=NADDU&api_key=zjsgHD-jbXKBVezYzyqT"
response = requests.get(url)
latest_value = postRequestHook(response)
print(latest_value)
