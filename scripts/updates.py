import os
import requests
import glob
import json
import time
import csv
import pandas as pd
import logging
import datetime as dt

def get_topics(base_url='http://localhost:24601'):
    """
    Get topics from the server after upload using existing endpoints.
    Uses /relay_csv endpoint which returns CSV data of relay streams.
    """
    relay_csv_url = f"{base_url}/relay_csv"
    
    try:
        response = requests.get(relay_csv_url)
        if response.ok:
            # Parse CSV response to extract topics
            import pandas as pd
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            if df.empty:
                print("  No relay streams found")
                return {'status': 'success', 'topics': []}
            
            topics = []
            for _, row in df.iterrows():
                # Create topic ID from stream components
                topic = f"{row.get('source', 'satori')}|{row.get('author', '')}|{row.get('stream', '')}|{row.get('target', '')}"
                topics.append({
                    'topic': topic,
                    'source': row.get('source', 'satori'),
                    'author': row.get('author', ''),
                    'stream': row.get('stream', ''),
                    'target': row.get('target', ''),
                })
            return {'status': 'success', 'topics': topics}
        else:
            print(f"  Failed to get topics: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error getting topics from {relay_csv_url}: {str(e)}")
        return None

# def upload_all_chunks(chunks_folder='chunks', base_url='http://localhost:24601', delay_between_uploads=2):
#     """
#     Upload all CSV files from the chunks folder to the web application.
#     Gets topics after each successful upload.
#     """
#     upload_url = f"{base_url}/upload_datastream_csv"
    
#     # Find all CSV files in chunks folder
#     csv_files = glob.glob(os.path.join(chunks_folder, '*.csv'))
#     csv_files.sort()  # Ensure proper order
    
#     print(f"Found {len(csv_files)} CSV files to upload")
    
#     upload_results = []
    
#     for i, csv_file in enumerate(csv_files, 1):
#         filename = os.path.basename(csv_file)
        
#         print(f"\n[{i}/{len(csv_files)}] Processing {filename}...")
        
#         try:
#             with open(csv_file, 'rb') as file:
#                 files = {'file': (filename, file, 'text/csv')}
#                 response = requests.post(upload_url, files=files)
                
#             if response.ok:
#                 print(f"✓ Successfully uploaded {filename}")
                
#                 # Wait a moment for the server to process
#                 time.sleep(1)
                
#                 # Get topics after successful upload
#                 print("  Retrieving topics...")
#                 topics_data = get_topics(base_url)
                
#                 if topics_data:
#                     topics = topics_data.get('topics', [])
#                     topic_count = len(topics)
#                     print(f"  ✓ Retrieved {topic_count} topics")
                    
#                     # # Show some topic examples (first 3)
#                     # if topics:
#                     #     print("  Sample topics:")
#                     #     for topic in topics[:3]:
#                     #         if isinstance(topic, dict):
#                     #             print(f"    - {topic.get('topic', topic.get('stream', 'Unknown'))}")
#                     #         else:
#                     #             print(f"    - {topic}")
#                     #     if len(topics) > 3:
#                     #         print(f"    ... and {len(topics) - 3} more")
                    
#                     upload_results.append({
#                         'file': filename,
#                         'status': 'success',
#                         'topics_count': topic_count,
#                         'topics': topics
#                     })
#                 else:
#                     print("  Could not retrieve topics")
#                     upload_results.append({
#                         'file': filename,
#                         'status': 'success',
#                         'topics_count': 0,
#                         'topics': []
#                     })
                    
#             else:
#                 print(f"✗ Failed to upload {filename}: {response.status_code}")
#                 print(f"  Response: {response.text}")
#                 upload_results.append({
#                     'file': filename,
#                     'status': 'failed',
#                     'error': f"HTTP {response.status_code}: {response.text}",
#                     'topics_count': 0,
#                     'topics': []
#                 })
                
#         except Exception as e:
#             print(f"✗ Error uploading {filename}: {str(e)}")
#             upload_results.append({
#                 'file': filename,
#                 'status': 'error',
#                 'error': str(e),
#                 'topics_count': 0,
#                 'topics': []
#             })
        
#         # Add delay between uploads to avoid overwhelming the server
#         if i < len(csv_files):
#             print(f"  Waiting {delay_between_uploads} seconds before next upload...")
#             time.sleep(delay_between_uploads)
    
#     # Summary
#     print(f"\n{'='*50}")
#     print("UPLOAD SUMMARY")
#     print(f"{'='*50}")
    
#     successful_uploads = [r for r in upload_results if r['status'] == 'success']
#     failed_uploads = [r for r in upload_results if r['status'] != 'success']
    
#     print(f"Total files: {len(csv_files)}")
#     print(f"Successful uploads: {len(successful_uploads)}")
#     print(f"Failed uploads: {len(failed_uploads)}")
    
#     if successful_uploads:
#         total_topics = sum(r['topics_count'] for r in successful_uploads)
#         print(f"Total topics after all uploads: {total_topics}")
        
#         # Get final topics list
#         print("\nGetting final topics list...")
#         final_topics = get_topics(base_url)
#         if final_topics:
#             final_count = len(final_topics.get('topics', []))
#             print(f"Final topic count: {final_count}")
    
#     if failed_uploads:
#         print(f"\nFailed uploads:")
#         for result in failed_uploads:
#             print(f"  - {result['file']}: {result.get('error', 'Unknown error')}")
    
#     # Save results to file
#     results_file = 'upload_results.json'
#     with open(results_file, 'w') as f:
#         json.dump(upload_results, f, indent=2)
#     print(f"\nDetailed results saved to {results_file}")
    
#     return upload_results
def upload_all_chunks(chunks_folder='chunks', base_url='http://localhost:24601', 
                                 delay_between_uploads=2, download_history_flag=True):
    """
    Upload all CSV files from the chunks folder to the web application.
    Gets topics after each successful upload and optionally downloads historical data.
    """
    upload_url = f"{base_url}/upload_datastream_csv"
    
    # Find all CSV files in chunks folder
    csv_files = glob.glob(os.path.join(chunks_folder, '*.csv'))
    csv_files.sort()  # Ensure proper order
    
    print(f"Found {len(csv_files)} CSV files to upload")
    
    upload_results = []
    all_topics_data = []  # Store all topics for history download
    
    for i, csv_file in enumerate(csv_files, 1):
        filename = os.path.basename(csv_file)
        
        print(f"\n[{i}/{len(csv_files)}] Processing {filename}...")
        
        try:
            with open(csv_file, 'rb') as file:
                files = {'file': (filename, file, 'text/csv')}
                response = requests.post(upload_url, files=files)
                
            if response.ok:
                print(f"✓ Successfully uploaded {filename}")
                
                # Wait a moment for the server to process
                time.sleep(1)
                
                # Get topics after successful upload
                print("  Retrieving topics...")
                topics_data = get_topics(base_url)
                
                if topics_data:
                    topics = topics_data.get('topics', [])
                    topic_count = len(topics)
                    print(f"  ✓ Retrieved {topic_count} topics")
                    
                    # Store topics for potential history download
                    all_topics_data.extend(topics)
                    
                    upload_results.append({
                        'file': filename,
                        'status': 'success',
                        'topics_count': topic_count,
                        'topics': topics
                    })
                else:
                    print("  Could not retrieve topics")
                    upload_results.append({
                        'file': filename,
                        'status': 'success',
                        'topics_count': 0,
                        'topics': []
                    })
                    
            else:
                print(f"✗ Failed to upload {filename}: {response.status_code}")
                print(f"  Response: {response.text}")
                upload_results.append({
                    'file': filename,
                    'status': 'failed',
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'topics_count': 0,
                    'topics': []
                })
                
        except Exception as e:
            print(f"✗ Error uploading {filename}: {str(e)}")
            upload_results.append({
                'file': filename,
                'status': 'error',
                'error': str(e),
                'topics_count': 0,
                'topics': []
            })
        
        # Add delay between uploads to avoid overwhelming the server
        if i < len(csv_files):
            print(f"  Waiting {delay_between_uploads} seconds before next upload...")
            time.sleep(delay_between_uploads)
    
    # Summary
    print(f"\n{'='*50}")
    print("UPLOAD SUMMARY")
    print(f"{'='*50}")
    
    successful_uploads = [r for r in upload_results if r['status'] == 'success']
    failed_uploads = [r for r in upload_results if r['status'] != 'success']
    
    print(f"Total files: {len(csv_files)}")
    print(f"Successful uploads: {len(successful_uploads)}")
    print(f"Failed uploads: {len(failed_uploads)}")
    
    if successful_uploads:
        total_topics = sum(r['topics_count'] for r in successful_uploads)
        print(f"Total topics after all uploads: {total_topics}")
        
        # Get final topics list
        print("\nGetting final topics list...")
        final_topics = get_topics(base_url)
        if final_topics:
            final_count = len(final_topics.get('topics', []))
            print(f"Final topic count: {final_count}")
            
            # Download historical data if requested
            if download_history_flag and final_topics.get('topics'):
                print(f"\n{'='*50}")
                print("DOWNLOADING HISTORICAL DATA")
                print(f"{'='*50}")
                
                # Convert topics to DataFrame for download_history function
                topics_df = pd.DataFrame(final_topics['topics'])
                download_history(topics_df)
    
    if failed_uploads:
        print(f"\nFailed uploads:")
        for result in failed_uploads:
            print(f"  - {result['file']}: {result.get('error', 'Unknown error')}")
    
    # Save results to file
    results_file = 'upload_results.json'
    with open(results_file, 'w') as f:
        json.dump(upload_results, f, indent=2)
    print(f"\nDetailed results saved to {results_file}")
    
    return upload_results
# def upload_single_chunk_and_get_topics(csv_file, base_url='http://localhost:24601'):
#     """
#     Upload a single CSV file and get topics.
#     Useful for testing or selective uploads.
#     """
#     upload_url = f"{base_url}/upload_datastream_csv"
    
#     if not os.path.exists(csv_file):
#         print(f"File not found: {csv_file}")
#         return None
    
#     filename = os.path.basename(csv_file)
#     print(f"Uploading {filename}...")
    
#     try:
#         with open(csv_file, 'rb') as file:
#             files = {'file': (filename, file, 'text/csv')}
#             response = requests.post(upload_url, files=files)
            
#         if response.ok:
#             print(f"✓ Successfully uploaded {filename}")
            
#             # Get topics after upload
#             time.sleep(1)
#             topics_data = get_topics(base_url)
            
#             if topics_data:
#                 topics = topics_data.get('topics', [])
#                 print(f"✓ Retrieved {len(topics)} topics")
#                 return topics
#             else:
#                 print("⚠ Could not retrieve topics")
#                 return []
#         else:
#             print(f"✗ Failed to upload {filename}: {response.status_code}")
#             return None
            
#     except Exception as e:
#         print(f"✗ Error uploading {filename}: {str(e)}")
#         return None

# import requests


def download_history(df):
    """
    Download historical data for each stream in the CSV and save to individual directories
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create main history directory if it doesn't exist
    history_base_dir = "history"
    if not os.path.exists(history_base_dir):
        os.makedirs(history_base_dir)
        print(f"Created history directory: {history_base_dir}")
    
    print(f"Starting download of historical data for {len(df)} streams...")
    
    for index, row in df.iterrows():
        try:
            # Get stream name for directory
            stream_name = row.get('stream', f'stream_{index}')
            
            # Determine which URL to use - prioritize 'uri' over 'url'
            api_url = None
            if 'uri' in row and pd.notna(row['uri']) and str(row['uri']).strip():
                api_url = str(row['uri']).strip()
            elif 'url' in row and pd.notna(row['url']) and str(row['url']).strip():
                api_url = str(row['url']).strip()
            
            if not api_url:
                logging.warning(f"No valid URI or URL found for stream: {stream_name}")
                continue
            
            print(f"[{index + 1}/{len(df)}] Downloading history for {stream_name}")
            logging.info(f"Downloading history for {stream_name} from {api_url}")
            
            # Make API request
            response = requests.get(api_url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                
                # Initialize variables
                time_series_data = []
                
                # Try different data extraction patterns based on API response structure
                try:
                    # Pattern 1: FRED API format (observations)
                    if 'observations' in data:
                        for obs in data['observations']:
                            date_str = obs.get('date')
                            value = obs.get('value')
                            if date_str and value is not None:
                                time_series_data.append({
                                    'date': date_str,
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # Pattern 2: World Bank format ([1][0].value)
                    elif isinstance(data, list) and len(data) > 1:
                        for item in data[1]:
                            date_str = item.get('date')
                            value = item.get('value')
                            if date_str and value is not None:
                                # World Bank uses year only, convert to full date
                                time_series_data.append({
                                    'date': f"{date_str}-01-01",
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # Pattern 3: EIA format (response.data[].generation/price/customers)
                    elif 'response' in data and 'data' in data['response']:
                        for item in data['response']['data']:
                            period = item.get('period')
                            # Try different value fields based on what's available
                            value = (item.get('generation') or 
                                    item.get('price') or 
                                    item.get('customers') or 
                                    item.get('value'))
                            
                            if period and value is not None:
                                # EIA uses YYYY-MM format, convert to full date
                                if len(period) == 7:  # YYYY-MM format
                                    date_str = f"{period}-01"
                                elif len(period) == 10:  # YYYY-MM-DD format
                                    date_str = period
                                else:
                                    date_str = f"{period}-01-01"
                                time_series_data.append({
                                    'date': date_str,
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # Pattern 4: Alpha Vantage time series format
                    elif 'Time Series (Daily)' in data:
                        time_series = data['Time Series (Daily)']
                        for date_str, values in time_series.items():
                            # Get the open price or first available price
                            value = values.get('1. open') or values.get('4. close')
                            if value is not None:
                                time_series_data.append({
                                    'date': date_str,
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # Pattern 5: Datatable format
                    elif 'datatable' in data and 'data' in data['datatable']:
                        datatable = data['datatable']['data']
                        for row_data in datatable:
                            if len(row_data) > 3: 
                                date_str = row_data[0] 
                                value = row_data[3]  
                            elif len(row_data) == 3:  
                                date_str = row_data[0]  
                                value = row_data[2]
                            else:
                                continue
                                
                            if date_str and value is not None:
                                time_series_data.append({
                                    'date': date_str,
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # If we found data, process and save it
                    if time_series_data:
                        # Sort by date in ascending order
                        time_series_data.sort(key=lambda x: x['date'])
                        
                        # Create CSV file path
                        csv_filename = f"{stream_name}.csv"
                        csv_filepath = os.path.join(history_base_dir, csv_filename)
                        
                        # Write CSV file
                        with open(csv_filepath, 'w', newline='') as csvfile:
                            csvwriter = csv.writer(csvfile)
                            csvwriter.writerow(['index', 'value'])
                            
                            for item in time_series_data:
                                try:
                                    # Parse date and format consistently
                                    date_obj = dt.datetime.strptime(item['date'], item['date_format'])
                                    formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')
                                    
                                    # Convert value to float
                                    value = float(item['value'])
                                    
                                    csvwriter.writerow([formatted_date, value])
                                except (ValueError, KeyError) as e:
                                    logging.warning(f"Skipping invalid data for {stream_name}: {e}")
                                    continue
                    
                        print(f"  Saved {len(time_series_data)} data points to {csv_filename}")
                        logging.info(f"Data saved to {csv_filepath}")
                    else:
                        print(f"  No time series data found for {stream_name}")
                        logging.warning(f"No time series data found in response for {stream_name}")
                        # Log the structure for debugging
                        logging.debug(f"Response structure: {list(data.keys()) if isinstance(data, dict) else 'List response'}")
                
                except Exception as e:
                    print(f"  Error processing data for {stream_name}: {e}")
                    logging.error(f"Error processing data for {stream_name}: {e}")
                    logging.debug(f"Response data: {json.dumps(data, indent=2)[:500]}...")
            
            else:
                print(f"  Failed to retrieve data for {stream_name} (Status: {response.status_code})")
                logging.error(f"Failed to retrieve data for {stream_name}. Status code: {response.status_code}")
        
        except Exception as e:
            print(f"  Error downloading history for {stream_name}: {str(e)}")
            logging.error(f"Error downloading history for stream {stream_name}: {str(e)}")
            continue
    
    print(f"\nHistorical data download completed. Files saved in '{history_base_dir}' directory.")

if __name__ == "__main__":
    # Upload all chunks and get topics after each
    upload_all_chunks()
    
    # Alternative: Upload single file for testing
    # topics = upload_single_chunk_and_get_topics('chunks/chunk_001.csv')
    # if topics:
    #     print(f"Got {len(topics)} topics")