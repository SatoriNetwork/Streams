import os
import requests
import glob
import json
import time
import csv
import pandas as pd
import logging
import datetime as dt

def upload_history_csv(topic, csv_file_path, base_url='http://localhost:24601'):
    """
    Upload a history CSV file to the neuron using the merge_history_csv endpoint
    
    Args:
        topic (str): The topic string for the stream
        csv_file_path (str): Path to the CSV file to upload
        base_url (str): Base URL of the neuron server
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(csv_file_path):
        print(f"  History file not found: {csv_file_path}")
        return False
    
    merge_url = f"{base_url}/merge_history_csv/{topic}"
    
    try:
        with open(csv_file_path, 'rb') as file:
            files = {'file': (os.path.basename(csv_file_path), file, 'text/csv')}
            data = {'topic': json.dumps(topic)}
            response = requests.post(merge_url, files=files, data=data)
        
        if response.ok:
            print(f" Successfully merged history for topic: {topic}")
            return True
        else:
            print(f" Failed to merge history for topic: {topic} (Status: {response.status_code})")
            print(f"    Response: {response.text}")
            return False
            
    except Exception as e:
        print(f" Error uploading history for topic {topic}: {str(e)}")
        return False

def download_and_upload_history(topics_list, base_url='http://localhost:24601'):
    """
    Download historical data for topics and immediately upload them to the neuron
    
    Args:
        topics_list (list): List of topic dictionaries from get_topics()
        base_url (str): Base URL of the neuron server
    """
    print(f"\n{'='*50}")
    print("DOWNLOADING AND UPLOADING HISTORICAL DATA")
    print(f"{'='*50}")
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create temporary history directory
    history_base_dir = "temp_history"
    if not os.path.exists(history_base_dir):
        os.makedirs(history_base_dir)
        print(f"Created temporary history directory: {history_base_dir}")
    
    successful_merges = 0
    failed_merges = 0
    
    for index, topic_data in enumerate(topics_list):
        try:
            # Extract topic information
            topic = topic_data.get('topic', '')
            stream_name = topic_data.get('stream', f'stream_{index}')
            
            # Get the API URL - check both 'uri' and 'url' fields in the original relay CSV
            # Since we don't have direct access to the original data, we'll need to reconstruct it
            # For now, let's assume the topic_data contains the necessary URL information
            api_url = topic_data.get('uri') or topic_data.get('url')
            
            if not api_url:
                print(f"[{index + 1}/{len(topics_list)}] No API URL found for {stream_name}, skipping...")
                continue
            topic_str = json.dumps(topic, separators=(',', ':')) if isinstance(topic, dict) else topic
            print(f"[{index + 1}/{len(topics_list)}] Processing {stream_name} (Topic: {topic_str})")    
            # print(f"[{index + 1}/{len(topics_list)}] Processing {stream_name} (Topic: {topic})")
            
            # Download historical data
            response = requests.get(api_url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                time_series_data = []
                
                # Extract time series data using the same patterns as download_history
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
                    
                    # Pattern 2: World Bank format
                    elif isinstance(data, list) and len(data) > 1:
                        for item in data[1]:
                            date_str = item.get('date')
                            value = item.get('value')
                            if date_str and value is not None:
                                time_series_data.append({
                                    'date': f"{date_str}-01-01",
                                    'value': value,
                                    'date_format': '%Y-%m-%d'
                                })
                    
                    # Pattern 3: EIA format
                    elif 'response' in data and 'data' in data['response']:
                        for item in data['response']['data']:
                            period = item.get('period')
                            value = (item.get('generation') or 
                                    item.get('price') or 
                                    item.get('customers') or 
                                    item.get('value'))
                            
                            if period and value is not None:
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
                    
                    # Pattern 4: Alpha Vantage format
                    elif 'Time Series (Daily)' in data:
                        time_series = data['Time Series (Daily)']
                        for date_str, values in time_series.items():
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
                    
                    # If we found data, create CSV and upload it
                    if time_series_data:
                        # Sort by date in ascending order
                        time_series_data.sort(key=lambda x: x['date'])
                        
                        # Create temporary CSV file
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
                        
                        print(f"  Downloaded {len(time_series_data)} data points")
                        
                        # Upload the history CSV to the neuron
                        if upload_history_csv(topic, csv_filepath, base_url):
                            successful_merges += 1
                        else:
                            failed_merges += 1
                        
                        # Clean up temporary file
                        try:
                            os.remove(csv_filepath)
                        except:
                            pass
                    else:
                        print(f"  No time series data found for {stream_name}")
                        failed_merges += 1
                
                except Exception as e:
                    print(f"  Error processing data for {stream_name}: {e}")
                    failed_merges += 1
                    continue
            
            else:
                print(f"  Failed to retrieve data for {stream_name} (Status: {response.status_code})")
                failed_merges += 1
        
        except Exception as e:
            print(f"  Error processing {stream_name}: {str(e)}")
            failed_merges += 1
            continue
        
        # Small delay between operations
        time.sleep(0.5)
    
    # Clean up temporary directory
    try:
        os.rmdir(history_base_dir)
    except:
        pass
    
    print(f"\nHistory merge summary:")
    print(f"  Successful merges: {successful_merges}")
    print(f"  Failed merges: {failed_merges}")
    print(f"  Total processed: {len(topics_list)}")

def upload_all_chunks_with_history_merge(chunks_folder='chunks', base_url='http://localhost:24601', 
                                        delay_between_uploads=70, merge_history_flag=True):
    """
    Enhanced version that uploads chunks and immediately merges historical data for each chunk
    """
    upload_url = f"{base_url}/upload_datastream_csv"
    
    # Find all CSV files in chunks folder
    csv_files = glob.glob(os.path.join(chunks_folder, '*.csv'))
    csv_files.sort()  # Ensure proper order
    
    print(f"Found {len(csv_files)} CSV files to upload")
    
    upload_results = []
    
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
                    
                    # If we have topics and history merge is enabled, process them
                    if merge_history_flag and topics:
                        # We need to get the original CSV data to match topics with URLs
                        # Read the uploaded chunk to get the URL information
                        chunk_df = pd.read_csv(csv_file)
                        
                        # Match topics with their URLs from the chunk
                        enhanced_topics = []
                        for topic_info in topics:
                            # Find matching row in chunk based on stream name or other identifiers
                            stream_name = topic_info.get('stream', '')
                            author = topic_info.get('author', '')
                            
                            # Look for matching row in chunk_df
                            matching_rows = chunk_df[
                                (chunk_df.get('stream', '') == stream_name) & 
                                (chunk_df.get('author', '') == author)
                            ]
                            
                            if not matching_rows.empty:
                                row = matching_rows.iloc[0]
                                # Add URL information to topic
                                enhanced_topic = topic_info.copy()
                                enhanced_topic['uri'] = row.get('uri', row.get('url', ''))
                                enhanced_topics.append(enhanced_topic)
                        print(enhanced_topic)
                        # Download and upload historical data for this chunk
                        if enhanced_topics:
                            print(1)
                            download_and_upload_history(enhanced_topics, base_url)
                    
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
        print(f"Total topics processed: {total_topics}")
    
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

# Keep your existing functions
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
                # topic = f"{row.get('source', 'satori')}|{row.get('author', '')}|{row.get('stream', '')}|{row.get('target', '')}"
                topic = {
                    "source": row.get('source', 'satori'),
                    "author": row.get('author', ''),  # This should be the wallet's pubkey
                    "stream": row.get('stream', ''),
                    "target": row.get('target', '')
                }
                print(topic)
                topics.append({
                    'topic': topic,
                    'source': row.get('source', 'satori'),
                    'author': row.get('author', ''),
                    'stream': row.get('stream', ''),
                    'target': row.get('target', ''),
                    'uri': row.get('uri', row.get('url', ''))
                })
            return {'status': 'success', 'topics': topics}
        else:
            print(f"  Failed to get topics: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error getting topics from {relay_csv_url}: {str(e)}")
        return None

if __name__ == "__main__":
    # Use the enhanced function that merges history after each chunk
    upload_all_chunks_with_history_merge(merge_history_flag=True)