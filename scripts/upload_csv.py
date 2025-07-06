import os
import requests
import glob

def upload_all_chunks(chunks_folder='chunks', upload_url='http://localhost:24601/upload_datastream_csv'):
    """
    Upload all CSV files from the chunks folder to the web application.
    """
    
    # Find all CSV files in chunks folder
    csv_files = glob.glob(os.path.join(chunks_folder, '*.csv'))
    csv_files.sort()  # Ensure proper order
    
    print(f"Found {len(csv_files)} CSV files to upload")
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        
        try:
            with open(csv_file, 'rb') as file:
                files = {'file': (filename, file, 'text/csv')}
                response = requests.post(upload_url, files=files)
                
            if response.ok:
                print(f"✓ Successfully uploaded {filename}")
            else:
                print(f"✗ Failed to upload {filename}: {response.status_code}")
                
        except Exception as e:
            print(f"✗ Error uploading {filename}: {str(e)}")

if __name__ == "__main__":
    upload_all_chunks()