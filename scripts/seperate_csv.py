import pandas as pd
import os
import math

def chunk_csv(input_file, chunk_size=50, output_folder='chunks'):
    """
    Split a CSV file into smaller chunks and save them in a specified folder.

    """
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Read the master CSV file
    df = pd.read_csv(input_file)
    
    # Calculate number of chunks needed
    total_rows = len(df)
    num_chunks = math.ceil(total_rows / chunk_size)
    
    print(f"Total rows: {total_rows}")
    print(f"Chunk size: {chunk_size}")
    print(f"Number of chunks: {num_chunks}")
    
    # Split and save chunks
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_rows)
        
        # Extract chunk
        chunk_df = df.iloc[start_idx:end_idx]
        
        # Create filename with zero-padded numbers for proper sorting
        chunk_filename = f"chunk_{i+1:03d}.csv"
        chunk_path = os.path.join(output_folder, chunk_filename)
        
        # Save chunk to CSV
        chunk_df.to_csv(chunk_path, index=False)
        
        print(f"Saved {chunk_filename} with {len(chunk_df)} rows")
    
    print(f"\nAll chunks saved in '{output_folder}' folder")

if __name__ == "__main__":
    # input_csv = 'Datastreams - Sheet1.csv'
    input_csv = os.path.join(os.path.expanduser("~"), "Downloads", "Datastreams - Sheet1.csv")
    
    # Split into 50-row chunks and save in 'chunks' folder
    chunk_csv(input_csv, chunk_size=50, output_folder='chunks')