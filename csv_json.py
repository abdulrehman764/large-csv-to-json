import os
import shutil
import subprocess
import pandas as pd
import json
import boto3
from pprint import pprint

# Run AWS SSO login with the specified profile
subprocess.run(["aws", "sso", "login", "--profile", "opahlab"], check=True)

# Initialize the S3 session with the "opahlab" profile
session = boto3.Session(profile_name="opahlab")
s3_client = session.client('s3')

# Set directory and S3 path information
files_tmp_directory = r'C:\snowflake-new-architecture\filestemp'
raw_files_directory = os.path.join(files_tmp_directory, 'raw_files1')
converted_json_directory = os.path.join(files_tmp_directory, 'converted_json')

# Create directories if they do not exist
os.makedirs(raw_files_directory, exist_ok=True)
os.makedirs(converted_json_directory, exist_ok=True)

target_bucket_name = 'fortuna-merged-arl'
target_raw_path = '3334/rawfiles/'
target_json_path = '3334/convertedjson/'

# Function to safely remove a file or directory
def safe_remove_path(path):
    if os.path.exists(path):
        try:
            if os.path.isfile(path):
                os.remove(path)
                print(f"Successfully removed file: {path}")
            else:
                shutil.rmtree(path)
                print(f"Successfully removed directory: {path}")
        except PermissionError as e:
            print(f"Failed to remove {path}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while removing {path}: {e}")
    else:
        print(f"Path does not exist: {path}")

# List objects in the specified S3 bucket and prefix to create metadatadf
objects = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=target_raw_path)
metadata = []
for obj in objects.get('Contents', []):
    file_name = obj['Key'].split('/')[-1]
    if file_name.startswith("._"):  # Skip hidden or temporary files
        continue
    dir_path = obj['Key']
    file_size = obj['Size']
    last_modified = obj['LastModified']

    metadata.append({
        'bucket': target_bucket_name,
        'file_name': file_name,
        'dir_path': dir_path,
        'file_path': f"s3://{target_bucket_name}/{dir_path}",
        'file_size': file_size,
        'last_modified': last_modified
    })

# Convert the metadata list to a DataFrame
metadatadf = pd.DataFrame(metadata)

# Process each file in metadatadf
for _, row in metadatadf.iterrows():
    try:
        source_bucket_name = row['bucket']
        file_name = row['file_name']
        dir_path = row['dir_path']
        file_size = row['file_size']
        local_file = os.path.join(raw_files_directory, file_name)

        # Skip if file_name is empty or is actually a directory
        if not file_name or file_name.endswith('/'):
            print(f"Skipping directory or empty file name: {dir_path}")
            continue

        # Download the file from the source S3 bucket
        s3_client.download_file(source_bucket_name, dir_path, local_file)
        pprint(f"Downloaded file: {dir_path}")
        
        # Define a size threshold for chunk reading
        size_threshold = 1 * 1024 ** 3  # 5 GB

        output_json_path = os.path.join(converted_json_directory, f'{file_name}.json')

        if file_size <= size_threshold:
            # Read and process smaller files fully
            df = pd.read_csv(local_file, low_memory=False)
            df.to_json(output_json_path, orient='records', lines=True)
        else:
            # Process large files row-by-row to avoid memory error
            with open(output_json_path, 'w') as f:
                for chunk in pd.read_csv(local_file, chunksize=100000, low_memory=False):
                    # Convert each chunk to JSON and write to the file
                    chunk.to_json(f, orient='records', lines=True)

        # Upload the JSON file to S3
        s3_client.upload_file(output_json_path, target_bucket_name, f'{target_json_path}{file_name}.json')
        print(f"Uploaded JSON: {output_json_path}")

        # Upload the raw file to S3
        s3_client.upload_file(local_file, target_bucket_name, f'{target_raw_path}{file_name}')
        print(f"Uploaded Raw File: {local_file}")

        # Clean up local files
        safe_remove_path(local_file)
        safe_remove_path(output_json_path)

    except Exception as e:
        print(f"Error processing {dir_path}: {str(e)}")
        continue


