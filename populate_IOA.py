import requests
import zipfile
import os
import shutil
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import ast

import pandas as pd
import requests
import os
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers
import csv


# Connect to Elasticsearch
from elasticsearch import Elasticsearch

from elasticsearch import Elasticsearch

# CSV Download and update to elasticsearch instance
requests.packages.urllib3.disable_warnings()

def download_and_extract_zip(url, destination_folder):
    """
    Download zip files to computer based on google storage url link

    Attributes:
      url: google storage url of zip file
      destination_folder: folder for where file should be temporarily placed
    """
    # Modify the URL to use storage.googleapis.com
    url = url.replace('https://storage.cloud.google.com', 'https://storage.googleapis.com')
    
    # Send a GET request to the URL to download the zip file
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Create the destination folder if it doesn't exist
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)
        
        # Save the downloaded ZIP file
        zip_file_path = os.path.join(destination_folder, 'downloaded.zip')
        with open(zip_file_path, 'wb') as f:
            f.write(response.content)
        print(f"ZIP file downloaded to {zip_file_path}")
        
        # Extract the ZIP file
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
                print(f"ZIP file extracted to {destination_folder}")
        except zipfile.BadZipFile:
            print(f"Failed to extract ZIP file, not a valid zip file: {zip_file_path}")
        
        # Optionally, you can remove the zip file after extraction
        os.remove(zip_file_path)
        print(f"Deleted the ZIP file {zip_file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")


def csv_to_elastic(csv_file, name, index_name="tweets_test", dataset=""):
    """
    Reads a CSV file and inserts structured tweet data into Elasticsearch.
    
    Attributes:
      csv_file: csv file to add to ES
      name: name of the file to add as metadata
      index_name: name of index to be inserted into
      dataset: name of the dataset stored in Google Storage (ex: Venezuela, Russia)
    """

    int_columns = {"follower_count", "following_count", "like_count", "quote_count", "reply_count", "retweet_count"}
    date_columns = {"account_creation_date", "tweet_time"}

    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        actions = []
        
        for row in reader:
            # Ensure appropriate columns inserted as int
            for col in int_columns:
                if col in row and row[col]:
                    try:
                        row[col] = int(row[col])
                    except:
                        row[col] = None

            # Ensure appropriate columns inserted as date
            for col in date_columns:
              if col in row and row[col]:
                try:
                    # Parse the date and convert to ISO 8601
                    dt = datetime.strptime(row[col], "%Y-%m-%d %H:%M")
                    row[col] = dt.isoformat()  # 'YYYY-MM-DDTHH:MM:SS'
                except ValueError:
                    row[col] = None  # or handle as needed
            
            # Parse hashtags, urls, and user_mentions into proper list
            if 'hashtags' in row and row['hashtags']:
              try:
                row['hashtags'] = ast.literal_eval(row['hashtags'])
              except Exception:
                row['hashtags'] = []
            if 'urls' in row and row['urls']:
              try:
                row['urls'] = ast.literal_eval(row['urls'])
              except Exception:
                row['urls'] = []
            if 'user_mentions' in row and row['user_mentions']:
              try:
                row['user_mentions'] = ast.literal_eval(row['user_mentions'])
              except Exception:
                row['user_mentions'] = []
            
            action = {
                "_index": index_name,
                "_source": {
                    **row,
                    "dataset": dataset,
                    "file_name": name
                  }
            }
            actions.append(action)

        if actions:
            helpers.bulk(client, actions)
            print(f"✅ Inserted data from {csv_file} into Elasticsearch.")
        else:
            print(f"⚠️ No valid data found in {csv_file}.")


def empty_directory_if_exists(directory_path):
    """
    Empties the specified directory if it exists.
    If the directory does not exist, it does nothing.

    Args:
        directory_path: The path to the directory to empty.
    """
    if os.path.exists(directory_path):
        try:
            shutil.rmtree(directory_path)  # Remove the directory and its contents
            os.makedirs(directory_path)   # Recreate the directory
        except Exception as e:
            print(f"Error emptying directory: {e}")

# CONNECT TO ES
# TODO: get credentials from VM or .env file
es_host = None
es_port = None
es_username = None
es_password = None

# Create the Elasticsearch client with HTTPS and authentication
client = Elasticsearch([f'https://{es_host}:{es_port}'], 
                   basic_auth=(es_username, es_password),
                   verify_certs=False)


print("Connection to ES Server successful!\n\n")

# Create a new index
print("Creating a new index and mapping\n\n")
index_name = "ioa-tweets"
mapping = {
    "mappings": {
        "properties": {
            "dataset": {"type": "keyword"},      # single keyword field
            "hashtags": {"type": "keyword"},     # list of hashtags as keywords
            "urls": {"type": "keyword"}          # list of full URLs as keywords
        }
    }
}
client.indices.create(index=index_name, body=mapping, ignore=400)



# TODO: fill in folder where zips should be downloaded to
download_folder = ""

# 1) DOWNLOAD FILES
print("Starting Files Download\n\n")
# TODO: Fill in file containing twitter zip files
file_table_path = "../Twitter_IOs.csv"
df = pd.read_csv(file_table_path)

# Filter for only tweet files
tweet_files = df[df['filename'].str.contains('tweets_csv', na=False)]

# Loop through the tweet files and download them
empty_directory_if_exists("./extracted_files")
for index, row in tqdm(tweet_files.iterrows(), total=len(tweet_files)):
    # Correctly construct the file URL (remove redundant filename addition)
    file_url = row["Link"]  # The link in the CSV is already correct
    response = requests.get(file_url)
    if 'application/zip' in response.headers.get('Content-Type', '') or file_url.endswith('.zip'):
        print(f"Downloading file from {file_url}")
        download_and_extract_zip(file_url, "./extracted_files")

        # Insert into ES index
        print("Inserting into ES...\n\n")
        for filename in os.listdir(download_folder):
          if filename.endswith(".csv"):  # Ensure we're processing only tweet CSV files
            csv_to_elastic(os.path.join(download_folder, filename), filename, index_name, filename.split("_", 1)[0])

        # Delete extracted file
        empty_directory_if_exists("./extracted_files")

