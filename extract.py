import requests
from pyspark.sql import SparkSession
from google.cloud import storage

# Initialize Spark session
spark = SparkSession.builder.appName("TfL-Extract").getOrCreate()

# TfL API parameters
api_url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
params = {"app_key": "e08d99ee40564396a78d896a70788546"}  

# Fetch data from API
response = requests.get(api_url, params=params)
if response.status_code == 200:
    data = response.json()
else:
    print("Failed to fetch data:", response.status_code)
    exit()
# Save data to GCS
bucket_name = "tfl-data-bucket"
file_name = "raw/tube_status.json"
client = storage.Client()
bucket = client.get_bucket(bucket_name)
blob = bucket.blob(file_name)
blob.upload_from_string(response.text)

print("Data successfully saved to GCS.")