import io
import os
import requests
import pandas as pd
import gzip
from pathlib import Path
from google.cloud import storage

init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"

BUCKET = os.environ.get("GCP_GCS_BUCKET","de_taxi_data")

def upload_to_gcs(bucket, object_name, local_file):
    #client = storage.Client()
    client = storage.Client.from_service_account_json("key.json")
    bucket = client.get_bucket(BUCKET)
    blob = bucket.blob(object_name)
    print(blob.upload_from_filename(local_file))


def web_to_gcs(year, service):
    for i in range(12):

        month = '0'+str(i+1)
        month = month[-2:]

        file_name = service + "_tripdata_" + year + "-" + month + ".csv.gz"
        print(f"Downloading: {init_url}")

        file_data = requests.get(init_url).content
        with open(file_name, 'wb') as f:
            f.write(file_data)

        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


file_name ='fhv_tripdata_2019-12.csv.gz'
service ='fhv'
upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)


