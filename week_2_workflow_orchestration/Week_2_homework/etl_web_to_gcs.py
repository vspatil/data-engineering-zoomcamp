#from pathlib import path
import pandas as pd
from prefect import flow , task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow.parquet as pq
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints = True)
def write_to_gcs(file) -> None :
    gcs_block = GcsBucket.load("de-bucket-gcs")
    gcs_block.upload_from_path(from_path=file,to_path='yellow_tripdata_201903.parquet')
    return


@flow()
def etl_web_to_gcs() -> None :
   """ this is ETL function"""
   dataset_file = "../data/yellow_tripdata_2019-03.parquet"
   write_to_gcs(dataset_file)

if __name__ =='__main__':
    etl_web_to_gcs()
   
    