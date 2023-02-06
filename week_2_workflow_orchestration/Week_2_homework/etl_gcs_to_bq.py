from prefect import flow , task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd


@task()
def extract_from_gcs(dataset_file):
    gcs_path = dataset_file
    gcs_block = GcsBucket.load("de-bucket-gcs")
    gcs_block.get_directory(from_path = gcs_path , local_path = '.')
    return gcs_path

@task
def transform(file):
    df = pd.read_parquet(file)
    return df

@task(log_prints = True)
def write_gbq(df):
    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table = "de_trips_data.rides_green",
        project_id = 'peerless-return-376206',
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize =500000,
        if_exists = "append"
    )

@flow (log_prints = True)
def etl_gcs_to_bq():
    dataset_file ='yellow_tripdata_201903.parquet'
    file = extract_from_gcs(dataset_file)
    clean_df = transform (file)
    write_gbq(clean_df)
    


if __name__ == "__main__":
    etl_gcs_to_bq()

