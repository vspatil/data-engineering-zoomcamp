from prefect import flow , task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd


#@task()
def extract_from_gcs(dataset_file):
    gcs_path = f"fhv/{dataset_file}"
    #gcs_path = dataset_file
    gcs_block = GcsBucket.load("de-bucket-gcs")
    gcs_block.get_directory(from_path = gcs_path , local_path = '.')
    return gcs_path
    #print(gcs_path)
    
#@task
def transform(file):
    df = pd.read_csv(file)
    return (df.head(2))
    #print(df.head())

#@task(log_prints = True)
def write_gbq(df):
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    print("inside writing")
    print(df.head(1))

    df.to_gbq(
        destination_table = "de_trips_data.fhv_tripsdata_2019",
        project_id = 'peerless-return-376206',
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize =500000,
        if_exists = "append"
    )

#@flow (log_prints = True)
def etl_gcs_to_bq():
    dataset_file ="fhv_tripdata_2019-02.csv.gz"
    file = extract_from_gcs(dataset_file)
    clean_df = transform (file)
    write_gbq(clean_df)
    


if __name__ == "__main__":
    etl_gcs_to_bq()

