from google.cloud import storage
import pandas as pd
import pandas_gbq as pgbq
import io

# my credentials not in git
import credentials as cred


def init_blob_gcs():
    storage_client = storage.Client(cred.PROJECT_ID)
    bucket = storage_client.get_bucket(cred.BUCKET_NAME)
    blob = bucket.blob(cred.FILE_NAME)
    return blob


def dl_from_gcs_to_df_way1():
    blob = init_blob_gcs()
    # download as a string to put on dataframe : way 1
    contents = blob.download_as_string()
    contents_csv = contents.decode("utf-8")
    data_from_gcs_w1 = pd.read_csv(io.StringIO(contents_csv))
    print(data_from_gcs_w1)
    return data_from_gcs_w1


def dl_from_gcs_to_df_way2():
    # download as a string to put on dataframe : way 2
    data_from_gcs_w2 = pd.read_csv(cred.GSUTIL_URI_FILE)
    print(data_from_gcs_w2)
    return data_from_gcs_w2


def dl_from_gcs_to_local():
    blob = init_blob_gcs()
    # to get the CSV file
    blob.download_to_filename("testname.csv")


def df_from_local_csv(filename):
    df = pd.read_csv(filename)
    return df


def df_to_bq(df: pd.DataFrame, tableId: str = cred.TABLE_ID_USANAMES):
    pgbq.to_gbq(df, tableId, project_id=cred.PROJECT_ID, if_exists="replace")


if __name__ == "__main__":
    print("dl_up_gcp")
    df = df_from_local_csv("testname.csv")
    print(df)
