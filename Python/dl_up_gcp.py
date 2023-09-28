from google.cloud import storage
import pandas as pd
import pandas_gbq as pgbq
import io

# my credentials not in git
import credentials as cred


def init_blob_gcs(
    cred: str = cred.PROJECT_ID,
    bucket_name: str = cred.BUCKET_NAME,
    file: str = cred.FILE_NAME,
):
    """
    Initializes and returns a Google Cloud Storage blob object.

    Args:
        None

    Returns:
        google.cloud.storage.blob.Blob: A blob object representing the specified file in Google Cloud Storage.
    """
    storage_client = storage.Client(cred)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file)
    return blob


def df_from_gcs_to_df_way1(
    cred: str = cred.PROJECT_ID,
    bucket_name: str = cred.BUCKET_NAME,
    file: str = cred.FILE_NAME,
) -> pd.DataFrame:
    """
    Downloads data from Google Cloud Storage and returns it as a DataFrame (way 1).

    Args:
        None

    Raises:
        None

    Returns:
        pandas.DataFrame: The data from Google Cloud Storage as a DataFrame."""
    blob = init_blob_gcs(cred, bucket_name, file)
    # download as a string to put on dataframe : way 1
    contents = blob.download_as_string()
    contents_csv = contents.decode("utf-8")
    data_from_gcs_w1 = pd.read_csv(io.StringIO(contents_csv))
    return data_from_gcs_w1


def df_from_gcs_to_df_way2(gsutil_uri: str = cred.GSUTIL_URI_FILE) -> pd.DataFrame:
    """
    Downloads data from Google Cloud Storage and returns it as a DataFrame (way 2).

    Args:
        None

    Raises:
        None

    Returns:
        pandas.DataFrame: The data from Google Cloud Storage as a DataFrame."""
    # download as a string to put on dataframe : way 2
    data_from_gcs_w2 = pd.read_csv(gsutil_uri)
    return data_from_gcs_w2


def dl_from_gcs_to_local(filename: str = cred.LOCAL_FILE_NAME):
    """
    Downloads a file from Google Cloud Storage to the local system.

    Args:
        None

    Raises:
        None

    Returns:
        None"""
    blob = init_blob_gcs()
    # to get the CSV file
    blob.download_to_filename(filename)


def df_from_local_csv(filename: str = cred.LOCAL_FILE_NAME) -> pd.DataFrame:
    """
    Reads a CSV file and returns its contents as a DataFrame.

    Args:
        filename (str): The path to the CSV file to be read.

    Raises:
        None

    Returns:
        pandas.DataFrame: The contents of the CSV file as a DataFrame."""
    df = pd.read_csv(filename)
    return df


def df_to_bq(df: pd.DataFrame, tableId: str = cred.TABLE_ID_USANAMES):
    """
    Uploads a DataFrame to Google BigQuery.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.
        tableId (str): The ID of the BigQuery table to which the DataFrame should be uploaded. Default is cred.TABLE_ID_USANAMES.

    Raises:
        None

    Returns:
        None"""
    pgbq.to_gbq(df, tableId, project_id=cred.PROJECT_ID, if_exists="replace")


if __name__ == "__main__":
    print("dl_up_gcp")
    df = dl_from_gcs_to_local()
    print(df)
