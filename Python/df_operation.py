import pandas as pd

# my library
import dl_up_gcp as dl

# my credentials not in git
import credentials as cred


def add_statename_df_togbq(
    df_main: pd.DataFrame,
    df_col: pd.DataFrame,
    to_gbq: bool = False,
    table_id: str | None = None,
) -> pd.DataFrame:
    """
    Adds state names to a DataFrame by merging it with another DataFrame based on the 'state' column.

    Args:
        df_main (pandas.DataFrame): The main DataFrame to which state names will be added.
        df_col (pandas.DataFrame): The DataFrame containing state names, with a column 'Abbreviation' for matching.
        to_gbq (bool): If True, uploads the resulting DataFrame to Google BigQuery. Default is False.
        table_id (str | None): The ID of the BigQuery table to which the DataFrame should be uploaded. Default is None.

    Raises:
        None

    Returns:
        pandas.DataFrame: The main DataFrame with state names added."""
    df = df_main.merge(df_col, left_on="state", right_on="Abbreviation")
    df.drop("state", axis=1, inplace=True)
    df.rename(columns={"State": "state", "Abbreviation": "abbreviation"}, inplace=True)
    if to_gbq:
        dl.df_to_bq(df, table_id)
    return df


def state_abreviation_tobq(
    df: pd.DataFrame,
    to_gbq: bool = False,
    table_id: str | None = None,
) -> pd.DataFrame:
    """
    Converts a DataFrame containing state abbreviations to a new DataFrame with unique values.

    Args:
        df (pandas.DataFrame): The input DataFrame containing state abbreviations.
        to_gbq (bool): If True, uploads the resulting DataFrame to Google BigQuery. Default is False.
        table_id (str | None): The ID of the BigQuery table to which the DataFrame should be uploaded. Default is None.

    Raises:
        None

    Returns:
        pandas.DataFrame: A new DataFrame with unique state abbreviations."""
    # Un peu inutile étant donné que j'ai déjà le state.csv mais ça me fait utiliser les fonctions
    df_state = df.drop(["gender", "year", "name", "number"], axis=1)
    df_state.drop_duplicates(inplace=True, ignore_index=True)
    if to_gbq:
        dl.df_to_bq(df_state, table_id)
    return df_state


def name_occurence_df(
    df: pd.DataFrame,
    to_gbq: bool = False,
    table_id: str | None = None,
) -> pd.DataFrame:
    """
    Generates a DataFrame that summarizes name occurrences.

    Args:
        df (pandas.DataFrame): The input DataFrame containing name data.
        to_gbq (bool): If True, uploads the resulting DataFrame to Google BigQuery. Default is False.
        table_id (str | None): The ID of the BigQuery table to which the DataFrame should be uploaded. Default is None.

    Raises:
        None

    Returns:
        pandas.DataFrame: A new DataFrame summarizing name occurrences."""
    df_name_oc = df.drop(["gender", "year", "state", "abbreviation"], axis=1)
    df_name_oc = df_name_oc.groupby("name").sum()
    # print(df_name_oc)
    if to_gbq:
        dl.df_to_bq(df_name_oc, table_id)
    return df_name_oc


def sort_by_names_occurence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts a DataFrame by the 'number' column in descending order.

    Args:
        df (pandas.DataFrame): The input DataFrame to be sorted.

    Raises:
        None

    Returns:
        pandas.DataFrame: The sorted DataFrame."""
    df.sort_values(by="number", ascending=False, inplace=True)
    print(df)
    return df


if __name__ == "__main__":
    print("df operation")
    df_names = dl.df_from_local_csv(cred.LOCAL_FILE_NAME)
    df_states = dl.df_from_local_csv(cred.STATE_CSV)
    df_full = add_statename_df_togbq(df_names, df_states)
    print(df_full)
    df_state2 = state_abreviation_tobq(df_full, False, cred.TABLE_ID_STATES)
    print(df_state2)
    df_name_oc = name_occurence_df(df_full, False, cred.TABLE_ID_NAMEOCC)
    print(df_name_oc)
    df_name_oc_sorted = sort_by_names_occurence(df_name_oc)
