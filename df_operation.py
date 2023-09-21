import pandas as pd
import dl_up_gcp as dl


def add_statename_df_togbq(
    df_main: pd.DataFrame,
    df_col: pd.DataFrame,
    to_gbq: bool = False,
    table_id: str | None = None,
) -> pd.DataFrame:
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
    df_name_oc = df.drop(["gender", "year", "state", "abbreviation"], axis=1)
    df_name_oc = df_name_oc.groupby("name").sum()
    # print(df_name_oc)
    if to_gbq:
        dl.df_to_bq(df_name_oc, table_id)
    return df_name_oc


def sort_by_names_occurence(df: pd.DataFrame) -> pd.DataFrame:
    df.sort_values(by="number", ascending=False, inplace=True)
    print(df)
    return df


if __name__ == "__main__":
    print("df operation")
    df_names = dl.df_from_local_csv("testname.csv")
    df_states = dl.df_from_local_csv("states.csv")
    df_full = add_statename_df_togbq(df_names, df_states)
    df_state2 = state_abreviation_tobq(df_full, False, "dwh_usnames.states")
    df_name_oc = name_occurence_df(df_full, False, "dwh_usnames.names_occurences")
    df_name_oc_sorted = sort_by_names_occurence(df_name_oc)
