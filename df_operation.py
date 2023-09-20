import pandas as pd
import dl_up_gcp as dl

if __name__ == "__main__":
    print("df operation")
    df = dl.df_from_local_csv("testname.csv")
    print(df)
