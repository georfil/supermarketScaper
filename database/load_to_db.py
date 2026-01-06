import pandas as pd
from db_init import initialiseDB
# from .db_loadElements import getIDS


def removeExistingIDs(df):
    df_final = df.copy()
    existingIDs = getIDS()
    df_final = df_final[~df_final['product_id'].isin(existingIDs)]
    return df_final

def load_to_db(tables: dict[str, pd.DataFrame]={}):
    cursor, conn = initialiseDB()
    print(conn)


load_to_db()