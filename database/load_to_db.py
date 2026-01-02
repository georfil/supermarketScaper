import pandas as pd
from .db_init import initialiseDB
from .db_loadElements import getIDS

CURSOR, CNXN = initialiseDB()


def removeExistingIDs(df):
    df_final = df.copy()
    existingIDs = getIDS()
    df_final[~df_final['product_id'].isin(existingIDs)]
    return df_final

def load_to_db(tables: dict[str, pd.DataFrame]):


    for table_name, df in tables.items():  # iterate key/value pairs 
        if df is None or df.empty:
            continue

        df_to_load = removeExistingIDs(df)

        # If your table has a DATE column and df has datetime strings
        if "created_at" in df_to_load.columns:
            df_to_load["created_at"] = pd.to_datetime(df_to_load["created_at"]).dt.date



        cols = list(df_to_load.columns)
        col_list_sql = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)

        sql = f"INSERT INTO {table_name} ({col_list_sql}) VALUES ({placeholders})"

        # executemany likes a list of tuples (not a generator) [web:372]
        rows = list(df_to_load.itertuples(index=False, name=None))  # tuples per row [web:364]
        CURSOR.executemany(sql, rows)

    CNXN.commit()
