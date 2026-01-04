import pandas as pd
from .db_init import initialiseDB
from .db_loadElements import getIDS



def removeExistingIDs(df):
    df_final = df.copy()
    existingIDs = getIDS()
    df_final = df_final[~df_final['product_id'].isin(existingIDs)]
    return df_final


def executemany_batched(cursor, cnxn, sql, rows, batch_size=100, fastExecute = True):
    # rows: list[tuple] (or any sliceable sequence)
    cursor.fast_executemany = fastExecute
    print(rows)
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i:i+batch_size])
        cnxn.commit()


def load_to_db(tables: dict[str, pd.DataFrame]):

    cursor, cnxn = initialiseDB()
    for table_name, df in tables.items():  # iterate key/value pairs 
        if df is None or df.empty:
            continue

        if table_name == "dbo.products":
            df_to_load = removeExistingIDs(df)
        else:
            df_to_load = df.copy()

        # If your table has a DATE column and df has datetime strings
        if "created_at" in df_to_load.columns:
            df_to_load["created_at"] = pd.to_datetime(df_to_load["created_at"]).dt.date



        cols = list(df_to_load.columns)
        col_list_sql = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)

        sql = f"INSERT INTO {table_name} ({col_list_sql}) VALUES ({placeholders})"

        # executemany likes a list of tuples (not a generator) [web:372]
        rows = list(df_to_load.itertuples(index=False, name=None))  # tuples per row [web:364]

    if rows:
        executemany_batched(cursor, cnxn, sql, rows, batch_size=100)



    cursor.close()
    cnxn.close()

def load_details_to_db(tables: dict[str, pd.DataFrame]):
    cursor, cnxn = initialiseDB()
    for table_name, df in tables.items():  # iterate key/value pairs 
        if df is None or df.empty:
            continue
        
        
        if table_name == "dbo.products":

            cols = list(df.columns)
            set_cols = [c for c in cols if c != "product_id"]
            set_sql = ", ".join(f"[{c}]=?" for c in set_cols)
            sql = f"UPDATE {table_name} SET {set_sql} WHERE [product_id]=?"

            rows = [tuple(x) for x in df[set_cols + ["product_id"]].to_numpy()]
            fastExecute = False



        else:

            cols = list(df.columns)
            col_list_sql = ", ".join(f"[{c}]" for c in cols)
            placeholders = ", ".join("?" for _ in cols)

            sql = f"INSERT INTO {table_name} ({col_list_sql}) VALUES ({placeholders})"

            # executemany likes a list of tuples (not a generator) [web:372]
            rows = list(df.itertuples(index=False, name=None))  # tuples per row [web:364]
            fastExecute = True

        
        if rows:
            executemany_batched(cursor, cnxn, sql, rows, batch_size=100, fastExecute=fastExecute)


    cursor.close()
    cnxn.close()