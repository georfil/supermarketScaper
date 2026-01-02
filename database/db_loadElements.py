from  .db_init import initialiseDB
import pandas as pd

CURSOR, CNXN = initialiseDB()

def getCategories():
    CURSOR.execute("SELECT * FROM dbo.productTypes")
    cols = [c[0] for c in CURSOR.description]   # column names [web:176]
    rows = CURSOR.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    return df




def getSupermarkets():
    CURSOR.execute("SELECT * FROM dbo.supermarkets")
    cols = [c[0] for c in CURSOR.description]   # column names [web:176]
    rows = CURSOR.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    return df

def getIDS():
    CURSOR.execute("SELECT product_id FROM dbo.products")
    rows = CURSOR.fetchall()
    return rows[0]

