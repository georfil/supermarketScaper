from  .db_init import initialiseDB
import pandas as pd


def getCategories():
    cursor, cnxn = initialiseDB()
    cursor.execute("SELECT * FROM dbo.productTypes")
    cols = [c[0] for c in cursor.description]  
    rows = cursor.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    cnxn.close()
    return df

def getSupermarkets():
    cursor, cnxn = initialiseDB()
    cursor.execute("SELECT * FROM dbo.supermarkets")
    cols = [c[0] for c in cursor.description]   
    rows = cursor.fetchall()
    df = pd.DataFrame.from_records(rows, columns=cols)
    cnxn.close()
    return df

def getIDSforDetailsSearch():
    cursor, cnxn = initialiseDB()
    cursor.execute("SELECT product_id, original_product_code, supermarket_code FROM dbo.products WHERE searchedDetails = 0")
    rows = cursor.fetchall()
    cnxn.close()
    return rows


