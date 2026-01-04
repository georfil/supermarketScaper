import pyodbc
import os
from dotenv import load_dotenv
from time import sleep

HERE = os.path.dirname(os.path.abspath(__file__))          # ...\supermarketScaper\database
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))   # ...\supermarketScaper
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))    



def _pick_driver():
    available = pyodbc.drivers()
    print(available)
    for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"):
        if d in available:
            return d
    raise RuntimeError(f"No SQL Server ODBC driver found. Available drivers: {available}")

def initialiseDB(max_retries=3, base_delay=2):
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_DATABASE"]
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]

    driver = _pick_driver()

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            cnxn = pyodbc.connect(conn_str, timeout=10)
            cursor = cnxn.cursor()
            cursor.fast_executemany = True
            print("Established connection with db")
            return cursor, cnxn
        except pyodbc.Error as e:
            last_err = e
            if attempt == max_retries:
                raise
            sleep(base_delay * attempt)

    raise last_err
