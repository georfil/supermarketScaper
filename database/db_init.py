import pyodbc
import os
from dotenv import load_dotenv
from time import sleep

HERE = os.path.dirname(os.path.abspath(__file__))          # ...\supermarketScaper\database
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))   # ...\supermarketScaper
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))    

def initialiseDB(max_retries=6, base_delay=2):
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_DATABASE"]      # or DB_DATABASE, but keep consistent
    username = os.environ["DB_USERNAME"]      # or DB_USERNAME
    password = os.environ["DB_PASSWORD"]

    conn_str = (
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            cnxn = pyodbc.connect(conn_str, timeout=20)
            cursor = cnxn.cursor()
            cursor.fast_executemany = True
            print("Established connection with DB")
            return cursor, cnxn
        except pyodbc.Error as e:
            last_err = e
            if attempt == max_retries:
                raise
            sleep(base_delay * attempt)  # simple backoff

    raise last_err
