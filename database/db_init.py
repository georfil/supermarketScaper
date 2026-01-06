import pyodbc
import os
from dotenv import load_dotenv
from time import sleep

HERE = os.path.dirname(os.path.abspath(__file__))          # ...\supermarketScaper\database
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))   # ...\supermarketScaper
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))    



def pick_driver():
    available = pyodbc.drivers()
    for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"):
        if d in available:
            return d
    raise RuntimeError(f"No SQL Server ODBC driver found. Available drivers: {available}")

def initialiseDB(max_retries=3, base_delay=3):
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_DATABASE"]
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]

    driver = pick_driver()
    print(f"Picked Driver:{driver}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Connecting to DB (attempt {attempt})...")
            cnxn = pyodbc.connect(conn_str)
            cursor = cnxn.cursor()
            cursor.fast_executemany = True
            print("✅ Database connection established")
            return cursor, cnxn

        except pyodbc.Error as e:
            # First failure is expected when DB is paused
            print("⏳ Database is waking up...")
            if attempt == max_retries:
                raise RuntimeError("DB did not wake up in time") from e
            sleep(base_delay * attempt)
