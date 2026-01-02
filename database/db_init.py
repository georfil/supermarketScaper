import pyodbc
import os
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))          # ...\supermarketScaper\database
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))   # ...\supermarketScaper
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))    

def initialiseDB():
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_DATABASE"]
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    cursor.fast_executemany = True  # key
    print("Established Conenction with db")
    return cursor, cnxn
