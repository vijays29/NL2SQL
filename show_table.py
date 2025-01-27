import os
import mysql.connector
from dotenv import load_dotenv
import json  # Import the JSON module

# Load environment variables
load_dotenv()

# Environment variables
hostName = os.getenv("HOST_NAME")
userName = os.getenv("USER_NAME")
password = os.getenv("PASSWORD")
dataBaseName = os.getenv("DATABASE_NAME")

def table(query):
    mydb = None
    try:
        # Connect to the database
        mydb = mysql.connector.connect(
            host=hostName, user=userName, password=password, database=dataBaseName
        )
        cur = mydb.cursor()  # Corrected the typo from cur=mydb.cur() to mydb.cursor()

        # Fetch all tables in the database
        cur.execute(query)
        tables = cur.fetchall()

        result = {}
        for (table_name,) in tables:
            # Fetch all columns for each table
            cur.execute(f"DESCRIBE {table_name}")
            columns = cur.fetchall()
            result[table_name] = [column[0] for column in columns]

        # Convert the result dictionary to JSON format
        return json.dumps({"tables_and_fields": result}, indent=4,separators=(',', ':'),
                           sort_keys=True)

    finally:
        # Ensure resources are properly closed
        if mydb and mydb.is_connected():
            cur.close()
            mydb.close()
