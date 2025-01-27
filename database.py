import os
from dotenv import load_dotenv
import mysql.connector
from fastapi import HTTPException

load_dotenv()
hostName=os.getenv("HOST_NAME")
userName=os.getenv("USER_NAME")
password=os.getenv("PASSWORD")
dataBaseName=os.getenv("DATABASE_NAME")

"""

Description:
This module handles database connection and schema retrieval for the application.  It uses MySQL as the database.
It provides functions to fetch the database schema details and execute SQL statements. It also provides methods to handle exceptions 
such as connection refused or incorrect SQL syntax.

Dependencies:
- os: For accessing environment variables.
- dotenv: For loading environment variables from a .env file.
- mysql.connector: For connecting to and interacting with the MySQL database.

Configuration:
- HOST_NAME: The hostname or IP address of the MySQL server.  Loaded from the environment variable "HOST_NAME".
- USER_NAME: The username for connecting to the MySQL database. Loaded from the environment variable "USER_NAME".
- PASSWORD: The password for the MySQL user. Loaded from the environment variable "PASSWORD".
- DATABASE_NAME: The name of the MySQL database to connect to. Loaded from the environment variable "DATABASE_NAME".
Ensure these environment variables are correctly configured in a `.env` file.

"""

def fetch_schema_details():
    """
    Returns a list of strings representing the database schema details.

    This function provides a static definition of the database schema, describing the tables and their columns.
    In a real-world scenario, this information would ideally be dynamically fetched from the database metadata itself
    using SQL queries like `SHOW CREATE TABLE`.  However, for demonstration purposes, it's hardcoded here.

    Returns:
        list[str]: A list of strings, where each string describes a table and its columns.
    """
    
    return [
        "student table: roll_number INT PRIMARY KEY, sname VARCHAR(30), dept VARCHAR(5), sem INT",
        "exam table: regno INT PRIMARY KEY, rollno_number INT, dept VARCHAR(5), grade varchar(3), FOREIGN KEY (student_id) REFERENCES student(student_id)",
        "placement table: placement_id INT PRIMARY KEY, student_id INT, company_name VARCHAR(100), placement_date DATE, FOREIGN KEY (student_id) REFERENCES student(student_id)"
    ]

def db_connection(user_nl_squery: str) -> list[dict]:

    """
    Establishes a connection to the MySQL database and executes the given SQL statement.

    This function connects to the MySQL database using credentials loaded from environment variables.
    It executes the provided SQL statement and returns the results as a list of dictionaries.

    Args:
        user_nl_squery (str): The SQL statement to execute.  It is crucial to ensure this statement is properly sanitized
                             to prevent SQL injection vulnerabilities.  (This function does *not* perform sanitization.)

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents a row in the result set.
                    The keys of the dictionary are the column names.
                    Returns a dictionary with an "error" key if an error occurs during database connection or query execution.
    """
    mydb=None
    try:
        mydb=mysql.connector.connect(host=hostName,user=userName,
                                    password=password,database=dataBaseName)
        mycursor=mydb.cursor()
        mycursor.execute(user_nl_squery)
        results = mycursor.fetchall()

        if not results:
            return []
        columns=[column[0] for column in mycursor.description]
        data=[dict(zip(columns,row)) for row in results]
        return data
    
    except mysql.connector.Error:
        raise HTTPException(status_code=400,detail="Error while executing query")
    #db connection error
    except Exception:
        raise HTTPException(status_code=400,detail="Failed connect db")
    
    finally:
        if mydb and mydb.is_connected():
            mycursor.close()
            mydb.close()
