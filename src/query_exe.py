"""
Module to execute SQL queries on a MySQL database.

Modules Used:
    - os: Fetch environment variables.
    - mysql.connector: MySQL database connection.
    - fastapi: Exception handling.

Classes:
    - Mysql: Manages a MySQL connection pool and executes queries.

Functions:
    - Db_Output_Gen: Fetches query results.
"""

import os
import mysql.connector
from mysql.connector import pooling,Error
from dotenv import load_dotenv
from fastapi import HTTPException
load_dotenv()

class Mysql:
    """Handles MySQL connection pooling and query execution."""
    
    def __init__(self):
        try:
            pool_size=int(os.getenv('POOL_SIZE',5))
            self.pool=pooling.MySQLConnectionPool(
                pool_name="mysql_pool",
                pool_size=pool_size,
                pool_reset_session=True,
                host=os.getenv('HOST_NAME'),
                user=os.getenv('USER_NAME'),
                password=os.getenv('PASSWORD'),
                database=os.getenv('DATABASE_NAME'),
                connect_timeout=10,
            )
        except Error as e:
            raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

    def Execute_Query(self, generated_sql: str):
        connection=None
        try:
            connection=self.pool.get_connection()
            mycursor=connection.cursor()
            mycursor.execute(generated_sql)
            columns = [col[0] for col in mycursor.description]
            results = [dict(zip(columns, row)) for row in mycursor.fetchall()]
            if not results:
                return []
            return results
        
        except mysql.connector.Error as e:
            raise HTTPException(status_code=400, detail=f"Error while executing query: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        finally:
            if connection and connection.is_connected():
                mycursor.close()
                connection.close()

Db_Instance = Mysql()

def Db_Output_Gen(query: str,params=None)->list[dict] | None:
     """Fetches SQL query results."""
     return Db_Instance.Execute_Query(query)