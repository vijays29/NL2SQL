import mysql.connector
from src.config import get_db_config
from fastapi import HTTPException
from pydantic import BaseModel

class QueryRequest(BaseModel):

    """
    Model to validate incoming request body.
    """
    get_table:str


def fetch_table_details(get_table:str) -> list[dict] | None:

    """
    Fetches table details (table names and their columns) from the database.
    
    Args:
        get_table (str): SQL get_table to fetch table names.
    
    Returns:
        dict: Dictionary with table names as keys and list of column names as values.

    Raises:
        HTTPException: If a database error occurs.
    """
     
    connection = None
    con=get_db_config()
    try:
        connection = mysql.connector.connect(
            host=con["host"], user=con["user"], password=con["password"], database=con["database"]
        )
        mycursor = connection.cursor()
        mycursor.execute(get_table)
        tables = mycursor.fetchall()

        result = {}
        for (table_name,) in tables:
            mycursor.execute(f"DESCRIBE {table_name}")
            columns = mycursor.fetchall()
            result[table_name] = [column[0] for column in columns]
        return {"tables_and_fields": result}
    
    except Exception:
        raise HTTPException(status_code = 500,detail = "Unexpected server error.")
    


    finally:
        if connection and connection.is_connected():
            mycursor.close()
            connection.close()
