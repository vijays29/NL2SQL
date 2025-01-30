import mysql.connector
from fastapi import HTTPException
from src.config import get_db_config

def db_output_gen(generated_sql:str,params=None) -> list[dict] | None:

    """
    Establishes a connection to the MySQL database and executes the given SQL statement.
    
    Args:
        generated_sql (str): The SQL statement to execute.  It is crucial to ensure this statement is properly sanitized
                             to prevent SQL injection vulnerabilities.  (This function does *not* perform sanitization.)

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents a row in the result set.
                    The keys of the dictionary are the column names.
                    Returns a dictionary with an "error" key if an error occurs during database connection or query execution.
    """
    connection=None
    
    try:
        con=get_db_config()
        connection=mysql.connector.connect(host=con["host"],user=con["user"],
                                    password=con["password"],database=con["database"])
        mycursor=connection.cursor()
        mycursor.execute(generated_sql,params=None)
        results=mycursor.fetchall()

        if not results:
            return []
        columns = [column[0] for column in mycursor.description]
        data = [dict(zip(columns,row)) for row in results]
        return data
    
    except mysql.connector.Error as e:
        raise HTTPException(status_code = 400,detail = "Error while executing query.please enter valid query")
    except Exception:
        raise HTTPException(status_code = 500,detail = "Unexpected server error.")
    
    finally:
        if connection and connection.is_connected():
            mycursor.close()
            connection.close()