"""
A class for managing connections and executing queries on an Oracle database.

This class provides a connection pool for efficient database access and
handles query execution, error handling, and connection management.
"""

from src.utils.config import settings
import oracledb
from fastapi import HTTPException
from src.utils.logger import get_logger

logger=get_logger(__name__)

class OracleDB:
    def __init__(self):
        """
        Initializes the OracleDB connection pool.

        Sets up a connection pool using environment variables for database credentials
        and connection details.  Handles potential database connection errors during
        initialization.

        Raises:
            HTTPException:
                - 500 Internal Server Error: If there is an error connecting to the database
                  or if required environment variables are missing.
        """
        self.pool = None
        try:
            self.pool = oracledb.SessionPool(
                user = settings.DB_USER,
                password = settings.DB_PASS,
                dsn = f"{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_SERVICE_NAME}",
                min = settings.DB_MIN_CONNECTIONS,
                max = settings.DB_MAX_CONNECTIONS,
                increment = settings.DB_CONNECTION_INCREMENT,
                threaded = True,
                encoding = "UTF-8"
            )

            logger.info("Database connection pool initialized successfully.")

        except oracledb.DatabaseError as e:
            logger.error(f"Database connection error: {e}")
            raise HTTPException(status_code=500, detail=f"Unable to establish database connection.")

    def Execute_Query(self, sql_query: str):

        """
        Executes an SQL query against the Oracle database.

        This function acquires a connection from the connection pool, executes the
        provided SQL query, and returns the results as a list of dictionaries.
        Each dictionary represents a row in the result set, with keys corresponding
        to column names.  The function handles database errors during query execution
        and ensures that the connection is released back to the pool.

        Args:
            sql_query (str): The SQL query to execute.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries representing the query results.
                                  Returns an empty list if the query returns no data.

        Raises:
            HTTPException:
                - 400 Bad Request: If there is an error executing the query.
                - 500 Internal Server Error: If an unexpected error occurs during query execution.
        """

        connection = None
        try:
            connection = self.pool.acquire()
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]

                if not results:
                    logger.info(f"Query returned no results: {sql_query}")
                    return []
                
                logger.debug(f"Query executed successfully and returned {len(results)} rows.")
                return results
            
        except oracledb.DatabaseError as e:
            logger.error(f"Database error during query execution: {e}")
            raise HTTPException(status_code=400, detail=f"Error executing the query")
        
        except Exception as e:
            logger.exception(f"Unexpected error while executing query: {sql_query}")
            raise HTTPException(status_code=500, detail="Internal server errorduring query execution.")
        
        finally:
            if connection:
                connection.close()
                logger.info("Database connection released back to pool.")

    def close_pool(self):
        """
        Closes the OracleDB connection pool.
        
        Ensures that all database connections are properly closed and releases resources.
        """
        if self.pool:
            self.pool.close()
            logger.info("Database connection pool closed.")

db_instance = OracleDB()

def Db_Output_Gen(query: str,params=None) ->list[dict]:

    """
    Executes a SQL query and returns the results.

    This function serves as a wrapper around the `OracleDB.Execute_Query` method,
    allowing for easier execution of SQL queries.

    Args:
        query (str): The SQL query to execute.
        params (dict, optional): Additional parameters (currently unused). Defaults to None.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the query results.
    """
    
    return db_instance.Execute_Query(query)