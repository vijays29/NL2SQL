"""
A class for managing connections and executing queries on an Oracle database.

This class provides a connection pool for efficient database access and
handles query execution, error handling, and connection management.
"""

import os
import logging
from dotenv import load_dotenv
import oracledb
from fastapi import HTTPException
load_dotenv()

if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
        self.pool = None  # Initialize pool to None
        try:
            #oracledb.init_oracle_client(lib_dir=os.getenv("ORACLE_INSTANT_CLIENT_DIR"))
            db_user = os.getenv("DB_USER")
            db_pass = os.getenv("DB_PASS")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT", "1521")
            db_service_name = os.getenv("DB_SERVICE_NAME")

            if not all([db_user, db_pass, db_host, db_service_name]):
                logger.error("Missing required environment variables.")
                raise ValueError("Missing required environment variables.")
            
            # connection pooling parameters
            min_connections = int(os.getenv("DB_MIN_CONNECTIONS", 2))  # Default to 2
            max_connections = int(os.getenv("DB_MAX_CONNECTIONS", 5))  # Default to 5
            connection_increment = int(os.getenv("DB_CONNECTION_INCREMENT", 1)) # Default to 1

            self.pool = oracledb.SessionPool(
                user=db_user,
                password=db_pass,
                dsn=f"{db_host}:{db_port}/{db_service_name}",
                min=min_connections,
                max=max_connections,
                increment=connection_increment,
                threaded=True,
                encoding="UTF-8"
            )

            logger.info("Database connection pool created successfully.")

        except oracledb.DatabaseError as e:
            logger.error(f"Database connection error: {e}")
            raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

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
                
                logger.debug(f"Query executed successfully and returned {len(results)} rows.") #Use debug level for detail logging
                return results
        except oracledb.DatabaseError as e:
            logger.error(f"Database error while executing the query: {e}")
            raise HTTPException(status_code=400, detail=f"while executing the query: {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred while executing the query: {sql_query}")
            raise HTTPException(status_code=500, detail="Internal server error.")
        finally:
            # Ensure connection is released back to the pool
            if connection:
                try:
                    connection.close()
                    logger.info("Database connection released back to pool.")
                except Exception as e:
                    logger.error(f"Error releasing database connection: {e}")

    def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            try:
                self.pool.close()
                logger.info("Database connection pool closed.")
            except Exception as e:
                logger.error(f"Error closing database connection pool: {e}")

Db_Instance = OracleDB()

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
    
    try:
        return Db_Instance.Execute_Query(query)
    except Exception as e:
        logger.exception(f"Error executing query: {query}")
