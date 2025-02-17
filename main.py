"""
Main entry point for the NL2SQL API using FastAPI.

This module initializes the FastAPI application and defines the `/data-requests`
endpoint for processing natural language queries.  It orchestrates the conversion
of NL queries to SQL, executes the SQL against the database, and returns the results
to the client.

Modules Used:
    - FastAPI:  For building the API.
    - Pydantic: For data validation and request/response models.
    - src.nl2sql_converter:  Handles the conversion of natural language to SQL.
    - src.query_exe:  Executes SQL queries against the database.

API Endpoints:
    - POST /data-requests:  Accepts a natural language query, converts it to SQL,
                             executes the query, and returns the results as a JSON response.
"""
import logging
from fastapi import FastAPI,HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from src.nl2sql_converter import Convert_Natural_Language_To_Sql
from src.oracle_db_query_exe import Db_Output_Gen

# Configure logging (if not already configured)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app=FastAPI()

class NlQueryRequest(BaseModel):

    """
    Request model for natural language queries.

    Attributes:
        user_query (str): The natural language query string.
    """
    user_query:str

@app.post("/data-requests")
async def Data_Requester(request:NlQueryRequest):

    """
    Processes a natural language query, converts it to SQL, and returns the results.

    This endpoint receives a natural language query, validates it, converts it into
    an SQL query using the `Convert_Natural_Language_To_Sql` function, executes
    the SQL query against the database using the `Db_Output_Gen` function, and
    returns the results as a JSON response.

    Args:
        request (NlQueryRequest):  The incoming request containing the natural language query.

    Returns:
        JSONResponse:  A JSON response containing the query results or an error message.
                       - `200 OK`:  Query executed successfully, and results are returned in the `Table_result` field.
                       - `400 Bad Request`: Invalid query (e.g., empty query, invalid table/column names, forbidden SQL commands). Detail contains specific error information.
                       - `404 Not Found`: Query executed successfully, but no data was found.
                       - `500 Internal Server Error`: An unexpected error occurred during processing.

    Raises:
        HTTPException:
            - 400 Bad Request: If the query is empty or if SQL generation fails due to invalid input.
            - 500 Internal Server Error: If an unexpected error occurs during query execution.
    """

    user_query=request.user_query.strip().lower()
    if not user_query.strip():
        raise HTTPException(status_code=400,detail="Query cannot be empty.Please enter a valid query.")
    
    generated_sql=Convert_Natural_Language_To_Sql(user_query)

    print(generated_sql)

    if not generated_sql:
        logger.warning(f"Failed to generate SQL for query: {user_query}")
        raise HTTPException(status_code=400,detail="Failed to generate SQL Query : Because of you Entered table modification query or tables/fields not present.Please check your request and try again.")
    
    logger.info(f"Generated SQL query: {generated_sql}")

    try:
        query_result=Db_Output_Gen(generated_sql)
        if query_result:
            return JSONResponse(content={"Table_result":query_result},status_code=200)
        else:
            logger.info(f"No data found for query: {user_query}")
            return JSONResponse(content={"Message":"No data found"},status_code=404)

    except HTTPException as e:
        logger.error(f"HTTP Exception: {e.detail}")
        raise HTTPException(
            status_code=e.status_code,
            detail=f"HTTP error occurred: {e.detail}. Please check your request and try again."
        )
    
    except Exception as e:
        logger.exception("An unexpected error occurred")
        raise HTTPException(status_code=500,detail="Internal server error: An unexpected error occurred while processing your request.")