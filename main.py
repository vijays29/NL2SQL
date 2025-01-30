import json
from pydantic import BaseModel
from src.database import db_output_gen
from fastapi import FastAPI,HTTPException
from src.nl2sql import convert_natural_language_to_sql
from fastapi.responses import JSONResponse
from src.words import forbidden_keywords
from src.config import get_db_config
from src.fetch_details_scehma import QueryRequest,fetch_table_details
get_db_config()

''' FastApi Application instance '''
app=FastAPI(title="NL2SQL API",
    description="An API that converts natural language queries to SQL and retrieves data from a database.",
    version="1.0.0",
    contact={
        "name": "company name",
        "email": "company@example.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    })

class DataQueryRequest(BaseModel):

    """
    Represents the request body for the /data-requests/ endpoint.

    Attributes:
        user_query (str): The natural language query to be converted to SQL.
    
    This class ensures that incoming data is structured correctly for processing.
    """
    user_query:str


@app.post("/details",response_model=dict, summary="Fetch Table Details", tags=["Database"])
async def fetch(req:QueryRequest):
    """
    Fetches fetch_table_details names and their column details from the database. 

    Request Body:
    - query (str): The SQL query to retrieve fetch_table_details names. (Only 'SHOW TABLES' queries are allowed.)

    Responses:
    - Returns a dictionary containing fetch_table_details names and their columns.
    - If the query is empty or invalid.
    - If a database error occurs.
    """
    user_req_table=req.get_table.strip().lower()
    if not user_req_table:
        raise HTTPException(status_code=400,detail="Query cannot be empty")
    
    if user_req_table =="show tables":
        result = fetch_table_details(user_req_table)
        return JSONResponse(content=result, status_code=200)
    
    raise HTTPException(status_code=400,detail={"Please follow the format : ' show tables ' your entered wrong format" : user_req_table})
    



@app.post("/data-requests",summary="Convert Natural Language Query to SQL",
    description="This endpoint takes a natural language query and converts it to a corresponding SQL query. The SQL query is then executed against the database, and the results are returned in a JSON format. If the query is invalid or contains forbidden keywords, an error is returned.",
    tags=["Data Requests"],)

async def data_requester(request:DataQueryRequest):

    """
    Handles the request to convert a natural language query to SQL and retrieve data.

    Args:
        request (DataQueryRequest): The request object containing the natural language query.
    
    This function does the following:
    - Validates the user_req_table input.
    - Converts the natural language query to a corresponding SQL query.
    - Executes the SQL query against the database.
    - Returns the results or an error message in JSON format.

    Returns:
        JSONResponse: A JSON response containing the query results or an error message.
    
    Raises:
        HTTPException:
            - 400: If the input query is invalid (empty or contains forbidden keywords) or if SQL generation fails.
            - 500: If there is an error decoding the database result to JSON.
    """
    user_nl_query=request.user_query.strip().lower()
    if not user_nl_query.strip():
        raise HTTPException(status_code=400,detail="Query cannot be empty.Please enter a valid query.")
    
    if any(keyword in user_nl_query for keyword in forbidden_keywords):
        raise HTTPException(status_code=400,detail="Invalid query request:Contains forbidden Keywords.")
    
    generated_sql=convert_natural_language_to_sql(user_nl_query)

    print(generated_sql)

    if not generated_sql:
        raise HTTPException(status_code=400,detail="Failed to generate SQL Query.")

    try:
        query_result=db_output_gen(generated_sql)
        if query_result:
            return JSONResponse(content={"Table_result":query_result},status_code=200)
        else:
            return JSONResponse(content={"Message":"No data found"},status_code=404)
    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=500,detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)