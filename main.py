import json
from fastapi import FastAPI,HTTPException
from nl2sql import convert_natural_language_to_sql
from database import db_connection
from pydantic import BaseModel
from fastapi.responses import JSONResponse

app=FastAPI()

class DataQueryRequest(BaseModel):

    """
    Represents the request body for the /data-requests/ endpoint.

    Attributes:
        user_query (str): The natural language query to be converted to SQL.
    """

    user_query:str

@app.post("/data-requests")

async def data_requester(request:DataQueryRequest):

    """
    Handles the request to convert a natural language query to SQL and retrieve data.

    Args:
        request (DataQueryRequest): The request object containing the natural language query.

    Returns:
        JSONResponse: A JSON response containing the query results or an error message.

    Raises:
        HTTPException:
            - 400: If the input query is invalid (empty or contains forbidden keywords) or if the SQL generation fails.
            - 500: If there is an error decoding the database result to JSON.
    """

    output={}
    user_nl_query=request.user_query.lower().strip()
    forbidden_keywords = ["drop", "delete","modify","alter"]
    if not user_nl_query.strip():
        raise HTTPException(status_code=400,detail="Please Enter valid user_nl_query")
    
    if any(keyword in user_nl_query for keyword in forbidden_keywords):
        raise HTTPException(status_code=400,detail="Failed to generate SQL.123")
    
    gen_sql_query=convert_natural_language_to_sql(user_nl_query)
    print (gen_sql_query)     
    if gen_sql_query:
        json_data_convert=db_connection(gen_sql_query)
        if json_data_convert:
            try:
                json_data=json.dumps(json_data_convert,indent=2)
                output["Table_result"]=json.loads(json_data)
            except json.JSONDecoder:
                raise HTTPException(status_code=500,details="Failed to decode database result to JSON")
        else:
            raise HTTPException(status_code=400,detail=f"{user_nl_query} : Such data not available in the table.")
    else:
        raise HTTPException(status_code=400,detail="Failed to generate SQL.Please provide valid details.check details like column names,table name")
    
    return JSONResponse(content=output)