import json
from fastapi import FastAPI,HTTPException
from nl2sql import nl_to_sql
from database import db_connection
from pydantic import BaseModel
from fastapi.responses import JSONResponse
app=FastAPI()

class QueryRequest(BaseModel):
    nl_query:str

@app.post("/nlquery/")
async def handle_req(request:QueryRequest):

    output={}
    nlquery=request.nl_query.lower()
    if not nlquery.strip():
        raise HTTPException(status_code=400,detail="Please Enter valid nlquery")
    else:
        forbidden_keywords = ["drop", "delete","modify","alter"]
        if any(keyword in nlquery for keyword in forbidden_keywords):
            raise HTTPException(status_code=400,detail="Failed to generate SQL.")
        else:
            sql_statement=nl_to_sql(nlquery)
            if sql_statement:

                sql_query=db_connection(sql_statement)

                if sql_query:

                    try:
                        jsonconvert=json.dumps(sql_query,indent=2)
                        output["query_results"]=json.loads(jsonconvert)
                    except json.JSONDecoder:
                        raise HTTPException(
                        status_code=500,details="Failed to decode database result to JSON"
                        )
                else:
                    output["query_results"]="Failed to retrieve data from the database."
            else:
                raise HTTPException(status_code=400,detail="Failed to generate SQL.")
    
            return JSONResponse(content=output)