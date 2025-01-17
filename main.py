from fastapi import FastAPI, HTTPException
from nl2sqlnew import nl_to_sql, db_connection
from fastapi.responses import JSONResponse
import json

app = FastAPI()


@app.get("/{nl_query}")
async def handle_req(nl_query: str):
    natural_language_query = nl_query
    sql_statement = nl_to_sql(natural_language_query)
    output = {}

    if sql_statement:
        output["generated_sql"] = sql_statement
        json_output = db_connection(sql_statement)
        if json_output:
            try:
               output["query_results"] = json.loads(json_output)
            except json.JSONDecodeError:
                raise HTTPException(
                   status_code=500, detail="Failed to decode database result to JSON"
                    )

        else:
            output["query_results"] = "Failed to retrieve data from the database."

    else:
       raise HTTPException(status_code=400, detail="Failed to generate SQL.")
       
    return JSONResponse(content=output)