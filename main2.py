from fastapi import FastAPI, HTTPException
from show_table import table
from pydantic import BaseModel
import json

app = FastAPI()


class ReqDetails(BaseModel):
    query: str


@app.post("/details")
async def fetch(req: ReqDetails):
    user_query = req.query.lower().strip()
    showdata_str = table(user_query)
    try:
        # Load the JSON string into a Python dictionary before returning it
        showdata = json.loads(showdata_str)
        return showdata
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")