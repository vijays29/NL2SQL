""" This is an example of a FastAPI application that demonstrates
    how to create a simple REST API with FastAPI.

    To run this application, you need to install FastAPI and Uvicorn.
    You can install them using pip:
        
            pip install uvicorn fastapi
        
    To run the application, you can use the following command:
        
            uvicorn main:app --reload
        
    The --reload flag is used to enable auto-reloading of the server

    You can access the API at http://localhost:8000/

    To run the application with different port, you can use the following command:
        
            uvicorn main:app --reload --port 8001
        
    You can access the API at http://localhost:8001/

    The application has the following routes:
        - /: Returns a simple message
        - /hello/{name}: Returns a greeting message with the given name
        - /data: Returns a list of sample data
        - /data/{id}: Returns the data with the given id
        - /data: Adds a new data to the list
        - /data/{id}: Updates the data with the given id
        - /data/{id}: Deletes the data with the given id
        - /query: Returns a simple message

    Handlers are the function that handles the request and returns the response.
    For example, the handler for the route /hello/{name} is the function greet_name.

    Best Practice:
        - Use the appropriate HTTP methods for the operations
        - Use the appropriate status codes for the responses
        - Use the appropriate path parameters, query parameters, and request body
        - Use the appropriate response body
        - Use the appropriate error handling
"""


import uuid
import time
from fastapi import FastAPI
from typing import Dict, Any
from nl2sqlnew import nl_to_sql, db_connection  # Import NL2SQL functions


sample_data = [
    {
        "id": 1,
        "Name": "John Doe",
        "Age": 30,
    },
    {
        "id": 2,
        "Name": "Jane Doe",
        "Age": 25,
    },
    {
        "id": 3,
        "Name": "John Smith",
        "Age": 35,
    }
]

app = FastAPI()


@app.get("/")
def hello_route():
    """
        curl -L http://localhost:8000/
    """
    now = time.time()
    return {
        "message": "Hello, World!",
        "time": now,
    }

@app.get("/hello/{name}")
def greet_name(name: str):
    """
        curl -L http://localhost:8000/hello/John
    """
    return {"message": f"Hello, {name}!"}

@app.get("/data")
def get_data():
    """
        curl -L http://localhost:8000/data
    """
    return sample_data

@app.get("/data/{id}")
def get_data_by_id(id: int):
    """
        curl -L http://localhost:8000/data/1
    """
    for data in sample_data:
        if data["id"] == id:
            return data
    return {"message": "Data not found!"}

@app.post("/data")
def add_data(data: Dict[str, Any]):
    """
        curl -L http://localhost:8000/data \
        -X POST -H "Content-Type: application/json" \
        -d '{"Name": "Micheal Doe", "Age": 30}'
    """

    id = uuid.uuid1()

    sample_data.append({
        "id": id.int,
        "Name": data["Name"],
        "Age": data["Age"],
    })
    return {"message": "Data added successfully!"}

@app.put("/data/{id}")
def update_data(id: int):
    """
        curl -L http://localhost:8000/data/1 \
            -X PUT -H "Content-Type: application/json" \
            -d '{"Name": "Matt Doe", "Age": 30}'
    """

    for data in sample_data:
        if data["id"] == id:
            data["Name"] = "Matt Doe"
            return {"message": "Data updated successfully!"}

    return {"message": "Data updated successfully!"}



@app.delete("/data/{id}")
def delete_data(id: int):
    """
        curl -L http://localhost:8000/data/1 -X DELETE
    """

    for data in sample_data:
        if data["id"] == id:
            sample_data.remove(data)
            return {"message": "Data deleted successfully!"}

    return {"message": "Data not found!"}


@app.post("/query")
def query_database(query: Dict[str, str]):
    """
        curl -L http://localhost:8000/query \
            -X POST -H "Content-Type: application/json" \
            -d '{"natural_language_query": "What is the name and major of the student with student_id 5?"}'

        Endpoint to execute SQL queries based on natural language input.
    """
    natural_language_query = query.get("natural_language_query")
    if not natural_language_query:
      return {"error": "Missing 'natural_language_query' in request body"}
    
    sql_statement = nl_to_sql(natural_language_query)

    if sql_statement:
        print("Generated SQL:", sql_statement)
        try:
            db_connection(sql_statement) # Directly use PrettyTable print
            return {"message": "Query executed successfully!"}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return {"error": f"Database error: {e}"}

    else:
        return {"message": "Failed to generate SQL."}