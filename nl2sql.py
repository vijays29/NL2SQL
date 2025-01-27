import os
from dotenv import load_dotenv
import google.generativeai as aimodel
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough,RunnableLambda
from database import fetch_schema_details
from fastapi import HTTPException

"""

Description:
This module provides functionality to convert natural language queries into SQL SELECT statements using the Google Gemini Pro large language model (LLM).
It interacts with a database schema fetched from the `database` module and constructs SQL queries based on user input.
The module incorporates error handling and security measures to prevent unauthorized database modifications.

Dependencies:
- os: For environment variable access.
- dotenv: For loading environment variables from a .env file.
- google.generativeai: For interacting with the Google Gemini Pro LLM.
- langchain.prompts: For creating prompt templates for the LLM.
- langchain_google_genai: For integrating the Google Gemini Pro LLM with Langchain.
- langchain_core.runnables: For creating a chain of operations.
- database: A custom module containing the `fetch_schema_details` function to retrieve the database schema.

Configuration:
- API_KEY:  The Google Gemini Pro API key, loaded from the environment variable "API_KEY".  Ensure this environment variable is set correctly.

"""

load_dotenv()
API_KEY=os.getenv("API_KEY")
aimodel.configure(api_key=API_KEY)

def convert_natural_language_to_sql(user_nl_query:str)->str:
        """
        Converts a natural language query into a SQL SELECT statement.

        This function takes a natural language query as input and returns a corresponding SQL SELECT statement,
        suitable for querying a database with a predefined schema (student, exam, placement).
        It leverages the Google Gemini Pro LLM to understand the natural language query and generate the appropriate SQL.

        The function includes safeguards to prevent database modifications (INSERT, UPDATE, DELETE, etc.) and to handle invalid queries
        or queries that reference non-existent columns in the database schema.  It also provides support for aggregation and ordering.

        Args:
            user_nl_query (str): The natural language query to convert.

        Returns:
            str: A SQL SELECT statement corresponding to the natural language query, or None if an error occurs or the query is invalid.
                 Returns "ERROR" if the LLM identifies a potentially harmful request to modify data or if there is an error in constructing the SQL query.
        """

        prompt_template = f"""
        Convert the following natural language query into a SQL SELECT statement suitable for a database with the following table structure:

        {'\n'.join(fetch_schema_details())}

        The database tables to query are student, exam, and placement.

        Ensure the generated query does NOT contain any INSERT, UPDATE, DELETE, DROP, ALTER, or other DDL/DML commands that would modify the database tables or its structure.
        Only provide the SELECT statement in your response. Do not include any additional text or explanation, just the query itself.
        If the query contains any statements other than SELECT statements, or if the user is asking to modify, drop, or insert data, respond with 'ERROR'.

        If the user asks to retrieve specific columns, the query should only retrieve those columns. If the user asks for "all" or does not specify any columns, then use SELECT *.

        If the user uses column names which do not exist in the schema, respond with 'ERROR'.
        If the user asks question with aggregation or ordering, use it.

        Now, convert the following natural language query: {user_nl_query}
        """
        prompt=PromptTemplate(template=prompt_template,input_variables=["user_nl_query"])
        llm=ChatGoogleGenerativeAI(model="gemini-pro",api_key=API_KEY,temperature=0)

        chain=(
                {"user_nl_query":RunnablePassthrough()}
                |prompt
                |llm
                |RunnableLambda(lambda x : x.content)
            )
        try:
            response=chain.invoke(user_nl_query)
            sql_query=response.strip()
            if "ERROR" in sql_query:
                  return None
            return sql_query
        except Exception as e:
              print(f"An error occurred:{e}")
              raise HTTPException(400,"Some inbuild error in generating the query or internal server error")