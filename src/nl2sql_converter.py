"""
Module for converting Natural Language queries into SQL SELECT statements using Google Gemini AI.

This module leverages the Google Gemini API to translate natural language questions
into executable SQL SELECT queries.  It uses Langchain for prompt management and
interaction with the LLM. The module also includes error handling for API key issues
and SQL generation failures.

Modules Used:
    - os:  For accessing environment variables (API keys).
    - dotenv:  For loading environment variables from a .env file.
    - google.generativeai:  The Google Gemini API client.
    - langchain:  For prompt engineering and LLM chain management.
    - fastapi:  For raising HTTP exceptions in case of errors.
    - src.schema_details:  For retrieving database schema metadata.

Functions:
    - Convert_Natural_Language_To_Sql: Converts a natural language query into an SQL SELECT statement.
"""

import os
import logging
from dotenv import load_dotenv
import google.generativeai as Aimodel
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough,RunnableLambda
from fastapi import HTTPException
from src.schema_details import Metadata

load_dotenv()

# Configure logging (if not already configured)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_KEY=os.getenv('API_KEY')

if not API_KEY:
    logger.error("API_KEY is missing. Set it in the environment variables.")
    raise HTTPException(status_code=500, detail="API_KEY is missing. Set it in the environment variables.")

Aimodel.configure(api_key=API_KEY)
def Convert_Natural_Language_To_Sql(user_query:str,params = None) -> str | None:   
        
        """
        Converts a natural language query into a SQL SELECT statement using Google Gemini.
        
        This function takes a natural language query as input and uses the Google Gemini API,
        guided by a carefully crafted prompt, to generate a corresponding SQL SELECT statement.
        It validates that the generated SQL adheres to specific rules (e.g., SELECT-only,
        schema adherence) and returns the SQL query string if successful.  If any error
        occurs during SQL generation or if the generated SQL violates the rules, it returns None.

        Args:
            user_query (str): The natural language query to be converted.
            params (dict, optional): Additional parameters (currently unused). Defaults to None.

        Returns:
            str | None: The generated SQL SELECT statement, or None if generation failed.

        Raises:
            HTTPException:
                - 500 Internal Server Error: If the Google Gemini API fails to generate a valid SQL query.
        """

        schema_details=Metadata()

        prompt_template = f"""

        Convert the following natural language query into a SQL SELECT statement suitable for a database with the following table structure:

        {'\n'.join(schema_details)}

        You are a highly skilled SQL query generation tool designed for enterprise database environments. 
        Your sole function is to translate natural language requests into valid and efficient SQL SELECT statements. 
        Adhere strictly to the following rules, and respond ONLY with the generated SQL query.
        Any deviation from these rules will result in an error.

        **Mandatory Rules:**

        1.  **SELECT-Only Operations:**
            *   Your output MUST be a valid SQL SELECT statement, and ONLY a SELECT statement.
            *   Any request implying data modification DML(e.g:INSERT, UPDATE, DELETE), schema alteration DDL(e.g:DROP, ALTER), transaction control TCL(e.g:COMMIT, ROLLBACK), or data control DCL(e.g:GRANT, REVOKE) should result in the immediate response: "ERROR".

        2.  **Schema Adherence & Validation:**
            *   You MUST ONLY use table and column names that exist within the provided database schema.  Assume the schema is available and correct.
            *   If a user specifies a table or column that does not exist within this schema, immediately respond with: "ERROR". No exceptions.

        3.  **Explicit Column Specification:**
            *   If the user explicitly lists the columns to be retrieved, ONLY include those columns in the SELECT statement.
            *   If the user requests "all columns," "all data," or provides no specific column list, use `SELECT *`.

        4.  **Precise Filtering & Conditions:**
            *   Translate all WHERE clause conditions precisely as stated.
            *   Ensure accurate handling of date ranges (using appropriate date/time functions if needed), numerical comparisons, and string matching (using LIKE or other relevant functions as needed).

        5.  **Aggregation and Ordering Implementation:**
            *   Correctly implement any requested aggregation functions (SUM, COUNT, AVG, MIN, MAX, etc.).
            *   Implement ORDER BY clauses exactly as requested, including the specified column(s) and sort order (ASC or DESC).  Default to ASC if not specified.

        6.  **LIMIT Clause Enforcement:**
            *   If the user provides a LIMIT clause, it MUST be included in the generated SQL query.

        7.  **Zero Tolerance for Additional Text:**
            *   Your output consists SOLELY of the generated SQL SELECT statement. Do NOT include any explanations, comments, or introductory text.  Failure to adhere to this is an error.

        8. **Assume Correct Grammar:**
            *  Assume that the user input is grammatically correct, though it might contain synonyms or multiple ways to ask the same question.

        **Process:**

        1.  Receive a natural language request.
        2.  Parse the request to identify:
            *   The target table(s).
            *   The desired columns.
            *   Any filtering conditions (WHERE clause).
            *   Any aggregation requirements.
            *   Any sorting requirements (ORDER BY clause).
            *   Any LIMIT requirements.
        3.  Construct a valid SQL SELECT statement that fulfills all requirements.
        4.  Output ONLY the SQL SELECT statement.  If any rule is violated, output "ERROR".

        Now, convert the following Natural Language Query: {user_query}
        """

        llm = ChatGoogleGenerativeAI(model="gemini-pro", api_key=API_KEY, temperature=0)
        prompt = PromptTemplate(template=prompt_template,input_variables=["user_query"])
        try:
              chain = (
                    {"user_query":RunnablePassthrough()}
                    |prompt
                    |llm
                    |RunnableLambda(lambda x:x.content)
              )

              sql_query = chain.invoke(user_query)
              return sql_query if "ERROR" not in sql_query else None
        
        except Exception as e:
              logger.exception(f"Failed to generate SQL query for user query: {user_query}")
              raise HTTPException(status_code=500,detail=f"Failed to generate Sql query.")
        