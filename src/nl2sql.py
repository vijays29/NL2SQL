import os
from dotenv import load_dotenv
import google.generativeai as aimodel
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough,RunnableLambda
from fastapi import HTTPException
from src.words import forbidden_keywords
from src.config import metadata

#Load Environment variables
load_dotenv()
API_KEY=os.getenv('API_KEY')
#Load database Configuration
FETCH_SCHEMA_DETAILS=metadata()
aimodel.configure(api_key=API_KEY)

def convert_natural_language_to_sql(user_query:str,params = None) -> str | None:
        
        """
        Converts a natural language query into a SQL SELECT statement.

        Args:
            user_query (str): The natural language query.

        Returns:
            str: A SQL SELECT statement corresponding to the natural language query, or None if an error occurs or the query is invalid.
                 Returns "ERROR" if the LLM identifies a potentially harmful request to modify data or if there is an error in constructing the SQL query.
        """

        if any(keyword in user_query for keyword in forbidden_keywords):
              return None

        prompt_template = f"""

        Convert the following natural language query into a SQL SELECT statement suitable for a database with the following table structure:

        {'\n'.join(FETCH_SCHEMA_DETAILS)}

        The database tables to query are student, exam, and placement.

        Ensure the generated query does NOT contain any INSERT, UPDATE, DELETE, DROP, ALTER, or other DDL/DML commands that would modify the database tables or its structure.
        Only provide the SELECT statement in your response. Do not include any additional text or explanation, just the query itself.
        If the query contains any statements other than SELECT statements, or if the user is asking to modify, drop, or insert data, respond with 'ERROR'.

        If the user asks to retrieve specific columns, the query should only retrieve those columns. 
        If the user asks for "all" or does not specify any columns, then use SELECT *.

        If the user uses column names which do not exist in the schema, Return "ERROR".
        If the user asks question with aggregation or ordering, use it.

        Now, convert the following Natural Language Query: {user_query}
        """
        llm=ChatGoogleGenerativeAI(model="gemini-pro",api_key=API_KEY,temperature=0)
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
              raise HTTPException(status_code=500,detail="Failed to generate Sql query.")