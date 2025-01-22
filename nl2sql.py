import os
from dotenv import load_dotenv
import google.generativeai as aimodel
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough,RunnableLambda

from database import fetch_schema_details

#store the api key in env for security.
load_dotenv()
API_KEY=os.getenv("API_KEY")
aimodel.configure(api_key=API_KEY)

def nl_to_sql(nl_query):
        #The prompt contains the natural language processing query
        #The prompt template contains the entire query no need to type the entire prompt
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

        Now, convert the following natural language query: {nl_query}
        """
        prompt=PromptTemplate(template=prompt_template,input_variables=["nl_query"])
        llm=ChatGoogleGenerativeAI(model="gemini-pro",api_key=API_KEY,temperature=0)

        #Create the RunnableSequence 
        chain=(
                {"nl_query":RunnablePassthrough()}
                |prompt
                |llm
                |RunnableLambda(lambda x : x.content)
            )
        try:
            response=chain.invoke(nl_query)
            sql_query=response.strip()
            if "ERROR" in sql_query:
                  return None
            return sql_query
        except Exception as e:
              print(f"An error occurred:{e}")
              return None