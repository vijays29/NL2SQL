import google.generativeai as genai
import json
import mysql.connector
from langchain.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.runnables import RunnablePassthrough, RunnableLambda

# Replace with your actual API key
GOOGLE_API_KEY = ""

genai.configure(api_key=GOOGLE_API_KEY)


def nl_to_sql(nl_query):
    prompt_template = """
        Convert the following natural language query into a SQL SELECT statement suitable for a database with the following table structure:

        student table: student_id INT PRIMARY KEY, student_name VARCHAR(100), major VARCHAR(50), gpa DECIMAL(3,2)
        exam table: exam_id INT PRIMARY KEY, student_id INT, exam_name VARCHAR(50), score INT, FOREIGN KEY (student_id) REFERENCES student(student_id)
        placement table: placement_id INT PRIMARY KEY, student_id INT, company_name VARCHAR(100), placement_date DATE, FOREIGN KEY (student_id) REFERENCES student(student_id)

        The database tables to query are student, exam, and placement.

        Ensure the generated query does NOT contain any INSERT, UPDATE, DELETE, DROP, ALTER, or other DDL/DML commands that would modify the database tables or its structure.
        Only provide the SELECT statement in your response. Do not include any additional text or explanation, just the query itself.
        If the query contains any statements other than SELECT statements, or if the user is asking to modify, drop, or insert data, respond with 'ERROR'.

        If the user asks to retrieve specific columns, the query should only retrieve those columns. If the user asks for "all" or does not specify any columns, then use SELECT *.

        If the user uses column names which do not exist in the schema, respond with 'ERROR'.
        If the user asks question with aggregation or ordering, use it.

        Now, convert the following natural language query: {nl_query}
        """
    prompt = PromptTemplate(template=prompt_template, input_variables=["nl_query"])
    llm = ChatGoogleGenerativeAI(model="gemini-pro", google_api_key=GOOGLE_API_KEY, temperature=0)

    # Create the RunnableSequence
    chain = (
        {"nl_query": RunnablePassthrough()}  # Pass the input as nl_query
        | prompt
        | llm
        | RunnableLambda(lambda x: x.content)  # Extract the text from the ChatMessage
    )

    try:
        response = chain.invoke(nl_query)  # Use invoke method and pass in the nl_query
        sql_query = response.strip()
        if "ERROR" in sql_query:
            return None
        return sql_query

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def db_connection(sql_statement):
    mydb = None  # Initialize mydb to None
    try:
        mydb = mysql.connector.connect(host="localhost", user="root", password="",
                                       database="Network")  # Replace database name
        mycursor = mydb.cursor()
        mycursor.execute(sql_statement)

        # Fetch all rows and column names
        results = mycursor.fetchall()
        columns = [column[0] for column in mycursor.description]

        # Convert results to a list of dictionaries
        json_data = []
        for row in results:
            json_data.append(dict(zip(columns, row)))

        # Return JSON string
        return json.dumps(json_data, indent=2)

    except mysql.connector.Error as err:
        return json.dumps({"error": f"Error while executing query: {err}"}, indent=2)
    except Exception as e:
        return json.dumps({"error": f"An unexpected error occurred: {e}"}, indent=2)
    finally:
        if mydb and mydb.is_connected():  # Check if mydb is not None and is_connected
            mycursor.close()
            mydb.close()