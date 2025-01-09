import google.generativeai as genai
import mysql.connector
from prettytable import PrettyTable

# Replace with your actual API key
GOOGLE_API_KEY = "AIzaSyBy-i1Tet_oK1Rl7c_VWtV97jyu21V7e0w"  # Replace with your actual API key

genai.configure(api_key=GOOGLE_API_KEY)


def nl_to_sql(nl_query):
    """
    Converts a natural language query to a SQL SELECT statement using the Gemini API.

    Args:
        nl_query: The natural language query to convert.

    Returns:
        The generated SQL SELECT statement (string), or None if there was an error.
    """
    prompt_json = {
        "prompt": """
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
        """,
        "parameters": {
            "nl_query": "{nl_query}"
        },
        "response_format": "sql",
        "constraints": {
            "sql_type": "select_only",
            "allow_ddl_dml_tcl_dcl_dql": False
        }
    }
    model = genai.GenerativeModel('gemini-pro')

    updated_prompt = prompt_json["prompt"].format(nl_query=nl_query)

    try:
        response = model.generate_content(
            updated_prompt,
            generation_config=genai.GenerationConfig(
                temperature=0,
            ),
        )
        sql_query = response.text.strip()
        if "ERROR" in sql_query:
            return None
        return sql_query

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def db_connection(sql_statement):
    """
    Connects to the database and executes the given SQL statement, printing the results in a table format resembling a database table.

    Args:
      sql_statement: The SQL statement to execute.
    """
    mydb = None  # Initialize mydb to None
    try:
        mydb = mysql.connector.connect(host="localhost", user="root", password="1@qwaszxV",
                                        database="Network")  # replace the database name
        mycursor = mydb.cursor()
        mycursor.execute(sql_statement)

        # Fetch all rows and column names
        results = mycursor.fetchall()
        columns = [column[0] for column in mycursor.description]

        # Create a PrettyTable instance
        table = PrettyTable()
        table.field_names = columns

        # Add rows to the table
        for row in results:
            table.add_row(row)

        # Print the table
        print(table)

    except mysql.connector.Error as err:
        print(f"Error while executing query: {err}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    finally:
        if mydb and mydb.is_connected():  # Check if mydb is not None and is_connected
            mycursor.close()
            mydb.close()


if __name__ == "__main__":
    natural_language_query = input("Enter your the nl query: ")
    sql_statement = nl_to_sql(natural_language_query)

    if sql_statement:
        print("Generated SQL:")
        db_connection(sql_statement)
    else:
        print("Failed to generate SQL.")