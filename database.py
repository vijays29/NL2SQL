import os
from dotenv import load_dotenv
import mysql.connector
from pydantic import BaseModel

load_dotenv()
hostName=os.getenv("HOST_NAME")
userName=os.getenv("USER_NAME")
password=os.getenv("PASSWORD")
dataBaseName=os.getenv("DATABASE_NAME")

dbConn = None


class Table(BaseModel):
    name: str
    fields: list[tuple]



def fetch_schema_details():
    """Returns Schema of all tables present in the database"""
    global dbConn
    if dbConn is None:
        raise Exception("DB Not connected")
    
    table_name_cursor = dbConn.cursor()
    table_name_cursor.execute("SHOW TABLES")

    tables = []
    for table_name in table_name_cursor:
        table_description_cursor = dbConn.cursor()

        table_description_cursor.execute(f"DESC {table_name[0]}")

        fields = [
            f for f in table_description_cursor
        ]

        tables.append(
            Table(
                name=table_name[0],
                fields=fields
            )
        )

    print(tables)

        


    return [
        "student table: roll_number INT PRIMARY KEY, sname VARCHAR(30), dept VARCHAR(5), sem INT",
        "exam table: regno INT PRIMARY KEY, rollno_number INT, dept VARCHAR(5), score INT, FOREIGN KEY (student_id) REFERENCES student(student_id)",
        "placement table: placement_id INT PRIMARY KEY, student_id INT, company_name VARCHAR(100), placement_date DATE, FOREIGN KEY (student_id) REFERENCES student(student_id)"
    ]

def connect_db() -> None :
    """Connect to DB"""
    global dbConn
    if dbConn is None:
        dbConn =mysql.connector.connect(host=hostName,user=userName,
                                    password=password,database=dataBaseName)
        



def db_connection(sql_statemnt: str) -> list[dict]:
    """Connects to Database, Runs the query and return data
    
    args:
        sql_statement (str): the SQL Query
    """
    mydb=None
    try:
        mydb=mysql.connector.connect(host=hostName,user=userName,
                                    password=password,database=dataBaseName)
        mycursor=mydb.cursor()
        mycursor.execute(sql_statemnt)
        results = mycursor.fetchall()

        columns=[column[0] for column in mycursor.description]

        data=[]
        for row in results:
            data.append(dict(zip(columns,row)))
        return data
    
    except mysql.connector.Error as err:
        return {"error":f"Error while executing query:{err}"}
    
    except Exception as e:
        return {"error":f"An unexpected error occurred:{e}"}
    
    finally:
        if mydb and mydb.is_connected():
            mycursor.close()
            mydb.close()
