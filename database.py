import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()
hostName=os.getenv("HOST_NAME")
userName=os.getenv("USER_NAME")
password=os.getenv("PASSWORD")
dataBaseName=os.getenv("DATABASE_NAME")

def db_connection(sql_statemnt):
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
