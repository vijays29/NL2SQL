import os
from dotenv import load_dotenv
load_dotenv()

#load environment variables
DB_HOST=os.getenv('HOST_NAME')
DB_USER=os.getenv('USER_NAME')
DB_PASS=os.getenv('PASSWORD')
DB_NAME=os.getenv('DATABASE_NAME')

def get_db_config(params=None)->dict | None:
    return {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASS,
        "database": DB_NAME,
    }

def metadata(params=None)-> list | None:
    return [
            "student table: roll_number INT PRIMARY KEY, sname VARCHAR(30), dept VARCHAR(5), sem INT",
            "exam table: regno INT PRIMARY KEY, roll_number INT Foreign key, dept VARCHAR(5), mark1 int,mark2,mark3 int,mark4 int,mark5 int,total int,average int ,grade varchar(3)",
            "placement table: placementID INT PRIMARY KEY, roll_number INT, dept char(5), company VARCHAR(100),salary int"
        ]
