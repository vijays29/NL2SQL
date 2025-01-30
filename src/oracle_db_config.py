import cx_Oracle
import os

class OracleDB:
    def __init__(self):
        self.pool = cx_Oracle.SessionPool(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dsn=cx_Oracle.makedsn(os.getenv("DB_HOST"), os.getenv("DB_PORT", "1521"), service_name=os.getenv("DB_SERVICE_NAME")),
            min=2,
            max=10,
            increment=1,
            threaded=True
        )

    def execute_query(self, sql_query: str):
        try:
            connection = self.pool.acquire()
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                columns = [col[0] for col in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                return results
        finally:
            self.pool.release(connection)

db_instance = OracleDB()

def db_output_gen(query: str):
    return db_instance.execute_query(query)
