#. Creating a Database in mysql

import mysql.connector
import time

# Establish a connection to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password=''
)

cursor = conn.cursor()
start_create_DB = time.time()

database_name = 'DW_Project1'
create_db_query = f"CREATE DATABASE {database_name}"
cursor.execute(create_db_query)
end_create_DB = time.time()
time_taken_create_table = end_create_DB - start_create_DB
print(f"Time taken to create DB: {time_taken_create_table} seconds")
cursor.close()
conn.close()
