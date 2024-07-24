import mysql.connector
import time
# MySQL connection details
hostname = "localhost"
username = "root"
password = ""
database_name = "DW_Project1"

# Establish the connection to MySQL
mydb = mysql.connector.connect(
    host=hostname,
    user=username,
    password=password,
    database=database_name
)
mycursor = mydb.cursor()

# SQL UPDATE query
sql_update_query = """
UPDATE Manufacturers
SET Labeler_Name = 'Eli'
WHERE Labeler_Name = 'ELI LILLY AND COMPANY'
"""

try:
    # Measure the start time
    start_time = time.time()

    # Execute the UPDATE query
    mycursor.execute(sql_update_query)

    # Commit changes to the database
    mydb.commit()

    # Measure the end time and calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time} seconds")

except mysql.connector.Error as error:
    # Rollback in case of error
    print(f"Error: {error}")
    mydb.rollback()

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()