
import mysql.connector
import time
# MySQL connection details
hostname = "localhost"
username = "root"
password = ""
database_name = "DW_Project1"


mydb = mysql.connector.connect(
    host=hostname,
    user=username,
    password=password,
    database=database_name
)
mycursor = mydb.cursor()

# SQL UPDATE query
sql_update_query1 = """
Drop table Drugs;
"""
sql_update_query2 = """
Drop table FDA;
"""
sql_update_query3 = """
Drop table Manufacturers;
"""
sql_update_query4 = """
Drop table Drug_Product_final1;
"""

try:
    # Measure the start time
    start_time1 = time.time()

    # Execute the UPDATE query
    mycursor.execute(sql_update_query1)

    # Commit changes to the database
    mydb.commit()
    end_time1 = time.time()
    start_time2 = time.time()

    # Execute the UPDATE query
    mycursor.execute(sql_update_query2)

    # Commit changes to the database
    mydb.commit()
    end_time2 = time.time()
    


    start_time3 = time.time()

    # Execute the UPDATE query
    mycursor.execute(sql_update_query3)

    # Commit changes to the database
    mydb.commit()
    end_time3 = time.time()

    start_time4 = time.time()

    # Execute the UPDATE query
    mycursor.execute(sql_update_query4)

    # Commit changes to the database
    mydb.commit()
    end_time4 = time.time()


    
    execution_time1 = end_time1 - start_time1
    print(f"Execution Time1: {execution_time1} seconds")
    execution_time2 = end_time2 - start_time2
    print(f"Execution Time2: {execution_time2} seconds")
    execution_time3 = end_time3 - start_time3
    print(f"Execution Time3: {execution_time3} seconds")
    execution_time4 = end_time4 - start_time4
    print(f"Execution Time4: {execution_time4} seconds")



except mysql.connector.Error as error:
    
    print(f"Error: {error}")
    mydb.rollback()

finally:
    mycursor.close()
    mydb.close()