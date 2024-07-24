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

# Create a cursor
mycursor = mydb.cursor()
start_time = time.time()
# SQL query
#Find FDA Application Numbers Associated with Multiple Labelers:



sql_query = """
SELECT dp.FDA_Application_Number, COUNT(DISTINCT dp.Labeler_Code) AS Associated_Labelers
FROM Drug_product_final1 dp
GROUP BY dp.FDA_Application_Number
HAVING COUNT(DISTINCT dp.Labeler_Code) > 1
Order by Associated_Labelers DESC;
"""

try:
    # Execute the query
    mycursor.execute(sql_query)
    end_time = time.time()
    # Fetch all the results
    results = mycursor.fetchall()
    num_rows = mycursor.rowcount

    # Display the results
    
    time_taken = end_time - start_time
    print(f"Time taken to execute the query : {time_taken} seconds")
    print(f"Number of Rows: {num_rows}\n")
    print("FDA_Application_Number\tAssociated_Labelers")
    for row in results:
        print(f"{row[0]}\t\t{row[1]}")

except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()
