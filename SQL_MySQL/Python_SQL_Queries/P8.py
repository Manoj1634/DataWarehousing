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
#This query filters the Manufacturers table to include only entries for the year 2023 and the 3rd quarter, then performs the counts for each drug category based on these filtered Manufacturers along with the Drugs and FDA tables. 



sql_query = """

SELECT 
    Drug_Category,
    COUNT(*) AS Drug_Count,
    COUNT(DISTINCT Labeler_Code) AS Manufacturer_Count,
    COUNT(DISTINCT FDA_Application_Number) AS FDA_Application_Count
FROM Drug_product_final1
WHERE Year = 2023 AND Quarter = 3
GROUP BY Drug_Category;



"""

try:
    # Execute the query
    mycursor.execute(sql_query)
    end_time = time.time()
    results = mycursor.fetchall()
    num_rows = mycursor.rowcount

    # Display the results
    
    time_taken = end_time - start_time
    print(f"Time taken to execute the query : {time_taken} seconds")
    print("Drug_Category\tDrug_Count\tManufacturer_Count\tFDA_Application_Count")
    for row in results:
        print(f"{row[0]}\t\t{row[1]}\t\t{row[2]}\t\t\t{row[3]}")


except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()
