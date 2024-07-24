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
#calculates the counts of drugs, manufacturers, and distinct FDA application numbers associated with each drug category based on the shared 'ID'.



sql_query = """

SELECT 
    d.Drug_Category AS Drug_Category,
    COUNT(*) AS Drug_Count,
    COUNT(DISTINCT m.ID) AS Manufacturer_Count,
    COUNT(DISTINCT f.ID) AS FDA_Application_Count
FROM Drugs d
LEFT JOIN Manufacturers m ON d.ID = m.ID
LEFT JOIN FDA f ON d.ID = f.ID
GROUP BY d.Drug_Category;


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
    print("Drug_Category\tDrug_Count\tManufacturer_Count\tFDA_Application_Count")
    for row in results:
        print(f"{row[0]}\t\t{row[1]}\t\t{row[2]}\t\t\t{row[3]}")


except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()
