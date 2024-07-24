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
#Counting Products by Manufacturer for a Specific Drug Category:'S'



sql_query = """

SELECT dp.Labeler_Name, COUNT(dp.Product_Code) AS Product_Count
FROM Drug_product_final1 dp
WHERE dp.Drug_Category = 'S'
GROUP BY dp.Labeler_Name
Order by Product_Count DESC;


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
    print("Labeler_Name\tProduct_Count")
    for row in results:
        print(f"{row[0]}\t\t{row[1]}")

except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()
