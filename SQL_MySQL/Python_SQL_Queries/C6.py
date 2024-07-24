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
#Generate a result set that combines the counts for specific drug categories, manufacturers for a drug type indicator, and product counts by year and quarter. 




sql_query = """

SELECT 
    d.Drug_Category AS Drug_Category,
    COUNT(*) AS Category_Count,
    d.Drug_Type_Indicator AS Drug_Type_Indicator,
    COUNT(DISTINCT m.Labeler_Name) AS Manufacturer_Count,
    m.Year AS Year,
    m.Quarter AS Quarter,
    COUNT(*) AS Product_Count
FROM Drugs d
JOIN Manufacturers m ON d.ID = m.ID
GROUP BY d.Drug_Category, d.Drug_Type_Indicator, m.Year, m.Quarter
ORDER BY d.Drug_Category, d.Drug_Type_Indicator, m.Year, m.Quarter;







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
    print("Drug_Category\tCategory_Count\tDrug_Type_Indicator\tManufacturer_Count\tYear\tQuarter\tProduct_Count")
    for row in results:
        print(f"{row[0]}\t\t{row[1]}\t\t{row[2]}\t\t\t{row[3]}\t\t\t{row[4]}\t{row[5]}\t{row[6]}")


except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close cursor and connection
    mycursor.close()
    mydb.close()
