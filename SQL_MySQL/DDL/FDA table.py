#Query to create a table
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
# SQL command to create the table
start_create_table_FDA = time.time()



create_table_FDA_query = """
CREATE TABLE FDA (
    ID INT,
    FDA_Application_Number VARCHAR(255),
    FDA_Therapeutic_Equivalence_Code VARCHAR(255),
    FDA_Product_Name VARCHAR(255),
    FOREIGN KEY (ID) REFERENCES Drug_product_final1(ID)

);



"""
# Execute the query to create the table
mycursor.execute(create_table_FDA_query)
end_create_table_FDA = time.time()
time_taken_create_table_FDA = end_create_table_FDA - start_create_table_FDA
print(f"Time taken to create table FDA: {time_taken_create_table_FDA} seconds")



# Table name
table_name = 'Drug_product_final1'
start_load_data_FDA = time.time()
# Prepare the SQL query to load data from CSV into the table
load_data_query_FDA = f"""
    INSERT INTO FDA (ID, FDA_Application_Number, FDA_Therapeutic_Equivalence_Code, FDA_Product_Name)
SELECT ID, FDA_Application_Number, FDA_Therapeutic_Equivalence_Code, FDA_Product_Name
FROM Drug_product_final1;



"""

# Execute the query to load data from CSV into the table
mycursor.execute(load_data_query_FDA)
end_load_data_FDA = time.time()
time_taken_load_data_FDA = end_load_data_FDA - start_load_data_FDA
print(f"Time taken to load data to FDA: {time_taken_load_data_FDA} seconds")

mydb.commit()
# Close the connection
mydb.close()