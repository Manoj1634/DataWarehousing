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
start_create_table = time.time()
create_table_query = """
CREATE TABLE Drug_product_final1 (
    ID INT PRIMARY KEY,
    Year INT,
    Quarter INT,
    Labeler_Name VARCHAR(255),
    NDC VARCHAR(255),
    Labeler_Code INT,
    Product_Code INT,
    Package_Size_Code INT,
    Drug_Category VARCHAR(255),
    Drug_Type_Indicator INT,
    Unit_Type VARCHAR(255),
    Units_Per_Pkg_Size DECIMAL(18, 3),
    FDA_Therapeutic_Equivalence_Code VARCHAR(255),
    FDA_Product_Name VARCHAR(255),
    Clotting_Factor_Indicator VARCHAR(1),
    Pediatric_Indicator VARCHAR(1),
    COD_Status DECIMAL(5, 1),
    FDA_Application_Number VARCHAR(255),
    Line_Extension_Drug_Indicator VARCHAR(255)
)
"""
# Execute the query to create the table
mycursor.execute(create_table_query)
end_create_table = time.time()
time_taken_create_table = end_create_table - start_create_table
print(f"Time taken to create table: {time_taken_create_table} seconds")

csv_file_path = '/Users/manojpadala/Downloads/final_dataset.csv' 
 # Replace with the path to your CSV file

# Table name
table_name = 'Drug_product_final1'
start_load_data = time.time()
# Prepare the SQL query to load data from CSV into the table
load_data_query = f"""
    LOAD DATA LOCAL INFILE '{csv_file_path}'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ','
    ENCLOSED BY '\"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
"""

# Execute the query to load data from CSV into the table
mycursor.execute(load_data_query)
end_load_data = time.time()
time_taken_load_data = end_load_data - start_load_data
print(f"Time taken to load data: {time_taken_load_data} seconds")

mydb.commit()
# Close the connection
mydb.close()
