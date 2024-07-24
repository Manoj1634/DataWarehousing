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
start_create_table_manufacturers = time.time()



create_table_manufacturers_query = """
CREATE TABLE Manufacturers (
    ID INT,
    Labeler_Name VARCHAR(255),
    Labeler_Code INT,
    NDC VARCHAR(255),
    Quarter INT,
    Year INT,
    FOREIGN KEY (ID) REFERENCES Drug_product_final1(ID)
);

"""
# Execute the query to create the table
mycursor.execute(create_table_manufacturers_query)
end_create_table_manufacturers = time.time()
time_taken_create_table_manufacturers = end_create_table_manufacturers - start_create_table_manufacturers
print(f"Time taken to create table manufacturers: {time_taken_create_table_manufacturers} seconds")



# Table name
table_name = 'Drug_product_final1'
start_load_data_manufacturers = time.time()
# Prepare the SQL query to load data from CSV into the table
load_data_query_manufacturers = f"""
    INSERT INTO Manufacturers (ID, Labeler_Name, Labeler_Code, NDC, Quarter, Year)
SELECT ID, Labeler_Name, Labeler_Code, NDC, Quarter, Year
FROM Drug_product_final1;

"""

# Execute the query to load data from CSV into the table
mycursor.execute(load_data_query_manufacturers)
end_load_data_manufacturers = time.time()
time_taken_load_data_manufacturers = end_load_data_manufacturers - start_load_data_manufacturers
print(f"Time taken to load data to manufacturers: {time_taken_load_data_manufacturers} seconds")

mydb.commit()
# Close the connection
mydb.close()