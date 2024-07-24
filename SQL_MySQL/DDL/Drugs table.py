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
start_create_table_Drugs = time.time()



create_table_Drugs_query = """
CREATE TABLE Drugs (
    ID INT,
    Product_Code INT,
    Package_Size_Code INT,
    Drug_Category VARCHAR(255),
    Drug_Type_Indicator INT,
    Unit_Type VARCHAR(255),
    Units_Per_Pkg_Size DECIMAL(18, 3),
    Line_Extension_Drug_Indicator VARCHAR(255),
    COD_Status DECIMAL(5, 1),
    NDC VARCHAR(255),
    Clotting_Factor_Indicator VARCHAR(1),
    Pediatric_Indicator VARCHAR(1),
    FOREIGN KEY (ID) REFERENCES Drug_product_final1(ID)

);


"""
# Execute the query to create the table
mycursor.execute(create_table_Drugs_query)
end_create_table_Drugs = time.time()
time_taken_create_table_Drugs = end_create_table_Drugs - start_create_table_Drugs
print(f"Time taken to create table Drugs: {time_taken_create_table_Drugs} seconds")



# Table name
table_name = 'Drug_product_final1'
start_load_data_Drugs = time.time()
# Prepare the SQL query to load data from CSV into the table
load_data_query_Drugs = f"""
    INSERT INTO Drugs (ID, Product_Code, Package_Size_Code, Drug_Category, Drug_Type_Indicator, Unit_Type, Units_Per_Pkg_Size, Line_Extension_Drug_Indicator, COD_Status, NDC, Clotting_Factor_Indicator, Pediatric_Indicator)
SELECT ID, Product_Code, Package_Size_Code, Drug_Category, Drug_Type_Indicator, Unit_Type, Units_Per_Pkg_Size, Line_Extension_Drug_Indicator, COD_Status, NDC, Clotting_Factor_Indicator, Pediatric_Indicator
FROM Drug_product_final1;


"""

# Execute the query to load data from CSV into the table
mycursor.execute(load_data_query_Drugs)
end_load_data_Drugs = time.time()
time_taken_load_data_Drugs = end_load_data_Drugs - start_load_data_Drugs
print(f"Time taken to load data to Drugs: {time_taken_load_data_Drugs} seconds")

mydb.commit()
# Close the connection
mydb.close()