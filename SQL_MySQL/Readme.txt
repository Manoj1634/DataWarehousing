## ﻿Within the realm of SQL, we're also evaluating performance differences between Python and MySQL Workbench.


We used Python to export a CSV file to a MySQL server, importing the data and creating tables through Python scripts. We created two databases within the MySQL server: DW_Project1 (utilized for Python-based queries) and DW_Project (used for queries in MySQL Workbench).


In DW_Project, 'Drug_product_final' acts as the parent table, while in DW_Project1, 'Drug_product_final1' serves as the parent table. For both parent tables, 'Manufacturers', 'Drugs', and 'FDA' are the corresponding children tables.


We designed a Hierarchical Model to observe performance difference in performances between Parent and Child tables. Refer to the Tables.txt document to review the table structures and column names.


Queries were executed in both Python and MySQL Workbench to assess performance variations.


*** The queries run in MySQL Workbench are documented in MySQL Queries.txt***


The SQL queries executed using Python are documented in the "Python Queries" file, while the Python scripts with SQL connectors used for creating databases and loading data onto the server are in the "Data Creation and Import" file.


Altogether, we've conducted 19 queries: 9 parent tables, 9 about  tables (ensuring identical outcomes for performance testing), and a deletion query. 


All 19 queries were executed using both Python and SQL Workbench.


*****
Detailed information and text files for these queries are available in the "Documents" directory.
*******
