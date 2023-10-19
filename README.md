# data_engineering_repo
This repository containts files showing my data engineering skills in 3 technologies: SQL, PowerBI(DAX) and Python(PySpark).  
  
**T-SQL**:  
Create SQL Server database for E-commerce store selling football clothing, footwear and accesories. Containts 4 files:
01_create_query.sql - creating tables and view
02_insert_query.sql - inserting data to tables
03_programmability.sql - create stored procedures, triggers and functions
04_test_query.sql - sample queries testing solution functionality
05_credentials.sql - creating credential to access the data in Power BI model  
  
**Power BI**:  
Sales report in Direct Query model based on SQL database. Requires configuration to access the data. Contains 3 bookmarks:
Summary - sales overwiew with metrics, Details - table with sales view details, Promo - discounted sale analysis
7 tables from DQ model, calcualated date table and measures table with DAX measures used in the report.
  
Configuration od DQ model report:  
you can create SQL database n your local SQL Server instance and choose Windows authentication mode  
you can create credentials from SQL query and use it in Database authentication mode to access Direct Query model  
make sure that Server in Data Sources setting is the name of your local SQL Server instance and Database is EcommerceFootballStore  
  
Sales report in Import model with materialized tables and view from SQL databas.  

**PySpark**:
Based on CSV files with data from 3 tables from E-commerce database and JSON configuration file data was transformed to map customer  
segmentation proccess. Ensure that you have pyspark and java installed on your local environment and configure all paths according to  
when you will save csv files, configuration json and where you want to save the result parquet file

*About author:
junior data engineer with almost 2 years experience in Big Data projects
you can contact with me by mail: maciekcz95@wp.pl*

