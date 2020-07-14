Project summary:
----------------
We have data from Sparkify company they collected on songs and user activity in their music streaming app. This data is found in S3 in the logs directory JSON of user activity as well as songs data in the application

The project is a pipeline (ETL), which works to extract data from S3 to REDSHIFT for Amazon. It transmits data to the data model that the company’s team will work on to analyze users ’behavior about listening to their songs



How it works:
-------------
Given the requirements of the analysis team interested in discovering the behavior of users when listening to songs, we have implemented the construction of a data model in the form of a star schema and the fact table is songplays and the dimension tables are songs, users, artists and timeز

Here is an explanation of the contents of the files:

dwh.cfg:
The configuration file that contains the link information to connect to redshift cluster

sql_queries.py:
It contains all the special queries for building data models related to facts table and dimension tables
Extract it from () and load it into tables.

create_tables.py:
Connects to redshift and creates the database. Once the database is created it executes the queries in sql_queries.py to build the data models.

etl.py:
It performs the ETL process where data is transferred from JSON files on S3 to the facts table on redshift, then the data is converted and loaded onto the spreadsheets.


How to use the scripts:
-----------------------

Requirements to run the codes:

IAM role must be created to enable redshift to transfer and upload data and build data models on it.
The linking information must also be updated in the actual user on dwh.cfg file

To build data models:
```
python create_tables.py
```

To implement the ETL into the tables:
```
python etl.py
```
