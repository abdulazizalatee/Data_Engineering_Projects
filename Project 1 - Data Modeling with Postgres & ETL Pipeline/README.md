#  Data Modeling with Postgres & ETL Pipeline for Sparkify 

### Introduction

Sparkify is a music streaming application. Users data is in JSON format, which has caused a difficult way to query the data.

### The Goal

Create a Postgres database and ETL pipeline to optimize queries to assist the Sparkify analytics team.

### ETL database and pipeline

Data sets were identified and created a star schema which includes:

* One Fact Sheet: **songplays**
* Four dimensional tables: **Users** , **Songs** , **Artists** and **Time**.

### How to

The source code contains three separate Python scripts:

1 - **sql_queries.py** contains all operations related to tables such as creating, dropping, adding, and querying.
 
2 - **create_tables.py** creates database, connect and drop all the tables required using sql_queries.

3- **etl.py** create a pipeline and etl to extract the data from json files and then insert all the data in the required tables.
