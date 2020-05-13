#  Data Modeling with Postgres & ETL Pipeline for Sparkify 
***
### Introduction
Sparkify is a music streaming application. Users data is in JSON format, which has caused a difficult way to query the data.
***
### The Goal
Create a Postgres database and ETL pipeline to optimize queries to assist the Sparkify analytics team.
***
### ETL database and pipeline
Data sets were identified and created a star schema which includes:

* One Fact Sheet: **songplays**
* Four dimensional tables: **Users**,**Songs**, **Artists**and **Time**.
