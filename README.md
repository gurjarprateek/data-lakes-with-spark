### CREATED ON - July 30 2019
### MODIFIED ON - July 30 2019
### DEVELOPER - PRATEEK GURJAR

This project covers the entire ETL process for our client Sparkify. Sparkify is a service that provides on demand music streaming for its customers. In the project we started with launching a Spark cluster using AWS EMR Service and created a dimensional model with Songplays as Fact Table and Songs, Artists, Time and Users as Dimension dataframes. The dimensional model is created using SparkSQL and is basically a STAR Schema. Our data is located in S3 buckets, we write ETL logic to move data from S3 buckets into a Spark, perform on the go transformations and save the data in parquet format in S3 buckets. 

### Technology Stack
For this project we used SparkSQL on AWS EMR to perform ETL on songs and logs data based in S3

### Data Sources
Data is stored in S3 buckets and we use spark.read command to load data into dataframe
1. /data/songs 
From the songs directory we created Songs and Artists dataframe, transformed it into parquet and dumped the parquet files in S3 bucket 
2. /data/logs
From the logs directory we created Time and Users Dimension tables. We used udfs for creating Time Dimension tables by converting timestamp column to datetime

We used song_id and artist_id from the song and artist dataframes in conjuction with other columns from logs dataset to create songplay dataframe.

### ETL Pipeline
etl.py file consists of Python functions for processing song_data and log_data. 
Following are the steps to run the ETL pipeline
1. In the project workspace, open a new terminal
2. Go in project directory and type following command
    python etl.py
    
Jupyter Notebook etl.ipynb was used to create the ETL in first place

### Future Enhancements
Based on dimension and fact tables we could now build data marts for Analytics and Reporting purposed. Some basic examples of report would be
- No of Paid and Free members from a location
- Artist and Song popularity by location
- Top Artists or Songs by Month/Year/Week


