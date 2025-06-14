# Project Sparkify S3 to Redshift ETL

This project is part of the Nanodegree Program in Data Engineering with AWS at Udacity and serves as a test project to apply data warehousing concepts and ETL pipeline development.

Sparkify, a music streaming startup, is expanding its operations and transitioning its data processes to the cloud. The company's data consists of JSON logs tracking user activity and JSON metadata on available songs, both stored in Amazon S3. To facilitate efficient analytics, this project implements an ETL pipeline that extracts, stages, and transforms this data into structured dimensional tables within Amazon Redshift.


## Project Overview

This project leverages AWS Redshift and data warehousing techniques to enable in-depth analysis of user listening behavior. The pipeline follows these key steps:

* Extracting data from JSON files stored in S3.
* Staging data in Amazon Redshift.
* Transforming data into optimized analytics tables using SQL queries.

The resulting tables provide valuable insights that support Sparkify’s analytics team in understanding user preferences and engagement patterns.

## Structure

1. initiate_redshift_cluster.py: Sets up the cluster, assigns roles, etc.

2. create_tables.py (with template): Creates tables in the Redshift cluster using data models, but the tables are still empty.

3. etl.py (with template): Loads data from S3 buckets; the data is initially in JSON format and needs to be converted into tabular data using Pandas before being loaded into the Redshift cluster database.

4. analyze_songs_data.py (TODO): The data is stored in the Redshift cluster and is ready for analysis, but the script has not been created yet.

5. clean_up_cluster.py: cleanup of AWS Redshift resources, including deleting a Redshift cluster and its associated IAM role and policy

