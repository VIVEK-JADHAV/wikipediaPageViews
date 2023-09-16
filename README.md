# ETL Pipeline using Airflow for Wiki Page Views

## Project Overview
This project involves building an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The primary objective is to extract data from Wiki Page Views, transform it into the required format, and load it into a SQL Server database. This pipeline enables users to analyze the pageviews of different companies such as Facebook, Amazon, Google, etc., on an hourly basis. The data can be used to test the hypothesis that an increase in a company's pageviews is associated with a positive sentiment and a potential increase in the company's stock price, while a decrease in pageviews may indicate a loss of interest and a potential stock price decrease.

## Project Source
The data source for this project is Wiki Page Views, which is provided by the Wikimedia Foundation. This source contains pageview data since 2015, available in machine-readable format. The pageviews can be downloaded in gzip format and are aggregated per hour per page. Each hourly dump is approximately 50 MB in gzipped text files and ranges from 200 to 250 MB in size when uncompressed.

## Building the Pipeline
The ETL pipeline consists of four main tasks:

1. **Get Data**: This task, implemented using a Bash Operator, executes a `curl` command to download the page views for a specific hour. It uses the `execution_date` parameter to extract the day, year, month, and hour values using Jinja templated strings.

2. **Extract Gz**: Also implemented using a Bash Operator, this task unzips the downloaded file and stores it in a temporary folder.

3. **Fetch Page Views**: Using a Python Operator, this task extracts the required columns from the data and filters for data related to the specified companies. It also includes an SQL script with `INSERT` statements to transform and insert the data into the SQL Server database.

4. **Write to SQL Server**: Implemented as an MsSqlOperator, this task establishes a connection to the SQL Server database and executes the SQL script generated in the previous task, effectively loading the transformed data into the database.

## Concepts of Airflow Used
Throughout this project, several key concepts of Apache Airflow were applied:

1. **Atomicity and Idempotency**: The pipeline design follows the principles of atomicity (each task is an atomic unit of work) and idempotency (re-running the pipeline does not cause issues).

2. **Runtime Parameters**: The `execution_date` parameter is used to dynamically fetch the data for the specified hour.

3. **Jinja Templated String**: Jinja templating is employed to define the loading date and extract relevant information from the `execution_date`.

4. **Connecting to External Systems**: The pipeline demonstrates the ability to connect to an external system, specifically a SQL Server database, to load the transformed data.

