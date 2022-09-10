# Data Pipelines with Airflow

--------------------------------------------

### Introduction

A music streaming company, Sparkify, has decided that it is time to introduce
more automation and monitoring to their data warehouse ETL pipelines and come to
the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high
grade data pipelines that are dynamic and built from reusable tasks, can be
monitored, and allow easy backfills. They have also noted that the data quality
plays a big part when analyses are executed on top the data warehouse and want
to run tests against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data
warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell
about user activity in the application and JSON metadata about the songs the
users listen to.

### ELT Process

The tool used for scheduling and orchestration is Apache Airflow.

The pipeline ELT can be seen in the following airflow DAG:

![DAG](./images/sparkify-dag.png)

### Sources

The sources are in an S3 bucket owned by Udacity.

* `Log data: s3://udacity-dend/log_data`
* `Song data: s3://udacity-dend/song_data`


### Project Structure

* dags
    * `sparkify_dag.py` - The DAG configuration file to setup Airflow
    * sql
        * `create_tables.sql` - Contains the DDL for all table initialization
* plugins
    * operators
        * `stage_redshift.py` - Read json from S3 and stage tables in Redshift
        * `load_fact.py` - Load the facts in Redshift from staging tables
        * `load_dimension.py` - Loads dimensions in Redshift from staging tables
        * `data_quality.py` - Basic data quality checking (has records)
    * helpers
        * `sql_queries` - Redshift statements used in the DAG's operators
