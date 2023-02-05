# csvs3tosnowflake

## Introduction

This is a Python script to publish CSV files from an Amazon S3 bucket to Snowflake tables.
## Prerequisites

    An active Snowflake account.
    A configured Amazon S3 bucket containing the CSV files you want to publish.
    A Snowflake role with the required permissions to create and load tables in Snowflake.
    Python 3 installed on your system.
    The following Python packages installed: snowflake-connector-python, boto
    Update the path on the top of the py files to the path on your system where python in installed

## Configuration

### Update the following configuration details in config.yaml

    Snowflake account name, username, password, and warehouse details.
    S3 bucket name and access key.

## Usage

Run the script using the following command:

    main.py -s bucketname:path/to/csvzipfile -c config.yaml
    OR
    main.py -b bucketname:path/for/the/prefix/to/filter -c config.yaml

The script will retrieve the list of CSV files from the S3 bucket and create tables in Snowflake with the same name as the file and load the data from the file to the tables.

## Note

The script assumes that the CSV files have headers and use comma (,) as the delimiter. If your CSV files have different format, modify the script accordingly. The script will create tables if they do not exist in Snowflake and if the table already exists, it will drop the table, create the table and load the data.
