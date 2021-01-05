# Project 4
https://github.com/leventarican/data-engineer-nd

## Summary
* load data from json files to partitioned parquet files on S3

![etl pipeline](data-flow.png)

## How To Run the Project
PREREQUIREMENT: running spark cluster

0. ensure to provide AWS credentials, with environment variables, config files, ...
1. run `etl.py`

## Project Repository files
* `etl.py`: extracts data from S3, transform data to analytics tables, write partitioned parquet files in table directories on S3.

## Additional Information

### AWS CLI
* download, unzip, update environment variable `$PATH` to `aws/dist` 
```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

unzip awscliv2.zip

aws --version
aws-cli/2.1.15 Python/3.7.3 Linux/5.4.0-58-generic exe/x86_64.ubuntu.20 prompt/off
```
