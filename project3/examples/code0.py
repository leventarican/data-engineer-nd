import pandas as pd
import boto3
import json

# AWS SDK python (boto3) example
# ##############################################################################

# ensure to install AWS SDK for python: boto3

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration

# create environment variable for access keys
# check with env (global only) or set (global and local)
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=

# boto3 will search in the environment variable for the keys.

def displayS3buckets():
    s3 = boto3.resource("s3")
    for bucket in s3.buckets.all():
        print(bucket.name)

if __name__ == "__main__":
    displayS3buckets()
