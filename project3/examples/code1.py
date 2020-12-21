import configparser
import pandas as pd
import boto3
import json

# AWS SDK python (boto3) example with a config file
# ##############################################################################

if __name__ == "__main__":

    # get configuration
    config = configparser.ConfigParser()
    config.read_file(open('ignore-config.cfg'))
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    # create client for S3
    s3 = boto3.resource('s3',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
        )

    # pull data from S3 bucket
    sampleDbBucket =  s3.Bucket("awssampledbuswest2")
    for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
        print(obj)
