import boto3
import os

# example for downloading json files (incl. file in directory)
# ##############################################################################
# https://s3.console.aws.amazon.com/s3/buckets/udacity-dend
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

BUCKET = 'udacity-dend'
KEY = 'log_json_path.json'
FILE_TYPE = "json"

def downloadFileFromS3():
    s3 = boto3.resource("s3")
    s3.Bucket(BUCKET).download_file(KEY, 'log_json_path.json')

def downloadDirectoryFromS3(remoteDirectoryName):
    """
    process a given directory.
    before download a json file create all directories locally then download file.
    e.g. log_data/2018/11/2018-11-05-events.json
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET)

    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        key = obj.key
        path, filename = os.path.split(key)
        print(f"key: {key}; path: {path}; filename: {filename}")
        os.makedirs(path, exist_ok=True)
        if FILE_TYPE in filename:
            s3.meta.client.download_file(Bucket=BUCKET, Key=key, Filename=key)

if __name__ == "__main__":
    # downloadFileFromS3()
    # downloadDirectoryFromS3("log_data")
    downloadDirectoryFromS3("song_data")