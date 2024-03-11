import glob
import os.path

import boto3


# Inspired by: https://stackoverflow.com/questions/4103773/efficient-way-of-having-a-function-only-execute-once-in-a-loop
from functools import wraps
def create_bucket_if_not_exists(bucket_name):
    """
    create a bucket when it not exists with the name "bucket_name"
    """
    s3 = boto3.client('s3')

    for bucket in s3.list_buckets()['Buckets']:
        if bucket_name in bucket['Name']:
            print(f'Bucket {bucket_name} already exists')
            return
    s3.create_bucket(Bucket=bucket_name)



def copy_data(bucket_name):
    """
    copy the data from project/starter/* to the newly created bucket
    """
    s3_client = boto3.client('s3')
    for file in glob.glob("project/starter/**", recursive=True):
        if os.path.isdir(file):
            continue
        filename = file.split('/')[-1]
        print(f'Copying {file} to s3://{bucket_name}/{filename}')
        s3_client.upload_file(file, bucket_name, filename)

def touch(marker: str):
    with(open(f"marker_{marker}.txt", 'wt')) as m:
        m.write("done")

def exists(marker: str):
    return os.path.exists(f"marker_{marker}.txt")


def create_customer_landing(bucket_name):
    glue = boto3.client('glue')
    # create a table with the name "customer_landing" in glue



def main():
    bucket_name = 'project-stedi-de-hamburg-harburg-2'

    step1 = "create_bucket_if_not_exists"
    if not exists(step1):
        create_bucket_if_not_exists(bucket_name)
        touch(step1)

    step2 = "copy_data"
    if not exists(step2):
        copy_data(bucket_name)
        touch(step2)

    step3 = "create_customer_landing"
    if not exists(step3):
        create_customer_landing(bucket_name)
        touch(step3)


if __name__ == '__main__':
    main()
