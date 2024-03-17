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
        if "accelerometer" in filename:
            full_filename = f"accelerometer/landing/{filename}"
        elif "customer" in filename:
            full_filename = f"customer/landing/{filename}"
        elif "step_trainer" in filename:
            full_filename = f"step_trainer/landing/{filename}"
        else:
            continue
        print(f'Copying {file} to s3://{bucket_name}/{full_filename}')
        s3_client.upload_file(file, bucket_name, full_filename)


def touch(marker: str):
    with(open(f"marker_{marker}.txt", 'wt')) as m:
        m.write("done")


def exists(marker: str):
    return os.path.exists(f"marker_{marker}.txt")


def create_database():
    glue = boto3.client('glue', region_name='us-east-1')

    for db in glue.get_databases()['DatabaseList']:
        if db['Name'] == 's3db':
            print('Database s3db already exists')
            return

    # create a database with the name "s3db" in glue
    response = glue.create_database(
        DatabaseInput={
            'Name': 's3db'
        }
    )

def create_accelerometer_landing(bucket_name):
    glue = boto3.client('glue', region_name='us-east-1')

    # create a table with the name "accelerometer" in glue
    # We have 3 column:
    # "user" is a "string",
    # "timestamp" is a "bigint",
    # "x" is a "double"
    # "y" is a "double"
    # "z" is a "double"
    # the data is stored in json format
    response = glue.create_table(
        DatabaseName='s3db',
        TableInput={
            'Name': 'accelerometer',
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'user',
                        'Type': 'string'
                    },
                    {
                        'Name': 'timestamp',
                        'Type': 'bigint'
                    },
                    {
                        'Name': 'x',
                        'Type': 'double'
                    },
                    {
                        'Name': 'y',
                        'Type': 'double'
                    },
                    {
                        'Name': 'z',
                        'Type': 'double'
                    }
                ],
                'Location': f's3://{bucket_name}/accelerometer/landing/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe'
                }
            }
        }
    )



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

    step3 = "create_database"
    if not exists(step3):
        create_database()
        touch(step3)

    step4 = "create_accelerometer_landing"
    if not exists(step4):
        create_accelerometer_landing(bucket_name)
        touch(step3)


if __name__ == '__main__':
    main()
