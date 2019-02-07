import uuid
import boto3
from botocore.exceptions import ClientError
import pytest
import pandas as pd
import pandasglue as pg


def create_aleatory_bucket():
    prefix = "pandasglue-test-"
    while True:
        rand = uuid.uuid4().hex
        bucket_name = prefix + rand
        try:
            boto3.client("s3").create_bucket(Bucket=bucket_name)
            break
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "BucketAlreadyExists":
                raise exc
    return bucket_name


def delete_bucket(name):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(name)
    bucket.objects.all().delete()
    bucket.delete()


def create_aleatory_database():
    prefix = "pandasglue_test_"
    while True:
        rand = uuid.uuid4().hex
        database_name = prefix + rand
        try:
            boto3.client("glue").create_database(DatabaseInput={"Name": database_name})
            break
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "AlreadyExistsException":
                raise exc
    return database_name


def delete_database(name):
    boto3.client("glue").delete_database(Name=name)


@pytest.fixture(scope="module")
def bucket():
    bucket = create_aleatory_bucket()
    print("Random bucket created with name: {}".format(bucket))
    yield bucket
    # delete_bucket(name=bucket)
    # print("Bucket deleted: {}".format(bucket))


@pytest.fixture(scope="module")
def database():
    database = create_aleatory_database()
    print("Random database created with name: {}".format(database))
    yield database
    # delete_database(name=database)
    # print("Database deleted: {}".format(database))


def test_pandasglue(bucket, database):
    print("Bucket: {}".format(bucket))
    print("Database: {}".format(database))
    df = pd.read_csv("sample_data/big-mac-index.csv")
    df = df[(df.name.isin(["Brazil", "Argentina"])) & (df.date > "2015")]
    pg.write_glue(
        df=df,
        database=database,
        table="bigmac",
        path="s3://{}/bigmac/".format(bucket),
        partition_cols=["name", "date"],
    )
    df2 = pg.read_glue(
        "select * from bigmac", database, "s3://{}/athena/".format(bucket)
    )
    assert len(df.index) == len(df2.index)
