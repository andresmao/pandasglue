from math import ceil
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.compat import guid
import s3fs

from .utils import get_boto_session

if sys.version_info.major > 2:
    xrange = range


def _get_fs(key=None, secret=None, profile_name=None):
    if profile_name:
        fs = s3fs.S3FileSystem(profile_name=profile_name)
    elif key and secret:
        fs = s3fs.S3FileSystem(key=key, secret=secret)
    else:
        fs = s3fs.S3FileSystem()
    return fs


def _mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def _type_arrow2athena(arrow_type):
    arrow_type = arrow_type.lower()
    if arrow_type in ["int64", "int32"]:
        return "bigint"
    if arrow_type == "bool":
        return "boolean"
    if arrow_type in ["timestamp", "date", "binary"]:
        return "string"
    return arrow_type.lower()


def _build_schema(schema, partition_cols):
    schema_build = []
    for field in list(schema):
        # if field.name.startswith("__index_level_"):
        #     pass  # index
        if field.name not in partition_cols:
            athena_type = _type_arrow2athena(str(field.type))
            schema_build.append((field.name, athena_type))
    return schema_build


def _write_data(df, fs, path, partition_cols=[], preserve_index=True):
    """
    Write the parquet files to s3
    """
    fs, path = pq._get_filesystem_and_path(fs, path[:-1])
    _mkdir_if_not_exists(fs, path)
    table = pa.Table.from_pandas(df)
    schema = _build_schema(schema=table.schema, partition_cols=partition_cols)
    if partition_cols is not None and len(partition_cols) > 0:
        partition_keys = [df[col] for col in partition_cols]
        data_df = df.drop(partition_cols, axis="columns")
        data_cols = df.columns.drop(partition_cols)
        if len(data_cols) == 0:
            raise ValueError("No data left to save outside partition columns")
        subschema = table.schema
        for col in table.schema.names:
            if col.startswith("__index_level_") or col in partition_cols:
                subschema = subschema.remove(subschema.get_field_index(col))
        partition_paths = []
        for keys, subgroup in data_df.groupby(partition_keys):
            if not isinstance(keys, tuple):
                keys = (keys,)
            subdir = "/".join(
                [
                    "{colname}={value}".format(colname=name, value=val)
                    for name, val in zip(partition_cols, keys)
                ]
            )
            subtable = pa.Table.from_pandas(
                subgroup, preserve_index=preserve_index, schema=subschema, safe=False
            )
            prefix = "/".join([path, subdir])
            _mkdir_if_not_exists(fs, prefix)
            outfile = guid() + ".parquet"
            full_path = "/".join([prefix, outfile])
            with fs.open(full_path, "wb") as f:
                pq.write_table(subtable, f)
                partition_path = full_path.rpartition("/")[0] + "/"
                partition_paths.append((partition_path, keys))
    else:
        outfile = guid() + ".parquet"
        full_path = "/".join([path, outfile])
        with fs.open(full_path, "wb") as f:
            pq.write_table(table, f)
        partition_paths = None
    return schema, partition_paths


def _table_exists(client, database, table):
    """
    Check if a specific Glue table exists
    """
    try:
        client.get_table(DatabaseName=database, Name=table)
        return True
    except client.exceptions.EntityNotFoundException:
        return False


def _create_table(client, database, table, schema, partition_cols, path):
    """
    Create Glue table
    """
    client.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table,
            "PartitionKeys": [{"Name": x, "Type": "string"} for x in partition_cols],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": "none",
                "typeOfData": "file",
            },
            "StorageDescriptor": {
                "Columns": [{"Name": x[0], "Type": x[1]} for x in schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
                "StoredAsSubDirectories": False,
                "SortColumns": [],
                "Parameters": {
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "classification": "parquet",
                    "compressionType": "none",
                    "typeOfData": "file",
                },
            },
        },
    )


def _add_partitions(client, database, table, partition_paths):
    """
    Add a list of partitions in a Glue table
    """
    partitions = list()
    for partition in partition_paths:
        partition = {
            u"StorageDescriptor": {
                u"InputFormat": u"org.apache.hadoop.mapred.TextInputFormat",
                u"Location": partition[0],
                u"SerdeInfo": {
                    u"Parameters": {u"serialization.format": u"1"},
                    u"SerializationLibrary": u"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
                u"StoredAsSubDirectories": False,
            },
            u"Values": partition[1],
        }
        partitions.append(partition)
    pages_num = int(ceil(len(partitions) / 100.0))
    for _ in range(pages_num):
        page = partitions[:100]
        del partitions[:100]
        client.batch_create_partition(
            DatabaseName=database, TableName=table, PartitionInputList=page
        )


def write(
    df,
    database,
    table,
    path,
    partition_cols=[],
    preserve_index=True,
    region=None,
    key=None,
    secret=None,
    profile_name=None,
):
    """
    Convert a given Pandas Dataframe to a Glue Parquet table
    """
    fs = _get_fs(key=key, secret=secret, profile_name=profile_name)
    schema, partition_paths = _write_data(
        df=df,
        fs=fs,
        path=path,
        partition_cols=partition_cols,
        preserve_index=preserve_index,
    )
    glue_client = get_boto_session(
        key=key, secret=secret, profile_name=profile_name, region=region
    ).client("glue")
    exists = _table_exists(client=glue_client, database=database, table=table)
    if not exists:
        _create_table(
            client=glue_client,
            database=database,
            table=table,
            schema=schema,
            partition_cols=partition_cols,
            path=path,
        )
    _add_partitions(
        client=glue_client,
        database=database,
        table=table,
        partition_paths=partition_paths,
    )
