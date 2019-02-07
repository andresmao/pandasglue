from .read import read
from .write import write


def read_glue(
    query, database, s3_output, region=None, key=None, secret=None, profile_name=None
):
    return read(
        query=query,
        database=database,
        s3_output=s3_output,
        region=region,
        key=key,
        secret=secret,
        profile_name=profile_name,
    )


def write_glue(
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
    return write(
        df=df,
        database=database,
        table=table,
        path=path,
        partition_cols=partition_cols,
        preserve_index=preserve_index,
        region=region,
        key=key,
        secret=secret,
        profile_name=profile_name,
    )
