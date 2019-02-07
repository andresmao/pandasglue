import boto3


def get_boto_session(key=None, secret=None, profile_name=None, region=None):
    """
    Return a configured boto3 Session object
    """
    if profile_name:
        session = boto3.Session(region_name=region, profile_name=profile_name)
    elif key and secret:
        session = boto3.Session(
            region_name=region, aws_access_key_id=key, aws_secret_access_key=secret
        )
    else:
        session = session = boto3.Session(region_name=region)
    return session
