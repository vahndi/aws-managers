


def s3_bucket_uri(bucket_name: str) -> str:
    """
    Return the path to the s3 bucket.

    :param bucket_name: Name of the S3 bucket.
    """
    return f's3://{bucket_name}/'


