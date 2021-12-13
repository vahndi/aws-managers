from botocore.client import BaseClient
from typing import List, Optional


class S3ContainerMixin(object):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """
    _bucket_name: str
    _bucket_uri: str
    prefix: Optional[str]
    _client: BaseClient

    def list_object_keys(self) -> List[str]:
        """
        Returns the value of 'Key' for each item in the container.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        """
        kwargs = dict(Bucket=self._bucket_name)
        if self.prefix is not None:
            kwargs['Prefix'] = self.prefix
        response = self._client.list_objects_v2(**kwargs)
        return [f['Key'] for f in response['Contents']]
