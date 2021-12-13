from typing import Optional, Dict


class AthenaTableManager(object):

    PARQUET_HIVE_SERDE = (
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    )

    def __init__(
            self,
            db_name: str,
            table_name: str,
            columns: Optional[Dict[str, str]] = None,
            partition_column: Optional[str] = None,
            encrypted: bool = False
    ):
        """
        Create a new AthenaTableManager.

        :param db_name: Name of the Athena database.
        :param table_name: Name of the Athena table.
        :param columns: Mapping from columns to data-types.
        :param partition_column: Name of partition column, if there is one.
        :param encrypted: Whether the S3 data is encrypted.
        """
        self.db_name: str = db_name
        self.table_name: str = table_name
        self.columns: Optional[Dict[str, str]] = columns
        self.partition_column: Optional[str] = partition_column
        self.encrypted: bool = encrypted
