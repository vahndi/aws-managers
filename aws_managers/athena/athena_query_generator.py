from typing import Dict, Optional, List, Union

from jinja2 import Environment, FileSystemLoader

from aws_managers.athena.athena_ser_des import AthenaSerDes
from aws_managers.athena.clauses.conjunctive_operators import \
    ConjunctiveOperator
from aws_managers.athena.operators.comparisons import ComparisonMixin
from aws_managers.paths.dirs import DIR_ATHENA_TEMPLATES


class AthenaQueryGenerator(object):

    SER_DES = {
        'parquet': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',

    }
    PARQUET_HIVE_SERDE = (
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    )

    def __init__(self):

        self.env = Environment(loader=FileSystemLoader(DIR_ATHENA_TEMPLATES))

    def create_table(
        self,
        database: str,
        table: str,
        columns: Dict[str, str],
        location: str,
        serialization_format: int = 1,
        row_format: str = AthenaSerDes.Parquet,
        partition_columns: Optional[List[str]] = None,
        encrypted: bool = False
    ) -> str:
        """
        Run this query to create a table pointing to parquet data in S3.

        :param database: Name of the database.
        :param table: Name of the table.
        :param columns: Mapping of column names to dtypes.
        :param location: S3 location to save the data.
        :param serialization_format: Serialization format.
        :param row_format: Row format.
        :param partition_columns: Mapping of partition column names to dtypes.
        :param encrypted: Whether the data is encrypted.
        """
        t = self.env.get_template('ddl/create_table.jinja2')
        return t.render(
            database=database,
            table=table,
            columns=columns,
            location=location,
            serialization_format=serialization_format,
            row_format=row_format,
            partition_columns=partition_columns,
            encrypted=str(encrypted).lower()
        )

    def repair_table(
            self,
            database: str,
            table: str
    ) -> str:
        """
        Run this query right after creating the table, if the S3 data is
        partitioned.

        :param database: Name of the database.
        :param table: Name of the table.
        """
        t = self.env.get_template('ddl/repair_table.jinja2')
        return t.render(database=database, table=table)

    def distinct(
            self,
            column: str,
            database: str, table: str,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
        """
        Select distinct values from a column.

        :param column: Name of the column to find a histogram of.
        :param database: Name of the database.
        :param table: Name of the table.
        :param where: Optional conditions to filter on.
        """
        t = self.env.get_template('dml/distinct.jinja2')
        return t.render(
            database=database,
            table=table, column=column,
            where=where
        )

    def distinct_combinations(
            self,
            columns: List[str],
            database: str,
            table: str,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
        """
        Select distinct combinations of values from more than one column.

        :param columns: List of column names.
        :param database: Name of the database.
        :param table: Name of the table.
        :param where: Optional conditions to filter on.
        """
        t = self.env.get_template('dml/distinct_combinations.jinja2')
        return t.render(
            database=database,
            table=table, columns=columns,
            where=where
        )

    def histogram(
            self,
            column: str,
            database: str,
            table: str
    ) -> str:
        """
        Returns a map containing the count of the number of times each input
        value occurs.

        https://prestodb.io/docs/current/functions/aggregate.html#histogram

        :param column: Name of the column to find a histogram of.
        :param database: Name of the database.
        :param table: Name of the table.
        """
        t = self.env.get_template('dml/histogram.jinja2')
        return t.render(
            database=database,
            table=table, column=column
        )

    def numeric_histogram(
            self,
            column: str,
            buckets: int,
            database: str,
            table: str
    ):
        """
        Computes an approximate histogram with up to buckets number of buckets
        for all values. This function is equivalent to the variant of
        numeric_histogram() that takes a weight, with a per-item weight of 1.
        In this case, the total weight in the returned map is the count of items
        in the bin.

        https://prestodb.io/docs/current/functions/aggregate.html#numeric_histogram

        :param column: Name of the column to find a histogram of.
        :param buckets: Number of buckets to create.
        :param database: Name of the database.
        :param table: Name of the table.
        """
        t = self.env.get_template('dml/numeric_histogram.jinja2')
        return t.render(
            database=database,
            table=table, column=column,
            buckets=buckets
        )
