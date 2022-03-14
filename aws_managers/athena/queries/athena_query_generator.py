from typing import Dict, Optional, List, Union, Tuple

from jinja2 import Environment, FileSystemLoader

from aws_managers.athena.reference.athena_ser_des import AthenaSerDes
from aws_managers.athena.clauses.conjunctive_operators import \
    ConjunctiveOperator
from aws_managers.athena.queries import ColumnQuery
from aws_managers.athena.operators.mixins import ComparisonMixin
from aws_managers.paths.dirs import DIR_ATHENA_TEMPLATES


class AthenaQueryGenerator(object):

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
        partitioned. This is the equivalent of selecting "Load partitions" from
        the related table dropdown in the Athena Tables list.

        :param database: Name of the database.
        :param table: Name of the table.
        """
        t = self.env.get_template('ddl/repair_table.jinja2')
        return t.render(database=database, table=table)

    def column_info(
            self,
            database: str,
            table: str
    ) -> str:
        """
        Get info on the table schema.

        https://docs.aws.amazon.com/athena/latest/ug/querying-glue-catalog.html#querying-glue-catalog-listing-columns

        ['table_catalog', 'table_schema', 'table_name', 'column_name',
         'ordinal_position', 'column_default', 'is_nullable', 'data_type',
         'comment', 'extra_info']

        :param database: Name of the database.
        :param table: Name of the table.
        """
        t = self.env.get_template('ddl/column_info.jinja2')
        return t.render(database=database, table=table)

    def aggregate(
            self,
            agg_name: str,
            columns: Union[str, ColumnQuery, List[Union[str, ColumnQuery]]],
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ):
        """
        Aggregate each column using the given function e.g. mean, max, min.

        :param agg_name: Name of the aggregate function to apply to each column.
        :param columns: Column or columns to take the aggregate of.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
                       integer percentage.
        :param where: Optional conditions to filter on.
        """
        if isinstance(columns, str) or isinstance(columns, ColumnQuery):
            columns = [columns]
        t = self.env.get_template('dml/aggregate.jinja2')
        return t.render(
            agg_name=agg_name,
            database=database,
            table=table,
            sample=sample,
            columns=columns,
            where=where
        )

    def aggregate_by_group(
            self,
            agg_name: str,
            agg_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]],
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
        """
        Aggregate each column by group(s) using the given function
        e.g. mean, max, min.

        :param agg_name: Name of the aggregate function to apply to each column.
        :param agg_columns: Column or columns to take the aggregate of.
        :param group_columns: Column or columns to group by.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
                       integer percentage.
        :param where: Optional conditions to filter on.
        """
        if isinstance(agg_columns, str):
            agg_columns = [agg_columns]
        if isinstance(group_columns, str):
            group_columns = [group_columns]
        t = self.env.get_template('dml/aggregate_by_group.jinja2')
        return t.render(
            agg_name=agg_name,
            database=database,
            table=table,
            sample=sample,
            agg_columns=agg_columns,
            group_columns=group_columns,
            where=where
        )

    def count_distinct(
            self,
            columns: Union[str, ColumnQuery, List[Union[str, ColumnQuery]]],
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
        """
        Count the number of distinct values in each column.

        :param columns: Name(s) of columns(s) to count distinct values of.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
                       integer percentage.
        sample: Optional[Tuple[str, int]] = None,
        :param where: Optional conditions to filter on.
        """
        if isinstance(columns, str) or isinstance(columns, ColumnQuery):
            columns = [columns]
        t = self.env.get_template('dml/count_distinct.jinja2')
        return t.render(
            database=database,
            table=table,
            sample=sample,
            columns=columns,
            where=where
        )

    def distinct(
            self,
            column: Union[str, ColumnQuery],
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None,
            order_by: Optional[Union[ColumnQuery, str, bool]] = True
    ) -> str:
        """
        Select distinct values from a column.

        :param column: Name of the column to find a histogram of.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
               integer percentage.
        :param where: Optional conditions to filter on.
        :param order_by: Name of column to order by, True to order by distinct
                         column or False to not specify order.
        """
        t = self.env.get_template('dml/distinct.jinja2')
        if order_by is True:
            order_by = column
        elif order_by is False:
            order_by = None
        return t.render(
            database=database,
            table=table,
            sample=sample,
            column=column,
            where=where,
            order_by=order_by
        )

    def distinct_combinations(
            self,
            columns: List[Union[str, ColumnQuery]],
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
            table=table,
            columns=columns,
            where=where
        )

    def histogram(
            self,
            column: Union[str, ColumnQuery],
            database: str,
            table: str,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
        """
        Returns a map containing the count of the number of times each input
        value occurs.

        https://prestodb.io/docs/current/functions/aggregate.html#histogram

        :param column: Name of the column to find a histogram of.
        :param database: Name of the database.
        :param table: Name of the table.
        :param where: Optional conditions to filter on.
        """
        t = self.env.get_template('dml/histogram.jinja2')
        return t.render(
            database=database,
            table=table,
            column=column,
            where=where
        )

    def numeric_histogram(
            self,
            column: Union[str, ColumnQuery],
            buckets: int,
            database: str,
            table: str,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ) -> str:
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
        :param where: Optional conditions to filter on.
        """
        t = self.env.get_template('dml/numeric_histogram.jinja2')
        return t.render(
            database=database,
            table=table,
            column=column,
            buckets=buckets,
            where=where
        )

    def approx_percentile(
            self,
            columns: Union[str, ColumnQuery, List[Union[str, ColumnQuery]]],
            percentile: float,
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ):
        """
        Returns the approximate percentile for all input values of the column at
        the given percentage. The value of percentage must be between zero and
        one and must be constant for all input rows.

        :param columns: Name of the column(s) to find percentiles of.
        :param percentile: Value of the percentile to calculate.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
                       integer percentage.
        :param where: Optional conditions to filter on.
        """
        if isinstance(columns, str) or isinstance(columns, ColumnQuery):
            columns = [columns]
        t = self.env.get_template('dml/approx_percentile.jinja2')
        return t.render(
            columns=columns,
            percentile=percentile,
            database=database,
            table=table,
            sample=sample,
            where=where
        )

    def approx_percentile_by_group(
            self,
            percentile_columns: Union[
                str, ColumnQuery, List[Union[str, ColumnQuery]]
            ],
            percentile: float,
            group_columns: Union[
                str, ColumnQuery, List[Union[str, ColumnQuery]]
            ],
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None
    ):
        """
        Returns the approximate percentile for all input values of the column at
        the given percentage for each group. The value of percentage must be
        between zero and one and must be constant for all input rows.

        :param percentile_columns: Name of the column(s) to find percentiles of.
        :param percentile: Value of the percentile to calculate.
        :param group_columns: Column or columns to group by.
        :param database: Name of the database.
        :param table: Name of the table.
        :param sample: Optional mapping of 'BERNOULLI' or 'SYSTEM' to an
                       integer percentage.
        :param where: Optional conditions to filter on.
        """
        if (
                isinstance(percentile_columns, str) or
                isinstance(percentile_columns, ColumnQuery)
        ):
            percentile_columns = [percentile_columns]
        if (
                isinstance(group_columns, str) or
                isinstance(group_columns, ColumnQuery)
        ):
            group_columns = [group_columns]
        t = self.env.get_template('dml/approx_percentile_by_group.jinja2')
        return t.render(
            percentile_columns=percentile_columns,
            group_columns=group_columns,
            percentile=percentile,
            database=database,
            table=table,
            sample=sample,
            where=where
        )
