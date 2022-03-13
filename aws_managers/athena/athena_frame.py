from typing import Optional, Union, List, Tuple

from awswrangler.athena import read_sql_query
from pandas import DataFrame, Index, Series

from aws_managers.athena.queries.athena_column_query_set import \
    AthenaColumnQuerySet
from aws_managers.athena.reference.athena_data_types import ATHENA_BOOLEAN_TYPES, \
    ATHENA_INTEGER_TYPES, ATHENA_REAL_TYPES, ATHENA_DATETIME_TYPES, \
    ATHENA_CHARACTER_TYPES, ATHENA_NUMERIC_TYPES
from aws_managers.athena.queries.athena_query_generator import AthenaQueryGenerator
from aws_managers.athena.athena_series import AthenaSeries
from aws_managers.athena.clauses.conjunctive_operators import \
    ConjunctiveOperator
from aws_managers.athena.operators.mixins import ComparisonMixin


class AthenaFrame(object):

    def __init__(
            self,
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None,
            column_info: Optional[DataFrame] = None
    ):
        """
        Create a new AthenaFrame.

        :param database: Name of the Athena database.
        :param table: Name of the Athena table.
        :param column_info: Column info from schema if this is a subset of an
                            existing frame. Leave as None for a new Frame.
        :param sample: Optional tuple of 'BERNOULLI' or 'SYSTEM' and an
                       integer percentage.
        :param where: Values for WHERE clause.
        """
        self._q: AthenaQueryGenerator = AthenaQueryGenerator()
        self._database: str = database
        self._table: str = table
        if isinstance(column_info, DataFrame):
            self._column_info: DataFrame = column_info
        else:
            self._column_info: DataFrame = self._execute(
                sql=self._q.column_info(
                    database=self._database, table=self._table)
            )
        self.column_query_set = AthenaColumnQuerySet(
            column_info=self._column_info)
        self._sample: Optional[Tuple[str, int]] = sample
        self._where = where

    def _execute(self, sql: str) -> DataFrame:
        """
        Execute a query.

        :param sql: Raw SQL to execute.
        """
        data = read_sql_query(sql=sql, database=self._database)
        return data

    @property
    def columns(self) -> Index:
        """
        The column labels of the table.
        """
        return Index(self._column_info['column_name'].to_list())

    @property
    def data_types(self) -> Series:
        """
        Return the data-types in the table.
        """
        return self._column_info.set_index('column_name')['data_type']

    # region sampling

    def bernoulli_sample(self, percentage: int) -> 'AthenaFrame':
        """
        Do sampling from the Frame using the Bernoulli method.
        """
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=('BERNOULLI', percentage),
            where=self._where,
            column_info=self._column_info,
        )

    def system_sample(self, percentage: int) -> 'AthenaFrame':
        """
        Do sampling from the Frame using the System method.
        """
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=('SYSTEM', percentage),
            where=self._where,
            column_info=self._column_info
        )

    # endregion

    # region select data-types

    def select_data_types(
            self,
            include: Optional[Union[str, List[str]]] = None,
            exclude: Optional[Union[str, List[str]]] = None
    ) -> 'AthenaFrame':
        """
        Return a subset of the Frame’s columns based on the column data-types.

        :param include: A selection of data-type names to be included.
        :param exclude: A selection of data-type names to be included.
        """
        if (
                all([include is None, exclude is None]) or
                all([include is not None, exclude is not None])
        ):
            raise ValueError('Must pass one and only one of include or exclude')

        if include is not None:
            if isinstance(include, str):
                include = [include]
            column_info = self._column_info.loc[
               self._column_info['data_type'].isin(include)
            ]
        else:
            if isinstance(exclude, str):
                exclude = [exclude]
            column_info = self._column_info.loc[
                ~self._column_info.isin(exclude)
            ]
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where,
            column_info=column_info
        )

    def select_numeric_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the numeric data types.
        """
        return self.select_data_types(include=ATHENA_NUMERIC_TYPES)

    def select_non_numeric_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the numeric data types.
        """
        return self.select_data_types(exclude=ATHENA_NUMERIC_TYPES)

    def select_boolean_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the boolean and binary data types.
        """
        return self.select_data_types(include=ATHENA_BOOLEAN_TYPES)

    def select_datetime_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the date and timestamp data types.
        """
        return self.select_data_types(include=ATHENA_DATETIME_TYPES)

    def select_integer_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the tinyint, smallint, integer and bigint
        data types.
        """
        return self.select_data_types(include=ATHENA_INTEGER_TYPES)

    def select_real_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the float and double data types.
        """
        return self.select_data_types(include=ATHENA_REAL_TYPES)

    def select_character_types(self) -> 'AthenaFrame':
        """
        Return a Frame with only the char, varchar and string data types.
        """
        return self.select_data_types(include=ATHENA_CHARACTER_TYPES)

    # endregion

    def count_distinct(self) -> Series:
        """
        Count number of distinct elements.
        """
        data = self._execute(self._q.count_distinct(
            columns=self.columns.to_list(),
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where
        ))
        return data.iloc[0]

    # region general aggregates

    def _agg(self, agg_name: str) -> Series:
        """
        Return the aggregate of the values.

        :param agg_name: Name of the aggregate function.
        """
        data = self._execute(self._q.aggregate(
            agg_name=agg_name,
            columns=self.columns.to_list(),
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where
        ))
        return data.iloc[0]

    def geometric_mean(self) -> Series:
        """
        Return the geometric mean of the values.
        """
        return self._agg('geometric_mean')

    def max(self) -> Series:
        """
        Return the maximum of the values.
        """
        return self._agg('max')

    def mean(self) -> Series:
        """
        Return the mean of the values.
        """
        return self._agg('avg')

    def median(self) -> Series:
        """
        Return the median of the values.
        """
        return self._agg('median')

    def min(self) -> Series:
        """
        Return the minimum of the values.
        """
        return self._agg('min')

    def sum(self) -> Series:
        """
        Return the sum of the values.
        """
        return self._agg('sum')

    # endregion

    # region aggregate by group

    def _agg_by_group(
            self,
            agg_name: str,
            sum_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ):
        """
        Aggregate one or more columns over grouping of one or more other
        columns.

        Returns a Series if there is only one sum column, otherwise a DataFrame.

        :param agg_name: Name of aggregation function.
        :param sum_columns: Columns to sum.
        :param group_columns: Columns to group by.
        """
        data = self._execute(sql=self._q.aggregate_by_group(
            agg_name=agg_name,
            agg_columns=sum_columns,
            group_columns=group_columns,
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where
        ))
        return data.set_index(sum_columns)[sum_columns]

    def sum_by_group(
            self,
            sum_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ) -> Union[DataFrame, Series]:
        """
        Sum one or more columns over grouping of one or more other columns.

        Returns a Series if there is only one sum column, otherwise a DataFrame.

        :param sum_columns: Columns to sum.
        :param group_columns: Columns to group by.
        """
        return self._agg_by_group(
            agg_name='sum',
            sum_columns=sum_columns, group_columns=group_columns
        )

    # endregion

    def __getitem__(self, item: Union[str, List[str]]):
        """
        Select a column or subset of columns.

        :param item: Name(s) of the column or columns to select.
        """
        if isinstance(item, str):
            return AthenaSeries(
                database=self._database,
                table=self._table,
                column=item,
                sample=self._sample,
                where=self._where,
                column_info=self._column_info.loc[
                    self._column_info['column_name'] == item
                ].iloc[0],
            )
        else:
            return AthenaFrame(
                database=self._database,
                table=self._table,
                sample=self._sample,
                where=self._where,
                column_info=self._column_info.loc[
                    self._column_info['column_name'].isin(item)
                ]
            )
