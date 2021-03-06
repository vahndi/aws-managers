from typing import Optional, Union, List, Tuple, Dict, Any

from awswrangler.athena import read_sql_query
from pandas import DataFrame, Index, Series

from aws_managers.athena.queries import ColumnQuery
from aws_managers.athena.queries.athena_column_query_set import \
    AthenaColumnQuerySet
from aws_managers.athena.reference.athena_data_types import \
    ATHENA_BOOLEAN_TYPES, ATHENA_CHARACTER_TYPES, ATHENA_DATETIME_TYPES, \
    ATHENA_INTEGER_TYPES, ATHENA_NUMERIC_TYPES, ATHENA_REAL_TYPES
from aws_managers.athena.queries.athena_query_generator import \
    AthenaQueryGenerator
from aws_managers.athena.athena_series import AthenaSeries
from aws_managers.athena.clauses.conjunctive_operators import \
    ConjunctiveOperator, And
from aws_managers.athena.operators.mixins import ComparisonMixin


class AthenaFrame(object):

    def __init__(
            self,
            database: str,
            table: str,
            sample: Optional[Tuple[str, int]] = None,
            where: Optional[Union[ComparisonMixin, ConjunctiveOperator]] = None,
            limit: Optional[int] = None,
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
        self._where: Optional[Union[
            ComparisonMixin, ConjunctiveOperator
        ]] = where
        self._limit: Optional[int] = limit

    # region metadata

    @property
    def database(self) -> str:
        return self._database

    @property
    def table(self) -> str:
        return self._table

    @property
    def meta(self) -> Dict[str, Any]:
        """
        Return a dict containing info on the table and any query clauses.
        """
        return dict(
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where,
            limit=self._limit,
        )

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

    # endregion

    # region query execution

    def _execute(self, sql: str) -> DataFrame:
        """
        Execute a query.

        :param sql: Raw SQL to execute.
        """
        data = read_sql_query(sql=sql, database=self._database)
        return data

    @property
    def _execution_kwargs(self) -> dict:
        return dict(
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where,
            limit=self._limit
        )

    # endregion

    def select(
            self,
            columns: Union[str, ColumnQuery, List[str], List[ColumnQuery]]
    ):
        """
        Do a basic selection using columns or column queries.
        """
        data = self._execute(sql=self._q.select(
            columns=columns,
            **self._execution_kwargs
        ))
        return data

    # region sampling

    def sample(self, n: int) -> DataFrame:
        """
        Sample n rows from the table.

        :param n: Number of rows to sample.
        """
        data = self._execute(sql=self._q.select(
            columns='*',
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where,
            limit=n
        ))
        return data

    def bernoulli_sample(self, percentage: int) -> 'AthenaFrame':
        """
        Do sampling from the Frame using the Bernoulli method.
        """
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=('BERNOULLI', percentage),
            where=self._where,
            limit=self._limit,
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
            limit=self._limit,
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
        Return a subset of the Frame???s columns based on the column data-types.

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
            limit=self._limit,
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
            **self._execution_kwargs
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
            **self._execution_kwargs
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

    def _agg_by_group(
            self,
            agg_name: str,
            agg_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ):
        """
        Aggregate one or more columns over grouping of one or more other
        columns.

        Returns a Series if there is only one sum column, otherwise a DataFrame.

        :param agg_name: Name of aggregation function.
        :param agg_columns: Columns to sum.
        :param group_columns: Columns to group by.
        """
        data = self._execute(sql=self._q.aggregate_by_group(
            agg_name=agg_name,
            agg_columns=agg_columns,
            group_columns=group_columns,
            **self._execution_kwargs
        ))
        return data.set_index(group_columns)[agg_columns]

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
            agg_columns=sum_columns,
            group_columns=group_columns
        )

    def min_by_group(
            self,
            min_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ) -> Union[DataFrame, Series]:
        """
        Take min of one or more columns over grouping of one or more other
        columns.

        Returns a Series if there is only one min column, otherwise a DataFrame.

        :param min_columns: Columns to take min of.
        :param group_columns: Columns to group by.
        """
        return self._agg_by_group(
            agg_name='min',
            agg_columns=min_columns,
            group_columns=group_columns
        )

    def max_by_group(
            self,
            max_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ) -> Union[DataFrame, Series]:
        """
        Take max of one or more columns over grouping of one or more other
        columns.

        Returns a Series if there is only one max column, otherwise a DataFrame.

        :param max_columns: Columns to take max of.
        :param group_columns: Columns to group by.
        """
        return self._agg_by_group(
            agg_name='max',
            agg_columns=max_columns,
            group_columns=group_columns
        )

    def mean_by_group(
            self,
            mean_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]]
    ) -> Union[DataFrame, Series]:
        """
        Take mean of one or more columns over grouping of one or more other
        columns.

        Returns a Series if there is only one mean column, otherwise a DataFrame.

        :param mean_columns: Columns to take mean of.
        :param group_columns: Columns to group by.
        """
        return self._agg_by_group(
            agg_name='avg',
            agg_columns=mean_columns,
            group_columns=group_columns
        )

    # endregion

    # region approximate aggregate functions

    def approx_percentile(
            self,
            columns: Union[str, List[str]],
            percentile: float
    ):
        """
        Returns the approximate percentile for all input values of the column at
        the given percentage. The value of percentage must be between zero and
        one and must be constant for all input rows.

        :param columns: Columns to find percentile of.
        :param percentile: Percentile value to find.
        """
        data = self._execute(sql=self._q.approx_percentile(
            columns=columns,
            percentile=percentile,
            **self._execution_kwargs
        ))
        return data

    def approx_percentile_by_group(
            self,
            percentile_columns: Union[str, List[str]],
            group_columns: Union[str, List[str]],
            percentile: float
    ):
        """
        Returns the approximate percentile for all input values of the column at
        the given percentage. The value of percentage must be between zero and
        one and must be constant for all input rows.

        :param percentile_columns: Columns to find percentile of.
        :param group_columns: Columns to find percentile of.
        :param percentile: Percentile value to find.
        """
        data = self._execute(sql=self._q.approx_percentile_by_group(
            percentile_columns=percentile_columns,
            group_columns=group_columns,
            percentile=percentile,
            **self._execution_kwargs
        ))
        return data.set_index(group_columns)[percentile_columns]

    # endregion

    def where(
            self,
            conditions: Union[ComparisonMixin, ConjunctiveOperator]
    ) -> 'AthenaFrame':
        """
        Return a new AthenaFrame matching the given condition(s).

        :param conditions: Column comparison or conjunction of column
        comparisons.
        """
        if self._where is not None:
            where = And([self._where, conditions])
        else:
            where = conditions
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=where,
            limit=self._limit,
            column_info=self._column_info
        )

    def limit(self, n: int) -> 'AthenaFrame':
        """
        Return a new AthenaFrame with a limit clause.

        :param n: number of rows to limit output to.
        """
        return AthenaFrame(
            database=self._database,
            table=self._table,
            sample=self._sample,
            where=self._where,
            limit=n,
            column_info=self._column_info
        )

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
                limit=self._limit,
                column_info=self._column_info.loc[
                    self._column_info['column_name'].isin(item)
                ]
            )
