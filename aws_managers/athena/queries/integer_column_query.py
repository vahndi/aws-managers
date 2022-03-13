from aws_managers.athena.queries.column_query import ColumnQuery
from aws_managers.athena.functions.aggregate import AvgMixin, \
    GeometricMeanMixin, MaxMixin, MinMixin, SumMixin
from aws_managers.athena.operators.comparisons import ScalarComparison


class IntegerColumnQuery(
    AvgMixin,
    GeometricMeanMixin,
    MaxMixin,
    MinMixin,
    SumMixin,
    ColumnQuery
):

    def __eq__(self, other: int) -> ScalarComparison:
        return ScalarComparison(str(self), '=', other)

    def __ne__(self, other):
        return ScalarComparison(str(self), '<>', other)

    def __lt__(self, other: int) -> ScalarComparison:
        return ScalarComparison(str(self), '<', other)

    def __gt__(self, other: int) -> ScalarComparison:
        return ScalarComparison(str(self), '>', other)

    def __le__(self, other: int) -> ScalarComparison:
        return ScalarComparison(str(self), '<=', other)

    def __ge__(self, other: int) -> ScalarComparison:
        return ScalarComparison(str(self), '>=', other)
