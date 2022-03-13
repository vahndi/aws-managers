from aws_managers.athena.queries.column_query import ColumnQuery
from aws_managers.athena.operators.comparisons import StringComparison


class StringColumnQuery(
    ColumnQuery
):

    def __eq__(self, other: int) -> StringComparison:
        return StringComparison(self.name, '=', other)

    def __ne__(self, other):
        return StringComparison(self.name, '<>', other)

    def __lt__(self, other: int) -> StringComparison:
        return StringComparison(self.name, '<', other)

    def __gt__(self, other: int) -> StringComparison:
        return StringComparison(self.name, '>', other)

    def __le__(self, other: int) -> StringComparison:
        return StringComparison(self.name, '<=', other)

    def __ge__(self, other: int) -> StringComparison:
        return StringComparison(self.name, '>=', other)
