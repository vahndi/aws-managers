from aws_managers.athena.columns.column import Column
from aws_managers.athena.operators.comparisons import ScalarComparison


class IntColumn(Column):

    def __eq__(self, other: int) -> ScalarComparison:
        return ScalarComparison(self.name, '=', other)

    def __ne__(self, other):
        return ScalarComparison(self.name, '<>', other)

    def __lt__(self, other: int) -> ScalarComparison:
        return ScalarComparison(self.name, '<', other)

    def __gt__(self, other: int) -> ScalarComparison:
        return ScalarComparison(self.name, '>', other)

    def __le__(self, other: int) -> ScalarComparison:
        return ScalarComparison(self.name, '<=', other)

    def __ge__(self, other: int) -> ScalarComparison:
        return ScalarComparison(self.name, '>=', other)
