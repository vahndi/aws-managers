"""
https://prestodb.io/docs/current/functions/aggregate.html
"""

"""
https://prestodb.io/docs/current/functions/aggregate.html#general-aggregate-functions
"""


class AvgMixin(object):

    name: str

    def avg(self) -> str:
        """
        Returns the average (arithmetic mean) of all input values.
        """
        return f'avg({self.name})'

    def mean(self) -> str:
        """
        Returns the average (arithmetic mean) of all input values.
        """
        return self.avg()


class CountMixin(object):

    name: str

    def count(self) -> str:
        """
        Returns the number of input rows.
        """
        return f'count({self.name})'


class GeometricMeanMixin(object):

    name: str

    def geometric_mean(self) -> str:
        return f'geometric_mean({self.name})'


class MaxMixin(object):

    name: str

    def max(self, n: int = 1) -> str:
        """
        Returns the maximum value of all input values, OR
        Returns n largest values of all input values of x.
        """
        if n == 1:
            return f'max({self.name})'
        else:
            return f'max({self.name}, {n})'


class MinMixin(object):

    name: str

    def min(self, n: int = 1) -> str:
        """
        Returns the minimum value of all input values, OR
        Returns n smallest values of all input values of x.
        """
        if n == 1:
            return f'min({self.name})'
        else:
            return f'min({self.name}, {n})'


class SumMixin(object):

    name: str

    def sum(self) -> str:
        """
        Returns the sum of all input values.
        """
        return f'sum({self.name})'
