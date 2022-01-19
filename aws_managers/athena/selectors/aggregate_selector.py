from numpy import arange


class AggregateSelector(object):

    @staticmethod
    def deciles_approx(
            column: str,
            min_decile: float = 0.0,
            max_decile: float = 1.0,
            as_name: str = None
    ) -> str:
        if as_name is None:
            as_name = f'{column}__deciles'
        str_deciles = ', '.join([
            str(round(val, 3))
            for val in arange(min_decile, max_decile + 0.1, 0.1)
        ])
        return f"approx_percentile({column}, ARRAY[{str_deciles}]) as {as_name}"

    @staticmethod
    def percentiles_approx(
            column: str,
            min_percentile: float = 0.0,
            max_percentile: float = 1.0,
            as_name: str = None
    ) -> str:
        if as_name is None:
            as_name = f'{column}__percentiles'
        str_pcts = ', '.join([
            str(round(val, 3))
            for val in arange(min_percentile, max_percentile + 0.01, 0.01)
        ])
        return f"approx_percentile({column}, ARRAY[{str_pcts}]) as {as_name}"
