"""
https://prestodb.io/docs/current/functions/window.html
"""
from typing import Optional, List, Union

"""
https://prestodb.io/docs/current/functions/window.html#value-functions
"""


class LagMixin(object):

    name: str

    def lag(
            self,
            offset: int = 1,
            partition_by: Optional[Union[str, List[str]]] = None,
            order_by: Optional[Union[str, List[str]]] = None,
            as_: Optional[str] = None
    ) -> str:
        """
        Returns the value at the specified offset from beginning the window.
        Offsets start at 1. The offset can be any scalar expression.
        If the offset is null or greater than the number of values in the
        window, null is returned.
        It is an error for the offset to be zero or negative.
        """
        str_out = f'lag({self.name}, {offset})'
        if partition_by is not None or order_by is not None:
            str_out += ' over ('
        if partition_by is not None:
            if not isinstance(partition_by, list):
                partition_by = [partition_by]
            str_out += 'partition by ' + ', '.join(map(str, partition_by))
        if partition_by is not None and order_by is not None:
            str_out += ' '
        if order_by is not None:
            if not isinstance(order_by, list):
                order_by = [order_by]
            str_out += 'order by ' + ', '.join(map(str, order_by))
        if partition_by is not None or order_by is not None:
            str_out += ')'
        if as_ is None:
            as_ = f'{self.name}__lag_{offset}'
        str_out += f' as {as_}'
        return str_out
