from typing import Iterable, Optional, Any


class CaseSelector(object):

    @staticmethod
    def between(
            column: str,
            lower: Iterable,
            upper: Iterable,
            then: Iterable,
            else_: Optional[Any] = None,
            as_: Optional[Any] = None
    ) -> str:

        str_out = 'CASE\n'
        for l, u, t in zip(lower, upper, then):
            str_t = t if not isinstance(t, str) else f"'{t}'"
            str_out += f'    WHEN {column} BETWEEN {l} AND {u} THEN {str_t}\n'
        if else_ is not None:
            str_out += f'    ELSE {else_}'
        str_out += 'END'
        if as_ is not None:
            str_out += f' AS {as_}'
        str_out += '\n'

        return str_out
