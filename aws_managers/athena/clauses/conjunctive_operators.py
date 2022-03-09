from typing import List, Union

from aws_managers.athena.operators.mixins import ComparisonMixin


class ConjunctiveOperator(object):

    name: str

    def __init__(
            self,
            items: List[Union[ComparisonMixin, 'ConjunctiveOperator']]
    ):

        self.items: List[Union[ComparisonMixin, ConjunctiveOperator]] = items

    @staticmethod
    def indent(string: str):

        lines = string.split('\n')
        lines = [
            f'    {line}' if line != '(' else line
            for line in lines
        ]
        return '\n'.join(lines)

    @staticmethod
    def indent_first(string: str):

        return '\n'.join([
            f'    {line}'
            for line in string.split('\n')
        ])

    def __str__(self):

        str_out = '(\n'
        num_items = len(self.items)
        for i, item in enumerate(self.items):
            if i == 0 and isinstance(item, ConjunctiveOperator):
                line = self.indent_first(str(item))
            else:
                line = self.indent(str(item))
            # line = self.indent(str(item))
            if i < num_items - 1:
                line += f' {self.name}'
                if isinstance(self.items[i + 1], ConjunctiveOperator):
                    line += ' '
                else:
                    line += '\n'
            # line += '\n'
            str_out += line
        str_out += '\n)'

        return str_out


class And(ConjunctiveOperator):

    name: str = 'AND'


class Or(ConjunctiveOperator):

    name: str = 'OR'

