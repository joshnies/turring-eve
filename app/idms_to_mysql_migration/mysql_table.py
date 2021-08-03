from typing import List

from app.idms_to_mysql_migration.mysql_column import MySQLColumn


class MySQLTable:
    """MySQL table representation."""

    def __init__(self, name: str, columns: List[MySQLColumn] = None):
        self.name = name
        self.columns: List[MySQLColumn] = list() if columns is None else columns.copy()

    def add_column(self, column: MySQLColumn):
        self.columns.append(column)

    def get_column_names(self) -> List[str]:
        """
        Get all column names for this table.

        :return: List of column names.
        """

        return list(map(lambda c: c.name, self.columns))

    def parse_idms_row(self, row: str) -> str:
        """
        Parse a single IDMS data row to MySQL values, intended for MySQL "INSERT" statements.

        :param row: IDMS data row.
        :return: MySQL values.
        """

        vals = list()
        remaining_row = row

        for col in self.columns:
            val = remaining_row[:col.length]

            if col.var_type in ['NUMERIC', 'BIGINT', 'INT']:
                # Migrate empty values for numeric types to "NULL"
                if len(val.strip()) == 0:
                    val = 'NULL'

                # Remove leading zeroes
                val = val.lstrip('0')
                if len(val) == 0:
                    val = '0'

            if col.var_type == 'DECIMAL':
                # Add decimal point to value
                left = val[:col.length_1].lstrip("0")

                if left == '':
                    left = '0'

                right = val[col.length_1:].rstrip("0")

                if right == '':
                    right = '0'

                val = f'{left}.{right}'

            if col.var_type == 'CHAR':
                # If "CHAR" type, simplify value as empty string
                if len(val.strip()) == 0:
                    val = ''

                # Escape single quotes
                val = val.replace("'", r"\'")

            quote = "'" if col.var_type == 'CHAR' else ''
            vals.append(f'{quote}{val}{quote}')

            remaining_row = remaining_row[col.length:]

        joined_vals = ', '.join(vals)
        return f'({joined_vals})'

    def has_column(self, name: str) -> bool:
        """
        :param name: Column name.
        :return: Whether this table has a column of the given name.
        """

        return name in self.get_column_names()
