from functools import reduce

import sqlparse


def format_query(sql: str) -> str:
    pipes = [
        sqlparse_format,
        strip,
        number_line,
    ]

    return reduce((lambda sql, pipe: pipe(sql)), pipes, sql)


def sqlparse_format(sql: str) -> str:
    return sqlparse.format(
        sql,
        reindent=True,
        indent_width=4,
        keyword_case='upper',
        indent_columns=True,
        use_space_around_operators=True,
    )


def strip(sql: str) -> str:
    return sql.strip()


def number_line(sql: str) -> str:
    lines = sql.split('\n')
    line_number_digits = len(str(len(lines)))
    return '\n'.join(
        f'/* {str(n).zfill(line_number_digits)} */  {line}'
        for n, line in enumerate(lines, start=1)
    )
