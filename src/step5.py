from typing import Callable

from common.query_decorator import query
from common.query_executor import execute_query
from common.query_expressions import expr_now_between_times
from common.sql_context import SqlContext


class ExecutionContext(object):
    def __init__(self, c: SqlContext):
        self.c = c
        self.error_exist = False


execution_functions = []


def add_query_for_execution(query_function: Callable, error_message: str):
    def execution_function(ctx: ExecutionContext):
        query_str = query_function(ctx.c)
        cnt = execute_query(query_str)
        if cnt != 0:
            print(error_message)
            ctx.error_exist = True

    execution_functions.append(execution_function)


@query
def validation_query_1(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_1
        WHERE {expr_now_between_times('start_dtime', 'end_dtime')}
    """


add_query_for_execution(validation_query_1, '### query_1 failed.')


@query
def validation_query_2(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_2
    """


add_query_for_execution(validation_query_2, '### query_2 failed.')


@query
def validation_query_3(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_3
    """


add_query_for_execution(validation_query_3, '### query_3 failed.')


@query
def validation_query_4(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_4
    """


add_query_for_execution(validation_query_4, '### query_4 failed.')


@query
def validation_query_5(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_5
    """


add_query_for_execution(validation_query_5, '### query_5 failed.')


def main():
    c = SqlContext()

    ctx = ExecutionContext(c)

    for execution_function in execution_functions:
        execution_function(ctx)

    if ctx.error_exist:
        print("### Validation is finished with error.")


if __name__ == '__main__':
    main()
