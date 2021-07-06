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


def counting_validation(error_message: str) -> Callable:
    def decorate(query_function: Callable) -> Callable:
        add_query_for_execution(query_function, error_message)
        return query_function

    return decorate


@counting_validation('### query_1 failed.')
@query
def validation_query_1(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_1
        WHERE {expr_now_between_times('start_dtime', 'end_dtime')}
    """


@counting_validation('### query_2 failed.')
@query
def validation_query_2(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_2
    """


@counting_validation('### query_3 failed.')
@query
def validation_query_3(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_3
    """


@counting_validation('### query_4 failed.')
@query
def validation_query_4(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_4
    """


@counting_validation('### query_5 failed.')
@query
def validation_query_5(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_5
    """


def main():
    c = SqlContext()

    ctx = ExecutionContext(c)

    for execution_function in execution_functions:
        execution_function(ctx)

    if ctx.error_exist:
        print("### Validation is finished with error.")


if __name__ == '__main__':
    main()
