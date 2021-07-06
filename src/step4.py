from common.query_decorator import query
from common.query_executor import execute_query
from common.query_expressions import expr_now_between_times
from common.sql_context import SqlContext


class ExecutionContext(object):
    def __init__(self, c: SqlContext):
        self.c = c
        self.error_exist = False


@query
def validation_query_1(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_1
        WHERE {expr_now_between_times('start_dtime', 'end_dtime')}
    """


def validate_query_1(ctx: ExecutionContext) -> None:
    query_1 = validation_query_1(ctx.c)
    cnt = execute_query(query_1)
    if cnt != 0:
        print('### query_1 failed.')
        ctx.error_exist = True


@query
def validation_query_2(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_2
    """


def validate_query_2(ctx: ExecutionContext) -> None:
    query_2 = validation_query_2(ctx.c)
    cnt = execute_query(query_2)
    if cnt != 0:
        print('### query_2 failed.')
        ctx.error_exist = True


@query
def validation_query_3(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_3
    """


def validate_query_3(ctx: ExecutionContext) -> None:
    query_3 = validation_query_3(ctx.c)
    cnt = execute_query(query_3)
    if cnt != 0:
        print('### query_3 failed.')
        ctx.error_exist = True


@query
def validation_query_4(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_4
    """


def validate_query_4(ctx: ExecutionContext) -> None:
    query_4 = validation_query_4(ctx.c)
    cnt = execute_query(query_4)
    if cnt != 0:
        print('### query_4 failed.')
        ctx.error_exist = True


@query
def validation_query_5(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_5
    """


def validate_query_5(ctx: ExecutionContext) -> None:
    query_5 = validation_query_5(ctx.c)
    cnt = execute_query(query_5)
    if cnt != 0:
        print('### query_5 failed.')
        ctx.error_exist = True


def main():
    c = SqlContext()

    ctx = ExecutionContext(c=c)

    validate_query_1(ctx)
    validate_query_2(ctx)
    validate_query_3(ctx)
    validate_query_4(ctx)
    validate_query_5(ctx)

    if ctx.error_exist:
        print("### Validation is finished with error.")


if __name__ == '__main__':
    main()
