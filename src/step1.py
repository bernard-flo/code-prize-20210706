from common.query_executor import execute_query
from common.query_expressions import expr_now_between_times
from common.sql_context import SqlContext


def validation_query_1(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_1
        WHERE {expr_now_between_times('start_dtime', 'end_dtime')}
    """


def validation_query_2(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_2
    """


def validation_query_3(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_3
    """


def validation_query_4(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_4
    """


def validation_query_5(c: SqlContext) -> str:
    return f"""
        SELECT count(*)
        FROM {c.db("settlement")}.must_empty_5
    """


def main():
    c = SqlContext()

    error_exist = False

    query_1 = validation_query_1(c)
    cnt = execute_query(query_1)
    if cnt != 0:
        print('### query_1 failed.')
        error_exist = True

    query_2 = validation_query_2(c)
    cnt = execute_query(query_2)
    if cnt != 0:
        print('### query_2 failed.')
        error_exist = True

    query_3 = validation_query_3(c)
    cnt = execute_query(query_3)
    if cnt != 0:
        print('### query_3 failed.')
        error_exist = True

    query_4 = validation_query_4(c)
    cnt = execute_query(query_4)
    if cnt != 0:
        print('### query_4 failed.')
        error_exist = True

    query_5 = validation_query_5(c)
    cnt = execute_query(query_5)
    if cnt != 0:
        print('### query_5 failed.')
        error_exist = True

    if error_exist:
        print("### Validation is finished with error.")


if __name__ == '__main__':
    main()
