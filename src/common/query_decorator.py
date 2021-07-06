from typing import Callable, Tuple, Any, Dict

from common.format_query import format_query

QueryFn = Callable[..., str]


def query(query_function: QueryFn) -> QueryFn:
    def wrapper(*args: Tuple[Any], **kwargs: Dict[str, Any]) -> str:
        original_sql = query_function(*args, **kwargs)

        formatted_sql = format_query(original_sql)

        return formatted_sql

    return wrapper
