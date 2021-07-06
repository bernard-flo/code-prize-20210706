def expr_now_between_times(begin_column: str, end_column: str):
    return f"""
        {begin_column} <= now()
        AND now() <= {end_column}
    """
