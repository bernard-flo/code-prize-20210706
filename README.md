# code-prize-20210706

* 단순 반복되는 코드를 function 단위로 추상화(python의 decorator 활용)하여 코드의 readability를 향상시킴
```python
@counting_validation('MetaValidationFailure', 'flo_mcp: tnmm_meta_free\nsame meta_type, meta_id detected: {cnt}')
@query
def query_meta_free_duplication(c: SQLContext) -> str:
    return f"""
        SELECT count(*) AS cnt
        FROM (
            SELECT meta_type, meta_id, count(*) AS cnt
            FROM {c.db("flo_mcp")}.tnmm_meta_free
            WHERE cast(svc_start_dt AS date) <= timestamp %(start_dtime)s 
                AND timestamp %(end_dtime_inclusive)s <= svc_end_dt
            GROUP BY meta_type, meta_id
            HAVING count(*) > 1
        )
    """
```

* `counting_validation()` decorator가 붙은 함수가 반환하는 쿼리를 수행하고 결과에 따라 slack에 error 메세지 전송
* `query` decorator를 함수에 붙여서 함수가 반환하는 쿼리를 손쉽게 formatting 할 수 있는 기능을 제공
