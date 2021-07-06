class SqlContext(object):
    def db(self, db_name: str) -> str:
        return f'{db_name}_dev'
