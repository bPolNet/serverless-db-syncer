from src.sql_file_parser import SqlFileParser


class SqlFileExecutor:
    def __init__(self, conn) -> None:
        self.__conn = conn

    def execute_sql_file(self, path_to_sql_file):
        print('starting executing script ' + path_to_sql_file)
        stmts = SqlFileParser.parse_sql(path_to_sql_file)

        sql_count = 0
        with self.__conn.cursor() as cursor:
            for stmt in stmts:
                cursor.execute(stmt)
                sql_count = sql_count + 1
            self.__conn.commit()

        return sql_count;
