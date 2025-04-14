from .operations import apply_operation

class SQLCompiler:
    def __init__(self, table_name, operations, conn):
        self.table_name = table_name
        self.operations = operations
        self.conn = conn

    # def compile(self):
    #     query = f"SELECT * FROM {self.table_name}"
    #     for op, val in self.operations:
    #         query = apply_operation(query, op, val)
    #     return query

    def compile(self):
        query = f"SELECT * FROM {self.table_name}"
        for op, val in self.operations:
            query = apply_operation(query, op, val, table_name=self.table_name, conn=self.conn)
        return query