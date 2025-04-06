from .operations import apply_operation

class SQLCompiler:
    def __init__(self, table_name, operations):
        self.table_name = table_name
        self.operations = operations

    def compile(self):
        query = f"SELECT * FROM {self.table_name}"
        for op, val in self.operations:
            query = apply_operation(query, op, val)
        return query