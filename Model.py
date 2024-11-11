from psycopg2 import sql
from exceptions import RequiredParameterNotSetError
from DatabaseConnection import DatabaseConnection


class Model:
    def __init__(self, db_connection: DatabaseConnection, model_definition: dict):
        """
        Initialize the Model with database connection, table name, primary keys, and columns.

        :param db_connection: Database connection object
        :param model_definition: Dictionary of model definition
        """

        self._db_connection: DatabaseConnection = db_connection
        self.name = None
        self.primary_keys = None
        self.columns = None

        self.__dict__.update(model_definition)

        if not self.name:
            raise RequiredParameterNotSetError('table_name')
        if not self.primary_keys or not isinstance(self.primary_keys, list):
            raise RequiredParameterNotSetError('primary_keys. Must provide a list of at least one primary key')
        if not self.columns:
            raise RequiredParameterNotSetError('columns. A Model() [table] should have at least one column')
        for pk in self.primary_keys:
            if pk not in self.columns:
                raise ValueError(f"Primary key '{pk}' must be defined in columns")

    def __repr__(self) -> str:
        return f'{self.name} -> ({", ".join([f"{x[0]}: {x[1]} <{True if x[0] in self.primary_keys else False}>" for x in self.columns.items()])})'

    def dropTable(self) -> None:
        query = sql.SQL("DROP TABLE IF EXISTS {table};").format(
            table=sql.Identifier(self.name)
        )
        self._db_connection.execute(query)

    def createTable(self) -> None:
        """Create the table with the specified columns and primary keys."""
        cols_def = [
            sql.SQL("{col} {type}").format(
                col=sql.Identifier(col), type=sql.SQL(col_type)
            ) for col, col_type in self.columns.items()
        ]
        primary_keys_def = sql.SQL(', ').join(sql.Identifier(pk) for pk in self.primary_keys)

        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns},
                PRIMARY KEY ({primary_keys})
            );
        """).format(
            table=sql.Identifier(self.name),
            columns=sql.SQL(', ').join(cols_def),
            primary_keys=primary_keys_def
        )
        self._db_connection.execute(query)

    def insertRow(self, update: bool = False, **values) -> int:
        """
        Insert a row into the table. Optionally, update existing rows if there is a conflict.

        :param update: Whether to update on conflict
        :param values: Column values to insert
        :return: The ID of the inserted row
        """
        columns_to_insert = []
        values_to_insert = []
        for col, col_type in self.columns.items():
            if col in values and values[col] is not None:
                columns_to_insert.append(sql.Identifier(col))
                values_to_insert.append(values[col])
            elif col in self.primary_keys and "SERIAL" in col_type and not update:
                continue
            elif col not in values:
                raise ValueError(f"Missing value for column '{col}'")

        columns = sql.SQL(', ').join(columns_to_insert)
        placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in values_to_insert)

        if update:
            updates = sql.SQL(', ').join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=col)
                for col in columns_to_insert if col.string not in self.primary_keys
            )
            conflict_clause = sql.SQL("ON CONFLICT ({pks}) DO UPDATE SET {updates}").format(
                pks=sql.SQL(', ').join(sql.Identifier(pk) for pk in self.primary_keys),
                updates=updates
            )
        else:
            conflict_clause = sql.SQL("ON CONFLICT DO NOTHING")

        query = sql.SQL("""
            INSERT INTO {table} ({columns}) VALUES ({values}) {conflict_clause} RETURNING id;
        """).format(
            table=sql.Identifier(self.name),
            columns=columns,
            values=placeholders,
            conflict_clause=conflict_clause
        )

        return self._db_connection.execute_return_id(query, values_to_insert)

    def deleteRow(self, row_values: list) -> None:
        """
        Delete a row based on primary key values.

        :param row_values: Values corresponding to the primary keys
        """
        if len(row_values) != len(self.primary_keys):
            raise ValueError(f"Provide values for the primary keys: {self.primary_keys}")

        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{pk} = %s").format(pk=sql.Identifier(pk)) for pk in self.primary_keys
        )
        query = sql.SQL("DELETE FROM {table} WHERE {where_clause};").format(
            table=sql.Identifier(self.name),
            where_clause=where_clause
        )
        self._db_connection.execute(query, row_values)

    def select(self, columns: list[str] = None, where: dict = None, order: list[str] = None, limit: int = None) -> list:
        """
        Select rows from the table based on specified columns, conditions, order, and limit.

        :param columns: Columns to select
        :param where: Conditions for selection
        :param order: Columns to order by
        :param limit: Number of rows to retrieve
        :return: List of selected rows
        """
        if columns:
            columns_sql = sql.SQL(', ').join([sql.Identifier(col) for col in columns])
        else:
            columns_sql = sql.SQL('*')

        where_sql = sql.SQL('')
        if where:
            conditions = [
                sql.SQL('{col} = %s').format(col=sql.Identifier(col)) for col in where.keys()
            ]
            where_sql = sql.SQL("WHERE") + sql.SQL(" AND ").join(conditions)

        order_sql = sql.SQL('')
        if order:
            order_sql = sql.SQL('ORDER BY') + sql.SQL(', ').join(sql.Identifier(col) for col in order)

        limit_sql = sql.SQL('')
        if limit:
            limit_sql = sql.SQL("LIMIT %s")

        query = sql.SQL("SELECT {columns} FROM {table} {where} {order} {limit};").format(
            columns=columns_sql,
            table=sql.Identifier(self.name),
            where=where_sql,
            order=order_sql,
            limit=limit_sql
        )

        # Parameters for placeholders:
        query_placeholders = list(where.values()) if where else []
        if limit:
            query_placeholders.append(limit)

        return self._db_connection.execute(query, query_placeholders, fetch=True)
