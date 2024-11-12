from psycopg2 import sql
from DatabaseConnection import DatabaseConnection


class RequiredParameterNotSetError(Exception):
    def __init__(self, parameter_name):
        self.msg = f"A required parameter is missing: {parameter_name}"
        super().__init__(self.msg)


class Model:
    """
    A Model represents on the program size, a table on the database side
    It serves as an interface and build queries and then execute them on the Database through DatabaseConnection
    """
    def __init__(self, db_connection: DatabaseConnection, model_definition: dict):
        """
        Initialize the Model with database connection, table name, primary keys, and columns.

        :param db_connection: Database connection object
        :param model_definition: Dictionary of model definition
        """

        self._db_connection: DatabaseConnection = db_connection
        self.name = model_definition.get("name")
        self.primary_keys = model_definition.get("primary_keys")
        self.columns = model_definition.get("columns")

        self.serial_primary_keys = [k for k, v in self.columns.items() if "SERIAL" in v]

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
        return f"{self.name} -> ({', '.join(f'{k}: {v} <PK>' if k in self.primary_keys else f'{k}: {v}' for k, v in self.columns.items())})"

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
        Insert a row into the table, optionally updating on conflict.
        For any value wanted to NULL, value should be None for specified columns.

        :param update: Update on conflict if True
        :param values: Values for each column
        :return: ID of the inserted row
        """
        columns_to_insert = [col for col in values if col in self.columns]
        values_to_insert = [values[col] for col in columns_to_insert]

        # serial primary keys shouldn't be specified in the values if no update is done, so we account for that
        if len(columns_to_insert)+len(self.serial_primary_keys) < len(self.columns):
            missing_cols = set(self.columns) - set(columns_to_insert)
            raise ValueError(f"Missing values for columns: {', '.join(missing_cols)}")

        columns = sql.SQL(', ').join(sql.Identifier(col) for col in columns_to_insert)
        placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in values_to_insert)

        if update:
            updates = sql.SQL(', ').join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                for col in columns_to_insert if col not in self.primary_keys
            )
            conflict_clause = sql.SQL("ON CONFLICT ({pk}) DO UPDATE SET {updates}").format(
                pk=sql.SQL(', ').join(sql.Identifier(pk) for pk in self.primary_keys),
                updates=updates
            )
        else:
            conflict_clause = sql.SQL("ON CONFLICT DO NOTHING")

        query = sql.SQL("""
            INSERT INTO {table} ({columns}) VALUES ({placeholders}) {conflict_clause} RETURNING id
        """).format(
            table=sql.Identifier(self.name),
            columns=columns,
            placeholders=placeholders,
            conflict_clause=conflict_clause
        )

        return self._db_connection.execute_return_id(query, values_to_insert)

    def deleteRow(self, row_values: list) -> None:
        """
        Delete a row based on primary key values.

        :param row_values: Values matching the primary keys, should have one value per primary key, to identify a row
        """
        if len(row_values) != len(self.primary_keys):
            raise ValueError("Provide values for all primary keys")

        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{pk} = %s").format(pk=sql.Identifier(pk)) for pk in self.primary_keys
        )
        query = sql.SQL("DELETE FROM {table} WHERE {where_clause}").format(
            table=sql.Identifier(self.name),
            where_clause=where_clause
        )
        self._db_connection.execute(query, row_values)

    def select(self, columns=None, where=None, order=None, limit=None) -> list:
        """
        Select rows with optional columns, conditions, ordering, and limit.

        :param columns: List of columns to select, default is all (*)
        :param where: Dictionary of conditions
        :param order: List of columns to order by
        :param limit: Maximum number of rows to retrieve
        :return: List of selected rows
        """
        columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns) if columns else sql.SQL('*')

        where_sql = sql.SQL('')
        if where:
            conditions = [sql.SQL("{col} = %s").format(col=sql.Identifier(col)) for col in where.keys()]
            where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)

        order_sql = sql.SQL('')
        if order:
            order_sql = sql.SQL("ORDER BY ") + sql.SQL(', ').join(sql.Identifier(col) for col in order)

        limit_sql = sql.SQL('')
        if limit is not None:
            limit_sql = sql.SQL("LIMIT %s")

        query = sql.SQL("SELECT {columns} FROM {table} {where} {order} {limit}").format(
            columns=columns_sql,
            table=sql.Identifier(self.name),
            where=where_sql,
            order=order_sql,
            limit=limit_sql
        )

        query_params = list(where.values()) if where else []
        if limit is not None:
            query_params.append(limit)

        return self._db_connection.execute(query, query_params, fetch=True)
