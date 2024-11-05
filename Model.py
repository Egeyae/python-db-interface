from psycopg2 import sql
from exceptions import RequiredParameterNotSet
from DBConn import DatabaseConnection

class Model:
    def __init__(self, db_connection: DatabaseConnection, table_name: str, primary_keys: list[str], **columns: str):
        """
        Initialize the Model with database connection, table name, primary keys, and columns.

        :param db_connection: Database connection object
        :param table_name: Name of the table in the database
        :param primary_keys: List of primary key column names
        :param columns: Column names and types as key-value pairs
        """
        self._db_connection: DatabaseConnection = db_connection
        self._table_name = table_name
        self._primary_keys = primary_keys
        self._columns = columns

        if not table_name:
            raise RequiredParameterNotSet('table_name')
        if not primary_keys or not isinstance(primary_keys, list):
            raise RequiredParameterNotSet('primary_keys. Must provide a list of primary keys')
        if not columns:
            raise RequiredParameterNotSet('columns. A table should have at least one column')

        for pk in primary_keys:
            if pk not in columns:
                raise ValueError(f"Primary key '{pk}' must be defined in columns")

    def __repr__(self) -> str:
        """Return string representation of the model."""
        return f'{self._table_name}: {self._columns}'

    def drop_table(self) -> None:
        """Drop the table if it exists in the database."""
        query = sql.SQL("DROP TABLE IF EXISTS {table};").format(
            table=sql.Identifier(self._table_name)
        )
        self._db_connection.execute(query)

    def create_table(self) -> None:
        """Create the table with the specified columns and primary keys."""
        cols_def = [
            sql.SQL("{col} {type}").format(
                col=sql.Identifier(col), type=sql.SQL(col_type)
            ) for col, col_type in self._columns.items()
        ]
        primary_keys_def = sql.SQL(', ').join(sql.Identifier(pk) for pk in self._primary_keys)

        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns},
                PRIMARY KEY ({primary_keys})
            );
        """).format(
            table=sql.Identifier(self._table_name),
            columns=sql.SQL(', ').join(cols_def),
            primary_keys=primary_keys_def
        )
        self._db_connection.execute(query)

    def insert_row(self, update: bool = False, **values) -> int:
        """
        Insert a row into the table. Optionally, update existing rows if there is a conflict.

        :param update: Whether to update on conflict
        :param values: Column values to insert
        :return: The ID of the inserted row
        """
        columns_to_insert = []
        values_to_insert = []
        for col, col_type in self._columns.items():
            if col in values and values[col] is not None:
                columns_to_insert.append(sql.Identifier(col))
                values_to_insert.append(values[col])
            elif col in self._primary_keys and "SERIAL" in col_type and not update:
                continue
            elif col not in values:
                raise ValueError(f"Missing value for column '{col}'")

        columns = sql.SQL(', ').join(columns_to_insert)
        placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in values_to_insert)

        if update:
            updates = sql.SQL(', ').join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=col)
                for col in columns_to_insert if col.string not in self._primary_keys
            )
            conflict_clause = sql.SQL("ON CONFLICT ({pks}) DO UPDATE SET {updates}").format(
                pks=sql.SQL(', ').join(sql.Identifier(pk) for pk in self._primary_keys),
                updates=updates
            )
        else:
            conflict_clause = sql.SQL("ON CONFLICT DO NOTHING")

        query = sql.SQL("""
            INSERT INTO {table} ({columns}) VALUES ({values}) {conflict_clause} RETURNING id;
        """).format(
            table=sql.Identifier(self._table_name),
            columns=columns,
            values=placeholders,
            conflict_clause=conflict_clause
        )

        return self._db_connection.execute_return_id(query, values_to_insert)

    def delete_row(self, row_values: list) -> None:
        """
        Delete a row based on primary key values.

        :param row_values: Values corresponding to the primary keys
        """
        if len(row_values) != len(self._primary_keys):
            raise ValueError(f"Provide values for the primary keys: {self._primary_keys}")

        where_clause = sql.SQL(" AND ").join(
            sql.SQL("{pk} = %s").format(pk=sql.Identifier(pk)) for pk in self._primary_keys
        )
        query = sql.SQL("DELETE FROM {table} WHERE {where_clause};").format(
            table=sql.Identifier(self._table_name),
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
            table=sql.Identifier(self._table_name),
            where=where_sql,
            order=order_sql,
            limit=limit_sql
        )

        # Parameters for placeholders:
        query_placeholders = list(where.values()) if where else []
        if limit:
            query_placeholders.append(limit)

        return self._db_connection.execute(query, query_placeholders, fetch=True)
