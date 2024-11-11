import psycopg2


class DatabaseConnection:
    def __init__(self, db_name, username, password, host='localhost', port=5432):
        self.name = db_name
        self.username = username
        self._password = password
        self.host = host
        self.port = port
        self._conn = None

    def connect(self):
        if not self._conn:
            self._conn = psycopg2.connect(
                database=self.name,
                user=self.username,
                password=self._password,
                host=self.host,
                port=self.port
            )

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute(self, query, values=None, fetch=False):
        self.connect()
        with self._conn.cursor() as cur:
            cur.execute(query, values or ())
            if fetch:
                return cur.fetchall()
            self._conn.commit()

    def execute_return_id(self, query, values=None):
        """For insert query where we need the id"""
        self.connect()
        with self._conn.cursor() as cur:
            cur.execute(query, values or ())
            self._conn.commit()
            return cur.lastrowid
            #return cur.fetchone()[0]

    def get_connection(self):
        return self._conn
