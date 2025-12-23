from dataclasses import fields

from pg8000 import connect, Connection
from sj_psql_db_tools.models import QueryResponse
from sj_psql_db_tools.query_generator import QueryGenerator


class PSQLDBConnector:
    _host = "localhost"
    _port = 5432
    _database = "postgres"
    _user = "postgres"

    _connection: Connection

    _q_gen = QueryGenerator()

    def __init__(self, **kwargs):
        self.host = kwargs.get("host", self._host)
        self.port = kwargs.get("port", self._port)
        self.database = kwargs.get("database", self._database)
        self.user = kwargs.get("user", self._user)
        self.password = kwargs.get("password")

        self._connection = connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

        self._cursor = self._connection.cursor()

    def __del__(self):
        try:
            if self._connection:
                self._connection.close()

        except AttributeError:
            ...

    def execute(self, query: str):
        self._cursor.execute(query)

        return QueryResponse(self._cursor)

    def getData(self, obj_name, **kwargs):
        return self.execute(
            self._q_gen.generate_select_query(
                obj_name,
                fields=kwargs.get("fields"),
                where_clause=kwargs.get("where_clause"),
                limit=kwargs.get("limit"),
                offset=kwargs.get("offset")
            )
        )
