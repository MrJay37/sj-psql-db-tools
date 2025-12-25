from pg8000 import connect, Connection, ProgrammingError
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

        self._autocommit = kwargs.get("autocommit", True)

        self._connection = connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def __del__(self):
        try:
            if self._connection:
                self._connection.close()

        except AttributeError:
            ...

    def execute(self, query: str):
        c = self._connection.cursor()

        try:
            c.execute(query)

        except ProgrammingError:
            c.execute("rollback;")  # Close current transaction before raising
            raise

        try:
            data = c.fetchall()

        except ProgrammingError:
            data = ()

        res =  QueryResponse(
            data=data,
            columns=[] if c.description is None else [desc[0] for desc in c.description]
        )

        if self._autocommit:
            c.execute(f"commit;")

        del c

        return res

    def getData(self, obj_name, **kwargs) -> QueryResponse:
        return self.execute(
            self._q_gen.generate_select_query(
                obj_name,
                fields=kwargs.get("fields"),
                where=kwargs.get("where_clause"),
                limit=kwargs.get("limit"),
                offset=kwargs.get("offset")
            )
        )

    def insertData(self, obj_name, data: list[dict], returning: bool | list | str = False) -> QueryResponse:
        return self.execute(
            self._q_gen.generate_insert_query(
                obj_name,
                data,
                returning
            )
        )

    def updateData(self, obj_name, update: dict, where: dict, returning: bool | list | str = False) -> QueryResponse:
        return self.execute(
            self._q_gen.generate_update_query(
                obj_name,
                update,
                where,
                returning
            )
        )