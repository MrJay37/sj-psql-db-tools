from pg8000 import Cursor


class QueryResponse:
    def __init__(self, cursor: Cursor):
        self._data = cursor.fetchall()
        self._columns = [desc[0] for desc in cursor.description]

    def as_dicts(self):
        return [dict(zip(self.columns, row)) for row in self._data]

    @property
    def data(self):
        return self._data

    @property
    def columns(self):
        return self._columns

    def __repr__(self):
        return f"QueryResponse(rows={len(self._data)}, columns={self._columns})"
