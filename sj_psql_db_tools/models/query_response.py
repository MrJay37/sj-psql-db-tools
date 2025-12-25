class QueryResponse:
    def __init__(self, data: tuple, columns: list):
        self._data = data
        self._columns = columns

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
