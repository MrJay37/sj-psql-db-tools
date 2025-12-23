from sj_psql_db_tools.models.field import Field


class DBObject:
    def __init__(
        self,
        schema_name: str,
        obj_name: str,
        fields: list[Field] | None = None
    ):
        self.schema_name = schema_name
        self.obj_name = obj_name
        self.fields = fields

    def get_full_name(self) -> str:
        return f'"{self.schema_name}"."{self.obj_name}"'
