from sj_psql_db_tools.models import DBObject, Field


class QueryGenerator:
    @staticmethod
    def generate_select_query(
            db_obj: DBObject,
            fields: list[Field] | None = None,
            where_clause: str | None = None,
            limit: int | None = None,
            offset: int | None = None
    ) -> str:
        fields_str = ", ".join([f'"{field.name}"' for field in (fields or db_obj.fields)])
        query = f'SELECT {fields_str} FROM {db_obj.get_full_name()}'

        if where_clause:
            query += f' WHERE {where_clause}'

        if limit is not None:
            query += f' LIMIT {limit}'

        if offset is not None:
            query += f' OFFSET {offset}'

        return query
