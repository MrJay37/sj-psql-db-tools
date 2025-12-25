import json
from sj_psql_db_tools.models import DBObject, Field, PSQLKeyword


class QueryGenerator:
    @staticmethod
    def format_value(value) -> str:
        if value is None:
            return 'NULL'

        elif isinstance(value, PSQLKeyword):
            return str(value)

        elif isinstance(value, list) or isinstance(value, dict):
            return f"'{json.dumps(value).replace("'", "''")}'"

        else:
            return f"'{str(value).replace("'", "''")}'"

    def generate_where_clause(self, where: dict) -> str:
        clauses = []

        for key, value in where.items():
            if value is None:
                clauses.append(f'"{key}" IS NULL')

            else:
                clauses.append(f'"{key}" = {self.format_value(value)}')

        return " AND ".join(clauses)

    def generate_select_query(
        self,
        db_obj: DBObject,
        fields: list[Field] | None = None,
        where: dict | None = None,
        limit: int | None = None,
        offset: int | None = None
    ) -> str:
        fields_str = ", ".join([f'"{field.name}"' for field in (fields or db_obj.fields)])

        query = f'SELECT {fields_str} FROM {db_obj.get_full_name()}'

        if where is not None:
            query += '\nWHERE ' + self.generate_where_clause(where)

        if limit is not None:
            query += f' LIMIT {limit}'

        if offset is not None:
            query += f' OFFSET {offset}'

        return query

    @staticmethod
    def generate_insert_query(
        db_obj: DBObject,
        records: list[dict],
        returning: bool | list | str = False
    ) -> str:
        field_names = [f'"{field}"' for field in records[0].keys()]
        values_list = []

        fields_info = {
            field_name: list(filter(lambda x: x.name == field_name.strip('"'), db_obj.fields))[0]
            for field_name in field_names
        }

        for record in records:
            values = []

            for field in field_names:
                value = record.get(field.strip('"'))

                field_info = fields_info[field]

                if value is None:
                    values.append('NULL')

                elif field_info.data_type in ['int4', 'int8', 'float4', 'float8', 'numeric', 'boolean']:
                    values.append(str(value))

                else:
                    values.append(f"'{str(value).replace("'", "''")}'")

            values_list.append(f"({', '.join(values)})")

        values_str = ", ".join(values_list)

        query = (
            f'INSERT INTO\n'
            f'  {db_obj.get_full_name()} ({", ".join(field_names)})\n'
            f'VALUES\n'
            f'  {values_str}\n'
        )

        if returning is True:
            query += 'RETURNING\n\t*'

        elif isinstance(returning, list):
            returning_fields = ', '.join([f'"{field}"' for field in returning])
            query += f' RETURNING\n\t{returning_fields}'

        return query + '\n;'

    def generate_update_query(
        self,
        db_obj: DBObject,
        update: dict,
        where: dict | None = None,
        returning: bool | list | str = False
    ) -> str:
        set_clauses = []

        for key, value in update.items():
            set_clauses.append(f'"{key}" = {self.format_value(value)}')

        set_clause_str = ", ".join(set_clauses)

        if returning is True:
            returning_clause = 'RETURNING *'

        elif isinstance(returning, list):
            returning_fields = ', '.join([f'"{field}"' for field in returning])
            returning_clause = f'RETURNING {returning_fields}'

        else:
            returning_clause = ''

        query = (
            f'UPDATE {db_obj.get_full_name()}\n'
            f'SET {set_clause_str}\n'
            f'WHERE {where}\n' +
            returning_clause
        ).strip('\n') + '\n;'

        return query

    def generate_delete_query(
        self,
        db_obj: DBObject,
        where: dict | None = None,
        returning: bool | list | str = False
    ) -> str:
        if returning is True:
            returning_clause = 'RETURNING *'

        elif isinstance(returning, list):
            returning_fields = ', '.join([f'"{field}"' for field in returning])
            returning_clause = f'RETURNING {returning_fields}'

        else:
            returning_clause = ''

        # noinspection SqlWithoutWhere
        query = (
            f'DELETE FROM {db_obj.get_full_name()}\n'
            f'WHERE {self.generate_where_clause(where)}\n' +
            returning_clause
        ).strip('\n') + '\n;'

        return query
