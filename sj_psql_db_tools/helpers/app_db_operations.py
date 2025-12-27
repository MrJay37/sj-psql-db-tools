import logging
from pg8000 import ProgrammingError
from sj_psql_db_tools.models import *
from sj_psql_db_tools.connector import PSQLDBConnector


def generateCreateTableQuery(table: DBObject, **kwargs) -> str:
    """

    :param table: Database table object to be created
    :keyword is_archive_table: Whether the table to be created is an archive table (default: False)
    :keyword serial_id_data_type: Data type for serialId field (default: int4)
    :keyword id_field_name: Name of the ID field (default: id)

    :return:
    """
    create_query = f"create table if not exists {table.get_full_name()}"

    serial_id_data_type = kwargs.get("serial_id_data_type", "int4")
    id_field_name = kwargs.get("id_field_name", "id")

    is_archive_table = kwargs.get("is_archive_table", False)

    if is_archive_table:
        fields = [
            f'"archiveSerialId" int8 generated always as identity primary key',
            f'"addedAt" timestamptz not null default now()',  # Indicates when the record was added to archive
            f'"serialId" {serial_id_data_type} not null',  # Original serialId from main table
            f'"{id_field_name}" uuid not null',  # No unique constraint, same record can be here twice
            # Archive table need not have any foreign key references, unnecessary
            f'"createdAt" timestamptz not null',
            f'"createdBy" uuid not null',
            f'"modifiedAt" timestamptz null',
            f'"modifiedBy" uuid null',
            f'"deletedAt" timestamptz null',
            f'"deletedBy" uuid null'
        ]

    else:
        fields = [
            f'"serialId" {serial_id_data_type} generated always as identity primary key',
            f'"{id_field_name}" uuid default gen_random_uuid() not null unique',
            # Audit fields always, by default, users table will always be present
            f'"createdAt" timestamptz not null default now()',
            f'"createdBy" uuid not null references "admin"."users"("id")',
            f'"modifiedAt" timestamptz null',
            f'"modifiedBy" uuid null references "admin"."users"("id")'
        ]

    for col in table.fields:
        if col.name in [id_field_name, "serialId", "createdAt", "createdBy", "modifiedAt", "modifiedBy"]:
            continue

        col_def = [f'"{col.name}"', col.data_type or "text"]

        if not col.is_nullable:
            col_def.append("not null")

        if col.default_value is not None:
            col_def.append(f'default \'{col.default_value}\'')

        fields.append(" ".join(col_def))

    fields_str = ",\n\t".join(fields)

    create_query += f"(\n\t{fields_str}" + "\n);"

    return create_query


def createArchiveTable(db: PSQLDBConnector, table: DBObject, **kwargs) -> None:
    db.execute(f'create schema if not exists "{table.schema_name}";')

    create_table_query = generateCreateTableQuery(table, **kwargs, is_archive_table=True)

    db.execute(create_table_query)

    logging.info(f"Table {table.get_full_name()} created successfully.")


def createUpsertArchiveFunction(db: PSQLDBConnector, function_name, table: DBObject, archive_table: DBObject) -> None:
    field_names = [f'"{col.name}"' for col in table.fields]

    function_query = (
        f"create or replace function {function_name}()\n"
        f"returns trigger\n"
        f"language plpgsql\n"
        f"as $$\n"
        f"begin\n"
        f"	insert into {archive_table.get_full_name()}({','.join(field_names)})\n"
        f"	values ({','.join([f'NEW.{col}' for col in field_names])});\n"
        f"	return null;\n"
        f"end;\n"
        f"$$;\n"
    )

    db.execute(function_query)


def createTriggers(db: PSQLDBConnector, table: DBObject, archive_table: DBObject) -> None:
    upsert_triggers = [
        {"action": "insert"},
        {"action": "update"}
    ]

    for trig in upsert_triggers:
        # Insert trigger
        function_name = f'"{table.schema_name}"."{table.obj_name}_{trig['action']}"'

        createUpsertArchiveFunction(db, function_name, table, archive_table)

        trigger_name = f'"{table.schema_name}_{table.obj_name}_{trig['action']}_trigger"'

        trigger_query = (
            f"create trigger {trigger_name}\n"
            f"after {trig['action']} on {table.get_full_name()}\n"
            f"for each row\n"
            f"execute function {function_name}();"
        )

        try:
            db.execute(trigger_query)

        except ProgrammingError as e:
            if e.args[0]['C'] == '42710':  # Trigger already exists
                logging.debug(f"Dropping existing trigger and recreating")
                db.execute(f"drop trigger {trigger_name} on {table.get_full_name()};")
                db.execute(trigger_query)

            else:
                raise e  # Reraise other exceptions

        logging.info(f"{trig['action']} trigger created")


def createDeleteRecordFunction(db: PSQLDBConnector, table: DBObject, archive_table: DBObject, **kwargs) -> None:
    field_definitions = [f'"{col.name}" {col.data_type}' for col in table.fields]

    field_names = [f'"{col.name}"' for col in table.fields]

    id_field_name = kwargs.get("id_field_name", "id")

    function_query = (
        f"create or replace function\n"
        f"	\"{table.schema_name}\".\"delete_{table.obj_name.strip('s')}\"(record_id uuid, deleted_by_id uuid)\n"
        f"returns table(\n"
        f"    {',\n\t'.join(field_definitions)}\n"
        f") language plpgsql as $$\n"
        f"begin\n"
        f"	return query with deleted_rows as (\n"
        f"		delete from\n"
        f"			{table.get_full_name()} a\n"
        f"		where\n"
        f"			a.\"{id_field_name}\" = record_id\n"
        f"		returning\n"
        f"			a.*\n"
        f"	)\n"
        f"	insert into {archive_table.get_full_name()} as b(\n"
        f"	    {', '.join(field_names)}, \"deletedAt\", \"deletedBy\"\n"
        f"	)\n"
        f"	select\n"
        f"	    {', '.join(['u.'+ x for x in field_names])}, now(), deleted_by_id\n"
        f"	from\n"
        f"		deleted_rows u\n"
        f"	returning\n"
        f"	    {',\n\t\t'.join(['b.'+ x for x in field_names])}\n"
        f"	;\n"
        f"end;\n"
        f"$$;"
    )

    try:
        db.execute(function_query)

    except ProgrammingError as e:
        if e.args[0]['C'] == '42P13':  # Function already exists
            logging.debug(f"Dropping existing delete function and recreating")
            db.execute(f'drop function "{table.schema_name}"."delete_{table.obj_name.strip("s")}"(uuid, uuid);')
            db.execute(function_query)

        else:
            raise e  # Reraise other exceptions

    logging.info(f"Delete function created")


def createTable(db: PSQLDBConnector, table: DBObject, **kwargs) -> None:
    """
    Creates a table in the database, with proper application convention

    :param db: Library's PSQL database connector
    :param table: Database table object to be created

    :keyword serial_id_data_type: Data type for serialId field (default: int4)
    :keyword id_field_name: Name of the ID field (default: id)
    :keyword create_archive_table: Whether to create archive table along with main table
    :keyword archive_db_obj: Database object's archive object to be created, will default to convention

    :return: None
    """
    # Create schema first
    db.execute(f'create schema if not exists "{table.schema_name}";')

    create_table_query = generateCreateTableQuery(table, **kwargs)

    db.execute(create_table_query)

    logging.info(f"Table {table.get_full_name()} created successfully.")

    if kwargs.get("create_archive_table", True):
        archive_table = kwargs.get('archive_db_obj') or DBObject(
            schema_name=f"__{table.schema_name}__",
            obj_name=table.obj_name,
            fields=table.fields
        )

        createArchiveTable(
            db,
            table=archive_table,
            **kwargs
        )

        createTriggers(db, table, archive_table)

        createDeleteRecordFunction(db, table, archive_table)


def insertRecords(db: PSQLDBConnector, table: DBObject, records: list[dict], created_by_id) -> QueryResponse:
    return db.insertData(
        table,
        data=[{
            **record,
            "createdBy": created_by_id,
            "createdAt": PSQLKeywords.now
        } for record in records],
        returning=True
    )


def updateRecord(
    db: PSQLDBConnector,
    table: DBObject,
    where: dict,
    update: dict,
    modified_by_id
) -> QueryResponse:
    return db.updateData(
        table,
        update={
            **update,
            "modifiedBy": modified_by_id,
            "modifiedAt": PSQLKeywords.now
        },
        where=where,
        returning=True
    )
