from sj_psql_db_tools.connector import PSQLDBConnector
from sj_psql_db_tools.helpers import *
from sj_psql_db_tools.models import *


def createDBConn(db_config: dict) -> PSQLDBConnector:
    return PSQLDBConnector(
        host=db_config.get("host"),
        port=db_config.get("port"),
        database=db_config.get("database"),
        user=db_config.get("user"),
        password=db_config.get("password")
    )