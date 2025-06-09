# schema.py
from clickhouse_driver import Client
from config import CLICKHOUSE_CONFIG

def get_clickhouse_client():
    return Client(**CLICKHOUSE_CONFIG)

def get_schema_description():
    client = get_clickhouse_client()
    tables = client.execute("SHOW TABLES")
    schema = ""

    for (table_name,) in tables:
        schema += f"\nTable: {table_name}\n"
        desc = client.execute(f"DESCRIBE TABLE {table_name}")
        for name, type_, *_ in desc:
            schema += f"- {name} ({type_})\n"
    return schema

def run_query(sql: str):
    client = get_clickhouse_client()
    return client.execute(sql)
