import configparser
from typing import List

from psycopg2.extensions import connection, cursor


class ETL:
    def __init__(self, cur: cursor, conn: connection) -> None:
        self.cur: cursor = cur
        self.conn: connection = conn

    def execute(self, queries: List[str]) -> None:
        for query in queries:
            self.cur.execute(query)
            self.conn.commit()

    def run(self, all_queries: List[List[str]]) -> None:
        for queries in all_queries:
            self.execute(queries)


def read_config(config_file_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_file_path)

    return config
