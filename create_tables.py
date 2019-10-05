from contextlib import closing

import psycopg2

from pipeline import ETL, read_config
from sql_queries import create_table_queries, drop_table_queries


def main():
    config = read_config("dwh.cfg")

    conn_str = "host={} dbname={} user={} password={} port={}".format(
        *config["CLUSTER"].values()
    )

    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            etl_pipeline = ETL(cur, conn)
            etl_pipeline.run([drop_table_queries, create_table_queries])


if __name__ == "__main__":
    main()
