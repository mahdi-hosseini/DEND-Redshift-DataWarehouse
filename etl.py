from contextlib import closing

import psycopg2

from pipeline import ETL, read_config
from sql_queries import (
    copy_table_queries,
    insert_table_queries,
    songplay_table_insert,
    update_table_queries,
)


def main():
    config = read_config("config/dwh.cfg")

    conn_str = "host={} dbname={} user={} password={} port={}".format(
        *config["CLUSTER"].values()
    )

    with closing(psycopg2.connect(conn_str)) as conn:
        with conn.cursor() as cur:
            etl_pipeline = ETL(cur, conn)
            etl_pipeline.run(
                [
                    copy_table_queries,
                    insert_table_queries,
                    update_table_queries,
                    songplay_table_insert,
                ]
            )


if __name__ == "__main__":
    main()
