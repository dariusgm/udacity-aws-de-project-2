import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, role_s3_read, event_path, songs_path, event_schema_path):
    for query in copy_table_queries:
        if "$iam" in query:
            query = query.replace("$iam", role_s3_read)
        if "$events" in query:
            query = query.replace("$events", event_path)
        if "$songs" in query:
            query = query.replace("$songs", songs_path)
        if "$event_schema" in query:
            query = query.replace("$event_schema", event_schema_path)
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host = config.get("CLUSTER", "host")
    db_name = config.get("CLUSTER", "db_name")
    db_user = config.get("CLUSTER", "db_user")
    db_password = config.get("CLUSTER", "db_password")
    db_port = config.get("CLUSTER", "db_port")
    role_s3_read = config.get("CLUSTER", "role_s3_read")
    event_path = config.get("AWS", "log_data")
    song_path = config.get("AWS", "song_data")
    event_schema_path = config.get("AWS", "log_schema_path")


    conn = psycopg2.connect(f"host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}")
    cur = conn.cursor()

    load_staging_tables(cur, conn, role_s3_read, event_path, song_path, event_schema_path)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()