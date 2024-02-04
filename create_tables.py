import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes the 'drop table' sql statements.
    :param cur: the database cursor
    :param conn: the database connection
    :returns: None
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes the 'create table' sql statements.
    :param cur: the database cursor
    :param conn: the database connection
    :returns: None
    """
    for query in create_table_queries:
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

    conn = psycopg2.connect(f"host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()