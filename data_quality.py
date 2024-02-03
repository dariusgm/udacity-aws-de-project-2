# Shared function that are used for all tables
import sys
from typing import List

def fetch_meta_data() -> str:
    """
    returns all metadata from the tables that we just created
    :return:
    """

    return f"""
    SELECT table_name, column_name FROM information_schema.columns where table_catalog = 'dwh' and table_schema = 'public' 
    """

def count_check(table: str) -> str:
    """
    :argument table: The table to check
    :return: Ready to use query
    """
    return f"SELECT COUNT(*) FROM {table};"


def null_check(table: str, column: str):
    """

    :param table: The table to check
    :param column: The column to check
    :return: Ready to use query
    """
    return f"""
    SELECT {column}, COUNT(*) AS null_count
    FROM {table}
    WHERE {column} IS NULL
    GROUP BY {column}
    ORDER BY null_count DESC;
    """


def duplicate_check(table: str, columns: List[str]) -> str:
    """
    :param table: The table to check
    :param columns: The columns that can indicate the duplicates.
    :return: Ready to use query
    """

    return f"""
    SELECT {",".join(columns)}
    FROM {table}
    GROUP BY {",".join(columns)}
    HAVING COUNT(*) > 1;
    """


def unique_check(table: str, column: str) -> str:
    """

    :param table: The table to check
    :param column: The column to check
    :return: Ready to use query
    """
    return f"""
    SELECT {column}, COUNT(DISTINCT {column}) AS unique_count
    FROM {table}
    GROUP BY {column}
    ORDER BY unique_count DESC;
    """


def outlier_check(table: str, column: str) -> str:
    """

    :param table:
    :param column:
    :return:
    """

    return f"""
    SELECT {column}, MAX({column}) AS max_value, MIN({column}) AS min_value,
    AVG({column}) AS mean, MEDIAN({column}) AS median, STDDEV({column}) AS std_dev
    FROM {table};
    """


def distribution_check(table: str, column: str) -> str:
    """
    :param table:
    :param column:
    :return:
    """

    return f"""
    SELECT {column}, COUNT(*) AS frequency
    FROM {table}
    GROUP BY {column}
    ORDER BY frequency DESC;
    """



import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def print_db_result(db_data: tuple):
    print(db_data)

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

    meta_query = fetch_meta_data()
    print(meta_query)
    cur.execute(meta_query)
    conn.commit()
    tables = set()
    table_column_pairs = set()
    for meta_result in cur.fetchall():
        table_name, column_name = meta_result
        tables.add(table_name)
        table_column_pairs.add(meta_result)


    for table in tables:
        count_table_query = count_check(table)
        cur.execute(count_table_query)
        conn.commit()
        count = cur.fetchone()[0]
        print(f"{count_table_query}: {count}")
        if count == 0:
            print(f"{table} have no records!", file=sys.stderr)

    for table_colum_pair in table_column_pairs:
        pass
    conn.close()


if __name__ == "__main__":
    main()