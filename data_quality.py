# Shared function that are used for all tables
import dataclasses
import sys
from typing import List, Tuple, Set, Any
import configparser
import psycopg2
from psycopg2 import errors, OperationalError


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
    Stack overflow helped here: https://stackoverflow.com/questions/239545/how-do-i-return-my-records-grouped-by-null-and-not-null
    :param table: The table to check
    :param column: The column to check
    :return: Ready to use query
    """
    return f"""
    SELECT
    COUNT(CASE WHEN {column} IS NULL THEN 1 ELSE NULL END) AS null_count,
    COUNT(CASE WHEN {column} IS NOT NULL THEN 1 ELSE NULL END) AS non_null_count
    FROM {table}
    """


def get_table_information(cur, conn) -> tuple[set[str], set[tuple[set[str, str]]]]:
    """
    Return the general table and column information.
    This prevents hard coding the table / columns
    :param cur: Database Cursor
    :param conn: Database connection
    :return: a tuple
     * first element is the table name
     * second element it a tuple with table, column pairs
    """
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
    return tables, table_column_pairs


def run_count_check(tables, cur, conn, report_writer):
    print("Running Check 'Count on Tables'")
    report_writer.write(f"# Count Check\n")
    report_writer.write(f"|Table|Count|Passed|\n")
    report_writer.write(f"|-----|-----|------|\n")
    for table in tables:
        count_table_query = count_check(table)
        cur.execute(count_table_query)
        conn.commit()
        count = cur.fetchone()[0]
        report_writer.write(f"|{table}|{count}|{count != 0}|\n")


def run_null_check(table_column_pairs, cur, conn, report_writer):
    print("Running Check 'Null on Columns'")
    report_writer.write(f"# Null Check\n")
    report_writer.write(f"|Table|Column|Null|Non-Null|Passed|\n")
    report_writer.write(f"|-----|------|----|--------|------|\n")
    for table_colum_pair in table_column_pairs:
        table_name, column_name = table_colum_pair
        null_check_query = null_check(table_name, column_name)
        cur.execute(null_check_query)
        conn.commit()
        null_count, non_null_count = cur.fetchone()
        total = null_count + non_null_count
        # more than 20% null is bad data quality
        report_writer.write(f"|{table_name}|{column_name}|{null_count}|{non_null_count}|{null_count * 0.2 < total}|\n")


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
    tables, table_column_pairs = get_table_information(cur, conn)

    with open("data-quality-report.md", 'wt') as report_writer:
        report_writer.write("# Data Quality Report\n")
        run_count_check(tables, cur, conn, report_writer)
        run_null_check(table_column_pairs, cur, conn, report_writer)

    conn.close()


if __name__ == "__main__":
    main()
