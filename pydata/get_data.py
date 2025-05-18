#!/usr/bin/python3
import argparse
import configparser
import csv
import sys
import logging
import psycopg2
from datetime import datetime

# Constants
CONFIG_FILE = "configs.ini"
CSV_FILENAME = f"RKMINFO-{datetime.now().strftime('%Y-%m-%d')}.csv"

def load_config():
    config = configparser.ConfigParser()
    config.read("configs.ini")
    return config['postgresql']

def connect(cfg):
    conn_strings = f"host={cfg['host']} dbname={cfg['dbname']} user={cfg['user']}"
    conn = psycopg2.connect(conn_strings)
    return conn

def query(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def check_biz_date(conn, date):
    sql = """
        SELECT daytype
        FROM rkm_calendar
        WHERE calendar_date = %s
    """
    is_biz_day = query(conn, sql, (date,))
    return is_biz_day and is_biz_day[0][0] == 'B'

def get_eod_data(conn):
    sql = """
        SELECT id, name, is_open_source, developed_by, category,
                description, status
        FROM tech_tools
    """
    return query(conn, sql)

def write_file(data):
    current_date = datetime.now().strftime('%Y%m%d%H%M%S')
    with open(CSV_FILENAME, 'w', newline='', encoding='utf8') as eod_file:
        writer = csv.writer(eod_file)
        writer.writerow(['H', current_date])

        for row in data:
            writer.writerow(['D', *row])
        
        writer.writerow(['T', len(data)])
    print(f"INFO : WROTE {len(data)} DATA ROWS TO '{CSV_FILENAME}'")

def main():
    parser = argparse.ArgumentParser(description="Generate EOD file")
    parser.add_argument('-n', '--by-pass-biz-check', action='store_true', help="Skip biz day check")
    args = parser.parse_args()

    config = load_config()
    conn = connect(config)

    # By pass business day check 
    if not args.by_pass_biz_check:
        today = datetime.now().date()
        if not check_biz_date(conn, today):
            print("INFO : Today is not a valid biz day. Skipping ...")
            conn.close()
            sys.exit(0)

    data = get_eod_data(conn)
    print(f"INFO : FETCHED {len(data)} ROWS.")
    write_file(data)
    conn.close()
    print("INFO : DATABASE CONNECTION CLOSED")

if __name__ == "__main__":
    main()
