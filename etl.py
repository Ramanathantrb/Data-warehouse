import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
       
        cur.execute(query)
        conn.commit()
        # Added a comment to track the progress
        print("Data copied into staging table")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    dwh_endpoint=''
    DB_NAME='dev'
    DB_USER='awsuser'
    DB_PASSWORD=''
    DB_PORT=5439
    
        # Added a comment to track the progress
    print('db')
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(dwh_endpoint, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()
        # Added a comment to track the progress
    print("DB connection") 
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()