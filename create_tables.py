import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("Executing query:", query)  # Add this line to print the query being executed
        cur.execute(query)
        conn.commit()
        print("Table dropped successfully")  # Add this line to indicate that the table was dropped


def create_tables(cur, conn):
    for query in create_table_queries:
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

    print('db')
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(dwh_endpoint, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()
    print("DB connection")
    drop_tables(cur, conn)  # Call drop_tables() before creating tables
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
