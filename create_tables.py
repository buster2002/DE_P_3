import configparser
import psycopg2
import time
from sql_queries import create_table_queries, drop_table_queries


"""drop database tables from drop_table_queries,
a list with DROP statements
"""
def drop_tables(cur, conn):
    print("Starting drop All Tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("DROPPED")

""" create database tables from create_table_queries,
a list with INSERT statements
"""
def create_tables(cur, conn):
    print("Starting Create All Tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("CREATED")
    count_tables(cur, conn)

"""
To check the total number of table's records
"""
def count_tables(cur, conn):
    table_list= ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
    print("Counting...")
    for table in table_list:
        try:
            qr= "SELECT count(*) FROM {}".format(table)
            cur.execute(qr)
            results = cur.fetchone()
            print("There are {} Records in {}".format(results, table))

        except Exception as e:
            print(e)
    conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to Redshift Cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected')

    drop_tables(cur, conn)

    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
