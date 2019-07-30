import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Loading files from S3 to Redshift after connected
"""
def load_staging_tables(cur, conn):
    print("Starting load_staging_tables\n")
    for query in copy_table_queries:
        print("EXECUTING {}".format(query))
        cur.execute(query)
        conn.commit()
    print("End load_staging_tables")

"""
Inserting the loaded data from staging to other tables
"""
def insert_tables(cur, conn):
    print("Starting insert_tables")
    for query in insert_table_queries:
        print("EXECUTING {}".format(query))
        cur.execute(query)
        conn.commit()
    print("End insert_tables")
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

    print("Create Connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected {}".format(cur))

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
