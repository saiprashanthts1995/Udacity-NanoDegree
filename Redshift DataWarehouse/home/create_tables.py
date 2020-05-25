import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Purpose of this particular udf is used to drop the tables
    
    :cur param: Cursor object to query  the database
    :conn param: Connection object to connect to the datbase
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Purpose of this particular udf is used to create the tables
    
    :cur param: Cursor object to query  the database
    :conn param: Connection object to connect to the datbase
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Purpose of this particular udf is used to call the above defined functions
    '''

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

print('hello')

if __name__ == "__main__":
    main()