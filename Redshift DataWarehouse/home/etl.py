import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Purpose of this particular udf is used to load the staging tables 
    
    :cur param: Cursor object to query  the database
    :conn param: Connection object to connect to the datbase
    '''

    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Purpose of this particular udf is used to load the dim and fact tables 
    
    :cur param: Cursor object to query  the database
    :conn param: Connection object to connect to the datbase
    '''   

    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    Purpose of this particular udf is used to call the above defined udf's
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()