import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # Get the params of the created Redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Connect to the Redshift Cluster
    KEY = '###'
    SECRET = '###'
    
    s3 = boto3.resource( 
                        's3',
                        region_name = 'us-west-2',
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET,
                        )
    
    # Drop tables and create new ones
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()