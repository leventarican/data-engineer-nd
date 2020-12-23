import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def check_connection(cur, conn):
    try: 
        cur.execute("SELECT * FROM debug;")
    except psycopg2.Error as e: 
        print("Error: select *")
        print (e)

    row = cur.fetchone()
    print(row)
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # host = DWH_ENDPOINT
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    check_connection(cur, conn)

    #drop_tables(cur, conn)
    #create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()