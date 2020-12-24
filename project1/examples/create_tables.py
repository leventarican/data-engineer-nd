import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the pythondb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to an existing database: developer-db
    conn = psycopg2.connect("host=localhost dbname=developer-db user=postgres password=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create pythondb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS pythondb")
    cur.execute("CREATE DATABASE pythondb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()

    conn = psycopg2.connect("host=localhost dbname=pythondb user=postgres password=postgres")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the database.
    - Establishes connection with the database and getscursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print("DONE.")

if __name__ == "__main__":
    main()
