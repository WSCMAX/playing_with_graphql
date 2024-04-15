import psycopg2
import os
import argparse  

# Environment variables for connection details
host_conn = os.environ['PGHOST']
port_var = os.environ['PGPORT']
user_var = os.environ['PGUSER']
farmers_market_user_pass = os.environ['FARMERSMARKETPASS']
farmers_market_user = os.environ['FARMERSMARKETUSER']
passwd = os.environ['PGPASSWORD']
def drop_database(cursor):
    try:
        # Terminate all existing connections to the target database
        cursor.execute("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'farmers_market'
              AND pid <> pg_backend_pid();
        """)
        print("All connections to the database have been terminated.")

        # Drop the database
        cursor.execute("DROP DATABASE IF EXISTS farmers_market")
        print("Database dropped successfully.")
    except psycopg2.Error as e:
        print(f"Database drop failed: {e}")

def create_database(cursor):
    try:
        cursor.execute("CREATE DATABASE farmers_market")
        print("Database created successfully.")
    except psycopg2.Error as e:
        print(f"Database creation failed: {e}")

def create_table(cursor,sql):
    try:
        cursor.execute(sql)
        print("Table created successfully.")
    except psycopg2.Error as e:
        print(f"Table creation failed: {e}")
def main(delete_db):
    # Connect to the default database
    conn = psycopg2.connect(
        host=host_conn,
        port=port_var,
        user=user_var,
        password=passwd,
        dbname='postgres'
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create the database
    if delete_db:
        drop_database(cursor)
    create_database(cursor)

    # Close the connection to the default database
    cursor.close()
    conn.close()

    # Connect to the new database
    conn = psycopg2.connect(
        host=host_conn,
        port=port_var,
        user=user_var,
        password=passwd,
        dbname='farmers_market'
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create the table
    users_sql = """
    CREATE TABLE IF NOT EXISTS users(
        user_id uuid DEFAULT gen_random_uuid() NOT NULL UNIQUE, 
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        photo_url VARCHAR(2048),
        password VARCHAR(255) NOT NULL,
        CONSTRAINT users_pkey PRIMARY KEY (user_id)
    )"""

    accounts_sql="""
            CREATE TABLE IF NOT EXISTS accounts(
                account_id uuid DEFAULT gen_random_uuid() NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL,
                photo_url VARCHAR(2048),
                CONSTRAINT accounts_pkey PRIMARY KEY (account_id)
            )
        """
    accounts_users_sql="""
            CREATE TABLE IF NOT EXISTS accounts_users(
                account_id UUID references accounts(account_id)  NOT NULL ,
                user_id UUID references users(user_id)  NOT NULL,
                CONSTRAINT account_users_pkey PRIMARY KEY (account_id,user_id)
            )
        """
    create_table(cursor,users_sql)
    create_table(cursor,accounts_sql)
    create_table(cursor,accounts_users_sql)

    # Clean up: Close cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage the farmers_market database.")
    parser.add_argument("--delete", action="store_true", help="Drop the farmers_market database if it exists.")
    args = parser.parse_args()

    main(delete_db=args.delete)