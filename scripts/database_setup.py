import psycopg2
from psycopg2 import sql
import os
import argparse
from dataclasses import dataclass, field
from typing import List, Optional
def check_existence(cur, sql):
    cur.execute(sql)
    return cur.fetchone() is not None
def connect_to_db(user,password,host_conn,port_var,dbname):
    try:
        conn = psycopg2.connect(
        host=host_conn,
        port=port_var,
        user=user,
        password=password,
        dbname=dbname
        )
        print(f"Connected to the database {dbname} on host {host_conn} port {port_var} with user {user}.")
        return conn
    except psycopg2.Error as e:
        print(f"Error: Could not make connection to the Postgres database\n{e}")
        exception = e
def create_database(cursor,dbname):
    database_exists_sql = sql.SQL("SELECT 1 FROM pg_database WHERE datname = {database}").format(database=sql.Literal(dbname))
    db_exists = check_existence(cursor, database_exists_sql)
    if db_exists:
        print(f"Database {dbname} already exists.")
        return
    create_database_exe(cursor,dbname)

def create_database_exe(cursor,dbname="farmers_market"):
    try:
        create_database_sql = sql.SQL("CREATE DATABASE {database}").format(database=sql.Identifier(dbname))
        print(create_database_sql.as_string(cursor))
        cursor.execute(create_database_sql)
        print("Database created successfully.")
    except psycopg2.Error as e:
        print(f"Database creation failed: {e}")
        
def create_role(cursor,role,login='NOLOGIN',password="NULL",createDB="NOCREATEDB",createRole="NOCREATEROLE",inherit="INHERIT",replication="NOREPLICATION",bypassRLS="NOBYPASSRLS",connectionLimit=-1):
    try:
        cursor.execute(sql.SQL("CREATE ROLE {role} WITH {login} PASSWORD {password} {createDB} {createRole} {inherit} {replication} {bypassRLS} CONNECTION LIMIT {connectionLimit}").format(role=sql.Identifier(role), login=sql.SQL(login), password=sql.Literal(password), createDB=sql.SQL(createDB), createRole=sql.SQL(createRole), inherit=sql.SQL(inherit), replication=sql.SQL(replication), bypassRLS=sql.SQL(bypassRLS), connectionLimit=sql.Literal(connectionLimit)))
        # cursor.execute(sql.SQL("CREATE ROLE {role} WITH CREATEDB {login} PASSWORD {password} {createDB}").format(role=sql.Identifier(role), login=sql.SQL(login), password=sql.Literal(password)))
        response = cursor.statusmessage
        print(f"Role created successfully: {response}")
    except psycopg2.Error as e:
        print(f"Role creation failed: {e}")
def add_role_to_role(cursor,role_grant,role):
    try:
        assign_role_sql = sql.SQL("GRANT {role} TO {role_grant}").format(role=sql.Identifier(role), role_grant=sql.Identifier(role_grant))
        cursor.execute(assign_role_sql)
        print("Role assigned successfully.")
    except psycopg2.Error as e:
        print(f"Role assignment failed: {e}")
def apply_grants(cursor, role, level, dbname=None, tablename=None):
    try:
        if level == 'db_admin':
            if not dbname:
                raise ValueError("dbname is required for db_admin level.")
            cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {database} TO {role}").format(role=sql.Identifier(role), database=sql.Identifier(dbname)))
        if level == 'admin':
            cursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {database} TO {role}").format(role=sql.Identifier(role), database=sql.Identifier(dbname)))
        elif level == 'db_read':
            if not dbname:
                raise ValueError("dbname is required for db_read level.")
            cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE dbname TO {role}").format(role=sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {role}").format(role=sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}").format(role=sql.Identifier(role)))
        elif level == 'table_update':
            if not tablename:
                raise ValueError("tablename is required for table_update level.")
            cursor.execute(sql.SQL("GRANT UPDATE ON TABLE tablename TO {role}").format(role=sql.Identifier(role)))
        elif level == 'table_read':
            if not tablename:
                raise ValueError("tablename is required for table_update level.")
            cursor.execute(sql.SQL("GRANT SELECT ON TABLE tablename TO {role};").format(role=sql.Identifier(role)))
        else:
            raise ValueError("Unsupported level specified.")
        print(f"Grants applied successfully for role {role} at level {level}.")
    except Exception as e:
        print(f"Failed to apply grants: {e}")

def setup_db(dbname,user_var,passwd,host_conn,port_var,db_user,db_user_pass,host_dbname):
    conn=connect_to_db(user_var,passwd,host_conn,port_var,host_dbname)
    conn.set_session(autocommit=True)
    db_service_role=dbname+"_admin_role"
    db_service_role=dbname+"_admin_role"
    with conn.cursor() as cursor:
        create_database(cursor,dbname)
        create_role(cursor,db_service_role)
        apply_grants(cursor, db_service_role, 'db_admin', dbname)
        create_role(cursor,db_user,login='LOGIN',password=db_user_pass)
        add_role_to_role(cursor,db_user,db_service_role)
    
    conn.close()
    
def getCredentials():
    host_conn=os.getenv("PGHOST")
    port_var=os.getenv("PGPORT")
    user_var=os.getenv("PGUSER")
    passwd=os.getenv("PGPASSWORD")
    farmers_market_user=os.getenv("FARMERSMARKETUSER")
    farmers_market_user_pass=os.getenv("FARMERSMARKETPASS")
    return host_conn,port_var,user_var,passwd,farmers_market_user,farmers_market_user_pass

def main(delete_db):
    # Create the database
    host_conn,port_var,user_var,passwd,farmers_market_user,farmers_market_user_pass=getCredentials()
    db_service_role="db_service_role_farmers_market_admin"
    database="farmers_market"
    setup_db(database,user_var,passwd,host_conn,port_var,farmers_market_user,farmers_market_user_pass,'postgres')   

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage the farmers_market database.")
    parser.add_argument("--delete", action="store_true", help="Drop the farmers_market database if it exists.")
    args = parser.parse_args()

    main(delete_db=args.delete)