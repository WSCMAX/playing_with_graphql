import psycopg2
from psycopg2 import sql
import os
import argparse
from dataclasses import dataclass, field
from typing import List, Optional
import secrets
import string
from dotenv import dotenv_values
from dataclasses import dataclass

@dataclass
class DBConfig:
    dbname: str
    user: str
    password: str
    host: str
    port: int

alphabet = string.ascii_letters + string.digits + string.punctuation

def alter_role_password(cursor, role, password):
    try:
        cursor.execute(sql.SQL("ALTER ROLE {role} WITH PASSWORD {password}").format(role=sql.Identifier(role), password=sql.Literal(password)))
        print(f"Password set to {password} for {role} successfully.")
    except psycopg2.Error as e:
        print(f"Error setting password: {e}")
def check_existence(cur, sql):
    cur.execute(sql)
    return cur.fetchone() is not None
def modify_env_variable(file_path, key_to_update, new_value):
    # Initialize a dictionary to hold the environment variables
    env_variables = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split('=', 1)
                env_variables[key] = value

    # Read the existing .env file
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        try:
            f = open(file_path, "x")
            print(f"File {file_path} created.")
            f.close()
        except Exception as e:
            print(f"Error creating file {file_path}: {e}")
        return

    # Update the specific environment variable
    env_variables[key_to_update] = new_value

    # Write the updated content back to the .env file
    with open(file_path, 'w') as file:
        for key, value in env_variables.items():
            file.write(f'{key}={value}\n')

    print(f"Updated {key_to_update} in .env file successfully.")

def connect_to_db(db: DBConfig):
    try:
        conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        user=db.user,
        password=db.password,
        dbname=db.dbname
        )
        print(f"Connected to the database {db.dbname} on host {db.host} port {db.port} with user {db.user}.")
        return conn
    except psycopg2.Error as e:
        print(f"Error: Could not make connection to the Postgres database {db.dbname}\n{e}")
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
# def update_password_and_env(cursor, role, env_file):
#     new_password = ''.join(secrets.choice(alphabet) for i in range(20))
#     modify_env_variable(".env", "PGPASSWORD", new_password)
#     config = dotenv_values(".env")
#     new_password = config.get("PGPASSWORD")
#     print(f"New password: {new_password}")
#     with conn.cursor() as cursor:
#         alter_role_password(cursor, config.get("PGUSER"), config.get("PGPASSWORD"))
def setup_db(conn,config):
    with conn.cursor() as cursor:
        create_database(cursor,config.get("APPDATABASE"))
        create_role(cursor,cursor,config.get("APPDATABASE"))
        apply_grants(cursor,config.get("APPSERVICEROLE"), 'db_admin', cursor,config.get("APPDATABASE"))
        create_role(cursor,config.get("APPSERVICERUSER"),login='LOGIN',password=db_user_pass)
        add_role_to_role(cursor,db_user,db_service_role)
    
    conn.close()
    

def main(delete_db):
    # Create the database
    # modify_env_variable(".env", "PGDATABASE", "postgres")
    # host_conn,port_var,user_var,passwd,farmers_market_user,farmers_market_user_pass=getCredentials()
    config = dotenv_values(".env")
    # print(config.get("PGDATABASE"))
    # db_service_role="db_service_role_farmers_market_admin"
    # database="farmers_market"
    database_details=DBConfig(config.get("PGDATABASE"),config.get("PGUSER"),config.get("PGPASSWORD"),config.get("PGHOST"),config.get("PGPORT"))
    conn=connect_to_db(database_details)
    conn.set_session(autocommit=True)
    if database_details.password=="postgres":
        new_password = ''.join(secrets.choice(alphabet) for i in range(20))
        modify_env_variable(".env", "PGPASSWORD", new_password)
        config = dotenv_values(".env")
        new_password = config.get("PGPASSWORD")
        print(f"New password: {new_password}")
        with conn.cursor() as cursor:
            alter_role_password(cursor, config.get("PGUSER"), config.get("PGPASSWORD"))
        conn.close()
        database_details.password = new_password
        conn=connect_to_db(database_details)
    # setup_db(database,user_var,passwd,host_conn,port_var,farmers_market_user,farmers_market_user_pass,'postgres') 
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage the farmers_market database.")
    parser.add_argument("--delete", action="store_true", help="Drop the farmers_market database if it exists.")
    args = parser.parse_args()

    main(delete_db=args.delete)