
import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from sqlalchemy.exc import SQLAlchemyError
import json
import sys
from pathlib import Path

def data_postgres(db, dbase, fn, query=None, schema=None, name=None, value=None, method='fail'):
    '''
    Connect to a Postgres database and allows execution of statements, getting tables as pandas dataframes and 
    writing pandas dataframes to database.
    Parameters
    ----------
    db : str
        Name of the database. 
        Options include 'EDH_PROD' and 'EDH_NONPROD'.
        For the selected database credentials need to be stored in a secrets.json file in the following format:
    ------------
    {
        "EDH_NONPROD":
        {
          "username": "<username>",
          "password": "<password>"
        },
        "EDH_PROD":
        {
          "username": "<username>",
          "password": "<password>"
        },
    }
    ------------
    fn : str
        Type of function required to be performed. 
        Options include 'get', 'execute', 'write' and 'exists'.
        'get' used to get rows from a select statement.
        'execute' used to execute SQL statement that doesn't return rows.
        'write' used to write a pandas dataframe to a schema and table in the database.
        'exists' used to determine if a table exists
    sql : str
        SQL statement. 
        Required only for the 'get' and 'execute' functions and should be left empty or set to None otherwise.
    schema : str
        Schema in which a table is to be written. Required when using the 'write' function.
    name : str
        Name of the table in the database. Required when using the 'write' function.
    value : `pandas.DataFrame`
        Name of `pandas.DataFrame` that needs to be uploaded.
    method : str
        Method to used if a table with the same name already exists in the database.
        Options include:
        'fail', 'append' and 'replace'.
        'fail' if the function should fail to write a table with the same name and schema.
        'append' if the function should append the data at the end of the table
        'replace' if the function should replace an existing table if it exists.
    
    Returns
    -------
    out : pandas.DataFrame
        Only returned if the 'get' function is used with a select statement.
    '''
    username, password = get_creds(db)
    return sql_creds_params(db, dbase, fn, query, username, password, schema=schema, name=name, value=value, method=method)

def get_creds(database):
    """
    Load username and password for `database` from secrets.json.
    """
    try:
        base_dir = Path(__file__).resolve().parents[2]  # model_package
        secrets_path = base_dir / "secrets.json"

        with secrets_path.open() as file:   # use secrets_path here
            data = json.load(file)

        creds = data[database.upper()]
        return creds["sys_cfms_claims_username"], creds["sys_cfms_claims_password"]

    except Exception as e:
        print(f"[get_creds] Error loading credentials for {database}: {e}")
        sys.exit(1)

# def get_creds(database):

#     path_in_str = "./secrets.json"

#     try:
#         with open(path_in_str) as file:

#             #  load data into variable
#             data = json.load(file)
#             file.close()

#             username = data[database.upper()]["sys_sas_ama_username"]
#             password = data[database.upper()]["sys_sas_ama_password"]
            
#             return username, password
#     except:
#         print("error cannot find file")
#         sys.exit(1)

def sql_get(query, engine):
    try:
        out = pd.read_sql(query, engine)
        return out
    except SQLAlchemyError as error:
        print ("Error while reading table", error)
        raise
    finally:
        engine.dispose()


def sql_get_new(query, engine):
    """Use SQLAlchemy engine instead of psycopg2 cursor."""
    try:
        with engine.connect() as conn:
            stmt = text(query) if isinstance(query, str) else query
            result = conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        print("Error while reading table", e)
        raise
    finally:
        engine.dispose()

def sql_trunc_write(schema, name, value, engine, method='replace'):
    """
    Truncate table if exists and write DataFrame to Postgres.
    """
    try:
        with engine.begin() as conn:  # transaction-safe
            # Check if table exists
            table_exists = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :name
                    )
                """),
                {"schema": schema, "name": name}
            ).scalar()

            # Truncate if exists
            if table_exists:
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{name}"'))

            # Write DataFrame
            value.to_sql(
                name=name,
                con=conn,        
                schema=schema,
                if_exists=method,
                index=False
            )
        return len(value)
    except Exception as e:
        print("Error in trunc_write:", e)
        raise
    finally:
        engine.dispose()

def sql_write(schema, name, value, engine, method='replace'):
    """
    Write a DataFrame to Postgres safely using SQLAlchemy engine.
    """
    try:
        with engine.begin() as conn:
            value.to_sql(
                name=name,
                con=conn,        
                schema=schema,
                if_exists=method,
                index=False
            )
        return len(value)
    except Exception as e:
        print("Error while writing table:", e)
        raise
    finally:
        engine.dispose()

def sql_execute(query, engine):
    """
    Execute a SQL statement (non-SELECT) using SQLAlchemy engine.
    """
    try:
        with engine.begin() as conn:  # transaction-safe
            conn.execute(text(query))
    except Exception as e:
        print("Error while executing SQL statement:", e)
        raise
    finally:
        engine.dispose()

def sql_exists(schema, name, engine):
    """
    Check if a table exists in the given schema using SQLAlchemy engine.
    Returns True/False.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name   = :name
                    )
                """),
                {"schema": schema, "name": name}
            ).scalar()
            return result
    except Exception as e:
        print("Error while checking if table exists:", e)
        raise

def sql_creds_params(
    db, dbase, fn, query, db_user, db_password, schema=None, name=None, value=None, method='fail', query_list=False
):
    '''
    See `sql`, which is a wrapper function that hard-codes the use of environment variables for
    `db_user`, `db_prod_password`, `db_nonprod_password`
    The recommended way of inputting the password is by calling `getpass.getpass()`,
    For example
        import getpass
        pw = getpass.getpass()
        sql_creds_params( ..., pw, ...)
        ...
    which prompts interactively for the password.
    Parameters
    ----------
    db_user : str
        Username for relevant database server.
    db_prod_password : str
        Required only if `db` is 'EDH_PROD'
    db_nonprod_password : str
        Required only if `db` is 'EDH_NONPROD'.
    '''
    dbs = {
        'EDH_PROD': {'host': 'iagdcaprod.auiag.corp', 'port': '5432', 'user': db_user, 'password': db_password},
        'EDH_NONPROD': {'host': 'iagdcanonprod.auiag.corp', 'port': '5432', 'user': db_user, 'password': db_password},
    }
    engine = create_engine(
        'postgresql+psycopg2://'
        + dbs[db]['user']
        + ':'
        + dbs[db]['password']
        + '@'
        + dbs[db]['host']
        + ':'
        + dbs[db]['port']
        + '/'
        + dbase,
        connect_args={'sslmode': 'require'},
    )
    if query_list:
        try:
            # conn = psycopg2.connect(**dbs.get(db, {}), dbname=dbase)

            results = []
            if query and isinstance(query, list):
                for single_query in query:
                    if fn == 'get':
                        results.append(sql_get(single_query, engine))
                    elif fn == 'execute':
                        sql_execute(single_query, engine)
                    elif fn == 'get_new':
                        results.append(sql_get_new(single_query, engine))

            if fn == 'write':
                results.append(sql_write(schema, name, value, engine, method))
            elif fn == 'exists':
                results.append(sql_exists(schema, name, engine))

            return results if results else None
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting", error)
            raise
        # finally:
        #     if conn:
        #         conn.close()
    else:
        try:
            # conn = psycopg2.connect(**dbs.get(db, {}), dbname=dbase)

            functions = {
                'get': (sql_get, (query, engine)),
                'execute': (sql_execute, (query, engine)),
                'write': (sql_write, (schema, name, value, engine, method)),
                'exists': (sql_exists, (schema, name, engine)),
                'get_new': (sql_get_new, (query, engine)),
                "trunc_write": (sql_trunc_write, (schema, name, value, engine, method)),
            }
            do, args = functions.get(fn)
            out = do(*args)
            return out
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting", error)
            raise
        # finally:
        #     if conn:
        #         conn.close()

# %%
