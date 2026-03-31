import json

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def sql_get(sql, engine):
    try:
        out = pd.read_sql(sql, engine)
        return out
    except SQLAlchemyError as error:
        print("Error while reading table", error)
        raise
    finally:
        engine.dispose()


def sql_write(schema, name, value, engine, method):
    try:
        out = value.to_sql(
            name=name, con=engine, schema=schema, if_exists=method, index=False
        )
        return out
    except SQLAlchemyError as error:
        print("Error while writing table", error)
        raise
    finally:
        engine.dispose()


def sql_execute(sql, conn):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while executing SQL statement", error)
        raise
    finally:
        if conn:
            conn.close()


def sql_exists(schema, name, conn):
    try:
        cur = conn.cursor()
        sql = """SELECT EXISTS(
                        SELECT *
                        FROM information_schema.tables
                        WHERE
                          table_schema = '%s' AND
                          table_name = '%s'
                    );""" % (
            schema,
            name,
        )
        cur.execute(sql)
        conn.commit()
        exists = cur.fetchone()[0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while executing SQL statement", error)
        raise
    finally:
        if conn:
            conn.close()
    return exists


def sql(conf, fn, sql, schema=None, name=None, value=None, method="fail"):
    """
    Connect to a Postgres database and allows execution of statements, getting tables as pandas dataframes and
    writing pandas dataframes to database.
    Parameters
    ----------
    db : str
        Name of the database.
        Options include 'EDH_PROD' and 'EDH_NONPROD'.
        For the selected database credentials need to be stored as environment variables.
        'EDH_PROD' requires 'EDH_USER' and 'EDH_PROD_PASSWORD'.
        'EDH_NONPROD' requires 'EDH_USER' and 'EDH_NONPROD_PASSWORD'.
        If you prefer to supply credentials another way (e.g. `getpass.getpaass()`) the use
        `sql_creds_params`
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
    """
    key_path = f"{conf.secret_data}/secrets.json"
    
    with open(key_path) as file:
        data = json.load(file)
        file.close()

    return sql_creds_params(
        conf.db,
        fn,
        sql,
        data["EDH_PROD"]["username"],
        data["EDH_PROD"]["password"],
        data["EDH_NONPROD"]["password"],
        schema=schema,
        name=name,
        value=value,
        method=method,
    )


def sql_creds_params(
    db,
    fn,
    sql,
    db_user,
    db_prod_password=None,
    db_nonprod_password=None,
    schema=None,
    name=None,
    value=None,
    method="fail",
):
    """
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
    """
    dbs = {
        "EDH_PROD": {
            "host": "iagdcaprod.auiag.corp",
            "port": "5432",
            "database": "iadpprod",
            "user": db_user,
            "password": db_prod_password,
        },
        "EDH_NONPROD": {
            "host": "iagdcanonprod.auiag.corp",
            "port": "5432",
            "database": "iadpdev",
            "user": db_user,
            "password": db_nonprod_password,
        },
    }
    engine = create_engine(
        "postgresql://"
        + dbs[db]["user"]
        + ":"
        + dbs[db]["password"]
        + "@"
        + dbs[db]["host"]
        + ":"
        + dbs[db]["port"]
        + "/"
        + dbs[db]["database"]
    )
    try:
        conn = psycopg2.connect(**dbs.get(db, {}))
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting", error)
        raise

    functions = {
        "get": (sql_get, (sql, engine)),
        "execute": (sql_execute, (sql, conn)),
        "write": (sql_write, (schema, name, value, engine, method)),
        "exists": (sql_exists, (schema, name, conn)),
    }
    do, args = functions.get(fn)
    out = do(*args)
    return out
