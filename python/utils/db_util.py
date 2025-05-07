import inspect
import sys

import psycopg2
from psycopg2.extras import execute_batch

from python.utils.config import load_config_with_pass

integration_qa_db_config = load_config_with_pass('../config.ini', '../pass.ini', 'qacoredb')
integration_prod_db_config = load_config_with_pass('../config.ini', '../pass.ini', 'integrationprod')
coredw_prod_db_config = load_config_with_pass('../config.ini', '../pass.ini', 'redshift_coredw')


def execute_query(sql, query_vars, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute(sql, query_vars)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        _handle_db_exception(error)
    finally:
        if conn is not None:
            conn.close()


def execute_batch_query(sql, query_vars, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        execute_batch(cur, sql, query_vars)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        _handle_db_exception(error)
    finally:
        if conn is not None:
            conn.close()


def execute_fetch_all_query(sql, db_config):
    return execute_fetch_all_with_vars_query(sql, None, db_config)


def execute_fetch_all_with_vars_query(sql, query_vars, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        if query_vars:
            cur.execute(sql, query_vars)
        else:
            cur.execute(sql)
        response = cur.fetchall()
        cur.close()
        return response
    except (Exception, psycopg2.DatabaseError) as error:
        _handle_db_exception(error)
    finally:
        if conn is not None:
            conn.close()


def mogrify_query(sql, query_vars, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        query = cur.mogrify(sql, query_vars)
        cur.close()
        return query
    except (Exception, psycopg2.DatabaseError) as error:
        _handle_db_exception(error)
    finally:
        if conn is not None:
            conn.close()


def mogrify_fetch_all_query(sql, db_config):
    return mogrify_fetch_all_with_vars_query(sql, None, db_config)


def mogrify_fetch_all_with_vars_query(sql, query_vars, db_config):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        if query_vars:
            query = cur.mogrify(sql, query_vars)
        else:
            query = cur.mogrify(sql)
        cur.close()
        return query
    except (Exception, psycopg2.DatabaseError) as error:
        _handle_db_exception(error)
    finally:
        if conn is not None:
            conn.close()


def _handle_db_exception(error, note=inspect.currentframe().f_back.f_code.co_name):
    print(str(note) + ': ' + str(error), file=sys.stderr)
