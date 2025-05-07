import logging
import sys

import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor

from python.utils.config import config, load_config_with_pass


class IntegrationRepository:
    """
    This class is responsible for handling all the database operations for integrationprod, qacoredb and coredw.
    isDW is a boolean that determines if the database is coredw or integrationprod/qacoredb.
    """

    conn = None
    cur = None
    db_config = None
    schema = None
    is_dw = False

    def __init__(self):
        base_config = config('../config.ini', 'environment')
        env = base_config['env']
        is_dw = eval(base_config['is_dw'])
        if is_dw:
            self.db_config = load_config_with_pass('../config.ini', '../pass.ini', 'redshift_coredw')
            if env == 'qa':
                self.schema = 'test'
            elif env == 'prod':
                self.schema = 'tpa'
            else:
                raise ValueError("ENV must be set in config.ini as 'qa' or 'prod'")
        else:
            if env == 'qa':
                self.db_config = load_config_with_pass('../config.ini', '../pass.ini', 'qacoredb')
            elif env == 'prod':
                self.db_config = load_config_with_pass('../config.ini', '../pass.ini', 'integrationprod')
            else:
                raise ValueError("ENV must be set in config.ini as 'qa' or 'prod'")
        self.log = logging.Logger(name='IntegrationRepository')
        log_level = config('../config.ini', 'environment')['log_level']
        log_file = config('../config.ini', 'environment')['log_file']
        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.getLevelName(log_level))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        db_init_message = f'Initialized IntegrationRepository with config {self.db_config}'
        if is_dw:
            db_init_message += f' and {self.schema} schema'
        self.log.info(db_init_message)
        self.log.debug('Opening connection')
        self.conn = psycopg2.connect(**self.db_config)
        self.log.debug('Connection Opened')
        self.log.debug('Creating cursor')
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self.log.debug('Cursor retrieved')

    def __del__(self):
        if self.cur is not None:
            self.log.debug('Closing cursor')
            self.cur.close()
        if self.conn is not None:
            self.log.debug('Closing connection')
            self.conn.close()

    def execute_query(self, sql, query_vars):
        return self._execute(sql, query_vars)

    def execute_batch_query(self, sql, query_vars, page_size=None):
        return self._execute(sql, query_vars, is_batch=True, page_size=page_size)

    def execute_fetch_all(self, sql, query_vars):
        return self._execute(sql, query_vars, is_fetch=True)

    def mogrify_query(self, sql, query_vars):
        return self._execute(sql, query_vars, is_mogrify=True)

    def _execute(self, sql, query_vars, is_fetch=False, is_mogrify=False, is_batch=False, page_size=None):
        try:
            if (is_mogrify or is_batch) and not query_vars:
                raise ValueError('query_vars is required for mogrify and batch operations')
            if query_vars:
                if is_mogrify:
                    self.log.debug(f'Executing mogrify query: {sql}')
                    return self.cur.mogrify(sql, query_vars)
                elif is_batch:
                    self.log.debug(f'Executing batch query: {sql} with bound variables: {query_vars}')
                    if page_size:
                        execute_batch(self.cur, sql, query_vars, page_size=page_size)
                    else:
                        execute_batch(self.cur, sql, query_vars)
                else:
                    self.log.debug(f'Executing query: {sql} with bound variables: {query_vars}')
                    self.cur.execute(sql, query_vars)
            else:
                self.log.debug(f'Executing query: {sql}')
                self.cur.execute(sql)
            if is_fetch:
                self.log.debug(f'Fetching results')
                return self.cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            self._handle_error_db_error(error)
        else:
            if self.conn:
                self.conn.commit()

    def _handle_error_db_error(self, error):
        self.log.critical(msg='DB Exception occurred - rolling back', exc_info=True)
        if self.conn:
            self.conn.rollback()
