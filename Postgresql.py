# -*- coding: utf-8 -*-
__author__ = 'Palash Paul'

import time
from collections import namedtuple
import logging
import os
import datetime

from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor


class Database(object):
    _connection = None
    _cursor = None
    _log = None
    _log_fmt = None
    _cursor_factory = None
    _pool = None

    def __init__(self, pool, log=None, log_fmt=None, nt_cursor=True):
        self._log = log
        self._log_fmt = log_fmt
        self._cursor_factory = RealDictCursor if RealDictCursor else NamedTupleCursor if nt_cursor else DictCursor
        self._pool = pool
        self._connect()

    def _connect(self):
        """Connect to the postgres server"""
        try:
            self._connection = self._pool.get_conn()
            self._cursor = self._connection.cursor(cursor_factory=self._cursor_factory)
        except Exception as e:
            self._log_error('postgresql connection failed: ' + e)
            raise

    def _debug_write(self, msg):
        if msg and self._log:
            if isinstance(self._log, logging.Logger):
                self._log.debug(msg)
            else:
                self._log.write(msg + os.linesep)

    def _log_cursor(self, cursor):
        if not self._log:
            return

        if self._log_fmt:
            msg = self._log_fmt(cursor)
        else:
            msg = str(cursor.query)

        self._debug_write(msg)

    def _log_error(self, data):
        if not self._log:
            return

        if self._log_fmt:
            msg = self._log_fmt(data)
        else:
            msg = str(data)

        self._debug_write(msg)

    def fetchone(self, table, fields='*', where=None, order=None, offset=None):
        """Get a single result
            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
        """
        cur = self._select(table, fields, where, order, 1, offset)
        return cur.fetchone()

    def fetchall(self, table, fields='*', where=None, order=None, limit=None, offset=None):
        """Get all results
            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit, offset]
        """
        cur = self._select(table, fields, where, order, limit, offset)
        return cur.fetchall()

    def join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None, offset=None):
        """Run an inner left join query
            tables = (table1, table2)
            fields = ([fields from table1], [fields from table 2])  # fields to select
            join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """
        cur = self._join(tables, fields, join_fields, where, order, limit, offset)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple('Row', [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def insert_bulk(self, table, data, returning=None):
        cols, vals = self._format_insert_bulk(data)
        records_list_template = ','.join(['%s'] * len(vals))
        
        sql = 'INSERT INTO %s (%s) VALUES %s' % (table, cols, records_list_template)
        sql += self._returning(returning)
        cur = self.execute(sql, vals)
        return cur.fetchone() if returning else cur.rowcount
        
    def insert(self, table, data, returning=None):
        """Insert a record"""
        cols, vals = self._format_insert(data)
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table, cols, vals)
        sql += self._returning(returning)
        cur = self.execute(sql, list(data.values()))
        return cur.fetchone() if returning else cur.rowcount

    def update(self, table, data, where=None, returning=None):
        """Insert a record"""
        query = self._format_update(data)

        sql = 'UPDATE %s SET %s' % (table, query)
        sql += self._where(where) + self._returning(returning)
        cur = self.execute(sql, list(data.values()) + where[1] if where and len(where) > 1 else list(data.values()))
        return cur.fetchall() if returning else cur.rowcount

    def merge(self, table, data, conflict, returning=None):
        """Update a record"""
        cols, vals, conflt, update = self._format_merge(data,conflict)

        sql = 'INSERT INTO %s (%s) VALUES(%s) ON CONFLICT (%s) DO UPDATE SET %s' % (table, cols, vals, conflt, update )
        sql += self._returning(returning)
        cur = self.execute(sql, list(data.values()))
        return cur.fetchall() if returning else cur.rowcount
    
    def mergeupdate(self, table, data, conflict, update, returning=None):
        """Update a record"""
        cols, vals, conflt, update = self._format_merge_update(data,conflict,update)
        sql = 'INSERT INTO %s (%s) VALUES(%s) ON CONFLICT (%s) DO UPDATE SET %s ' % (table, cols, vals, conflt, update )
        sql += self._returning(returning)
        cur = self.execute(sql, list(data.values()))
        return cur.fetchone() if returning else cur.rowcount

    def delete(self, table, where=None, returning=None):
        """Delete rows based on a where condition"""
        sql = 'DELETE FROM %s' % table
        sql += self._where(where) + self._returning(returning)
        cur = self.execute(sql, where[1] if where and len(where) > 1 else None)
        return cur.fetchall() if returning else cur.rowcount
    
    def call(self, procedure_name, data=None):
        """call function based on a param condition"""
        if data is None:
            sql = 'CALL %s()' % (procedure_name)
            cur = self.execute(sql)
        else:
            vals = ",".join(["%s" for k in data])
            sql = 'CALL %s(%s)' % (procedure_name, vals)
            cur = self.execute(sql, list(data.values()))
    
    def callproc(self, procedure_name, data=None):
        """call function based on a param condition"""
        if data is None:
            sql = 'CALL %s()' % (procedure_name)
            cur = self.execute(sql)
            return cur.fetchall()
        else:
            vals = ",".join(["%s" for k in data])
            sql = 'CALL %s(%s)' % (procedure_name, vals)
            cur = self.execute(sql, list(data.values()))
            return cur.fetchall()
    
    def execute(self, sql, params=None):
        """Executes a raw query"""
        try:
            if self._log and self._log_fmt:
                self._cursor.timestamp = time.time()
            self._cursor.execute(sql, params)
            if self._log and self._log_fmt:
                self._log_cursor(self._cursor)
        except Exception as e:
            if self._log and self._log_fmt:
                self._log_error('execute() failed: ' + e)
            raise

        return self._cursor

    def truncate(self, table, restart_identity=False, cascade=False):
        """Truncate a table or set of tables
        db.truncate('tbl1')
        db.truncate('tbl1, tbl2')
        """
        sql = 'TRUNCATE %s'
        if restart_identity:
            sql += ' RESTART IDENTITY'
        if cascade:
            sql += ' CASCADE'
        self.execute(sql % table)

    def drop(self, table, cascade=False):
        """Drop a table"""
        sql = 'DROP TABLE IF EXISTS %s'
        if cascade:
            sql += ' CASCADE'
        self.execute(sql % table)

    def create(self, table, schema):
        """Create a table with the schema provided
        pg_db.create('my_table','id SERIAL PRIMARY KEY, name TEXT')"""
        self.execute('CREATE TABLE %s (%s)' % (table, schema))

    def commit(self):
        """Commit a transaction"""
        return self._connection.commit()

    def rollback(self):
        """Roll-back a transaction"""
        return self._connection.rollback()

    @property
    def is_open(self):
        """Check if the connection is open"""
        return self._connection.open

    def _format_insert(self, data):
        """Format insert dict values into strings"""
        cols = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])

        return cols, vals

    def _format_insert_bulk(self, data):
        """Format insert dict values into strings"""
        cols = ",".join(data[0].keys())
        arrval = []
        for d in data:
            vals = ",".join(["%s" for k in d])
            arrval.append(vals)

        return cols, arrval

    def _format_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _where(self, where=None):
        if where and len(where) > 0:
            return ' WHERE %s' % where[0]
        return ''
    
    def _format_merge(self, data,conflict):
        """Format update dict values into string"""
        def _typecast(v):
            if type(v) == int or type(v) == float:
                return v
            elif type(v) == str:
                return '\''+v+'\''
            elif type(v) == bool:
                return v
            elif isinstance(v, datetime.date):
                x = datetime.datetime.strftime(v, '%Y-%m-%d %H:%M:%S.%f')
                return 'to_timestamp(\''+ x + '\', ''\'yyyy-mm-dd hh24:mi:ss.%f''\')'
            
        cols = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])
        conflt = ",".join(conflict)
        
        import copy
        updt = copy.deepcopy(data)
               
        for col in conflict:
            if col in updt:
                del updt[col]
        
        sql=",".join([k+"="+str(_typecast(v)) for k,v in updt.items()])
        return cols, vals, conflt, sql
    
    def _format_merge_update(self, data,conflict, update):
        """Format update dict values into string"""
        def _typecast(v):
            if type(v) == int or type(v) == float:
                return v
            elif type(v) == str:
                return '\''+v+'\''
            elif type(v) == bool:
                return v
            elif isinstance(v, datetime.date):
                x = datetime.datetime.strftime(v, '%Y-%m-%d %H:%M:%S.%f')
                return 'to_timestamp(\''+ x + '\', ''\'yyyy-mm-dd hh24:mi:ss.%f''\')'
            
        cols = ",".join(data.keys())
        vals = ",".join(["%s" for k in data])
        conflt = ",".join(conflict)
        
        import copy
        updt = copy.deepcopy(data)
               
        for col in conflict:
            if col in updt:
                del updt[col]
        
        for col in list(updt.keys()):
            if col not in update:
                del updt[col]
        
        sql=",".join([k+"="+str(_typecast(v)) for k,v in updt.items()])
        return cols, vals, conflt, sql

    def _order(self, order=None):
        sql = ''
        if order:
            sql += ' ORDER BY %s' % order[0]

            if len(order) > 1:
                sql += ' %s' % order[1]
        return sql

    def _limit(self, limit):
        if limit:
            return ' LIMIT %d' % limit
        return ''

    def _offset(self, offset):
        if offset:
            return ' OFFSET %d' % offset
        return ''

    def _returning(self, returning):
        if returning:
            return ' RETURNING %s' % returning
        return ''

    def _select(self, table=None, fields=(), where=None, order=None, limit=None, offset=None):
        """Run a select query"""
        sql = 'SELECT %s FROM %s' % (",".join(fields), table) \
              + self._where(where) \
              + self._order(order) \
              + self._limit(limit) \
              + self._offset(offset)
        return self.execute(sql, where[1] if where and len(where) == 2 else None)

    def _join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None, offset=None):
        """Run an inner left join query"""

        fields = [tables[0] + "." + f for f in fields[0]] + [tables[1] + "." + f for f in fields[1]]

        sql = 'SELECT {0:s} FROM {1:s} LEFT JOIN {2:s} ON ({3:s} = {4:s})'.format(
            ','.join(fields),
            tables[0],
            tables[1],
            '{0}.{1}'.format(tables[0], join_fields[0]),
            '{0}.{1}'.format(tables[1], join_fields[1]))

        sql += self._where(where) + self._order(order) + self._limit(limit) + self._offset(offset)

        return self.execute(sql, where[1] if where and len(where) > 1 else None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not isinstance(exc_value, Exception):
            self._debug_write('Committing transaction')
            self.commit()
        else:
            self._debug_write('Rolling back transaction')
            self.rollback()

        self._cursor.close()

    def __del__(self):
        if self._connection:
            self._pool.put_conn(self._connection, fail_silently=True)