#! /usr/bin/python
# $Id$

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest

import sys

import pgdb

import dbapi20

# check whether the "with" statement is supported
no_with = sys.version_info[:2] < (2, 5)

# We need a database to test against.
# If LOCAL_PyGreSQL.py exists we will get our information from that.
# Otherwise we use the defaults.
dbname = 'dbapi20_test'
dbhost = ''
dbport = 5432
try:
    from LOCAL_PyGreSQL import *
except ImportError:
    pass


class test_PyGreSQL(dbapi20.DatabaseAPI20Test):

    driver = pgdb
    connect_args = ()
    connect_kw_args = {'database': dbname,
        'host': '%s:%d' % (dbhost or '', dbport or -1)}

    lower_func = 'lower'  # For stored procedure test

    def setUp(self):
        # Call superclass setUp in case this does something in the future
        dbapi20.DatabaseAPI20Test.setUp(self)
        try:
            con = self._connect()
            con.close()
        except pgdb.Error:  # try to create a missing database
            import pg
            try:  # first try to log in as superuser
                db = pg.DB('postgres', dbhost or None, dbport or -1,
                    user='postgres')
            except Exception:  # then try to log in as current user
                db = pg.DB('postgres', dbhost or None, dbport or -1)
            db.query('create database ' + dbname)


    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_row_factory(self):
        class myCursor(pgdb.pgdbCursor):
            def row_factory(self, row):
                d = {}
                for idx, col in enumerate(self.description):
                    d[col[0]] = row[idx]
                return d

        con = self._connect()
        cur = myCursor(con)
        ret = cur.execute("select 1 as a, 2 as b")
        self.assert_(ret is cur, 'execute() should return cursor')
        self.assertEqual(cur.fetchone(), {'a': 1, 'b': 2})

    def test_cursor_iteration(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1 union select 2 union select 3")
        self.assertEqual([r[0] for r in cur], [1, 2, 3])

    def test_fetch_2_rows(self):
        Decimal = pgdb.decimal_type()
        values = ['test', pgdb.Binary('\xff\x52\xb2'),
            True, 5, 6, 5.7, Decimal('234.234234'), Decimal('75.45'),
            '2011-07-17', '15:47:42', '2008-10-20 15:25:35', '15:31:05',
            7897234]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %s ("
                "stringtest varchar,"
                "binarytest bytea,"
                "booltest bool,"
                "integertest int4,"
                "longtest int8,"
                "floattest float8,"
                "numerictest numeric,"
                "moneytest money,"
                "datetest date,"
                "timetest time,"
                "datetimetest timestamp,"
                "intervaltest interval,"
                "rowidtest oid)" % table)
            for s in ('numeric', 'monetary', 'time'):
                cur.execute("set lc_%s to 'C'" % s)
            for _i in range(2):
                cur.execute("insert into %s values ("
                    "%%s,%%s,%%s,%%s,%%s,%%s,%%s,"
                    "'%%s'::money,%%s,%%s,%%s,%%s,%%s)" % table, values)
            cur.execute("select * from %s" % table)
            rows = cur.fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], values)
            self.assertEqual(rows[0], rows[1])
        finally:
            con.close()

    def test_sqlstate(self):
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute("select 1/0")
        except pgdb.DatabaseError, error:
            self.assert_(isinstance(error, pgdb.ProgrammingError))
            # the SQLSTATE error code for division by zero is 22012
            self.assertEqual(error.sqlstate, '22012')

    def test_float(self):
        try:
            nan = float('nan')
        except ValueError:  # Python < 2.6
            nan = 3.0e999 - 1.5e999999
        try:
            inf = float('inf')
        except ValueError:  # Python < 2.6
            inf = 3.0e999 * 1.5e999999
        try:
            from math import isnan, isinf
        except ImportError:  # Python < 2.6
            isnan = lambda x: x != x
            isinf = lambda x: not isnan(x) and isnan(x * 0)
        try:
            from math import isnan, isinf
        except ImportError:  # Python < 2.6
            isnan = lambda x: x != x
            isinf = lambda x: not isnan(x) and isnan(x * 0)
        self.assert_(isnan(nan) and not isinf(nan))
        self.assert_(isinf(inf) and not isnan(inf))
        values = [0, 1, 0.03125, -42.53125, nan, inf, -inf]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                "create table %s (n smallint, floattest float)" % table)
            params = enumerate(values)
            cur.executemany("insert into %s values(%%s,%%s)" % table, params)
            cur.execute("select * from %s order by 1" % table)
            rows = cur.fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[1] for row in rows]
        for inval, outval in zip(values, rows):
            if isinf(inval):
                self.assert_(isinf(outval))
                if inval < 0:
                    self.assert_(outval < 0)
                else:
                    self.assert_(outval > 0)
            elif isnan(inval):
                self.assert_(isnan(outval))
            else:
                self.assertEqual(inval, outval)

    def test_bool(self):
        values = [False, True, None, 't', 'f', 'true', 'false']
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                "create table %s (n smallint, booltest bool)" % table)
            params = enumerate(values)
            cur.executemany("insert into %s values (%%s,%%s)" % table, params)
            cur.execute("select * from %s order by 1" % table)
            rows = cur.fetchall()
        finally:
            con.close()
        rows = [row[1] for row in rows]
        values[3] = values[5] = True
        values[4] = values[6] = False
        self.assertEqual(rows, values)

    def test_set_decimal_type(self):
        decimal_type = pgdb.decimal_type()
        self.assert_(decimal_type is not None and callable(decimal_type))
        con = self._connect()
        try:
            cur = con.cursor()
            self.assert_(pgdb.decimal_type(int) is int)
            cur.execute('select 42')
            value = cur.fetchone()[0]
            self.assert_(isinstance(value, int))
            self.assertEqual(value, 42)
            self.assert_(pgdb.decimal_type(float) is float)
            cur.execute('select 4.25')
            value = cur.fetchone()[0]
            self.assert_(isinstance(value, float))
            self.assertEqual(value, 4.25)
        finally:
            con.close()
            pgdb.decimal_type(decimal_type)
        self.assert_(pgdb.decimal_type() is decimal_type)

    def test_nextset(self):
        con = self._connect()
        cur = con.cursor()
        self.assertRaises(con.NotSupportedError, cur.nextset)

    def test_setoutputsize(self):
        pass  # not supported

    def test_connection_errors(self):
        con = self._connect()
        self.assertEqual(con.Error, pgdb.Error)
        self.assertEqual(con.Warning, pgdb.Warning)
        self.assertEqual(con.InterfaceError, pgdb.InterfaceError)
        self.assertEqual(con.DatabaseError, pgdb.DatabaseError)
        self.assertEqual(con.InternalError, pgdb.InternalError)
        self.assertEqual(con.OperationalError, pgdb.OperationalError)
        self.assertEqual(con.ProgrammingError, pgdb.ProgrammingError)
        self.assertEqual(con.IntegrityError, pgdb.IntegrityError)
        self.assertEqual(con.DataError, pgdb.DataError)
        self.assertEqual(con.NotSupportedError, pgdb.NotSupportedError)

    @unittest.skipIf(no_with, 'context managers not supported')
    def test_connection_as_contextmanager(self):
        table = self.table_prefix + 'booze'
        con = self._connect()
        # wrap "with" statements to avoid SyntaxError in Python < 2.5
        exec """from __future__ import with_statement\nif True:
        try:
            cur = con.cursor()
            cur.execute("create table %s (n smallint check(n!=4))" % table)
            with con:
                cur.execute("insert into %s values (1)" % table)
                cur.execute("insert into %s values (2)" % table)
            try:
                with con:
                    cur.execute("insert into %s values (3)" % table)
                    cur.execute("insert into %s values (4)" % table)
            except con.ProgrammingError, error:
                self.assertTrue('check' in str(error).lower())
            with con:
                cur.execute("insert into %s values (5)" % table)
                cur.execute("insert into %s values (6)" % table)
            try:
                with con:
                    cur.execute("insert into %s values (7)" % table)
                    cur.execute("insert into %s values (8)" % table)
                    raise ValueError('transaction should rollback')
            except ValueError, error:
                self.assertEqual(str(error), 'transaction should rollback')
            with con:
                cur.execute("insert into %s values (9)" % table)
            cur.execute("select * from %s order by 1" % table)
            rows = cur.fetchall()
            rows = [row[0] for row in rows]
        finally:
            con.close()\n"""
        self.assertEqual(rows, [1, 2, 5, 6, 9])

    def test_cursor_connection(self):
        con = self._connect()
        cur = con.cursor()
        self.assertEqual(cur.connection, con)
        cur.close()

    @unittest.skipIf(no_with, 'context managers not supported')
    def test_cursor_as_contextmanager(self):
        con = self._connect()
        # wrap "with" statements to avoid SyntaxError in Python < 2.5
        exec """from __future__ import with_statement\nif True:
        with con.cursor() as cur:
            self.assertEqual(cur.connection, con)\n"""

    def test_pgdb_type(self):
        self.assertEqual(pgdb.STRING, pgdb.STRING)
        self.assertNotEqual(pgdb.STRING, pgdb.INTEGER)
        self.assertNotEqual(pgdb.STRING, pgdb.BOOL)
        self.assertNotEqual(pgdb.BOOL, pgdb.INTEGER)
        self.assertEqual(pgdb.INTEGER, pgdb.INTEGER)
        self.assertNotEqual(pgdb.INTEGER, pgdb.NUMBER)
        self.assertEqual('char', pgdb.STRING)
        self.assertEqual('varchar', pgdb.STRING)
        self.assertEqual('text', pgdb.STRING)
        self.assertNotEqual('numeric', pgdb.STRING)
        self.assertEqual('numeric', pgdb.NUMERIC)
        self.assertEqual('numeric', pgdb.NUMBER)
        self.assertEqual('int4', pgdb.NUMBER)
        self.assertNotEqual('int4', pgdb.NUMERIC)
        self.assertEqual('int2', pgdb.SMALLINT)
        self.assertNotEqual('int4', pgdb.SMALLINT)
        self.assertEqual('int2', pgdb.INTEGER)
        self.assertEqual('int4', pgdb.INTEGER)
        self.assertEqual('int8', pgdb.INTEGER)
        self.assertNotEqual('int4', pgdb.LONG)
        self.assertEqual('int8', pgdb.LONG)
        self.assert_('char' in pgdb.STRING)
        self.assert_(pgdb.NUMERIC <= pgdb.NUMBER)
        self.assert_(pgdb.NUMBER >= pgdb.INTEGER)
        self.assert_(pgdb.TIME <= pgdb.DATETIME)
        self.assert_(pgdb.DATETIME >= pgdb.DATE)


if __name__ == '__main__':
    unittest.main()
