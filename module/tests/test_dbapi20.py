#! /usr/bin/python
# -*- coding: utf-8 -*-
# $Id$

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest

import pgdb

try:
    from . import dbapi20
except (ImportError, ValueError, SystemError):
    import dbapi20

# We need a database to test against.
# If LOCAL_PyGreSQL.py exists we will get our information from that.
# Otherwise we use the defaults.
dbname = 'dbapi20_test'
dbhost = ''
dbport = 5432
try:
    from .LOCAL_PyGreSQL import *
except (ImportError, ValueError):
    try:
        from LOCAL_PyGreSQL import *
    except ImportError:
        pass

try:
    long
except NameError:  # Python >= 3.0
    long = int

try:
    from collections import OrderedDict
except ImportError:  # Python 2.6 or 3.0
    OrderedDict = None


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

    def test_cursor_type(self):

        class TestCursor(pgdb.Cursor):
            pass

        con = self._connect()
        self.assertIs(con.cursor_type, pgdb.Cursor)
        cur = con.cursor()
        self.assertIsInstance(cur, pgdb.Cursor)
        self.assertNotIsInstance(cur, TestCursor)
        con.cursor_type = TestCursor
        cur = con.cursor()
        self.assertIsInstance(cur, TestCursor)
        cur = con.cursor()
        self.assertIsInstance(cur, TestCursor)
        con = self._connect()
        self.assertIs(con.cursor_type, pgdb.Cursor)
        cur = con.cursor()
        self.assertIsInstance(cur, pgdb.Cursor)
        self.assertNotIsInstance(cur, TestCursor)

    def test_row_factory(self):

        class TestCursor(pgdb.Cursor):

            def row_factory(self, row):
                return dict(('column %s' % desc[0], value)
                    for desc, value in zip(self.description, row))

        con = self._connect()
        con.cursor_type = TestCursor
        cur = con.cursor()
        self.assertIsInstance(cur, TestCursor)
        res = cur.execute("select 1 as a, 2 as b")
        self.assertIs(res, cur, 'execute() should return cursor')
        res = cur.fetchone()
        self.assertIsInstance(res, dict)
        self.assertEqual(res, {'column a': 1, 'column b': 2})
        cur.execute("select 1 as a, 2 as b union select 3, 4 order by 1")
        res = cur.fetchall()
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertIsInstance(res[0], dict)
        self.assertEqual(res[0], {'column a': 1, 'column b': 2})
        self.assertIsInstance(res[1], dict)
        self.assertEqual(res[1], {'column a': 3, 'column b': 4})

    def test_build_row_factory(self):

        class TestCursor(pgdb.Cursor):

            def build_row_factory(self):
                keys = [desc[0] for desc in self.description]
                return lambda row: dict((key, value)
                    for key, value in zip(keys, row))

        con = self._connect()
        con.cursor_type = TestCursor
        cur = con.cursor()
        self.assertIsInstance(cur, TestCursor)
        cur.execute("select 1 as a, 2 as b")
        res = cur.fetchone()
        self.assertIsInstance(res, dict)
        self.assertEqual(res, {'a': 1, 'b': 2})
        cur.execute("select 1 as a, 2 as b union select 3, 4 order by 1")
        res = cur.fetchall()
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertIsInstance(res[0], dict)
        self.assertEqual(res[0], {'a': 1, 'b': 2})
        self.assertIsInstance(res[1], dict)
        self.assertEqual(res[1], {'a': 3, 'b': 4})

    def test_cursor_with_named_columns(self):
        con = self._connect()
        cur = con.cursor()
        res = cur.execute("select 1 as abc, 2 as de, 3 as f")
        self.assertIs(res, cur, 'execute() should return cursor')
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3))
        self.assertEqual(res._fields, ('abc', 'de', 'f'))
        self.assertEqual(res.abc, 1)
        self.assertEqual(res.de, 2)
        self.assertEqual(res.f, 3)
        cur.execute("select 1 as one, 2 as two union select 3, 4 order by 1")
        res = cur.fetchall()
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertIsInstance(res[0], tuple)
        self.assertEqual(res[0], (1, 2))
        self.assertEqual(res[0]._fields, ('one', 'two'))
        self.assertIsInstance(res[1], tuple)
        self.assertEqual(res[1], (3, 4))
        self.assertEqual(res[1]._fields, ('one', 'two'))

    def test_cursor_with_unnamed_columns(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1, 2, 3")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3))
        old_py = OrderedDict is None  # Python 2.6 or 3.0
        # old Python versions cannot rename tuple fields with underscore
        if old_py:
            self.assertEqual(res._fields, ('column_0', 'column_1', 'column_2'))
        else:
            self.assertEqual(res._fields, ('_0', '_1', '_2'))
        cur.execute("select 1 as one, 2, 3 as three")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3))
        if old_py:  # cannot auto rename with underscore
            self.assertEqual(res._fields, ('one', 'column_1', 'three'))
        else:
            self.assertEqual(res._fields, ('one', '_1', 'three'))
        cur.execute("select 1 as abc, 2 as def")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2))
        if old_py:
            self.assertEqual(res._fields, ('column_0', 'column_1'))
        else:
            self.assertEqual(res._fields, ('abc', '_1'))

    def test_colnames(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1, 2, 3")
        names = cur.colnames
        self.assertIsInstance(names, list)
        self.assertEqual(names, ['?column?', '?column?', '?column?'])
        cur.execute("select 1 as a, 2 as bc, 3 as def, 4 as g")
        names = cur.colnames
        self.assertIsInstance(names, list)
        self.assertEqual(names, ['a', 'bc', 'def', 'g'])

    def test_coltypes(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1::int2, 2::int4, 3::int8")
        types = cur.coltypes
        self.assertIsInstance(types, list)
        self.assertEqual(types, ['int2', 'int4', 'int8'])

    def test_description_fields(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 123456789::int8 as col")
        desc = cur.description
        self.assertIsInstance(desc, list)
        self.assertEqual(len(desc), 1)
        desc = desc[0]
        self.assertIsInstance(desc, tuple)
        self.assertEqual(len(desc), 7)
        self.assertEqual(desc.name, 'col')
        self.assertEqual(desc.type_code, 'int8')
        self.assertIsNone(desc.display_size)
        self.assertIsInstance(desc.internal_size, int)
        self.assertEqual(desc.internal_size, 8)
        self.assertIsNone(desc.precision)
        self.assertIsNone(desc.scale)
        self.assertIsNone(desc.null_ok)

    def test_cursor_iteration(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1 union select 2 union select 3")
        self.assertEqual([r[0] for r in cur], [1, 2, 3])

    def test_fetch_2_rows(self):
        Decimal = pgdb.decimal_type()
        values = ('test', pgdb.Binary(b'\xff\x52\xb2'),
            True, 5, 6, 5.7, Decimal('234.234234'), Decimal('75.45'),
            '2011-07-17', '15:47:42', '2008-10-20 15:25:35', '15:31:05',
            7897234)
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("set datestyle to 'iso'")
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
            row0 = rows[0]
            self.assertEqual(row0, values)
            self.assertEqual(row0, rows[1])
            self.assertIsInstance(row0[0], str)
            self.assertIsInstance(row0[1], bytes)
            self.assertIsInstance(row0[2], bool)
            self.assertIsInstance(row0[3], int)
            self.assertIsInstance(row0[4], long)
            self.assertIsInstance(row0[5], float)
            self.assertIsInstance(row0[6], Decimal)
            self.assertIsInstance(row0[7], Decimal)
            self.assertIsInstance(row0[8], str)
            self.assertIsInstance(row0[9], str)
            self.assertIsInstance(row0[10], str)
            self.assertIsInstance(row0[11], str)
        finally:
            con.close()

    def test_sqlstate(self):
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute("select 1/0")
        except pgdb.DatabaseError as error:
            self.assertTrue(isinstance(error, pgdb.ProgrammingError))
            # the SQLSTATE error code for division by zero is 22012
            self.assertEqual(error.sqlstate, '22012')

    def test_float(self):
        nan, inf = float('nan'), float('inf')
        from math import isnan, isinf
        self.assertTrue(isnan(nan) and not isinf(nan))
        self.assertTrue(isinf(inf) and not isnan(inf))
        values = [0, 1, 0.03125, -42.53125, nan, inf, -inf]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                "create table %s (n smallint, floattest float)" % table)
            params = enumerate(values)
            cur.executemany("insert into %s values (%%s,%%s)" % table, params)
            cur.execute("select * from %s order by 1" % table)
            rows = cur.fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[1] for row in rows]
        for inval, outval in zip(values, rows):
            if isinf(inval):
                self.assertTrue(isinf(outval))
                if inval < 0:
                    self.assertTrue(outval < 0)
                else:
                    self.assertTrue(outval > 0)
            elif isnan(inval):
                self.assertTrue(isnan(outval))
            else:
                self.assertEqual(inval, outval)

    def test_set_decimal_type(self):
        decimal_type = pgdb.decimal_type()
        self.assertTrue(decimal_type is not None and callable(decimal_type))
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertTrue(pgdb.decimal_type(int) is int)
            cur.execute('select 42')
            value = cur.fetchone()[0]
            self.assertTrue(isinstance(value, int))
            self.assertEqual(value, 42)
            self.assertTrue(pgdb.decimal_type(float) is float)
            cur.execute('select 4.25')
            value = cur.fetchone()[0]
            self.assertTrue(isinstance(value, float))
            self.assertEqual(value, 4.25)
        finally:
            con.close()
            pgdb.decimal_type(decimal_type)
        self.assertTrue(pgdb.decimal_type() is decimal_type)

    def test_unicode_with_utf8(self):
        table = self.table_prefix + 'booze'
        input = u"He wes Leovenaðes sone — liðe him be Drihten"
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %s (t text)" % table)
            try:
                cur.execute("set client_encoding=utf8")
                cur.execute(u"select '%s'" % input)
            except Exception:
                self.skipTest("database does not support utf8")
            output1 = cur.fetchone()[0]
            cur.execute("insert into %s values (%%s)" % table, (input,))
            cur.execute("select * from %s" % table)
            output2 = cur.fetchone()[0]
            cur.execute("select t = '%s' from %s" % (input, table))
            output3 = cur.fetchone()[0]
            cur.execute("select t = %%s from %s" % table, (input,))
            output4 = cur.fetchone()[0]
        finally:
            con.close()
        if str is bytes:  # Python < 3.0
            input = input.encode('utf8')
        self.assertIsInstance(output1, str)
        self.assertEqual(output1, input)
        self.assertIsInstance(output2, str)
        self.assertEqual(output2, input)
        self.assertIsInstance(output3, bool)
        self.assertTrue(output3)
        self.assertIsInstance(output4, bool)
        self.assertTrue(output4)

    def test_unicode_with_latin1(self):
        table = self.table_prefix + 'booze'
        input = u"Ehrt den König seine Würde, ehret uns der Hände Fleiß."
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %s (t text)" % table)
            try:
                cur.execute("set client_encoding=latin1")
                cur.execute(u"select '%s'" % input)
            except Exception:
                self.skipTest("database does not support latin1")
            output1 = cur.fetchone()[0]
            cur.execute("insert into %s values (%%s)" % table, (input,))
            cur.execute("select * from %s" % table)
            output2 = cur.fetchone()[0]
            cur.execute("select t = '%s' from %s" % (input, table))
            output3 = cur.fetchone()[0]
            cur.execute("select t = %%s from %s" % table, (input,))
            output4 = cur.fetchone()[0]
        finally:
            con.close()
        if str is bytes:  # Python < 3.0
            input = input.encode('latin1')
        self.assertIsInstance(output1, str)
        self.assertEqual(output1, input)
        self.assertIsInstance(output2, str)
        self.assertEqual(output2, input)
        self.assertIsInstance(output3, bool)
        self.assertTrue(output3)
        self.assertIsInstance(output4, bool)
        self.assertTrue(output4)

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

    def test_connection_as_contextmanager(self):
        table = self.table_prefix + 'booze'
        con = self._connect()
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
            except con.ProgrammingError as error:
                self.assertTrue('check' in str(error).lower())
            with con:
                cur.execute("insert into %s values (5)" % table)
                cur.execute("insert into %s values (6)" % table)
            try:
                with con:
                    cur.execute("insert into %s values (7)" % table)
                    cur.execute("insert into %s values (8)" % table)
                    raise ValueError('transaction should rollback')
            except ValueError as error:
                self.assertEqual(str(error), 'transaction should rollback')
            with con:
                cur.execute("insert into %s values (9)" % table)
            cur.execute("select * from %s order by 1" % table)
            rows = cur.fetchall()
            rows = [row[0] for row in rows]
        finally:
            con.close()
        self.assertEqual(rows, [1, 2, 5, 6, 9])

    def test_cursor_connection(self):
        con = self._connect()
        cur = con.cursor()
        self.assertEqual(cur.connection, con)
        cur.close()

    def test_cursor_as_contextmanager(self):
        con = self._connect()
        with con.cursor() as cur:
            self.assertEqual(cur.connection, con)

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
        self.assertTrue('char' in pgdb.STRING)
        self.assertTrue(pgdb.NUMERIC <= pgdb.NUMBER)
        self.assertTrue(pgdb.NUMBER >= pgdb.INTEGER)
        self.assertTrue(pgdb.TIME <= pgdb.DATETIME)
        self.assertTrue(pgdb.DATETIME >= pgdb.DATE)


if __name__ == '__main__':
    unittest.main()
