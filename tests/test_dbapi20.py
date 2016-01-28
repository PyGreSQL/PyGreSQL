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

from datetime import datetime

try:
    long
except NameError:  # Python >= 3.0
    long = int

try:
    from collections import OrderedDict
except ImportError:  # Python 2.6 or 3.0
    OrderedDict = None


class PgBitString:
    """Test object with a PostgreSQL representation as Bit String."""

    def __init__(self, value):
        self.value = value

    def __pg_repr__(self):
         return "B'{0:b}'".format(self.value)


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

    def test_callproc_no_params(self):
        con = self._connect()
        cur = con.cursor()
        # note that now() does not change within a transaction
        cur.execute('select now()')
        now = cur.fetchone()[0]
        res = cur.callproc('now')
        self.assertIsNone(res)
        res = cur.fetchone()[0]
        self.assertEqual(res, now)

    def test_callproc_bad_params(self):
        con = self._connect()
        cur = con.cursor()
        self.assertRaises(TypeError, cur.callproc, 'lower', 42)
        self.assertRaises(pgdb.ProgrammingError, cur.callproc, 'lower', (42,))

    def test_callproc_one_param(self):
        con = self._connect()
        cur = con.cursor()
        params = (42.4382,)
        res = cur.callproc("round", params)
        self.assertIs(res, params)
        res = cur.fetchone()[0]
        self.assertEqual(res, 42)

    def test_callproc_two_params(self):
        con = self._connect()
        cur = con.cursor()
        params = (9, 4)
        res = cur.callproc("div", params)
        self.assertIs(res, params)
        res = cur.fetchone()[0]
        self.assertEqual(res, 2)

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
        cur.execute("select 123456789::int8 col0,"
            " 123456.789::numeric(41, 13) as col1,"
            " 'foobar'::char(39) as col2")
        desc = cur.description
        self.assertIsInstance(desc, list)
        self.assertEqual(len(desc), 3)
        cols = [('int8', 8, None), ('numeric', 41, 13), ('bpchar', 39, None)]
        for i in range(3):
            c, d = cols[i], desc[i]
            self.assertIsInstance(d, tuple)
            self.assertEqual(len(d), 7)
            self.assertIsInstance(d.name, str)
            self.assertEqual(d.name, 'col%d' % i)
            self.assertIsInstance(d.type_code, str)
            self.assertEqual(d.type_code, c[0])
            self.assertIsNone(d.display_size)
            self.assertIsInstance(d.internal_size, int)
            self.assertEqual(d.internal_size, c[1])
            if c[2] is not None:
                self.assertIsInstance(d.precision, int)
                self.assertEqual(d.precision, c[1])
                self.assertIsInstance(d.scale, int)
                self.assertEqual(d.scale, c[2])
            else:
                self.assertIsNone(d.precision)
                self.assertIsNone(d.scale)
            self.assertIsNone(d.null_ok)

    def test_type_cache(self):
        con = self._connect()
        cur = con.cursor()
        type_cache = con.type_cache
        self.assertNotIn('numeric', type_cache)
        type_info = type_cache['numeric']
        self.assertIn('numeric', type_cache)
        self.assertEqual(type_info, 'numeric')
        self.assertEqual(type_info.oid, 1700)
        self.assertEqual(type_info.type, 'b')  # base
        self.assertEqual(type_info.category, 'N')  # numeric
        self.assertEqual(type_info.delim, ',')
        self.assertIs(con.type_cache[1700], type_info)
        self.assertNotIn('pg_type', type_cache)
        type_info = type_cache['pg_type']
        self.assertIn('numeric', type_cache)
        self.assertEqual(type_info.type, 'c')  # composite
        self.assertEqual(type_info.category, 'C')  # composite
        cols = type_cache.columns('pg_type')
        self.assertEqual(cols[0].name, 'typname')
        typname = type_cache[cols[0].type]
        self.assertEqual(typname, 'name')
        self.assertEqual(typname.type, 'b')  # base
        self.assertEqual(typname.category, 'S')  # string
        self.assertEqual(cols[3].name, 'typlen')
        typlen = type_cache[cols[3].type]
        self.assertEqual(typlen, 'int2')
        self.assertEqual(typlen.type, 'b')  # base
        self.assertEqual(typlen.category, 'N')  # numeric
        cur.close()
        cur = con.cursor()
        type_cache = con.type_cache
        self.assertIn('numeric', type_cache)
        cur.close()
        con.close()
        con = self._connect()
        cur = con.cursor()
        type_cache = con.type_cache
        self.assertNotIn('pg_type', type_cache)
        self.assertEqual(type_cache.get('pg_type'), type_info)
        self.assertIn('pg_type', type_cache)
        self.assertIsNone(type_cache.get(
            self.table_prefix + '_surely_does_not_exist'))
        cur.close()
        con.close()

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
            cur.execute("set standard_conforming_strings to on")
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
        values = [0, 1, 0.03125, -42.53125, nan, inf, -inf,
            'nan', 'inf', '-inf', 'NaN', 'Infinity', '-Infinity']
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                "create table %s (n smallint, floattest float)" % table)
            params = enumerate(values)
            cur.executemany("insert into %s values (%%d,%%s)" % table, params)
            cur.execute("select floattest from %s order by n" % table)
            rows = cur.fetchall()
            self.assertEqual(cur.description[0].type_code, pgdb.FLOAT)
            self.assertNotEqual(cur.description[0].type_code, pgdb.ARRAY)
            self.assertNotEqual(cur.description[0].type_code, pgdb.RECORD)
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[0] for row in rows]
        for inval, outval in zip(values, rows):
            if inval in ('inf', 'Infinity'):
                inval = inf
            elif inval in ('-inf', '-Infinity'):
                inval = -inf
            elif inval in ('nan', 'NaN'):
                inval = nan
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

    def test_datetime(self):
        values = ['2011-07-17 15:47:42', datetime(2016, 1, 20, 20, 15, 51)]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("set datestyle to 'iso'")
            cur.execute(
                "create table %s (n smallint, ts timestamp)" % table)
            params = enumerate(values)
            cur.executemany("insert into %s values (%%d,%%s)" % table, params)
            cur.execute("select ts from %s order by n" % table)
            rows = cur.fetchall()
            self.assertEqual(cur.description[0].type_code, pgdb.DATETIME)
            self.assertNotEqual(cur.description[0].type_code, pgdb.ARRAY)
            self.assertNotEqual(cur.description[0].type_code, pgdb.RECORD)
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[0] for row in rows]
        for inval, outval in zip(values, rows):
            if isinstance(inval, datetime):
                inval = inval.strftime('%Y-%m-%d %H:%M:%S')
            self.assertEqual(inval, outval)

    def test_insert_array(self):
        values = [(None, None), ([], []), ([None], [[None], ['null']]),
            ([1, 2, 3], [['a', 'b'], ['c', 'd']]),
            ([20000, 25000, 25000, 30000],
            [['breakfast', 'consulting'], ['meeting', 'lunch']]),
            ([0, 1, -1], [['Hello, World!', '"Hi!"'], ['{x,y}', ' x y ']])]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %s"
                " (n smallint, i int[], t text[][])" % table)
            params = [(n, v[0], v[1]) for n, v in enumerate(values)]
            # Note that we must explicit casts because we are inserting
            # empty arrays.  Otherwise this is not necessary.
            cur.executemany("insert into %s values"
                " (%%d,%%s::int[],%%s::text[][])" % table, params)
            cur.execute("select i, t from %s order by n" % table)
            d = cur.description
            self.assertEqual(d[0].type_code, pgdb.ARRAY)
            self.assertNotEqual(d[0].type_code, pgdb.RECORD)
            self.assertEqual(d[0].type_code, pgdb.NUMBER)
            self.assertEqual(d[0].type_code, pgdb.INTEGER)
            self.assertEqual(d[1].type_code, pgdb.ARRAY)
            self.assertNotEqual(d[1].type_code, pgdb.RECORD)
            self.assertEqual(d[1].type_code, pgdb.STRING)
            rows = cur.fetchall()
        finally:
            con.close()
        self.assertEqual(rows, values)

    def test_select_array(self):
        values = ([1, 2, 3, None], ['a', 'b', 'c', None])
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select %s::int[], %s::text[]", values)
            row = cur.fetchone()
        finally:
            con.close()
        self.assertEqual(row, values)

    def test_insert_record(self):
        values = [('John', 61), ('Jane', 63),
                  ('Fred', None), ('Wilma', None),
                  (None, 42), (None, None)]
        table = self.table_prefix + 'booze'
        record = self.table_prefix + 'munch'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create type %s as (name varchar, age int)" % record)
            cur.execute("create table %s (n smallint, r %s)" % (table, record))
            params = enumerate(values)
            cur.executemany("insert into %s values (%%d,%%s)" % table, params)
            cur.execute("select r from %s order by n" % table)
            type_code = cur.description[0].type_code
            self.assertEqual(type_code, record)
            self.assertEqual(type_code, pgdb.RECORD)
            self.assertNotEqual(type_code, pgdb.ARRAY)
            columns = con.type_cache.columns(type_code)
            self.assertEqual(columns[0].name, 'name')
            self.assertEqual(columns[1].name, 'age')
            self.assertEqual(con.type_cache[columns[0].type], 'varchar')
            self.assertEqual(con.type_cache[columns[1].type], 'int4')
            rows = cur.fetchall()
        finally:
            cur.execute('drop table %s' % table)
            cur.execute('drop type %s' % record)
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[0] for row in rows]
        self.assertEqual(rows, values)
        self.assertEqual(rows[0].name, 'John')
        self.assertEqual(rows[0].age, 61)

    def test_select_record(self):
        value = (1, 25000, 2.5, 'hello', 'Hello World!', 'Hello, World!',
            '(test)', '(x,y)', ' x y ', 'null', None)
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select %s as test_record", [value])
            self.assertEqual(cur.description[0].name, 'test_record')
            self.assertEqual(cur.description[0].type_code, 'record')
            row = cur.fetchone()[0]
        finally:
            con.close()
        # Note that the element types get lost since we created an
        # untyped record (an anonymous composite type). For the same
        # reason this is also a normal tuple, not a named tuple.
        text_row = tuple(None if v is None else str(v) for v in value)
        self.assertEqual(row, text_row)

    def test_custom_type(self):
        values = [3, 5, 65]
        values = list(map(PgBitString, values))
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            params = enumerate(values)  # params have __pg_repr__ method
            cur.execute(
                'create table "%s" (n smallint, b bit varying(7))' % table)
            cur.executemany("insert into %s values (%%s,%%s)" % table, params)
            cur.execute("select * from %s" % table)
            rows = cur.fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        con = self._connect()
        try:
            cur = con.cursor()
            params = (1, object())  # an object that cannot be handled
            self.assertRaises(pgdb.InterfaceError, cur.execute,
                "insert into %s values (%%s,%%s)" % table, params)
        finally:
            con.close()

    def test_set_decimal_type(self):
        decimal_type = pgdb.decimal_type()
        self.assertTrue(decimal_type is not None and callable(decimal_type))
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertTrue(pgdb.decimal_type(int) is int)
            cur.execute('select 42')
            self.assertEqual(cur.description[0].type_code, pgdb.INTEGER)
            value = cur.fetchone()[0]
            self.assertTrue(isinstance(value, int))
            self.assertEqual(value, 42)
            self.assertTrue(pgdb.decimal_type(float) is float)
            cur.execute('select 4.25')
            self.assertEqual(cur.description[0].type_code, pgdb.NUMBER)
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
            cur.execute("select booltest from %s order by n" % table)
            rows = cur.fetchall()
            self.assertEqual(cur.description[0].type_code, pgdb.BOOL)
        finally:
            con.close()
        rows = [row[0] for row in rows]
        values[3] = values[5] = True
        values[4] = values[6] = False
        self.assertEqual(rows, values)

    def test_json(self):
        inval = {"employees":
            [{"firstName": "John", "lastName": "Doe", "age": 61}]}
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute("create table %s (jsontest json)" % table)
            except pgdb.ProgrammingError:
                self.skipTest('database does not support json')
            params = (pgdb.Json(inval),)
            cur.execute("insert into %s values (%%s)" % table, params)
            cur.execute("select jsontest from %s" % table)
            outval = cur.fetchone()[0]
            self.assertEqual(cur.description[0].type_code, pgdb.JSON)
        finally:
            con.close()
        self.assertEqual(inval, outval)

    def test_jsonb(self):
        inval = {"employees":
            [{"firstName": "John", "lastName": "Doe", "age": 61}]}
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute("create table %s (jsonbtest jsonb)" % table)
            except pgdb.ProgrammingError:
                self.skipTest('database does not support jsonb')
            params = (pgdb.Json(inval),)
            cur.execute("insert into %s values (%%s)" % table, params)
            cur.execute("select jsonbtest from %s" % table)
            outval = cur.fetchone()[0]
            self.assertEqual(cur.description[0].type_code, pgdb.JSON)
        finally:
            con.close()
        self.assertEqual(inval, outval)

    def test_execute_edge_cases(self):
        con = self._connect()
        try:
            cur = con.cursor()
            sql = 'invalid'  # should be ignored with empty parameter list
            cur.executemany(sql, [])
            sql = 'select %d + 1'
            cur.execute(sql, [(1,), (2,)])  # deprecated use of execute()
            self.assertEqual(cur.fetchone()[0], 3)
            sql = 'select 1/0'  # cannot be executed
            self.assertRaises(pgdb.ProgrammingError, cur.execute, sql)
            cur.close()
            con.rollback()
            if pgdb.shortcutmethods:
                res = con.execute('select %d', (1,)).fetchone()
                self.assertEqual(res, (1,))
                res = con.executemany('select %d', [(1,), (2,)]).fetchone()
                self.assertEqual(res, (2,))
        finally:
            con.close()
        sql = 'select 1'  # cannot be executed after connection is closed
        self.assertRaises(pgdb.OperationalError, cur.execute, sql)

    def test_fetchmany_with_keep(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertEqual(cur.arraysize, 1)
            cur.execute('select * from generate_series(1, 25)')
            self.assertEqual(len(cur.fetchmany()), 1)
            self.assertEqual(len(cur.fetchmany()), 1)
            self.assertEqual(cur.arraysize, 1)
            cur.arraysize = 3
            self.assertEqual(len(cur.fetchmany()), 3)
            self.assertEqual(len(cur.fetchmany()), 3)
            self.assertEqual(cur.arraysize, 3)
            self.assertEqual(len(cur.fetchmany(size=2)), 2)
            self.assertEqual(cur.arraysize, 3)
            self.assertEqual(len(cur.fetchmany()), 3)
            self.assertEqual(len(cur.fetchmany()), 3)
            self.assertEqual(len(cur.fetchmany(size=2, keep=True)), 2)
            self.assertEqual(cur.arraysize, 2)
            self.assertEqual(len(cur.fetchmany()), 2)
            self.assertEqual(len(cur.fetchmany()), 2)
            self.assertEqual(len(cur.fetchmany(25)), 3)
        finally:
            con.close()

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
        self.assertEqual(pgdb.ARRAY, pgdb.ARRAY)
        self.assertNotEqual(pgdb.ARRAY, pgdb.STRING)
        self.assertEqual('_char', pgdb.ARRAY)
        self.assertNotEqual('char', pgdb.ARRAY)
        self.assertEqual(pgdb.RECORD, pgdb.RECORD)
        self.assertNotEqual(pgdb.RECORD, pgdb.STRING)
        self.assertNotEqual(pgdb.RECORD, pgdb.ARRAY)
        self.assertEqual('record', pgdb.RECORD)
        self.assertNotEqual('_record', pgdb.RECORD)


if __name__ == '__main__':
    unittest.main()
