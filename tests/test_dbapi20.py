#!/usr/bin/python

from __future__ import annotations

import gc
import unittest
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from typing import Any, ClassVar
from uuid import UUID as Uuid  # noqa: N811

import pgdb

from . import dbapi20
from .config import dbhost, dbname, dbpasswd, dbport, dbuser


class PgBitString:
    """Test object with a PostgreSQL representation as Bit String."""

    def __init__(self, value):
        self.value = value

    def __pg_repr__(self):
        return f"B'{self.value:b}'"


class TestPgDb(dbapi20.DatabaseAPI20Test):

    driver = pgdb
    connect_args = ()
    connect_kw_args: ClassVar[dict[str, Any]] = {
        'database': dbname, 'host': f"{dbhost or ''}:{dbport or -1}",
        'user': dbuser, 'password': dbpasswd}

    lower_func = 'lower'  # For stored procedure test

    def setUp(self):
        super().setUp()
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
        super().tearDown()

    def test_version(self):
        v = pgdb.version
        self.assertIsInstance(v, str)
        self.assertIn('.', v)
        self.assertEqual(pgdb.__version__, v)

    def _check_application_name(self, application_name: str) -> None:
        kwargs = {**self.connect_kw_args,
                  'application_name': application_name}
        con = pgdb.connect(**kwargs)
        try:
            cur = con.cursor()
            cur.execute("select current_setting('application_name')")
            self.assertEqual(cur.fetchone(), (application_name,))
        finally:
            con.close()

    def test_connect_kwargs(self):
        self._check_application_name('PyGreSQL DB API 2.0 Test')

    def test_connect_kwargs_with_special_chars(self):
        self._check_application_name(
            'Single \' and double " quote and \\ backslash!')

    def test_connect_kwargs_with_special_chars_without_space(self):
        self._check_application_name("it's")
        self._check_application_name('back\\slash')
        self._check_application_name("'")
        self._check_application_name('\\')
        self._check_application_name('a=b')

    def test_connect_kwargs_with_empty_value(self):
        self._check_application_name('')

    def test_connect_kwargs_with_special_dbname(self):
        special_dbname = 'funny"db\'with\\quotes'
        kwargs = {**self.connect_kw_args,
                  'database': special_dbname, 'application_name': 'test'}
        with self.assertRaises(pgdb.Error) as cm:
            pgdb.connect(**kwargs)
        # the name must arrive unchanged at the server
        self.assertIn(f'"{special_dbname}"', str(cm.exception))

    def test_percent_sign(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select %s, 'a %% sign'", ('a % sign',))
        self.assertEqual(cur.fetchone(), ('a % sign', 'a % sign'))
        cur.execute("select 'a % sign'")
        self.assertEqual(cur.fetchone(), ('a % sign',))
        cur.execute("select 'a %% sign'")
        self.assertEqual(cur.fetchone(), ('a % sign',))

    def test_paramstyles(self):
        self.assertEqual(pgdb.paramstyle, 'pyformat')
        con = self._connect()
        cur = con.cursor()
        # parameters can be passed as tuple
        cur.execute("select %s, %s, %s", (123, 'abc', True))
        self.assertEqual(cur.fetchone(), (123, 'abc', True))
        # parameters can be passed as dict
        cur.execute("select %(one)s, %(two)s, %(one)s, %(three)s", {
            "one": 123, "two": "abc", "three": True
        })
        self.assertEqual(cur.fetchone(), (123, 'abc', 123, True))

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

    def test_callproc_qualified_name(self):
        con = self._connect()
        cur = con.cursor()
        params = ('ABC',)
        res = cur.callproc('pg_catalog.lower', params)
        self.assertIs(res, params)
        self.assertEqual(cur.fetchone(), ('abc',))
        # parts of the name are taken literally, also with special chars
        cur.execute('create function pg_temp."My.%s\'""Func"(a int)'
                    ' returns int language sql as $$ select a + 1 $$')
        res = cur.callproc('pg_temp."My.%s\'""Func"', (1,))
        self.assertEqual(res, (1,))
        self.assertEqual(cur.fetchone(), (2,))
        self.assertRaises(
            pgdb.ProgrammingError, cur.callproc, 'pg_catalog.LOWER', params)
        con.rollback()
        self.assertRaises(TypeError, cur.callproc, b'lower', params)
        self.assertRaises(ValueError, cur.callproc, 'pg_catalog..lower')
        self.assertRaises(ValueError, cur.callproc, '"lower')

    def test_callproc_procedure(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_add(a int, inout b int)'
                    ' language plpgsql as $$ begin b := a + b; end $$')
        cur.execute('create procedure pg_temp.test_nop(a int)'
                    ' language plpgsql as $$ begin end $$')
        # the input/output parameter is replaced in a copy
        params = (1, 2)
        res = cur.callproc('pg_temp.test_add', params)
        self.assertEqual(res, (1, 3))
        self.assertEqual(params, (1, 2))
        self.assertEqual(cur.description[0].name, 'b')
        self.assertEqual(cur.rowcount, 1)
        # the returned row can still be fetched
        self.assertEqual(cur.fetchall(), [(3,)])
        list_params = [1, 2]
        res = cur.callproc('pg_temp.test_add', list_params)
        self.assertEqual(res, [1, 3])
        self.assertIsNot(res, list_params)
        self.assertEqual(list_params, [1, 2])
        # without output parameters, the procedure returns nothing
        res = cur.callproc('pg_temp.test_nop', (1,))
        self.assertEqual(res, (1,))
        self.assertIsNone(cur.description)
        # NULL values are returned as None
        res = cur.callproc('pg_temp.test_add', (1, None))
        self.assertEqual(res, (1, None))

    def test_callproc_procedure_with_out_params(self):
        con = self._connect()
        cnx = con._cnx
        assert cnx is not None
        if cnx.server_version < 140000:
            self.skipTest("OUT parameters of procedures need PostgreSQL 14")
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_out(a int, out b int,'
                    ' inout c date, out d text)'
                    ' language plpgsql as $$ begin'
                    " b := a * 2; c := c + a; d := 'done'; end $$")
        res = cur.callproc('pg_temp.test_out',
                           (3, None, date(2026, 11, 20), None))
        # the values are cast to the proper Python types
        self.assertEqual(res, (3, 6, date(2026, 11, 23), 'done'))
        self.assertEqual(cur.fetchone(), (6, date(2026, 11, 23), 'done'))

    def test_callproc_procedure_with_defaults(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_add(a int,'
                    ' inout b int default 5)'
                    ' language plpgsql as $$ begin b := a + b; end $$')
        res = cur.callproc('pg_temp.test_add', (1, 2))
        self.assertEqual(res, (1, 3))
        # the value of an omitted parameter can only be fetched
        res = cur.callproc('pg_temp.test_add', (1,))
        self.assertEqual(res, (1,))
        self.assertEqual(cur.fetchone(), (6,))

    def test_callproc_overloaded_names(self):
        con = self._connect()
        cur = con.cursor()
        # a function and a procedure with the same name
        cur.execute('create function pg_temp.test_mix(a int) returns int'
                    ' language sql as $$ select a * 2 $$')
        cur.execute('create procedure pg_temp.test_mix(a int, inout b int)'
                    ' language plpgsql as $$ begin b := a + b; end $$')
        params = (2,)
        self.assertIs(cur.callproc('pg_temp.test_mix', params), params)
        self.assertEqual(cur.fetchone(), (4,))
        self.assertEqual(cur.callproc('pg_temp.test_mix', (2, 3)), (2, 5))
        # procedures with different output parameters
        cur.execute('create procedure pg_temp.test_ovl(inout a int, b int)'
                    ' language plpgsql as $$ begin a := a + b; end $$')
        cur.execute('create procedure pg_temp.test_ovl(a text, inout b text)'
                    ' language plpgsql as $$ begin b := a || b; end $$')
        self.assertRaises(pgdb.ProgrammingError,
                          cur.callproc, 'pg_temp.test_ovl', (1, 2))

    def test_callproc_unqualified_procedure(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('create schema test_callproc_schema')
        cur.execute('set local search_path to test_callproc_schema, public')
        cur.execute('create procedure "Test %s Proc"(inout a int)'
                    ' language plpgsql as $$ begin a := a + 1; end $$')
        self.assertEqual(cur.callproc('Test %s Proc', (1,)), (2,))
        con.rollback()

    def test_callproc_variadic_routines(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_va(inout a int)'
                    ' language plpgsql as $$ begin a := 0; end $$')
        cur.execute('create function pg_temp.test_va(variadic a int[])'
                    ' returns int language sql'
                    ' as $$ select array_length(a, 1) $$')
        # only the variadic function can be called with two parameters
        params = (5, 6)
        self.assertIs(cur.callproc('pg_temp.test_va', params), params)
        self.assertEqual(cur.fetchone(), (2,))
        # a variadic argument without default needs at least one value
        cur.execute('create procedure pg_temp.test_vb()'
                    ' language plpgsql as $$ begin end $$')
        cur.execute('create function pg_temp.test_vb(variadic a int[])'
                    ' returns int language sql'
                    ' as $$ select array_length(a, 1) $$')
        self.assertEqual(cur.callproc('pg_temp.test_vb', ()), ())
        self.assertIsNone(cur.description)

    def test_callproc_independent_of_global_options(self):
        from pg import get_bool, set_bool
        con = self._connect()
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_ovl(inout a int)'
                    ' language plpgsql as $$ begin a := 0; end $$')
        cur.execute('create procedure pg_temp.test_ovl('
                    'a int, inout b int, c int)'
                    ' language plpgsql as $$ begin b := a + b + c; end $$')
        use_bool = get_bool()
        try:
            for value in (False, True):
                set_bool(value)
                self.assertEqual(
                    cur.callproc('pg_temp.test_ovl', (1, 2, 3)), (1, 6, 3))
        finally:
            set_bool(use_bool)

    def test_callproc_errors(self):
        con = self._connect()
        cur = con.cursor()
        self.assertRaises(pgdb.DataError, cur.callproc, 'low\0er', ('A',))
        # a name that cannot be encoded in the client encoding
        cnx = con._cnx
        assert cnx is not None
        error = (pgdb.ProgrammingError if cnx.encoding == 'utf-8'
                 else pgdb.DataError)  # function not found or not encodable
        self.assertRaises(error, cur.callproc, 'l中wer', ('A',))

    def test_callproc_with_closed_connection(self):
        con = self._connect()
        cur = con.cursor()
        con.close()
        self.assertRaises(pgdb.OperationalError,
                          cur.callproc, 'lower', ('A',))

    def test_callproc_procedure_with_transaction_control(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute('create procedure pg_temp.test_commit(inout a int)'
                    ' language plpgsql as $$ begin commit; a := a + 1; end $$')
        con.commit()
        # procedures with transaction control need autocommit
        self.assertRaises(pgdb.InternalError,
                          cur.callproc, 'pg_temp.test_commit', (1,))
        con.rollback()
        con.autocommit = True
        self.assertEqual(cur.callproc('pg_temp.test_commit', (1,)), (2,))

    def test_cursor_type(self):

        class TestCursor(pgdb.Cursor):
            @staticmethod
            def row_factory(row):
                return row  # not used

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

            def row_factory(self, row):  # type: ignore[override]
                description = self.description
                assert isinstance(description, list)
                return {f'column {desc[0]}': value
                        for desc, value in zip(description, row, strict=True)}

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

        # noinspection PyAbstractClass
        class TestCursor(pgdb.Cursor):

            def build_row_factory(self):
                description = self.description
                assert isinstance(description, list)
                keys = [desc[0] for desc in description]
                return lambda row: {
                    key: value for key, value in zip(keys, row, strict=True)}

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

    # noinspection PyUnresolvedReferences
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

    # noinspection PyUnresolvedReferences
    def test_cursor_with_unnamed_columns(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1, 2, 3")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3))
        self.assertEqual(res._fields, ('_0', '_1', '_2'))
        cur.execute("select 1 as one, 2, 3 as three")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3))
        self.assertEqual(res._fields, ('one', '_1', 'three'))

    # noinspection PyUnresolvedReferences
    def test_cursor_with_badly_named_columns(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1 as abc, 2 as def")
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2))
        self.assertEqual(res._fields, ('abc', '_1'))
        cur.execute(
            'select 1 as snake_case, 2 as "CamelCase",'
            ' 3 as "kebap-case", 4 as "_bad", 5 as "0bad", 6 as "bad$"')
        res = cur.fetchone()
        self.assertIsInstance(res, tuple)
        self.assertEqual(res, (1, 2, 3, 4, 5, 6))
        self.assertEqual(res._fields[:2], ('snake_case', 'CamelCase'))
        fields = ('_2', '_3', '_4', '_5')
        self.assertEqual(res._fields[2:], fields)

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

    # noinspection PyUnresolvedReferences
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
            self.assertEqual(d.name, f'col{i}')
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

    def test_type_cache_info(self):
        con = self._connect()
        try:
            cur = con.cursor()
            type_cache = con.type_cache
            self.assertNotIn('numeric', type_cache)
            type_info = type_cache['numeric']
            self.assertIn('numeric', type_cache)
            self.assertEqual(type_info, 'numeric')
            self.assertEqual(type_info.oid, 1700)
            self.assertEqual(type_info.len, -1)
            self.assertEqual(type_info.type, 'b')  # base
            self.assertEqual(type_info.category, 'N')  # numeric
            self.assertEqual(type_info.delim, ',')
            self.assertEqual(type_info.relid, 0)
            self.assertIs(con.type_cache[1700], type_info)
            self.assertNotIn('pg_type', type_cache)
            type_info = type_cache['pg_type']
            self.assertIn('pg_type', type_cache)
            self.assertEqual(type_info.type, 'c')  # composite
            self.assertEqual(type_info.category, 'C')  # composite
            cols = type_cache.get_fields('pg_type')
            if cols[0].name == 'oid':  # PostgreSQL < 12
                del cols[0]
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
        finally:
            con.close()
        con = self._connect()
        try:
            cur = con.cursor()
            type_cache = con.type_cache
            self.assertNotIn('pg_type', type_cache)
            self.assertEqual(type_cache.get('pg_type'), type_info)
            self.assertIn('pg_type', type_cache)
            self.assertIsNone(type_cache.get(
                self.table_prefix + '_surely_does_not_exist'))
            cur.close()
        finally:
            con.close()

    def test_type_cache_lookup(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute('create domain pg_temp."Test $2 Type" as int')
            type_cache = con.type_cache
            int4 = type_cache[23]
            self.assertEqual(int4, 'int4')
            for name in ('int4', '"int4"', 'pg_catalog.int4',
                         'pg_catalog."int4"'):
                self.assertIs(type_cache[name], int4, name)
            typ = type_cache['Test $2 Type']
            self.assertEqual(typ, 'Test $2 Type')
            self.assertIs(type_cache['"Test $2 Type"'], typ)
            self.assertIs(type_cache['pg_temp."Test $2 Type"'], typ)
            self.assertIs(type_cache['pg_temp.Test $2 Type'], typ)
            # names can also be passed as raw bytes
            self.assertIs(type_cache[b'Test $2 Type'], typ)
            self.assertIs(type_cache[b'pg_catalog.int4'], int4)
            # names are taken literally, they are not SQL type names
            for name in ('INT4', 'integer', 'int[]', 'pg_catalog.INT4',
                         'PG_CATALOG.int4', 'pg_catalog.int4[]',
                         'public.int4'):
                self.assertIsNone(type_cache.get(name), name)
            cur.close()
        finally:
            con.close()

    def test_type_cache_failing_lookup_in_transaction(self):
        con = self._connect()
        try:
            cur = con.cursor()
            type_cache = con.type_cache
            # failing lookups must not abort the current transaction
            for name in ('pg_temp.int4', 'test_no_such_type',
                         'no_such_schema.int4', 'a.b.c', 'int[', '"int4', '',
                         "it's", "a'$1", b"a'$1", b'a.b.c', 0, 999999999,
                         -1, 2 ** 32, 2 ** 64):
                self.assertIsNone(type_cache.get(name), name)
            for key in None, True, 1.5:
                self.assertRaises(TypeError, type_cache.get, key)
            cur.execute('select 1')
            self.assertEqual(cur.fetchone()[0], 1)
            cur.close()
        finally:
            con.close()

    def test_type_cache_internal_names_are_literal(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # the names of the element types of arrays, which are used to
            # look up typecasts, must be taken literally
            cur.execute('create domain pg_temp."pg_catalog.bool" as int')
            cur.execute('select array[2]::pg_temp."pg_catalog.bool"[]')
            self.assertEqual(cur.fetchone()[0], [2])
            typ = con.type_cache['"pg_catalog.bool"']
            self.assertEqual(typ.base, 'int4')
            cur.close()
        finally:
            con.close()

    def test_type_cache_outside_search_path(self):
        # typecasts for composite types, domains and arrays must be created
        # from the OIDs of the types, not from their names, since the types
        # may not be in the search path or their names may be ambiguous
        con = self._connect()
        try:
            cur = con.cursor()
            execute = cur.execute

            def get(what):
                execute(f'select {what}')
                return cur.fetchone()

            execute('create schema test_types_schema')
            execute('create type test_types_schema.t as (a int, b text)')
            execute('create domain test_types_schema.d as int')
            execute('create type test_types_schema.o'
                    ' as (n test_types_schema.d, t test_types_schema.t)')
            execute('create table test_types_schema.r'
                    ' (i int, t test_types_schema.t,'
                    ' ts test_types_schema.t[])')
            execute("insert into test_types_schema.r values"
                    " (1, (2, 'b'), array[(3, 'c')]::test_types_schema.t[])")
            r = get("(1, 'a')::test_types_schema.t")[0]
            self.assertEqual(r, (1, 'a'))
            self.assertEqual(r._fields, ('a', 'b'))
            r = get("(4, (1, 'a'))::test_types_schema.o")[0]
            self.assertEqual(r, (4, (1, 'a')))
            self.assertEqual(r.t.b, 'a')
            self.assertEqual(get('* from test_types_schema.r'),
                             (1, (2, 'b'), [(3, 'c')]))
            self.assertEqual(get('r from test_types_schema.r r')[0],
                             (1, (2, 'b'), [(3, 'c')]))
            self.assertEqual(
                get("array[(1, 'a')]::test_types_schema.t[]")[0], [(1, 'a')])
            self.assertEqual(get('array[3]::test_types_schema.d[]')[0], [3])
            # a different type with the same name in the search path
            execute('create type pg_temp.t as (x text)')
            self.assertEqual(get("row('z')::pg_temp.t")[0], ('z',))
            self.assertEqual(get("(1, 'a')::test_types_schema.t")[0], (1, 'a'))
            self.assertEqual(get("row('z')::pg_temp.t")[0]._fields, ('x',))
            self.assertEqual(get(
                "array[(1, 'a')]::test_types_schema.t[],"
                " array[row('z')]::pg_temp.t[]"), ([(1, 'a')], [('z',)]))
            # also when fetching many rows with a row caster
            execute("select array[(1, 'a')]::test_types_schema.t[],"
                    " array[row('z')]::pg_temp.t[]"
                    " from generate_series(1, 6)")
            self.assertEqual(cur.fetchall()[5], ([(1, 'a')], [('z',)]))
            # typecasts set for the name of a type are used for all types
            # with that name and for fields and elements of that type
            type_cache = con.type_cache
            type_cache.set_typecast('t', lambda v: f'T{v}')
            try:
                self.assertEqual(
                    get("(4, (1, 'a'))::test_types_schema.o")[0],
                    (4, 'T(1,a)'))
                self.assertEqual(get("row('z')::pg_temp.t")[0], 'T(z)')
            finally:
                type_cache.reset_typecast('t')
            self.assertEqual(
                get("(4, (1, 'a'))::test_types_schema.o")[0], (4, (1, 'a')))
            cur.close()
        finally:
            con.close()

    def test_type_cache_global_typecast_for_type_without_typecast(self):
        # a global typecast set for a type without a typecast must be used
        # by the connection even after values of that type have been cast
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create type pg_temp.test_mood as enum ('happy')")
            sql = ("select 'happy'::pg_temp.test_mood,"
                   " array['happy']::pg_temp.test_mood[]")
            cur.execute(sql)
            self.assertEqual(cur.fetchone(), ('happy', ['happy']))
            pgdb.set_typecast('test_mood', str.upper)
            try:
                cur.execute(sql)
                self.assertEqual(cur.fetchone(), ('HAPPY', ['HAPPY']))
            finally:
                pgdb.reset_typecast('test_mood')
            cur.close()
        finally:
            con.close()

    def test_type_cache_records_with_invalid_python_names(self):
        # composite types with names that cannot be used for named tuples
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute('create type pg_temp.test_keywords'
                        ' as ("class" int, "_x" int)')
            cur.execute('create type pg_temp."test type" as (a int)')
            cur.execute('select (1, 2)::pg_temp.test_keywords,'
                        ' row(1)::pg_temp."test type"')
            r, s = cur.fetchone()
            self.assertEqual(r, (1, 2))
            self.assertEqual(type(r).__name__, 'test_keywords')
            self.assertEqual(r._fields, ('_0', '_1'))
            self.assertEqual(s, (1,))
            self.assertEqual(type(s).__name__, 'Record')
            self.assertEqual(s.a, 1)
            cur.close()
        finally:
            con.close()

    def test_type_cache_typecast_with_type_code_without_oid(self):
        # type codes without OID are looked up by name
        con = self._connect()
        try:
            from pgdb.typecode import TypeCode
            typ = TypeCode('int4')
            self.assertIs(con.type_cache.get_typecast(typ), int)
            self.assertEqual(con.type_cache.typecast('42', typ), 42)
        finally:
            con.close()

    def test_type_cache_array_of_records(self):
        # the element type of arrays of anonymous records is a pseudo type
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select array[(1, 'a'), (2, 'b')]")
            self.assertEqual(cur.fetchone()[0], [('1', 'a'), ('2', 'b')])
            cur.close()
        finally:
            con.close()

    def test_type_cache_global_typecast_for_array_name(self):
        # a global typecast set for the name of an array type is used,
        # like in the pg module
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create type pg_temp.test_mood as enum ('happy')")
            pgdb.set_typecast('_test_mood', str.upper)
            try:
                cur.execute("select array['happy']::pg_temp.test_mood[]")
                self.assertEqual(cur.fetchone()[0], '{HAPPY}')
            finally:
                pgdb.reset_typecast('_test_mood')
            cur.close()
        finally:
            con.close()

    def test_type_cache_reset_all_typecasts(self):
        # typecasts that need the connection must still work after
        # resetting all typecasts of the connection
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select '2020-01-02'::date")
            self.assertEqual(cur.fetchone()[0], date(2020, 1, 2))
            con.type_cache.reset_typecast()
            cur.execute("select '2020-01-02'::date")
            self.assertEqual(cur.fetchone()[0], date(2020, 1, 2))
            cur.close()
        finally:
            con.close()

    def test_type_cache_reset_array_typecast(self):
        # resetting the typecast for an array type must not store an array
        # cast derived from its name, which ignores the delimiter
        con = self._connect()
        try:
            cur = con.cursor()
            con.type_cache.reset_typecast('_box')
            cur.execute("select array['(1,1),(0,0)'::box,"
                        " '(2,2),(1,1)'::box]")
            self.assertEqual(cur.fetchone()[0],
                             ['(1,1),(0,0)', '(2,2),(1,1)'])
            cur.close()
        finally:
            con.close()

    def test_type_cache_array_with_other_delimiter(self):
        # the elements of box arrays are separated by semicolons
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select array['(1,1),(0,0)'::box,"
                        " '(2,2),(1,1)'::box]")
            self.assertEqual(cur.fetchone()[0],
                             ['(1,1),(0,0)', '(2,2),(1,1)'])
            cur.close()
        finally:
            con.close()

    def test_type_cache_after_altering_composite_type(self):
        # typecasts for composite types must use the current fields
        # after resetting the typecasts
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute('create type pg_temp.test_alter as (a int)')
            cur.execute('select row(1)::pg_temp.test_alter')
            self.assertEqual(cur.fetchone()[0], (1,))
            cur.execute('alter type pg_temp.test_alter add attribute b int')
            con.type_cache.reset_typecast()
            cur.execute('select row(1, 2)::pg_temp.test_alter')
            r = cur.fetchone()[0]
            self.assertEqual(r, (1, 2))
            self.assertEqual(r._fields, ('a', 'b'))
            cur.close()
        finally:
            con.close()

    def test_type_cache_reset_does_not_derive_casts_from_names(self):
        # resetting a typecast must not store an array cast derived from
        # the name, which would be used for other types with that name
        con = self._connect()
        try:
            cur = con.cursor()
            con.type_cache.reset_typecast('int4')
            cur.execute('create type pg_temp."_int4" as (a int)')
            cur.execute('select row(1)::pg_temp."_int4", array[2, 3]')
            self.assertEqual(cur.fetchone(), ((1,), [2, 3]))
            cur.close()
        finally:
            con.close()

    def test_type_cache_typecast_with_qualified_names(self):
        # names are taken like the parts of table names
        con = self._connect()
        try:
            type_cache = con.type_cache
            self.assertIs(type_cache.get_typecast('pg_catalog.int4'), int)
            self.assertIs(type_cache.get_typecast('pg_catalog."int4"'), int)
            self.assertEqual(type_cache.typecast('42', 'pg_catalog.int4'), 42)
            cur = con.cursor()
            cur.execute('create domain pg_temp."pg_catalog.bool" as int')
            cast = type_cache.get_typecast('"pg_catalog.bool"')
            self.assertIs(cast, int)
            self.assertEqual(
                type_cache.typecast('42', '"pg_catalog.bool"'), 42)
            cur.close()
        finally:
            con.close()

    def test_type_cache_with_underscore_names(self):
        # only array types are cast as arrays, not types whose names only
        # start with an underscore, and the element types of arrays are
        # not derived from the names of the array types
        con = self._connect()
        try:
            cur = con.cursor()
            execute = cur.execute
            execute('create type pg_temp._q as (u int)')
            execute('create type pg_temp.q as (v int)')  # array type is _q_1
            execute('select row(1)::pg_temp._q')
            self.assertEqual(cur.fetchone()[0], (1,))
            execute('select array[row(2)]::pg_temp.q[],'
                    ' array[row(3)]::pg_temp._q[]')
            r = cur.fetchone()
            self.assertEqual(r, ([(2,)], [(3,)]))
            self.assertEqual(r[0][0]._fields, ('v',))
            # these types have element types, but are not cast like arrays
            execute("select '1 2'::oidvector, 'ab'::name, '(1,2)'::point")
            self.assertEqual(cur.fetchone(), ('1 2', 'ab', '(1,2)'))
            cur.close()
        finally:
            con.close()

    def test_type_cache_with_domains(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute('create domain pg_temp.test_dint as int')
            cur.execute('create domain pg_temp.test_dbytea as bytea')
            cur.execute('create domain pg_temp.test_ddbytea'
                        ' as pg_temp.test_dbytea')
            cur.execute('create domain pg_temp.test_dints as int[]')
            cur.execute('create type pg_temp.test_rtype as'
                        ' (i pg_temp.test_dint, b pg_temp.test_ddbytea,'
                        ' a pg_temp.test_dints)')
            type_cache = con.type_cache
            dint = type_cache['test_dint']
            self.assertEqual(dint, 'test_dint')
            self.assertEqual(dint.type, 'd')  # domain
            self.assertEqual(dint.category, 'N')  # numeric
            self.assertIs(dint.base, type_cache['int4'])
            self.assertIs(type_cache['test_ddbytea'].base,
                          type_cache['bytea'])  # fully resolved
            self.assertIsNone(type_cache['int4'].base)
            fields = type_cache.get_fields('test_rtype')
            self.assertEqual(fields, [('i', 'test_dint'),
                                      ('b', 'test_ddbytea'),
                                      ('a', 'test_dints')])
            # domains are cast like their base types
            self.assertIs(type_cache.get_typecast('test_dint'), int)
            self.assertEqual(type_cache.typecast('42', 'test_dint'), 42)
            cmd = ("select 42::pg_temp.test_dint,"
                   " row(42, 'A\\101\\000'::bytea, '{1,2}')"
                   "::pg_temp.test_rtype, '{1,2}'::pg_temp.test_dint[],"
                   " array['\\x00ff'::pg_temp.test_dbytea]")
            cur.execute(cmd)
            # the description reports the base type for domain values
            self.assertEqual(cur.description[0].type_code, 'int4')
            self.assertEqual(cur.description[1].type_code, 'test_rtype')
            self.assertEqual(cur.description[2].type_code, '_test_dint')
            row = cur.fetchone()
            self.assertEqual(row[0], 42)
            self.assertEqual(row[1], (42, b'AA\x00', [1, 2]))
            self.assertIsInstance(row[1].b, bytes)
            self.assertEqual(row[2], [1, 2])
            self.assertEqual(row[3], [b'\x00\xff'])
            # unless a typecast has been set for the domain, which is used
            # for elements of arrays and fields of records
            type_cache.set_typecast('test_dint', lambda v: f'<{v}>')
            self.assertEqual(type_cache.typecast('42', 'test_dint'), '<42>')
            cur.execute(cmd)
            row = cur.fetchone()
            self.assertEqual(row[0], 42)
            self.assertEqual(row[1].i, '<42>')
            self.assertEqual(row[2], ['<1>', '<2>'])
            type_cache.reset_typecast('test_dint')
            cur.execute(cmd)
            row = cur.fetchone()
            self.assertEqual(row[1].i, 42)
            self.assertEqual(row[2], [1, 2])
            cur.close()
        finally:
            con.close()

    def test_type_cache_typecast(self):
        con = self._connect()
        try:
            cur = con.cursor()
            type_cache = con.type_cache
            self.assertIs(type_cache.get_typecast('int4'), int)
            cast_int = lambda v: f'int({v})'  # noqa: E731
            type_cache.set_typecast('int4', cast_int)
            query = 'select 2::int2, 4::int4, 8::int8'
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 2)
            self.assertEqual(i4, 'int(4)')
            self.assertEqual(i8, 8)
            self.assertEqual(type_cache.typecast(42, 'int4'), 'int(42)')
            type_cache.set_typecast(['int2', 'int8'], cast_int)
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 'int(4)')
            self.assertEqual(i8, 'int(8)')
            type_cache.reset_typecast('int4')
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 'int(8)')
            type_cache.reset_typecast(['int2', 'int8'])
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 2)
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 8)
            type_cache.set_typecast(['int2', 'int8'], cast_int)
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 'int(8)')
            type_cache.reset_typecast()
            cur.execute(query)
            i2, i4, i8 = cur.fetchone()
            self.assertEqual(i2, 2)
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 8)
            cur.close()
        finally:
            con.close()

    def test_cursor_iteration(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1 union select 2 union select 3 order by 1")
        self.assertEqual([r[0] for r in cur], [1, 2, 3])

    def test_cursor_invalidation(self):
        con = self._connect()
        cur = con.cursor()
        cur.execute("select 1 union select 2")
        self.assertEqual(cur.fetchone(), (1,))
        self.assertFalse(con.closed)
        con.close()
        self.assertTrue(con.closed)
        self.assertRaises(pgdb.OperationalError, cur.fetchone)

    def test_fetch_2_rows(self):
        values = ('test', pgdb.Binary(b'\xff\x52\xb2'),
                  True, 5, 6, 5.7, Decimal('234.234234'), Decimal('75.45'),
                  pgdb.Date(2011, 7, 17), pgdb.Time(15, 47, 42),
                  pgdb.Timestamp(2008, 10, 20, 15, 25, 35),
                  pgdb.Interval(15, 31, 5), 7897234)
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("set datestyle to iso")
            cur.execute(
                f"create table {table} ("
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
                "rowidtest oid)")
            cur.execute("set standard_conforming_strings to on")
            for s in ('numeric', 'monetary', 'time'):
                cur.execute(f"set lc_{s} to 'C'")
            for _i in range(2):
                cur.execute(
                    f"insert into {table} values ("
                    "%s,%s,%s,%s,%s,%s,%s,"
                    "'%s'::money,%s,%s,%s,%s,%s)", values)
            cur.execute(f"select * from {table}")
            rows = cur.fetchall()
            self.assertEqual(len(rows), 2)
            row0 = rows[0]
            self.assertEqual(row0, values)
            self.assertEqual(row0, rows[1])
            self.assertIsInstance(row0[0], str)
            self.assertIsInstance(row0[1], bytes)
            self.assertIsInstance(row0[2], bool)
            self.assertIsInstance(row0[3], int)
            self.assertIsInstance(row0[4], int)
            self.assertIsInstance(row0[5], float)
            self.assertIsInstance(row0[6], Decimal)
            self.assertIsInstance(row0[7], Decimal)
            self.assertIsInstance(row0[8], date)
            self.assertIsInstance(row0[9], time)
            self.assertIsInstance(row0[10], datetime)
            self.assertIsInstance(row0[11], timedelta)
        finally:
            con.close()

    def test_insert_bytes(self):
        values = [b'', b'Hello, World!', b'{a,b,c}', b'"Hi!"',
                  b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(f"create table {table} (n smallint, b bytea)")
            # plain bytes are passed as bytea, like pgdb.Binary
            cur.executemany(f"insert into {table} values (%d, %s)",
                            enumerate(values))
            cur.execute(f"select b from {table} order by n")
            rows = [row[0] for row in cur.fetchall()]
            cur.execute(f"select n from {table} where b = %s",
                        (values[-1],))
            n = cur.fetchone()[0]
        finally:
            con.close()
        self.assertEqual(rows, values)
        self.assertEqual(n, len(values) - 1)

    def test_select_bytea_array(self):
        values = ([b'a', b'', None, b'\x00\xff\\'],)
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select %s::bytea[]", values)
            row = cur.fetchone()
        finally:
            con.close()
        self.assertEqual(row, values)

    def test_integrity_error(self):
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("set client_min_messages = warning")
            cur.execute(f"create table {table} (i int primary key)")
            cur.execute(f"insert into {table} values (1)")
            cur.execute(f"insert into {table} values (2)")
            self.assertRaises(
                pgdb.IntegrityError, cur.execute,
                f"insert into {table} values (1)")
        finally:
            con.close()

    def test_update_rowcount(self):
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(f"create table {table} (i int)")
            cur.execute(f"insert into {table} values (1)")
            cur.execute(f"update {table} set i=2 where i=2 returning i")
            self.assertEqual(cur.rowcount, 0)
            cur.execute(f"update {table} set i=2 where i=1 returning i")
            self.assertEqual(cur.rowcount, 1)
            cur.close()
            # keep rowcount even if cursor is closed (needed by SQLAlchemy)
            self.assertEqual(cur.rowcount, 1)
        finally:
            con.close()

    def test_sqlstate(self):
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute("select 1/0")
        except pgdb.DatabaseError as error:
            self.assertIsInstance(error, pgdb.DataError)
            # the SQLSTATE error code for division by zero is 22012
            # noinspection PyUnresolvedReferences
            self.assertEqual(error.sqlstate, '22012')

    def test_float(self):
        nan, inf = float('nan'), float('inf')
        from math import isinf, isnan
        self.assertTrue(isnan(nan) and not isinf(nan))
        self.assertTrue(isinf(inf) and not isnan(inf))
        values = [0, 1, 0.03125, -42.53125, nan, inf, -inf,
                  'nan', 'inf', '-inf', 'NaN', 'Infinity', '-Infinity']
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                f"create table {table} (n smallint, floattest float)")
            params = enumerate(values)
            cur.executemany(f"insert into {table} values (%d,%s)", params)
            cur.execute(f"select floattest from {table} order by n")
            rows = cur.fetchall()
            self.assertEqual(cur.description[0].type_code, pgdb.FLOAT)
            self.assertNotEqual(cur.description[0].type_code, pgdb.ARRAY)
            self.assertNotEqual(cur.description[0].type_code, pgdb.RECORD)
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        rows = [row[0] for row in rows]
        for inval, outval in zip(values, rows, strict=True):
            if inval in ('inf', 'Infinity'):
                inval = inf
            elif inval in ('-inf', '-Infinity'):
                inval = -inf
            elif inval in ('nan', 'NaN'):
                inval = nan
            if isinf(inval):  # type: ignore[arg-type]
                self.assertTrue(isinf(outval))
                if inval < 0:  # type: ignore[operator]
                    self.assertLess(outval, 0)
                else:
                    self.assertGreater(outval, 0)
            elif isnan(inval):  # type: ignore[arg-type]
                self.assertTrue(isnan(outval))
            else:
                self.assertEqual(inval, outval)

    def test_datetime(self):
        dt = datetime(2011, 7, 17, 15, 47, 42, 317509)
        values = [dt.date(), dt.time(), dt, dt.time(), dt]
        self.assertIsInstance(values[3], time)
        assert isinstance(values[3], time)  # type guard
        values[3] = values[3].replace(tzinfo=timezone.utc)
        self.assertIsInstance(values[4], datetime)
        assert isinstance(values[4], datetime)  # type guard
        values[4] = values[4].replace(tzinfo=timezone.utc)
        da = (dt.year, dt.month, dt.day)
        ti = (dt.hour, dt.minute, dt.second, dt.microsecond)
        tz = (timezone.utc,)
        inputs = [
            # input as objects
            values,
            # input as text
            [v.isoformat() for v in values],  # type: ignore[attr-defined]
            # # input using type helpers
            [pgdb.Date(*da), pgdb.Time(*ti),
             pgdb.Timestamp(*(da + ti)), pgdb.Time(*(ti + tz)),
             pgdb.Timestamp(*(da + ti + tz))]
        ]
        table = self.table_prefix + 'booze'
        con: pgdb.Connection = self._connect()
        try:
            cur = con.cursor()
            cur.execute("set timezone = UTC")
            cur.execute(f"create table {table} ("
                        "d date, t time,  ts timestamp,"
                        "tz timetz, tsz timestamptz)")
            for params in inputs:
                for datestyle in ('iso', 'postgres, mdy', 'postgres, dmy',
                                  'sql, mdy', 'sql, dmy', 'german'):
                    cur.execute(f"set datestyle to {datestyle}")
                    if not isinstance(params[0], str):
                        cur.execute("select %s,%s,%s,%s,%s", params)
                        row = cur.fetchone()
                        self.assertEqual(row, tuple(values))
                    cur.execute(
                        f"insert into {table}"
                        " values (%s,%s,%s,%s,%s)", params)
                    cur.execute(f"select * from {table}")
                    d = cur.description
                    self.assertIsInstance(d, list)
                    assert d is not None  # type guard
                    for i in range(5):
                        tc = d[i].type_code
                        self.assertEqual(tc, pgdb.DATETIME)
                        self.assertNotEqual(tc, pgdb.STRING)
                        self.assertNotEqual(tc, pgdb.ARRAY)
                        self.assertNotEqual(tc, pgdb.RECORD)
                    self.assertEqual(d[0].type_code, pgdb.DATE)
                    self.assertEqual(d[1].type_code, pgdb.TIME)
                    self.assertEqual(d[2].type_code, pgdb.TIMESTAMP)
                    self.assertEqual(d[3].type_code, pgdb.TIME)
                    self.assertEqual(d[4].type_code, pgdb.TIMESTAMP)
                    row = cur.fetchone()
                    self.assertEqual(row, tuple(values))
                    cur.execute(f"truncate table {table}")
        finally:
            con.close()

    def test_interval(self):
        td = datetime(2011, 7, 17, 15, 47, 42, 317509) - datetime(1970, 1, 1)
        inputs = [
            # input as objects
            td,
            # input as text
            f'{td.days} days {td.seconds} seconds'
            f' {td.microseconds} microseconds',
            # input using type helpers
            pgdb.Interval(td.days, 0, 0, td.seconds, td.microseconds)]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(f"create table {table} (i interval)")
            for param in inputs:
                for intervalstyle in ('sql_standard ', 'postgres',
                                      'postgres_verbose', 'iso_8601'):
                    cur.execute(f"set intervalstyle to {intervalstyle}")
                    # noinspection PyUnboundLocalVariable
                    cur.execute(f"insert into {table} values (%s)", [param])
                    cur.execute(f"select * from {table}")
                    tc = cur.description[0].type_code
                    self.assertEqual(tc, pgdb.DATETIME)
                    self.assertNotEqual(tc, pgdb.STRING)
                    self.assertNotEqual(tc, pgdb.ARRAY)
                    self.assertNotEqual(tc, pgdb.RECORD)
                    self.assertEqual(tc, pgdb.INTERVAL)
                    row = cur.fetchone()
                    self.assertEqual(row, (td,))
                    cur.execute(f"truncate table {table}")
        finally:
            con.close()

    def test_infinite_interval(self):
        con = self._connect()
        try:
            cnx = con._cnx
            assert cnx is not None
            if cnx.server_version < 170000:
                self.skipTest("infinite intervals need PostgreSQL 17")
            cur = con.cursor()
            cur.execute(
                "select 'infinity'::interval, '-infinity'::interval,"
                " array['infinity', '-infinity', '1 day']::interval[]")
            row = cur.fetchone()
            self.assertEqual(row, (
                timedelta.max, timedelta.min,
                [timedelta.max, timedelta.min, timedelta(1)]))
            # the extreme values are passed as finite intervals
            for d in timedelta.max, timedelta.min:
                cur.execute(
                    "select %s::interval, isfinite(%s::interval)", (d, d))
                self.assertEqual(cur.fetchone(), (d, True))
        finally:
            con.close()

    def test_oid8(self):
        con = self._connect()
        try:
            cnx = con._cnx
            assert cnx is not None
            if cnx.server_version < 190000:
                self.skipTest("the oid8 type needs PostgreSQL 19")
            cur = con.cursor()
            big = 2**64 - 1  # oid8 is unsigned
            # integers are passed as literals, and PostgreSQL has no cast
            # from numeric to oid8, so the big value is passed as a string
            cur.execute(
                "select %s::oid8, array[%s::oid8, 0::oid8],"
                " array[[1, 2], [3, 4]]::oid8[], %s::oid8",
                (str(big), str(big), 2**63 - 1))
            self.assertEqual(cur.fetchone(), (
                big, [big, 0], [[1, 2], [3, 4]], 2**63 - 1))
            self.assertEqual(
                [d.type_code for d in cur.description],
                ['oid8', '_oid8', '_oid8', 'oid8'])
            cur.execute('create type pg_temp.test_rtype'
                        ' as (o oid8, a oid8[])')
            cur.execute(
                "select row(%s::oid8, array[1, %s::oid8])::pg_temp.test_rtype",
                (str(big), str(big)))
            r = cur.fetchone()[0]
            self.assertEqual(r, (big, [1, big]))
            self.assertIsInstance(r.o, int)
        finally:
            con.close()

    def test_bytearray_and_memoryview_params(self):
        con = self._connect()
        try:
            cur = con.cursor()
            for value in (bytearray(b'ab'), bytearray(), memoryview(b'ab')):
                for params in ((value,), ([value],), ((1, value),),
                               (pgdb.Hstore(k=value),)):
                    self.assertRaisesRegex(
                        pgdb.InterfaceError, 'Do not know how to adapt',
                        cur.execute, 'select %s', params)
                self.assertRaises(pgdb.InterfaceError, cur.execute,
                                  'select %(v)s', {'v': value})
            self.assertRaises(pgdb.InterfaceError, str,
                              pgdb.Hstore({memoryview(b'k'): 'v'}))
            # wrapped with Binary, they are passed as binary data
            cur.execute('select %s::bytea, %s::bytea', (
                pgdb.Binary(bytearray(b'a\0')), pgdb.Binary(memoryview(b'b'))))
            self.assertEqual(cur.fetchone(), (b'a\0', b'b'))
        finally:
            con.close()

    def test_hstore(self):
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute("select 'k=>v'::hstore")
        except pgdb.DatabaseError:
            try:
                cur.execute("create extension hstore")
            except pgdb.DatabaseError:
                self.skipTest("hstore extension not enabled")
        finally:
            con.close()
        d = {'k': 'v', 'foo': 'bar', 'baz': 'whatever',
             '1a': 'anything at all', '2=b': 'value = 2', '3>c': 'value > 3',
             '4"c': 'value " 4', "5'c": "value ' 5", 'hello, world': '"hi!"',
             'None': None, 'NULL': 'NULL', 'empty': '',
             'back\\': '\\slash', '\\': '\\\\',
             'tab\tkey': 'line\nbreak', ' pad ': '\r\f\v',
             'v\vtab': 'null'}
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select %s::hstore", (pgdb.Hstore(d),))
            result = cur.fetchone()[0]
        finally:
            con.close()
        self.assertIsInstance(result, dict)
        self.assertEqual(result, d)

    def test_hstore_with_undecodable_text(self):
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute("select 'k=>v'::hstore")
            except pgdb.DatabaseError:
                self.skipTest("hstore extension not enabled")
            # with SQL_ASCII, the server sends text without conversion
            cur.execute('set client_encoding=sql_ascii')
            # chr(228) is non-ASCII in all server encodings
            cur.execute("select chr(228), hstore('k' || chr(228),"
                        " 'v' || chr(228)) || 'x=>\"it''s\"'")
            raw, h = cur.fetchone()
            self.assertIsInstance(raw, bytes)
            self.assertEqual(h, {b'k' + raw: b'v' + raw, 'x': "it's"})
            # the raw text can be written back (this wrote the
            # representation of the bytes objects before)
            h = pgdb.Hstore(h)
            cur.execute('select %s::hstore', (h,))
            self.assertEqual(cur.fetchone()[0], h)
            cur.execute('select %(h)s::hstore, %(x)s', {'h': h, 'x': 1})
            self.assertEqual(cur.fetchone(), (h, 1))
            cur.execute("select %s = (hstore('k' || chr(228),"
                        " 'v' || chr(228)) || 'x=>\"it''s\"', 'x')",
                        ((h, 'x'),))
            self.assertIs(cur.fetchone()[0], True)
            cur.execute('select %s::hstore[]', ([h, None],))
            self.assertEqual(cur.fetchone()[0], [h, None])
            cur.execute(
                "select %s::hstore = hstore('k' || chr(228), 'v' || chr(228))",
                (pgdb.Hstore({b'k' + raw: b'v' + raw}),))
            self.assertIs(cur.fetchone()[0], True)
        finally:
            con.close()

    def test_uuid(self):
        self.assertIs(Uuid, pgdb.Uuid)
        d = Uuid('{12345678-1234-5678-1234-567812345678}')
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select %s::uuid", (d,))
            result = cur.fetchone()[0]
        finally:
            con.close()
        self.assertIsInstance(result, Uuid)
        self.assertEqual(result, d)

    def test_network_types(self):
        values: list[tuple[Any, str]] = [
            (IPv4Address('192.168.0.1'), 'inet'),
            (IPv6Address('2001:db8::1'), 'inet'),
            (IPv4Interface('192.168.0.1/24'), 'inet'),
            (IPv6Interface('2001:db8::1/64'), 'inet'),
            (IPv4Network('192.168.0.0/24'), 'cidr'),
            (IPv6Network('2001:db8::/32'), 'cidr')]
        con = self._connect()
        try:
            cur = con.cursor()
            for value, typ in values:
                with self.subTest(value=value):
                    cur.execute(
                        "select %s, pg_typeof(%s)::text", (value, value))
                    self.assertEqual(cur.fetchone(), (str(value), typ))
            a = IPv4Address('192.168.0.1')
            i = IPv6Interface('2001:db8::1/64')
            n = IPv4Network('192.168.0.0/24')
            cur.execute(
                "select %s, pg_typeof(%s)::text, %s::text",
                ([a, i, n, None], [[n, None]], (a, n)))
            self.assertEqual(cur.fetchone(), (
                ['192.168.0.1', '2001:db8::1/64', '192.168.0.0/24', None],
                'cidr[]', '(192.168.0.1,192.168.0.0/24)'))
            # the scope id of an IPv6 address may contain quotes
            self.assertRaises(
                pgdb.DataError, cur.execute,
                "select %s", (IPv6Address("fe80::1%it's"),))
        finally:
            con.close()

    def test_insert_network_types(self):
        table = self.table_prefix + 'booze'
        a = IPv4Address('192.168.0.1')
        i = IPv6Interface('2001:db8::1/64')
        n = IPv6Network('2001:db8::/32')
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(f"create table {table}"
                        " (i inet, c cidr, ia inet[], ca cidr[])")
            cur.execute(f"insert into {table} values (%s, %s, %s, %s)",
                        (i, n, [a, i, None], [n]))
            cur.execute(f"select * from {table} where i = %s", (i,))
            self.assertEqual(cur.fetchone(), (
                '2001:db8::1/64', '2001:db8::/32',
                ['192.168.0.1', '2001:db8::1/64', None], ['2001:db8::/32']))
        finally:
            con.close()

    def test_network_types_as_ipaddress(self):
        from pgdb.cast import cast_cidr, cast_inet
        q = ("select '192.168.0.1'::inet, '2001:db8::1/64'::inet,"
             " '192.168.0.0/24'::cidr,"
             " array['10.0.0.1'::inet, null], array['2001:db8::/32'::cidr]")
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(q)
            r = cur.fetchone()
            self.assertEqual(tuple(r[:3]), (
                '192.168.0.1', '2001:db8::1/64', '192.168.0.0/24'))
            con.type_cache.set_typecast('inet', cast_inet)
            con.type_cache.set_typecast('cidr', cast_cidr)
            cur.execute(q)
            r = cur.fetchone()
            self.assertEqual(tuple(r), (
                IPv4Address('192.168.0.1'), IPv6Interface('2001:db8::1/64'),
                IPv4Network('192.168.0.0/24'),
                [IPv4Address('10.0.0.1'), None],
                [IPv6Network('2001:db8::/32')]))
            # round trip, also as fields of a composite type
            table = self.table_prefix + 'booze'
            i = IPv4Interface('192.168.0.1/24')
            n = IPv4Network('192.168.0.0/24')
            cur.execute(f"create table {table} (i inet, c cidr)")
            cur.execute(f"insert into {table} values (%s, %s)", (i, n))
            cur.execute(f"select *, {table} from {table}")
            r = cur.fetchone()
            self.assertEqual(tuple(r[:2]), (i, n))
            self.assertEqual(tuple(r[2]), (i, n))
        finally:
            con.close()

    def test_insert_array(self):
        values: list[tuple[Any, Any]] = [
            (None, None), ([], []), ([None], [[None], ['null']]),
            ([1, 2, 3], [['a', 'b'], ['c', 'd']]),
            ([20000, 25000, 25000, 30000],
             [['breakfast', 'consulting'], ['meeting', 'lunch']]),
            ([0, 1, -1], [['Hello, World!', '"Hi!"'], ['{x,y}', ' x y ']])]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(
                f"create table {table} (n smallint, i int[], t text[][])")
            params = [(n, v[0], v[1]) for n, v in enumerate(values)]
            # Note that we must explicit casts because we are inserting
            # empty arrays.  Otherwise this is not necessary.
            cur.executemany(
                f"insert into {table} values"
                " (%d,%s::int[],%s::text[][])", params)
            cur.execute(f"select i, t from {table} order by n")
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

    def test_select_undecodable_text(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # with SQL_ASCII, the server sends text without conversion
            cur.execute("set client_encoding=sql_ascii")
            cur.execute("create temp table test_person"
                        " (name text, age int, amount numeric, day date)")
            # chr(228) is non-ASCII in all server encodings
            cur.execute("select chr(228)")
            raw = cur.fetchone()[0]
            self.assertIsInstance(raw, bytes)
            cur.execute(
                "select 'w' || chr(228) || 'r', array['abc', 'w' || chr(228)],"
                " row('w' || chr(228), 61, 1.5, '2026-11-21')::test_person")
            row = cur.fetchone()
        finally:
            con.close()
        # undecodable text is returned as bytes, also inside arrays and
        # records (this raised SystemError before), but the other elements
        # are still decoded and cast
        self.assertEqual(row[0], b'w' + raw + b'r')
        self.assertEqual(row[1], ['abc', b'w' + raw])
        self.assertEqual(row[2], (b'w' + raw, 61, Decimal('1.5'),
                                  date(2026, 11, 21)))
        self.assertEqual(row[2].name, b'w' + raw)

    def test_unicode_list_and_tuple(self):
        value = ('Käse', 'Würstchen')
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute("select %s, %s", value)
            except pgdb.DatabaseError:
                self.skipTest('database does not support latin-1')
            row = cur.fetchone()
            cur.execute("select %s, %s", (list(value), tuple(value)))
            as_list, as_tuple = cur.fetchone()
        finally:
            con.close()
        self.assertEqual(as_list, list(row))
        self.assertEqual(as_tuple, tuple(row))

    def test_insert_record(self):
        values = [('John', 61), ('Jane', 63),
                  ('Fred', None), ('Wilma', None),
                  (None, 42), (None, None)]
        table = self.table_prefix + 'booze'
        record = self.table_prefix + 'munch'
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute(f"create type {record} as (name varchar, age int)")
            cur.execute(f"create table {table} (n smallint, r {record})")
            params = enumerate(values)
            cur.executemany(f"insert into {table} values (%d,%s)", params)
            cur.execute(f"select r from {table} order by n")
            type_code = cur.description[0].type_code
            self.assertEqual(type_code, record)
            self.assertEqual(type_code, pgdb.RECORD)
            self.assertNotEqual(type_code, pgdb.ARRAY)
            columns = con.type_cache.get_fields(type_code)
            self.assertEqual(columns[0].name, 'name')
            self.assertEqual(columns[1].name, 'age')
            self.assertEqual(con.type_cache[columns[0].type], 'varchar')
            self.assertEqual(con.type_cache[columns[1].type], 'int4')
            rows = cur.fetchall()
        finally:
            cur.execute(f'drop table {table}')
            cur.execute(f'drop type {record}')
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
        values = list(map(PgBitString, values))  # type: ignore[arg-type]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            seq_params = enumerate(values)  # params have __pg_repr__ method
            cur.execute(
                f'create table "{table}" (n smallint, b bit varying(7))')
            cur.executemany(f"insert into {table} values (%s,%s)", seq_params)
            cur.execute(f"select * from {table}")
            rows = cur.fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), len(values))
        con = self._connect()
        try:
            cur = con.cursor()
            params = (1, object())  # an object that cannot be handled
            self.assertRaises(
                pgdb.InterfaceError, cur.execute,
                f"insert into {table} values (%s,%s)", params)
        finally:
            con.close()

    def test_set_decimal_type(self):
        from pgdb.cast import decimal_type
        self.assertIs(decimal_type(), Decimal)
        con = self._connect()
        try:
            cur = con.cursor()
            # change decimal type globally to int

            class CustomDecimal(str):

                def __init__(self, value: Any) -> None:
                    self.value = value

                def __str__(self) -> str:
                    return str(self.value).replace('.', ',')

            self.assertIs(decimal_type(CustomDecimal), CustomDecimal)
            cur.execute('select 4.25')
            self.assertEqual(cur.description[0].type_code, pgdb.NUMBER)
            value = cur.fetchone()[0]
            self.assertIsInstance(value, CustomDecimal)
            self.assertEqual(str(value), '4,25')
            # change decimal type again to float
            self.assertIs(decimal_type(float), float)
            cur.execute('select 4.25')
            self.assertEqual(cur.description[0].type_code, pgdb.NUMBER)
            value = cur.fetchone()[0]
            # the connection still uses the old setting
            self.assertIsInstance(value, str)
            self.assertEqual(str(value), '4,25')
            # bust the cache for type functions for the connection
            con.type_cache.reset_typecast()
            cur.execute('select 4.25')
            self.assertEqual(cur.description[0].type_code, pgdb.NUMBER)
            value = cur.fetchone()[0]
            # now the connection uses the new setting
            self.assertIsInstance(value, float)
            self.assertEqual(value, 4.25)
        finally:
            con.close()
            decimal_type(Decimal)
        self.assertIs(decimal_type(), Decimal)

    def test_global_typecast(self):
        try:
            query = 'select 2::int2, 4::int4, 8::int8'
            self.assertIs(pgdb.get_typecast('int4'), int)
            cast_int = lambda v: f'int({v})'  # noqa: E731
            pgdb.set_typecast('int4', cast_int)
            con = self._connect()
            try:
                i2, i4, i8 = con.cursor().execute(query).fetchone()
            finally:
                con.close()
            self.assertEqual(i2, 2)
            self.assertEqual(i4, 'int(4)')
            self.assertEqual(i8, 8)
            pgdb.set_typecast(['int2', 'int8'], cast_int)
            con = self._connect()
            try:
                i2, i4, i8 = con.cursor().execute(query).fetchone()
            finally:
                con.close()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 'int(4)')
            self.assertEqual(i8, 'int(8)')
            pgdb.reset_typecast('int4')
            con = self._connect()
            try:
                i2, i4, i8 = con.cursor().execute(query).fetchone()
            finally:
                con.close()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 'int(8)')
            pgdb.reset_typecast(['int2', 'int8'])
            con = self._connect()
            try:
                i2, i4, i8 = con.cursor().execute(query).fetchone()
            finally:
                con.close()
            self.assertEqual(i2, 2)
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 8)
            pgdb.set_typecast(['int2', 'int8'], cast_int)
            con = self._connect()
            try:
                i2, i4, i8 = con.cursor().execute(query).fetchone()
            finally:
                con.close()
            self.assertEqual(i2, 'int(2)')
            self.assertEqual(i4, 4)
            self.assertEqual(i8, 'int(8)')
        finally:
            pgdb.reset_typecast()
        con = self._connect()
        try:
            i2, i4, i8 = con.cursor().execute(query).fetchone()
        finally:
            con.close()
        self.assertEqual(i2, 2)
        self.assertEqual(i4, 4)
        self.assertEqual(i8, 8)

    def test_set_typecast_for_arrays(self):
        query = 'select ARRAY[1,2,3]'
        try:
            con = self._connect()
            try:
                r = con.cursor().execute(query).fetchone()[0]
            finally:
                con.close()
            self.assertIsInstance(r, list)
            self.assertEqual(r, [1, 2, 3])
            pgdb.set_typecast('anyarray', lambda v, basecast: v)
            con = self._connect()
            try:
                r = con.cursor().execute(query).fetchone()[0]
            finally:
                con.close()
            self.assertIsInstance(r, str)
            self.assertEqual(r, '{1,2,3}')
        finally:
            pgdb.reset_typecast()
        con = self._connect()
        try:
            r = con.cursor().execute(query).fetchone()[0]
        finally:
            con.close()
        self.assertIsInstance(r, list)
        self.assertEqual(r, [1, 2, 3])

    def test_unicode_with_utf8(self):
        table = self.table_prefix + 'booze'
        s = "He wes Leovenaðes sone — liðe him be Drihten"
        con = self._connect()
        cur = con.cursor()
        try:
            cur.execute(f"create table {table} (t text)")
            try:
                cur.execute("set client_encoding=utf8")
                cur.execute(f"select '{s}'")
            except Exception:
                self.skipTest("database does not support utf8")
            output1 = cur.fetchone()[0]
            cur.execute(f"insert into {table} values (%s)", (s,))
            cur.execute(f"select * from {table}")
            output2 = cur.fetchone()[0]
            cur.execute(f"select t = '{s}' from {table}")
            output3 = cur.fetchone()[0]
            cur.execute(f"select t = %s from {table}", (s,))
            output4 = cur.fetchone()[0]
        finally:
            con.close()
        self.assertIsInstance(output1, str)
        self.assertEqual(output1, s)
        self.assertIsInstance(output2, str)
        self.assertEqual(output2, s)
        self.assertIsInstance(output3, bool)
        self.assertTrue(output3)
        self.assertIsInstance(output4, bool)
        self.assertTrue(output4)

    def test_unicode_with_latin1(self):
        table = self.table_prefix + 'booze'
        s = "Ehrt den König seine Würde, ehret uns der Hände Fleiß."
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(f"create table {table} (t text)")
            try:
                cur.execute("set client_encoding=latin1")
                cur.execute(f"select '{s}'")
            except Exception:
                self.skipTest("database does not support latin1")
            output1 = cur.fetchone()[0]
            cur.execute(f"insert into {table} values (%s)", (s,))
            cur.execute(f"select * from {table}")
            output2 = cur.fetchone()[0]
            cur.execute(f"select t = '{s}' from {table}")
            output3 = cur.fetchone()[0]
            cur.execute(f"select t = %s from {table}", (s,))
            output4 = cur.fetchone()[0]
        finally:
            con.close()
        self.assertIsInstance(output1, str)
        self.assertEqual(output1, s)
        self.assertIsInstance(output2, str)
        self.assertEqual(output2, s)
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
            cur.execute(f"create table {table} (n smallint, booltest bool)")
            params = enumerate(values)
            cur.executemany(f"insert into {table} values (%s,%s)", params)
            cur.execute(f"select booltest from {table} order by n")
            rows = cur.fetchall()
            self.assertEqual(cur.description[0].type_code, pgdb.BOOL)
        finally:
            con.close()
        rows = [row[0] for row in rows]
        values[3] = values[5] = True
        values[4] = values[6] = False
        self.assertEqual(rows, values)

    def test_literal(self):
        con = self._connect()
        try:
            cur = con.cursor()
            value = "lower('Hello')"
            cur.execute("select %s, %s", (value, pgdb.Literal(value)))
            row = cur.fetchone()
        finally:
            con.close()
        self.assertEqual(row, (value, 'hello'))

    def test_json(self):
        inval = {"employees": [
            {"firstName": "John", "lastName": "Doe", "age": 61}]}
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute(f"create table {table} (jsontest json)")
            except pgdb.ProgrammingError:
                self.skipTest('database does not support json')
            params = (pgdb.Json(inval),)
            cur.execute(f"insert into {table} values (%s)", params)
            cur.execute(f"select jsontest from {table}")
            outval = cur.fetchone()[0]
            self.assertEqual(cur.description[0].type_code, pgdb.JSON)
        finally:
            con.close()
        self.assertEqual(inval, outval)

    def test_jsonb(self):
        inval = {"employees": [
            {"firstName": "John", "lastName": "Doe", "age": 61}]}
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute(f"create table {table} (jsonbtest jsonb)")
            except pgdb.ProgrammingError:
                self.skipTest('database does not support jsonb')
            params = (pgdb.Json(inval),)
            cur.execute(f"insert into {table} values (%s)", params)
            cur.execute(f"select jsonbtest from {table}")
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
            self.assertRaises(pgdb.DataError, cur.execute, sql)
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

    def test_fetchall_with_various_sizes(self):
        # we test this because there are optimizations based on result size
        con = self._connect()
        try:
            for n in (1, 3, 5, 7, 10, 100, 1000):
                cur = con.cursor()
                try:
                    cur.execute('select n, n::text as s, n % 2 = 1 as b'
                                f' from generate_series(1, {n}) as s(n)')
                    res = cur.fetchall()
                    self.assertEqual(len(res), n, res)
                    self.assertEqual(len(res[0]), 3)
                    self.assertEqual(res[0].n, 1)
                    self.assertEqual(res[0].s, '1')
                    self.assertIs(res[0].b, True)
                    self.assertEqual(len(res[-1]), 3)
                    self.assertEqual(res[-1].n, n)
                    self.assertEqual(res[-1].s, str(n))
                    self.assertIs(res[-1].b, n % 2 == 1)
                finally:
                    cur.close()
        finally:
            con.close()

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

    def help_nextset_setup(self, _cur):
        pass  # helper not needed

    def help_nextset_teardown(self, _cur):
        pass  # helper not needed

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

    def test_nul_characters(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # these were silently truncated before
            for params in (['a\0b'], [['a', 'b\0c']], [pgdb.Json('a\0b')]):
                with self.assertRaises(pgdb.DataError) as cm:
                    cur.execute("select %s", params)
                self.assertIn('NUL', str(cm.exception))
            self.assertRaises(
                pgdb.DataError, cur.execute, "select %(x)s", {'x': 'a\0b'})
            # this raised an OperationalError ("Internal error") before
            self.assertRaises(pgdb.DataError, cur.execute, "select 'a\0b'")
            # bytes are passed as bytea, so they can contain NUL bytes
            cur.execute("select %s::bytea", [b'a\0b'])
            self.assertEqual(cur.fetchone()[0], b'a\0b')
        finally:
            con.close()

    def test_unencodable_characters(self):
        con = self._connect()
        try:
            cur = con.cursor()
            try:
                cur.execute("set client_encoding=latin1")
            except pgdb.DatabaseError:
                self.skipTest("database does not support latin1")
            # these raised an OperationalError ("Internal error") before
            with self.assertRaises(pgdb.DataError) as cm:
                cur.execute("select %s", ['мир'])
            self.assertIsInstance(cm.exception.__cause__, UnicodeEncodeError)
            self.assertRaises(pgdb.DataError, cur.execute, "select 'мир'")
            # a malformed operation is still an internal error
            self.assertRaises(
                pgdb.OperationalError, cur.execute, "select %x", ['a'])
        finally:
            con.close()

    def test_lost_connection(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select pg_backend_pid()")
            pid = cur.fetchone()[0]
            con.rollback()
            other = self._connect()
            try:
                other.cursor().execute(f"select pg_terminate_backend({pid})")
            finally:
                other.close()
            # this raised a ProgrammingError before
            self.assertRaises(pgdb.OperationalError, cur.execute, "select 1")
            # this raised an OperationalError for a LookupError before
            with self.assertRaises(pgdb.Error) as cm:
                cur.execute("select 1")
            self.assertEqual(str(cm.exception), "Connection has been lost")
        finally:
            con.close()

    def test_transaction(self):
        table = self.table_prefix + 'booze'
        con1 = self._connect()
        cur1 = con1.cursor()
        self.execute_ddl1(cur1)
        con1.commit()
        con2 = self._connect()
        cur2 = con2.cursor()
        cur2.execute(f"select name from {table}")
        self.assertIsNone(cur2.fetchone())
        cur1.execute(f"insert into {table} values('Schlafly')")
        cur2.execute(f"select name from {table}")
        self.assertIsNone(cur2.fetchone())
        con1.commit()
        cur2.execute(f"select name from {table}")
        self.assertEqual(cur2.fetchone(), ('Schlafly',))
        con2.close()
        con1.close()

    def test_commit_and_rollback_busy_connection(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create temp table test_busy (n int)")
            con.commit()
            cur.execute("insert into test_busy values (1)")
            errors: list[Exception] = []

            def receiver(_notice):
                for func in (con.commit, con.rollback):
                    try:
                        func()
                    except Exception as e:
                        errors.append(e)

            cnx = con._cnx
            assert cnx is not None
            cnx.query('set client_min_messages=notice')
            cnx.set_notice_receiver(receiver)
            cur.execute("do $$ begin raise notice 'busy'; end $$")
            cnx.set_notice_receiver(None)
            self.assertEqual(len(errors), 2)
            for error in errors:
                self.assertIsInstance(error, pgdb.OperationalError)
                self.assertIsInstance(error.__cause__, pgdb.InterfaceError)
            # the transaction has not been marked as ended
            con.rollback()
            cur.execute("select count(*) from test_busy")
            self.assertEqual(cur.fetchone(), (0,))
        finally:
            con.close()

    def test_autocommit(self):
        table = self.table_prefix + 'booze'
        con1 = self._connect()
        con1.autocommit = True
        cur1 = con1.cursor()
        self.execute_ddl1(cur1)
        con2 = self._connect()
        cur2 = con2.cursor()
        cur2.execute(f"select name from {table}")
        self.assertIsNone(cur2.fetchone())
        cur1.execute(f"insert into {table} values('Shmaltz Pastrami')")
        cur2.execute(f"select name from {table}")
        self.assertEqual(cur2.fetchone(), ('Shmaltz Pastrami',))
        con2.close()
        con1.close()

    def test_connection_as_contextmanager(self):
        table = self.table_prefix + 'booze'
        for autocommit in False, True:
            con = self._connect()
            con.autocommit = autocommit
            try:
                cur = con.cursor()
                if autocommit:
                    cur.execute(f"truncate table {table}")
                else:
                    cur.execute(
                        f"create table {table} (n smallint check(n!=4))")
                with con:
                    cur.execute(f"insert into {table} values (1)")
                    cur.execute(f"insert into {table} values (2)")
                try:
                    with con:
                        cur.execute(f"insert into {table} values (3)")
                        cur.execute(f"insert into {table} values (4)")
                except con.IntegrityError as error:
                    self.assertIn('check', str(error).lower())
                with con:
                    cur.execute(f"insert into {table} values (5)")
                    cur.execute(f"insert into {table} values (6)")
                try:
                    with con:
                        cur.execute(f"insert into {table} values (7)")
                        cur.execute(f"insert into {table} values (8)")
                        raise ValueError('transaction should rollback')
                except ValueError as error:
                    self.assertEqual(str(error), 'transaction should rollback')
                with con:
                    cur.execute(f"insert into {table} values (9)")
                cur.execute(f"select * from {table} order by 1")
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
        self.assertIn('char', pgdb.STRING)
        self.assertLess(pgdb.NUMERIC, pgdb.NUMBER)
        self.assertGreaterEqual(pgdb.NUMBER, pgdb.INTEGER)
        self.assertLessEqual(pgdb.TIME, pgdb.DATETIME)
        self.assertGreaterEqual(pgdb.DATETIME, pgdb.DATE)
        self.assertEqual(pgdb.ARRAY, pgdb.ARRAY)
        self.assertNotEqual(pgdb.ARRAY, pgdb.STRING)
        self.assertEqual('_char', pgdb.ARRAY)
        self.assertNotEqual('char', pgdb.ARRAY)
        self.assertEqual(pgdb.RECORD, pgdb.RECORD)
        self.assertNotEqual(pgdb.RECORD, pgdb.STRING)
        self.assertNotEqual(pgdb.RECORD, pgdb.ARRAY)
        self.assertEqual('record', pgdb.RECORD)
        self.assertNotEqual('_record', pgdb.RECORD)

    def test_no_close(self):
        data = ('hello', 'world')
        con = self._connect()
        cur = con.cursor()
        cur.build_row_factory = lambda: tuple
        cur.execute("select %s, %s", data)
        row = cur.fetchone()
        self.assertEqual(row, data)

    def test_change_row_factory_cache_size(self):
        from pg import RowCache
        queries = ['select 1 as a, 2 as b, 3 as c', 'select 123 as abc']
        con = self._connect()
        cur = con.cursor()
        for maxsize in (None, 0, 1, 2, 3, 10, 1024):
            RowCache.change_size(maxsize)
            for _i in range(3):
                for q in queries:
                    cur.execute(q)
                    r = cur.fetchone()
                    if q.endswith('abc'):
                        self.assertEqual(r, (123,))
                        self.assertEqual(r._fields, ('abc',))
                    else:
                        self.assertEqual(r, (1, 2, 3))
                        self.assertEqual(r._fields, ('a', 'b', 'c'))
            info = RowCache.row_factory.cache_info()
            self.assertEqual(info.maxsize, maxsize)
            self.assertEqual(info.hits + info.misses, 6)
            self.assertEqual(info.hits,
                             0 if maxsize is not None and maxsize < 2 else 4)

    def test_memory_leaks(self):
        ids: set = set()
        objs: list = []
        add_ids = ids.update
        gc.collect()
        objs[:] = gc.get_objects()
        add_ids(id(obj) for obj in objs)
        self.test_no_close()
        gc.collect()
        objs[:] = gc.get_objects()
        objs[:] = [obj for obj in objs if id(obj) not in ids]
        self.assertEqual(len(objs), 0)

    def test_cve_2018_1058(self):
        # internal queries should use qualified table and operator names,
        # see https://nvd.nist.gov/vuln/detail/CVE-2018-1058
        con = self._connect()
        cur = con.cursor()
        execute = cur.execute
        try:
            execute("SET client_min_messages TO WARNING")
            execute("SET TIMEZONE TO 'UTC'")
            execute("SHOW TIMEZONE")
            self.assertEqual(cur.fetchone()[0], 'UTC')
            execute("""
                CREATE OR REPLACE FUNCTION public.bad_eq(oid, integer)
                RETURNS boolean AS $$
                BEGIN
                  SET TIMEZONE TO 'Europe/Athens';
                  RETURN oideq($1, $2::oid);
                END
                $$ LANGUAGE plpgsql
                """)
            execute("DROP OPERATOR IF EXISTS public.= (oid, integer)")
            execute("""
                CREATE OPERATOR public.= (
                  PROCEDURE = public.bad_eq,
                  LEFTARG = oid, RIGHTARG = integer
                );
                """)
            # the following select changes the time zone as a side effect if
            # internal query uses unqualified = operator as it did earlier
            execute("SELECT 1")
            execute("SHOW TIMEZONE")  # make sure time zone has not changed
            self.assertEqual(cur.fetchone()[0], 'UTC')
        finally:
            execute("DROP OPERATOR IF EXISTS public.= (oid, integer)")
            execute("DROP FUNCTION IF EXISTS public.bad_eq(oid, integer)")
            cur.close()
            con.close()


if __name__ == '__main__':
    unittest.main()
