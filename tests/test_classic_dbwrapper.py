#!/usr/bin/python

"""Test the classic PyGreSQL interface.

Sub-tests for the DB wrapper object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.
"""

from __future__ import annotations

import gc
import json
import os
import sys
import tempfile
import unittest
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import StringIO
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from operator import itemgetter
from time import strftime
from typing import Any, ClassVar, List  # noqa: UP035
from unittest.mock import patch
from uuid import UUID

import pg  # the module under test

from .config import dbhost, dbname, dbpasswd, dbport, dbuser

debug = False  # let DB wrapper print debugging output

windows = os.name == 'nt'

# There is a known a bug in libpq under Windows which can cause
# the interface to crash when calling PQhost():
do_not_ask_for_host = windows
do_not_ask_for_host_reason = 'libpq issue on Windows'


def DB():  # noqa: N802
    """Create a DB wrapper object connecting to the test database."""
    db = pg.DB(dbname, dbhost, dbport, user=dbuser, passwd=dbpasswd)
    if debug:
        db.debug = debug
    db.query("set client_min_messages=warning")
    return db


class TestDBClassInit(unittest.TestCase):
    """Test proper handling of errors when creating DB instances."""

    def test_bad_params(self):
        self.assertRaises(TypeError, pg.DB, invalid=True)

    # noinspection PyUnboundLocalVariable
    def test_delete_db(self):
        db = DB()
        del db.db
        self.assertRaises(pg.InternalError, db.close)
        del db

    def test_async_query_before_deletion(self):
        db = DB()
        query = db.send_query('select 1')
        self.assertEqual(query.getresult(), [(1,)])
        self.assertIsNone(query.getresult())
        self.assertIsNone(query.getresult())
        del db
        gc.collect()

    def test_async_query_after_deletion(self):
        db = DB()
        query = db.send_query('select 1')
        del db
        gc.collect()
        # the connection has been closed, so the results are lost
        self.assertRaises(TypeError, query.getresult)
        self.assertRaises(TypeError, query.getresult)

    def test_async_query_with_type_lookup(self):
        db = DB()  # new instance, so the date type is not yet cached
        try:
            query = db.send_query("select '2020-12-31'::date; select 2")
            # the type lookup runs a query, which must not discard the
            # pending second result of the asynchronous query
            self.assertEqual(query.getresult(), [(date(2020, 12, 31),)])
            self.assertEqual(query.getresult(), [(2,)])
            self.assertIsNone(query.getresult())
        finally:
            db.close()


class TestDBClassBasic(unittest.TestCase):
    """Test existence of the DB class wrapped pg connection methods."""

    def setUp(self):
        self.db = DB()

    def tearDown(self):
        with suppress(pg.InternalError):
            self.db.close()

    def test_all_db_attributes(self):
        attributes = [
            'abort', 'adapter',
            'backend_pid', 'begin',
            'cancel', 'change_password', 'clear', 'close', 'commit',
            'date_format', 'db', 'dbname', 'dbtypes',
            'debug', 'decode_json', 'delete',
            'delete_prepared', 'describe_prepared',
            'encode_json', 'encoding', 'encrypt_password',
            'end', 'endcopy', 'error',
            'escape_bytea', 'escape_identifier',
            'escape_literal', 'escape_string',
            'fileno', 'full_protocol_version',
            'get', 'get_as_dict', 'get_as_list',
            'get_attnames', 'get_cast_hook', 'get_databases',
            'get_generated', 'get_notice_receiver',
            'get_parameter', 'get_relations', 'get_tables',
            'getline', 'getlo', 'getnotify',
            'has_table_privilege', 'host', 'hostaddr',
            'insert', 'inserttable', 'is_non_blocking',
            'locreate', 'loimport',
            'notification_handler',
            'options',
            'parameter', 'pkey', 'pkeys', 'poll', 'port',
            'prepare', 'protocol_version', 'putline',
            'query', 'query_formatted', 'query_prepared',
            'release', 'reopen', 'reset', 'rollback',
            'savepoint', 'send_query', 'server_version',
            'set_cast_hook', 'set_non_blocking', 'set_notice_receiver',
            'set_parameter', 'socket', 'source',
            'ssl_attributes', 'ssl_in_use',
            'start', 'status',
            'transaction', 'truncate',
            'unescape_bytea', 'update', 'upsert',
            'use_regtypes', 'user',
        ]
        db_attributes = [a for a in self.db.__dir__() if not a.startswith('_')]
        self.assertEqual(attributes, db_attributes)

    def test_attribute_db(self):
        self.assertEqual(self.db.db.db, dbname)

    def test_attribute_dbname(self):
        self.assertEqual(self.db.dbname, dbname)

    def test_attribute_error(self):
        error = self.db.error
        self.assertTrue(not error or 'krb5_' in error)
        self.assertEqual(self.db.error, self.db.db.error)

    @unittest.skipIf(do_not_ask_for_host, do_not_ask_for_host_reason)
    def test_attribute_host(self):
        host = dbhost if dbhost and not dbhost.startswith('/') else 'localhost'
        self.assertIsInstance(self.db.host, str)
        self.assertEqual(self.db.host, host)
        self.assertEqual(self.db.db.host, host)

    def test_attribute_options(self):
        no_options = ''
        options = self.db.options
        self.assertEqual(options, no_options)
        self.assertEqual(options, self.db.db.options)

    def test_attribute_port(self):
        def_port = 5432
        port = self.db.port
        self.assertIsInstance(port, int)
        self.assertEqual(port, dbport or def_port)
        self.assertEqual(port, self.db.db.port)

    def test_attribute_protocol_version(self):
        protocol_version = self.db.protocol_version
        self.assertIsInstance(protocol_version, int)
        self.assertTrue(2 <= protocol_version < 4)
        self.assertEqual(protocol_version, self.db.db.protocol_version)

    def test_attribute_full_protocol_version(self):
        full_protocol_version = self.db.full_protocol_version
        self.assertIsInstance(full_protocol_version, int)
        self.assertEqual(
            full_protocol_version, self.db.db.full_protocol_version)

    def test_attribute_hostaddr(self):
        hostaddr = self.db.hostaddr
        self.assertIsInstance(hostaddr, str)
        self.assertEqual(hostaddr, self.db.db.hostaddr)

    def test_attribute_server_version(self):
        server_version = self.db.server_version
        self.assertIsInstance(server_version, int)
        self.assertGreaterEqual(server_version, 120000)  # >= 12.0
        self.assertLess(server_version, 200000)  # < 20.0
        self.assertEqual(server_version, self.db.db.server_version)

    def test_attribute_socket(self):
        socket = self.db.socket
        self.assertIsInstance(socket, int)
        self.assertGreaterEqual(socket, 0)

    def test_attribute_backend_pid(self):
        backend_pid = self.db.backend_pid
        self.assertIsInstance(backend_pid, int)
        self.assertGreaterEqual(backend_pid, 1)

    def test_attribute_ssl_in_use(self):
        ssl_in_use = self.db.ssl_in_use
        self.assertIsInstance(ssl_in_use, bool)
        self.assertFalse(ssl_in_use)

    def test_attribute_ssl_attributes(self):
        ssl_attributes = self.db.ssl_attributes
        self.assertIsInstance(ssl_attributes, dict)
        if ssl_attributes:
            self.assertEqual(ssl_attributes, {
                'cipher': None, 'compression': None, 'key_bits': None,
                'library': None, 'protocol': None})

    def test_attribute_status(self):
        status_ok = 1
        status = self.db.status
        self.assertIsInstance(status, int)
        self.assertEqual(status, status_ok)
        self.assertEqual(status, self.db.db.status)

    def test_attribute_user(self):
        no_user = 'Deprecated facility'
        user = self.db.user
        self.assertTrue(user)
        self.assertIsInstance(user, str)
        self.assertNotEqual(user, no_user)
        self.assertEqual(user, self.db.db.user)

    def test_method_escape_literal(self):
        self.assertEqual(self.db.escape_literal(''), "''")

    def test_method_escape_identifier(self):
        self.assertEqual(self.db.escape_identifier(''), '""')

    def test_method_escape_string(self):
        self.assertEqual(self.db.escape_string(''), '')

    def test_method_escape_bytea(self):
        self.assertEqual(self.db.escape_bytea('').replace(
            '\\x', '').replace('\\', ''), '')

    def test_method_unescape_bytea(self):
        self.assertEqual(self.db.unescape_bytea(''), b'')

    def test_method_decode_json(self):
        self.assertEqual(self.db.decode_json('{}'), {})

    def test_method_encode_json(self):
        self.assertEqual(self.db.encode_json({}), '{}')

    def test_method_query(self):
        query = self.db.query
        query("select 1+1")
        query("select 1+$1+$2", 2, 3)
        query("select 1+$1+$2", (2, 3))
        query("select 1+$1+$2", [2, 3])
        query("select 1+$1", 1)

    def test_method_query_empty(self):
        self.assertRaises(ValueError, self.db.query, '')

    def test_method_query_data_error(self):
        try:
            self.db.query("select 1/0")
        except pg.DataError as error:
            # noinspection PyUnresolvedReferences
            self.assertEqual(error.sqlstate, '22012')

    def test_method_endcopy(self):
        with suppress(OSError):
            self.db.endcopy()

    def test_method_close(self):
        self.db.close()
        try:
            self.db.reset()
        except pg.Error:
            pass
        else:
            self.fail('Reset should give an error for a closed connection')
        self.assertIsNone(self.db.db)
        self.assertRaises(pg.InternalError, self.db.close)
        self.assertRaises(pg.InternalError, self.db.query, 'select 1')
        self.assertRaises(pg.InternalError, getattr, self.db, 'status')
        self.assertRaises(pg.InternalError, getattr, self.db, 'error')
        self.assertRaises(pg.InternalError, getattr, self.db, 'absent')

    def test_method_reset(self):
        con = self.db.db
        self.db.reset()
        self.assertIs(self.db.db, con)
        self.db.query("select 1+1")
        self.db.close()
        self.assertRaises(pg.InternalError, self.db.reset)

    def test_method_cancel(self):
        r = self.db.cancel()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)
        self.db.query("select 1+1")
        pid = self.db.backend_pid
        other = DB()
        try:
            other.query(f"select pg_terminate_backend({pid})")
        finally:
            other.close()
        self.assertRaises(pg.DatabaseError, self.db.query, "select 1")
        self.assertRaises(pg.OperationalError, self.db.cancel)
        self.db.close()
        self.assertRaises(pg.InternalError, self.db.cancel)

    def test_method_reopen(self):
        con = self.db.db
        self.db.reopen()
        self.assertIsNot(self.db.db, con)
        con = self.db.db
        self.db.query("select 1+1")
        self.db.close()
        self.db.reopen()
        self.assertIsNot(self.db.db, con)
        self.db.query("select 1+1")
        self.db.close()

    def test_close_and_reopen_busy_connection(self):
        db = self.db
        con = db.db
        assert con is not None
        hook = con.get_cast_hook()
        self.assertIsNotNone(hook)
        db.query('create temp table test_busy (n int)')
        errors: list[Exception] = []

        def rows():
            for func in (db.close, db.reopen):
                try:
                    func()
                except Exception as e:
                    errors.append(e)
            yield 1,

        self.assertEqual(db.inserttable('test_busy', rows()), 1)
        self.assertEqual(len(errors), 2)
        for error in errors:
            self.assertIsInstance(error, pg.InterfaceError)
        # the connection is still open and has kept its cast hook
        self.assertIs(db.db, con)
        self.assertIs(con.get_cast_hook(), hook)
        self.assertEqual(db.query("select '2020-01-02'::date").getresult(),
                         [(date(2020, 1, 2),)])

    def test_existing_connection(self):
        db = pg.DB(self.db.db)
        self.assertIsNotNone(db.db)
        self.assertEqual(self.db.db, db.db)
        db.close()
        self.assertIsNone(db.db)
        self.assertIsNotNone(self.db.db)
        db.reopen()
        self.assertIsNotNone(db.db)
        self.assertEqual(self.db.db, db.db)
        db.close()
        self.assertIsNone(db.db)
        db = pg.DB(self.db)
        self.assertEqual(self.db.db, db.db)
        assert self.db.db is not None
        db = pg.DB(db=self.db.db)
        self.assertEqual(self.db.db, db.db)

    def test_existing_db_api2_connection(self):

        class FakeDbApi2Connection:

            def __init__(self, cnx):
                self._cnx = cnx

            def close(self):
                self._cnx.close()

        db2 = FakeDbApi2Connection(self.db.db)
        db = pg.DB(db2)  # type: ignore[call-overload]
        self.assertEqual(self.db.db, db.db)
        db.close()
        self.assertIsNone(db.db)
        db.reopen()
        self.assertIsNotNone(db.db)
        self.assertEqual(self.db.db, db.db)
        db.close()
        self.assertIsNone(db.db)
        db2.close()


class TestDBClass(unittest.TestCase):
    """Test the methods of the DB class wrapped pg connection."""

    maxDiff = 80 * 20

    cls_set_up = False

    regtypes = None

    @classmethod
    def setUpClass(cls):
        db = DB()
        db.query("drop table if exists test cascade")
        db.query("create table test ("
                 "i2 smallint, i4 integer, i8 bigint,"
                 " d numeric, f4 real, f8 double precision, m money,"
                 " v4 varchar(4), c4 char(4), t text)")
        db.query("create or replace view test_view as"
                 " select i4, v4 from test")
        db.close()
        cls.cls_set_up = True

    @classmethod
    def tearDownClass(cls):
        db = DB()
        db.query("drop table test cascade")
        db.close()

    def setUp(self):
        self.assertTrue(self.cls_set_up)
        self.db = DB()
        if self.regtypes is None:
            self.regtypes = self.db.use_regtypes()
        else:
            self.db.use_regtypes(self.regtypes)
        query = self.db.query
        query('set client_encoding=utf8')
        query("set lc_monetary='C'")
        query("set datestyle='ISO,YMD'")
        query('set standard_conforming_strings=on')
        query('set bytea_output=hex')

    def tearDown(self):
        self.doCleanups()
        self.db.close()

    def create_table(self, table, definition,
                    temporary=True, values=None):
        query = self.db.query
        if '"' not in table or '.' in table:
            table = f'"{table}"'
        if not temporary:
            q = f'drop table if exists {table} cascade'
            query(q)
            self.addCleanup(query, q)
        temporary = 'temporary table' if temporary else 'table'
        as_query = definition.startswith(('as ', 'AS '))
        if not as_query and not definition.startswith('('):
            definition = f'({definition})'
        cmd = ' '.join(['create', temporary, table, definition])
        query(cmd)
        if values:
            for params in values:
                if not isinstance(params, (list, tuple)):
                    params = [params]
                values = ', '.join(f'${n + 1}' for n in range(len(params)))
                cmd = f"insert into {table} values ({values})"
                query(cmd, params)

    def test_class_name(self):
        self.assertEqual(self.db.__class__.__name__, 'DB')

    def test_module_name(self):
        self.assertEqual(self.db.__module__, 'pg.db')
        self.assertEqual(self.db.__class__.__module__, 'pg.db')

    def test_escape_literal(self):
        f = self.db.escape_literal
        r: Any = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b"'plain'")
        r = f("plain")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "'plain'")
        r = f("that's käse".encode())
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, "'that''s käse'".encode())
        r = f("that's käse")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "'that''s käse'")
        self.assertEqual(f(r"It's fine to have a \ inside."),
                         r" E'It''s fine to have a \\ inside.'")
        self.assertEqual(f('No "quotes" must be escaped.'),
                         "'No \"quotes\" must be escaped.'")

    def test_escape_identifier(self):
        f = self.db.escape_identifier
        r = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'"plain"')
        r = f("plain")
        self.assertIsInstance(r, str)
        self.assertEqual(r, '"plain"')
        r = f("that's käse".encode())
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, '"that\'s käse"'.encode())
        r = f("that's käse")
        self.assertIsInstance(r, str)
        self.assertEqual(r, '"that\'s käse"')
        self.assertEqual(f(r"It's fine to have a \ inside."),
                         '"It\'s fine to have a \\ inside."')
        self.assertEqual(f('All "quotes" must be escaped.'),
                         '"All ""quotes"" must be escaped."')

    def test_password_functions(self):
        db = self.db
        # these methods are delegated to the connection
        encrypted = db.encrypt_password('secret', 'bob', 'md5')
        self.assertEqual(encrypted, db.db.encrypt_password(
            'secret', 'bob', 'md5'))
        self.assertTrue(encrypted.startswith('md5'))
        db.begin()
        try:  # changing the own password needs no privileges
            self.assertIsNone(db.change_password(db.user, 'other secret'))
        finally:
            db.rollback()
        with self.assertRaises(pg.ProgrammingError):
            db.change_password('test_nonexistent_role', 'secret')

    def test_escape_string(self):
        f = self.db.escape_string
        r = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b"plain")
        r = f("plain")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "plain")
        r = f("that's käse".encode())
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, "that''s käse".encode())
        r = f("that's käse")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "that''s käse")
        self.assertEqual(f(r"It's fine to have a \ inside."),
                         r"It''s fine to have a \ inside.")

    def test_escape_bytea(self):
        f = self.db.escape_bytea
        # note that escape_byte always returns hex output since Pg 9.0,
        # regardless of the bytea_output setting
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'\\x706c61696e')
        r = f('plain')
        self.assertIsInstance(r, str)
        self.assertEqual(r, '\\x706c61696e')
        r = f("das is' käse".encode())
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'\\x64617320697327206bc3a47365')
        r = f("das is' käse")
        self.assertIsInstance(r, str)
        self.assertEqual(r, '\\x64617320697327206bc3a47365')
        self.assertEqual(f(b'O\x00ps\xff!'), b'\\x4f007073ff21')

    def test_unescape_bytea(self):
        f = self.db.unescape_bytea
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f('plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(b"das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, "das is' käse".encode())
        r = f("das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, "das is' käse".encode())
        self.assertEqual(f(r'O\\000ps\\377!'), b'O\\000ps\\377!')
        self.assertEqual(f(r'\\x706c61696e'), b'\\x706c61696e')
        self.assertEqual(f(r'\\x746861742773206be47365'),
                         b'\\x746861742773206be47365')
        self.assertEqual(f(r'\\x4f007073ff21'), b'\\x4f007073ff21')

    def test_decode_json(self):
        f = self.db.decode_json
        self.assertIsNone(f('null'))
        data = {
            "id": 1, "name": "Foo", "price": 1234.5,
            "new": True, "note": None,
            "tags": ["Bar", "Eek"],
            "stock": {"warehouse": 300, "retail": 20}}
        text = json.dumps(data)
        r = f(text)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, data)
        self.assertIsInstance(r['id'], int)
        self.assertIsInstance(r['name'], str)
        self.assertIsInstance(r['price'], float)
        self.assertIsInstance(r['new'], bool)
        self.assertIsInstance(r['tags'], list)
        self.assertIsInstance(r['stock'], dict)

    def test_encode_json(self):
        f = self.db.encode_json
        self.assertEqual(f(None), 'null')
        data = {
            "id": 1, "name": "Foo", "price": 1234.5,
            "new": True, "note": None,
            "tags": ["Bar", "Eek"],
            "stock": {"warehouse": 300, "retail": 20}}
        text = json.dumps(data)
        r = f(data)
        self.assertIsInstance(r, str)
        self.assertEqual(r, text)

    def test_get_parameter(self):
        f = self.db.get_parameter
        self.assertRaises(TypeError, f)
        self.assertRaises(TypeError, f, None)
        self.assertRaises(TypeError, f, 42)
        self.assertRaises(TypeError, f, '')
        self.assertRaises(TypeError, f, [])
        self.assertRaises(TypeError, f, [''])
        self.assertRaises(pg.ProgrammingError, f, 'this_does_not_exist')
        r = f('standard_conforming_strings')
        self.assertEqual(r, 'on')
        r = f('lc_monetary')
        self.assertEqual(r, 'C')
        r = f('datestyle')
        self.assertEqual(r, 'ISO, YMD')
        r = f('bytea_output')
        self.assertEqual(r, 'hex')
        r = f(['bytea_output', 'lc_monetary'])
        self.assertIsInstance(r, list)
        self.assertEqual(r, ['hex', 'C'])
        r = f(('standard_conforming_strings', 'datestyle', 'bytea_output'))
        self.assertEqual(r, ['on', 'ISO, YMD', 'hex'])
        r = f({'bytea_output', 'lc_monetary'})
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'bytea_output': 'hex', 'lc_monetary': 'C'})
        r = f({'Bytea_Output', ' LC_Monetary '})
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'Bytea_Output': 'hex', ' LC_Monetary ': 'C'})
        s = dict.fromkeys(('bytea_output', 'lc_monetary'))
        r = f(s)
        self.assertIs(r, s)
        self.assertEqual(r, {'bytea_output': 'hex', 'lc_monetary': 'C'})
        s = dict.fromkeys(('Bytea_Output', ' LC_Monetary '))
        r = f(s)
        self.assertIs(r, s)
        self.assertEqual(r, {'Bytea_Output': 'hex', ' LC_Monetary ': 'C'})

    def test_get_parameter_server_version(self):
        r = self.db.get_parameter('server_version_num')
        self.assertIsInstance(r, str)
        s = self.db.server_version
        self.assertIsInstance(s, int)
        self.assertEqual(r, str(s))

    def test_get_parameter_all(self):
        f = self.db.get_parameter
        r = f('all')
        self.assertIsInstance(r, dict)
        self.assertEqual(r['standard_conforming_strings'], 'on')
        self.assertEqual(r['lc_monetary'], 'C')
        self.assertEqual(r['DateStyle'], 'ISO, YMD')
        self.assertEqual(r['bytea_output'], 'hex')

    def test_set_parameter(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertRaises(TypeError, f)
        self.assertRaises(TypeError, f, None)
        self.assertRaises(TypeError, f, 42)
        self.assertRaises(TypeError, f, '')
        self.assertRaises(TypeError, f, [])
        self.assertRaises(TypeError, f, [''])
        self.assertRaises(ValueError, f, 'all', 'invalid')
        self.assertRaises(ValueError, f, {
            'invalid1': 'value1', 'invalid2': 'value2'}, 'value')
        self.assertRaises(pg.ProgrammingError, f, 'this_does_not_exist')
        f('enable_seqscan', 'off')
        self.assertEqual(g('enable_seqscan'), 'off')
        f('datestyle', 'ISO, DMY')
        self.assertEqual(g('datestyle'), 'ISO, DMY')
        f(['enable_seqscan', 'datestyle'], ['on', 'ISO, DMY'])
        self.assertEqual(g('enable_seqscan'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, DMY')
        f(['enable_indexscan', 'enable_seqscan'], 'off')
        self.assertEqual(g('enable_indexscan'), 'off')
        self.assertEqual(g('enable_seqscan'), 'off')
        f(('enable_seqscan', 'datestyle'), ('on', 'ISO, YMD'))
        self.assertEqual(g('enable_seqscan'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')
        f(('enable_indexscan', 'enable_seqscan'), 'off')
        self.assertEqual(g('enable_indexscan'), 'off')
        self.assertEqual(g('enable_seqscan'), 'off')
        f({'enable_indexscan', 'enable_seqscan'}, 'on')
        self.assertEqual(g('enable_indexscan'), 'on')
        self.assertEqual(g('enable_seqscan'), 'on')
        self.assertRaises(
            ValueError, f,
            {'enable_indexscan', 'enable_seqscan'},
            ['off', 'on'])
        f({'enable_indexscan', 'enable_seqscan'},
          ['off', 'off'])
        self.assertEqual(g('enable_indexscan'), 'off')
        self.assertEqual(g('enable_seqscan'), 'off')
        f({'enable_seqscan': 'on', 'datestyle': 'ISO, YMD'})
        self.assertEqual(g('enable_seqscan'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')

    def test_reset_parameter(self):
        db = DB()
        f = db.set_parameter
        g = db.get_parameter
        r = g('enable_indexscan')
        self.assertIn(r, ('on', 'off'))
        eis, not_eis = r, 'off' if r == 'on' else 'on'
        r = g('enable_seqscan')
        self.assertIn(r, ('on', 'off'))
        ess, not_ess = r, 'off' if r == 'on' else 'on'
        f('enable_indexscan', not_eis)
        f('enable_seqscan', not_ess)
        self.assertEqual(g('enable_indexscan'), not_eis)
        self.assertEqual(g('enable_seqscan'), not_ess)
        f('enable_indexscan')
        f('enable_seqscan', None)
        self.assertEqual(g('enable_indexscan'), eis)
        self.assertEqual(g('enable_seqscan'), ess)
        f('enable_indexscan', not_eis)
        f('enable_seqscan', not_ess)
        self.assertEqual(g('enable_indexscan'), not_eis)
        self.assertEqual(g('enable_seqscan'), not_ess)
        f(['enable_indexscan', 'enable_seqscan'], None)
        self.assertEqual(g('enable_indexscan'), eis)
        self.assertEqual(g('enable_seqscan'), ess)
        f('enable_indexscan', not_eis)
        f('enable_seqscan', not_ess)
        self.assertEqual(g('enable_indexscan'), not_eis)
        self.assertEqual(g('enable_seqscan'), not_ess)
        f(('enable_indexscan', 'enable_seqscan'))
        self.assertEqual(g('enable_indexscan'), eis)
        self.assertEqual(g('enable_seqscan'), ess)
        f('enable_indexscan', not_eis)
        f('enable_seqscan', not_ess)
        self.assertEqual(g('enable_indexscan'), not_eis)
        self.assertEqual(g('enable_seqscan'), not_ess)
        f({'enable_indexscan', 'enable_seqscan'})
        self.assertEqual(g('enable_indexscan'), eis)
        self.assertEqual(g('enable_seqscan'), ess)
        db.close()

    def test_reset_parameter_all(self):
        db = DB()
        f = db.set_parameter
        self.assertRaises(ValueError, f, 'all', 0)
        self.assertRaises(ValueError, f, 'all', 'off')
        g = db.get_parameter
        r = g('enable_indexscan')
        self.assertIn(r, ('on', 'off'))
        eis, not_eis = r, 'off' if r == 'on' else 'on'
        r = g('enable_seqscan')
        self.assertIn(r, ('on', 'off'))
        ess, not_ess = r, 'off' if r == 'on' else 'on'
        f('enable_indexscan', not_eis)
        f('enable_seqscan', not_ess)
        self.assertEqual(g('enable_indexscan'), not_eis)
        self.assertEqual(g('enable_seqscan'), not_ess)
        f('all')
        self.assertEqual(g('enable_indexscan'), eis)
        self.assertEqual(g('enable_seqscan'), ess)
        db.close()

    def test_set_parameter_local(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('enable_seqscan'), 'on')
        self.db.begin()
        f('enable_seqscan', 'off', local=True)
        self.assertEqual(g('enable_seqscan'), 'off')
        self.db.end()
        self.assertEqual(g('enable_seqscan'), 'on')

    def test_set_parameter_session(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('enable_seqscan'), 'on')
        self.db.begin()
        f('enable_seqscan', 'off', local=False)
        self.assertEqual(g('enable_seqscan'), 'off')
        self.db.end()
        self.assertEqual(g('enable_seqscan'), 'off')

    def test_reset(self):
        db = DB()
        default_datestyle = db.get_parameter('datestyle')
        changed_datestyle = 'ISO, DMY'
        if changed_datestyle == default_datestyle:
            changed_datestyle = 'ISO, YMD'
        self.db.set_parameter('datestyle', changed_datestyle)
        r = self.db.get_parameter('datestyle')
        self.assertEqual(r, changed_datestyle)
        con = self.db.db
        q = con.query("show datestyle")
        self.db.reset()
        r = q.getresult()[0][0]
        self.assertEqual(r, changed_datestyle)
        q = con.query("show datestyle")
        r = q.getresult()[0][0]
        self.assertEqual(r, default_datestyle)
        r = self.db.get_parameter('datestyle')
        self.assertEqual(r, default_datestyle)
        db.close()

    def test_reopen(self):
        db = DB()
        default_datestyle = db.get_parameter('datestyle')
        changed_datestyle = 'ISO, DMY'
        if changed_datestyle == default_datestyle:
            changed_datestyle = 'ISO, YMD'
        self.db.set_parameter('datestyle', changed_datestyle)
        r = self.db.get_parameter('datestyle')
        self.assertEqual(r, changed_datestyle)
        con = self.db.db
        q = con.query("show datestyle")
        self.db.reopen()
        r = q.getresult()[0][0]
        self.assertEqual(r, changed_datestyle)
        self.assertRaises(TypeError, getattr, con, 'query')
        r = self.db.get_parameter('datestyle')
        self.assertEqual(r, default_datestyle)
        db.close()

    def test_create_table(self):
        table = 'test hello world'
        values = [(2, "World!"), (1, "Hello")]
        self.create_table(table, "n smallint, t varchar",
                         temporary=True, values=values)
        r = self.db.query(f'select t from "{table}" order by n').getresult()
        r = ', '.join(row[0] for row in r)
        self.assertEqual(r, "Hello, World!")

    def test_query(self):
        query = self.db.query
        table = 'test_table'
        self.create_table(table, "n integer")
        q = "insert into test_table values (1)"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '1')
        q = "insert into test_table select 2"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '1')
        q = "select n from test_table where n>1"
        r = query(q).getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertIsInstance(r, int)
        self.assertEqual(r, 2)
        q = "insert into test_table select 3 union select 4 union select 5"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '3')
        q = "update test_table set n=4 where n<5"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '4')
        # noinspection SqlWithoutWhere
        q = "delete from test_table"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '5')

    def test_multiple_queries(self):
        self.assertEqual(self.db.query(
            "create temporary table test_multi (n integer);"
            "insert into test_multi values (4711);"
            "select n from test_multi").getresult()[0][0], 4711)

    def test_query_with_params(self):
        query = self.db.query
        self.create_table('test_table', 'n1 integer, n2 integer')
        q = "insert into test_table values ($1, $2)"
        r = query(q, (1, 2))
        self.assertEqual(r, '1')
        r = query(q, [3, 4])
        self.assertEqual(r, '1')
        r = query(q, [5, 6])
        self.assertEqual(r, '1')
        q = "select * from test_table order by 1, 2"
        self.assertEqual(query(q).getresult(),
                         [(1, 2), (3, 4), (5, 6)])
        q = "select * from test_table where n1=$1 and n2=$2"
        self.assertEqual(query(q, 3, 4).getresult(), [(3, 4)])
        q = "update test_table set n2=$2 where n1=$1"
        r = query(q, 3, 7)
        self.assertEqual(r, '1')
        q = "select * from test_table order by 1, 2"
        self.assertEqual(query(q).getresult(),
                         [(1, 2), (3, 7), (5, 6)])
        q = "delete from test_table where n2!=$1"
        r = query(q, 4)
        self.assertEqual(r, '3')

    def test_send_query_with_params(self):
        send_query = self.db.send_query
        q = "select $1::int + $2::int"
        for args in ((1, 2), ((1, 2),), ([1, 2],)):
            with self.subTest(args=args):
                query = send_query(q, *args)
                self.assertEqual(query.getresult(), [(3,)])
                self.assertIsNone(query.getresult())
        query = send_query(b"select $1::text", 'test')
        self.assertEqual(query.getresult(), [('test',)])
        self.assertIsNone(query.getresult())
        query = send_query("select 1; select 2")
        self.assertEqual(query.getresult(), [(1,)])
        self.assertEqual(query.getresult(), [(2,)])
        self.assertIsNone(query.getresult())

    def test_empty_query(self):
        self.assertRaises(ValueError, self.db.query, '')

    def test_query_data_error(self):
        try:
            self.db.query("select 1/0")
        except pg.DataError as error:
            # noinspection PyUnresolvedReferences
            self.assertEqual(error.sqlstate, '22012')

    def test_query_formatted(self):
        f = self.db.query_formatted
        t = True if pg.get_bool() else 't'
        # test with tuple
        q = f("select %s::int, %s::real, %s::text, %s::bool",
              (3, 2.5, 'hello', True))
        r = q.getresult()[0]
        self.assertEqual(r, (3, 2.5, 'hello', t))
        # test with tuple, inline
        q = f("select %s, %s, %s, %s", (3, 2.5, 'hello', True), inline=True)
        r = q.getresult()[0]
        self.assertEqual(r, (3, 2.5, 'hello', t))
        # test with dict
        q = f("select %(a)s::int, %(b)s::real, %(c)s::text, %(d)s::bool",
              dict(a=3, b=2.5, c='hello', d=True))
        r = q.getresult()[0]
        self.assertEqual(r, (3, 2.5, 'hello', t))
        # test with dict, inline
        q = f("select %(a)s, %(b)s, %(c)s, %(d)s",
              dict(a=3, b=2.5, c='hello', d=True), inline=True)
        r = q.getresult()[0]
        self.assertEqual(r, (3, 2.5, 'hello', t))
        # test with dict and extra values
        q = f("select %(a)s||%(b)s||%(c)s||%(d)s||'epsilon'",
              dict(a='alpha', b='beta', c='gamma', d='delta', e='extra'))
        r = q.getresult()[0][0]
        self.assertEqual(r, 'alphabetagammadeltaepsilon')

    def test_query_formatted_with_any(self):
        f = self.db.query_formatted
        q = "select 2 = any(%s)"
        r = f(q, [[1, 3]]).getresult()[0][0]
        self.assertEqual(r, False if pg.get_bool() else 'f')
        r = f(q, [[1, 2, 3]]).getresult()[0][0]
        self.assertEqual(r, True if pg.get_bool() else 't')
        r = f(q, [[]]).getresult()[0][0]
        self.assertEqual(r, False if pg.get_bool() else 'f')
        r = f(q, [[None]]).getresult()[0][0]
        self.assertIsNone(r)

    def test_query_formatted_without_params(self):
        f = self.db.query_formatted
        q = "select 42"
        r = f(q).getresult()[0][0]
        self.assertEqual(r, 42)
        r = f(q, None).getresult()[0][0]
        self.assertEqual(r, 42)
        r = f(q, []).getresult()[0][0]
        self.assertEqual(r, 42)
        r = f(q, {}).getresult()[0][0]
        self.assertEqual(r, 42)

    def test_prepare(self):
        p = self.db.prepare
        self.assertIsNone(p('my query', "select 'hello'"))
        self.assertIsNone(p('my other query', "select 'world'"))
        self.assertRaises(
            pg.ProgrammingError, p, 'my query', "select 'hello, too'")

    def test_prepare_unnamed(self):
        p = self.db.prepare
        self.assertIsNone(p('', "select null"))
        self.assertIsNone(p(None, "select null"))

    def test_prepare_with_types(self):
        p, f = self.db.prepare, self.db.query_prepared
        q = 'select pg_typeof($1)::text, pg_typeof($2)::text'
        p('q1', q, [20, 25])
        self.assertEqual(f('q1', 1, 2).getresult(), [('bigint', 'text')])
        p('q2', q, ['int', 'text[]'])
        self.assertEqual(
            f('q2', 1, '{2}').getresult(), [('integer', 'text[]')])
        p('q3', q, types=(b'int2', 'pg_catalog.varchar(10)'))
        self.assertEqual(
            f('q3', 1, 2).getresult(), [('smallint', 'character varying')])
        p(None, q, ('int8', 'int'))
        self.assertEqual(f(None, 1, 2).getresult(), [('bigint', 'integer')])
        # types given as None or 0 or not given are inferred
        q = 'select $1, $2'
        p('q4', q, [None, 'numeric'])
        self.assertEqual(f('q4', 1, 2).getresult(), [('1', Decimal(2))])
        p('q5', q, [0, 'int2'])
        self.assertEqual(f('q5', 1, '3').getresult(), [('1', 3)])
        p('q6', q, ['date'])
        self.assertEqual(
            f('q6', '2026-11-22', 2).getresult(), [(date(2026, 11, 22), '2')])
        p('q7', q, [])
        self.assertEqual(f('q7', 1, 2).getresult(), [('1', '2')])
        p('q8', q, None)
        self.assertEqual(f('q8', 1, 2).getresult(), [('1', '2')])

    def test_prepare_with_db_types(self):
        p, f = self.db.prepare, self.db.query_prepared
        # the exact OID of a DbType is used, not its name, which can be
        # the simple type name 'int' or the registered name 'bigint'
        typ = self.db.dbtypes['int8']
        self.assertEqual(typ.simple, 'int')
        p('q', 'select pg_typeof($1)::text, $1', [typ])
        self.assertEqual(f('q', 2**40).getresult(), [('bigint', 2**40)])

    def test_prepare_with_attnames(self):
        table = 'test_prepare_attnames'
        self.create_table(table, 'a int8, b text[], c date')
        types = self.db.get_attnames(table)
        q = 'select pg_typeof($1)::text, pg_typeof($2)::text, $3'
        self.db.prepare('q', q, [types['a'], types['b'], types['c']])
        self.assertEqual(
            self.db.query_prepared('q', 1, '{x}', '2026-11-22').getresult(),
            [('bigint', 'text[]', date(2026, 11, 22))])

    def test_prepare_with_qualified_and_quoted_type_names(self):
        q = 'select pg_typeof($1)::text, pg_typeof($2)::text'
        self.db.prepare('q', q, ['pg_catalog.int4', '"char"'])
        self.assertEqual(
            self.db.query_prepared('q', 1, 'x').getresult(),
            [('integer', '"char"')])

    def test_prepare_with_many_type_names(self):
        # the distinct type names are looked up only once
        types = ['int', 'text'] * 1000
        self.db.prepare('q', 'select $1, $2', types)
        self.assertEqual(
            self.db.query_prepared('q', *range(2000)).getresult(),
            [(0, '1')])

    def test_prepare_with_invalid_types(self):
        p = self.db.prepare
        self.assertRaises(TypeError, p, 'q', 'select $1', 'int')
        self.assertRaises(TypeError, p, 'q', 'select $1', b'int')
        self.assertRaises(TypeError, p, 'q', 'select $1', bytearray(b'i'))
        self.assertRaises(TypeError, p, 'q', 'select $1', 23)
        self.assertRaises(TypeError, p, 'q', 'select $1', {'int'})
        self.assertRaises(TypeError, p, 'q', 'select $1', [1.0])
        self.assertRaises(ValueError, p, 'q', 'select $1', [-1])
        self.assertRaises(
            pg.ProgrammingError, p, 'q', 'select $1', ['no_such_type'])
        self.assertRaises(
            pg.ProgrammingError, p, 'q', 'select $1', ['int', 'bad type'])
        self.assertRaises(
            pg.OperationalError, self.db.describe_prepared, 'q')

    def test_query_prepared_without_params(self):
        f = self.db.query_prepared
        self.assertRaises(pg.OperationalError, f, 'q')
        p = self.db.prepare
        p('q1', "select 17")
        p('q2', "select 42")
        r = f('q1').getresult()[0][0]
        self.assertEqual(r, 17)
        r = f('q2').getresult()[0][0]
        self.assertEqual(r, 42)

    def test_query_prepared_with_params(self):
        p = self.db.prepare
        p('sum', "select 1 + $1 + $2 + $3")
        p('cat', "select initcap($1) || ', ' || $2 || '!'")
        f = self.db.query_prepared
        r = f('sum', 2, 3, 5).getresult()[0][0]
        self.assertEqual(r, 11)
        r = f('cat', 'hello', 'world').getresult()[0][0]
        self.assertEqual(r, 'Hello, world!')

    def test_query_prepared_unnamed_with_out_params(self):
        f = self.db.query_prepared
        self.assertRaises(pg.OperationalError, f, None)
        self.assertRaises(pg.OperationalError, f, '')
        p = self.db.prepare
        # make sure all types are known so that we will not
        # generate other anonymous queries in the background
        p('', "select 'empty'::varchar")
        r = f(None).getresult()[0][0]
        self.assertEqual(r, 'empty')
        r = f('').getresult()[0][0]
        self.assertEqual(r, 'empty')
        p(None, "select 'none'::varchar")
        r = f(None).getresult()[0][0]
        self.assertEqual(r, 'none')
        r = f('').getresult()[0][0]
        self.assertEqual(r, 'none')

    def test_query_prepared_unnamed_with_params(self):
        p = self.db.prepare
        p('', "select 1 + $1 + $2")
        f = self.db.query_prepared
        r = f('', 2, 3).getresult()[0][0]
        self.assertEqual(r, 6)
        r = f(None, 2, 3).getresult()[0][0]
        self.assertEqual(r, 6)
        p(None, "select 2 + $1 + $2")
        f = self.db.query_prepared
        r = f('', 3, 4).getresult()[0][0]
        self.assertEqual(r, 9)
        r = f(None, 3, 4).getresult()[0][0]
        self.assertEqual(r, 9)

    def test_describe_prepared(self):
        self.db.prepare('count', "select 1 as first, 2 as second")
        f = self.db.describe_prepared
        r = f('count').listfields()
        self.assertEqual(r, ('first', 'second'))

    def test_describe_prepared_unnamed(self):
        self.db.prepare('', "select null as anon")
        f = self.db.describe_prepared
        r = f().listfields()
        self.assertEqual(r, ('anon',))
        r = f(None).listfields()
        self.assertEqual(r, ('anon',))
        r = f('').listfields()
        self.assertEqual(r, ('anon',))

    def test_delete_prepared(self):
        f = self.db.delete_prepared
        f()
        e = pg.OperationalError
        self.assertRaises(e, f, 'myquery')
        p = self.db.prepare
        p('q1', "select 1")
        p('q2', "select 2")
        f('q1')
        f('q2')
        self.assertRaises(e, f, 'q1')
        self.assertRaises(e, f, 'q2')
        p('q1', "select 1")
        p('q2', "select 2")
        f()
        self.assertRaises(e, f, 'q1')
        self.assertRaises(e, f, 'q2')

    def test_delete_prepared_quotes_name(self):
        f = self.db.delete_prepared
        p = self.db.prepare
        e = pg.OperationalError
        names = ['MyQuery', 'my query', 'my "query"', "my 'query'",
                 'all', 'q1; select 1']
        for name in names:
            p(name, "select 1")
        p('myquery', "select 1")
        p('q1', "select 1")
        for name in names:
            f(name)
            self.assertRaises(e, f, name)
        # the other statements have not been deleted
        r = self.db.query(
            'select name from pg_catalog.pg_prepared_statements'
            ' order by name').getresult()
        self.assertEqual(r, [('myquery',), ('q1',)])
        p(b'MyBytes', "select 1")
        f(b'MyBytes')
        self.assertRaises(e, f, b'MyBytes')
        self.assertRaises(ValueError, f, 'my\0query')
        self.assertRaises(TypeError, f, 42)

    def test_pkey(self):
        query = self.db.query
        pkey = self.db.pkey
        self.assertRaises(KeyError, pkey, 'test')
        for t in ('pkeytest', 'primary key test'):
            self.create_table(f'{t}0', 'a smallint')
            self.create_table(f'{t}1', 'b smallint primary key')
            self.create_table(f'{t}2', 'c smallint, d smallint primary key')
            self.create_table(
                f'{t}3',
                'e smallint, f smallint, g smallint, h smallint, i smallint,'
                ' primary key (f, h)')
            self.create_table(
                f'{t}4',
                'e smallint, f smallint, g smallint, h smallint, i smallint,'
                ' primary key (h, f)')
            self.create_table(
                f'{t}5', 'more_than_one_letter varchar primary key')
            self.create_table(
                f'{t}6', '"with space" date primary key')
            self.create_table(
                f'{t}7',
                'a_very_long_column_name varchar, "with space" date, "42" int,'
                ' primary key (a_very_long_column_name, "with space", "42")')
            self.assertRaises(KeyError, pkey, f'{t}0')
            self.assertEqual(pkey(f'{t}1'), 'b')
            self.assertEqual(pkey(f'{t}1', True), ('b',))
            self.assertEqual(pkey(f'{t}1', composite=False), 'b')
            self.assertEqual(pkey(f'{t}1', composite=True), ('b',))
            self.assertEqual(pkey(f'{t}2'), 'd')
            self.assertEqual(pkey(f'{t}2', composite=True), ('d',))
            r = pkey(f'{t}3')
            self.assertIsInstance(r, tuple)
            self.assertEqual(r, ('f', 'h'))
            r = pkey(f'{t}3', composite=False)
            self.assertIsInstance(r, tuple)
            self.assertEqual(r, ('f', 'h'))
            r = pkey(f'{t}4')
            self.assertIsInstance(r, tuple)
            self.assertEqual(r, ('h', 'f'))
            self.assertEqual(pkey(f'{t}5'), 'more_than_one_letter')
            self.assertEqual(pkey(f'{t}6'), 'with space')
            r = pkey(f'{t}7')
            self.assertIsInstance(r, tuple)
            self.assertEqual(r, (
                'a_very_long_column_name', 'with space', '42'))
            # a newly added primary key will be detected
            query(f'alter table "{t}0" add primary key (a)')
            self.assertEqual(pkey(f'{t}0'), 'a')
            # a changed primary key will not be detected,
            # indicating that the internal cache is operating
            query(f'alter table "{t}1" rename column b to x')
            self.assertEqual(pkey(f'{t}1'), 'b')
            # we get the changed primary key when the cache is flushed
            self.assertEqual(pkey(f'{t}1', flush=True), 'x')

    def test_pkeys(self):
        pkeys = self.db.pkeys
        t = 'pkeys_test_'
        self.create_table(f'{t}0', 'a int')
        self.create_table(f'{t}1', 'a int primary key, b int')
        self.create_table(f'{t}2', 'a int, b int, c int, primary key (a, c)')
        self.assertRaises(KeyError, pkeys, f'{t}0')
        self.assertEqual(pkeys(f'{t}1'), ('a',))
        self.assertEqual(pkeys(f'{t}2'), ('a', 'c'))

    def test_get_databases(self):
        databases = self.db.get_databases()
        self.assertIn('template0', databases)
        self.assertIn('template1', databases)
        self.assertNotIn('not existing database', databases)
        self.assertIn('postgres', databases)
        self.assertIn(dbname, databases)

    def test_get_tables(self):
        get_tables = self.db.get_tables
        tables = ('A very Special Name', 'A_MiXeD_quoted_NaMe',
                  'Hello, Test World!', 'Zoro', 'a1', 'a2', 'a321',
                  'averyveryveryveryveryveryveryreallyreallylongtablename',
                  'b0', 'b3', 'x', 'xXx', 'xx', 'y', 'z')
        for t in tables:
            self.db.query(f'drop table if exists "{t}" cascade')
        before_tables = get_tables()
        self.assertIsInstance(before_tables, list)
        for t in before_tables:
            s = t.split('.', 1)
            self.assertGreaterEqual(len(s), 2)
            if len(s) > 2:
                self.assertTrue(s[1].startswith('"'))
            t = s[0]
            self.assertNotEqual(t, 'information_schema')
            self.assertFalse(t.startswith('pg_'))
        for t in tables:
            self.create_table(t, 'as select 0', temporary=False)
        current_tables = get_tables()
        new_tables = [t for t in current_tables if t not in before_tables]
        expected_new_tables = ['public.' + (
            f'"{t}"' if ' ' in t or t != t.lower() else t) for t in tables]
        self.assertEqual(new_tables, expected_new_tables)
        self.doCleanups()
        after_tables = get_tables()
        self.assertEqual(after_tables, before_tables)

    def test_get_system_tables(self):
        get_tables = self.db.get_tables
        result = get_tables()
        self.assertNotIn('pg_catalog.pg_class', result)
        self.assertNotIn('information_schema.tables', result)
        result = get_tables(system=False)
        self.assertNotIn('pg_catalog.pg_class', result)
        self.assertNotIn('information_schema.tables', result)
        result = get_tables(system=True)
        self.assertIn('pg_catalog.pg_class', result)
        self.assertNotIn('information_schema.tables', result)

    def test_get_partitioned_tables(self):
        query = self.db.query
        query("drop table if exists test_parted cascade")
        query("create table test_parted (n int) partition by range (n)")
        self.addCleanup(query, "drop table test_parted cascade")
        query("create table test_parted_1 partition of test_parted"
              " for values from (0) to (10)")
        query("create index test_parted_idx on test_parted (n)")
        result = self.db.get_tables()
        self.assertIn('public.test_parted', result)
        self.assertIn('public.test_parted_1', result)
        self.assertIn('public.test', result)
        self.assertNotIn('public.test_view', result)
        self.assertNotIn('public.test_parted_idx', result)
        result = self.db.get_relations('p')
        self.assertIn('public.test_parted', result)
        self.assertNotIn('public.test_parted_1', result)
        result = self.db.get_relations('r')
        self.assertNotIn('public.test_parted', result)
        self.assertIn('public.test_parted_1', result)
        result = self.db.get_relations('I')
        self.assertEqual(result, ['public.test_parted_idx'])
        # partitioned tables can be used like ordinary tables
        self.db.insert('test_parted', n=5)
        self.assertEqual(list(self.db.get_attnames('test_parted')), ['n'])
        self.assertEqual(
            query("select n from test_parted_1").getresult(), [(5,)])

    def test_get_relations(self):
        get_relations = self.db.get_relations
        result = get_relations()
        self.assertIn('public.test', result)
        self.assertIn('public.test_view', result)
        result = get_relations('rv')
        self.assertIn('public.test', result)
        self.assertIn('public.test_view', result)
        result = get_relations('r')
        self.assertIn('public.test', result)
        self.assertNotIn('public.test_view', result)
        result = get_relations('v')
        self.assertNotIn('public.test', result)
        self.assertIn('public.test_view', result)
        result = get_relations('cisSt')
        self.assertNotIn('public.test', result)
        self.assertNotIn('public.test_view', result)
        result = get_relations(['r', 'v'])
        self.assertIn('public.test', result)
        self.assertIn('public.test_view', result)

    def test_get_relations_checks_kinds(self):
        get_relations = self.db.get_relations
        self.assertEqual(get_relations("'"), [])
        self.assertEqual(get_relations("\\"), [])
        self.assertRaises(ValueError, get_relations, ["r') or true or ('x"])
        self.assertRaises(ValueError, get_relations, ["r' or 'x"])
        self.assertRaises(ValueError, get_relations, ['rv'])
        self.assertRaises(ValueError, get_relations, ['r', ''])
        self.assertRaises(TypeError, get_relations, [1])
        self.assertRaises(TypeError, get_relations, b'r')

    def test_get_system_relations(self):
        get_relations = self.db.get_relations
        result = get_relations()
        self.assertNotIn('pg_catalog.pg_class', result)
        self.assertNotIn('information_schema.tables', result)
        result = get_relations(system=False)
        self.assertNotIn('pg_catalog.pg_class', result)
        self.assertNotIn('information_schema.tables', result)
        result = get_relations(system=True)
        self.assertIn('pg_catalog.pg_class', result)
        self.assertIn('information_schema.tables', result)

    def test_get_attnames(self):
        get_attnames = self.db.get_attnames
        self.assertRaises(pg.ProgrammingError,
                          self.db.get_attnames, 'does_not_exist')
        self.assertRaises(pg.ProgrammingError,
                          self.db.get_attnames, 'has.too.many.dots')
        r = get_attnames('test')
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, dict(
                i2='smallint', i4='integer', i8='bigint', d='numeric',
                f4='real', f8='double precision', m='money',
                v4='character varying', c4='character', t='text'))
        else:
            self.assertEqual(r, dict(
                i2='int', i4='int', i8='int', d='num',
                f4='float', f8='float', m='money',
                v4='text', c4='text', t='text'))
        self.create_table('test_table',
                         'n int, alpha smallint, beta bool,'
                         ' gamma char(5), tau text, v varchar(3)')
        r = get_attnames('test_table')
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, dict(
                n='integer', alpha='smallint', beta='boolean',
                gamma='character', tau='text', v='character varying'))
        else:
            self.assertEqual(r, dict(
                n='int', alpha='int', beta='bool',
                gamma='text', tau='text', v='text'))

    def test_get_attnames_with_quotes(self):
        get_attnames = self.db.get_attnames
        table = 'test table for get_attnames()'
        self.create_table(
            table,
            '"Prime!" smallint, "much space" integer, "Questions?" text')
        r = get_attnames(table)
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, {
                'Prime!': 'smallint', 'much space': 'integer',
                'Questions?': 'text'})
        else:
            self.assertEqual(r, {
                'Prime!': 'int', 'much space': 'int', 'Questions?': 'text'})
        table = 'yet another test table for get_attnames()'
        self.create_table(table,
                         'a smallint, b integer, c bigint,'
                         ' e numeric, f real, f2 double precision, m money,'
                         ' x smallint, y smallint, z smallint,'
                         ' Normal_NaMe smallint, "Special Name" smallint,'
                         ' t text, u char(2), v varchar(2),'
                         ' primary key (y, u)')
        r = get_attnames(table)
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, {
                'a': 'smallint', 'b': 'integer', 'c': 'bigint',
                'e': 'numeric', 'f': 'real', 'f2': 'double precision',
                'm': 'money', 'normal_name': 'smallint',
                'Special Name': 'smallint', 'u': 'character',
                't': 'text', 'v': 'character varying', 'y': 'smallint',
                'x': 'smallint', 'z': 'smallint'})
        else:
            self.assertEqual(r, {
                'a': 'int', 'b': 'int', 'c': 'int',
                'e': 'num', 'f': 'float', 'f2': 'float', 'm': 'money',
                'normal_name': 'int', 'Special Name': 'int',
                'u': 'text', 't': 'text', 'v': 'text',
                'y': 'int', 'x': 'int', 'z': 'int'})

    def test_get_attnames_with_regtypes(self):
        get_attnames = self.db.get_attnames
        self.create_table(
            'test_table', 'n int, alpha smallint, beta bool,'
                          ' gamma char(5), tau text, v varchar(3)')
        use_regtypes = self.db.use_regtypes
        regtypes = use_regtypes()
        self.assertEqual(regtypes, self.regtypes)
        use_regtypes(True)
        try:
            r = get_attnames("test_table")
            self.assertIsInstance(r, dict)
        finally:
            use_regtypes(regtypes)
        self.assertEqual(r, dict(
            n='integer', alpha='smallint', beta='boolean',
            gamma='character', tau='text', v='character varying'))

    def test_get_attnames_without_regtypes(self):
        get_attnames = self.db.get_attnames
        self.create_table(
            'test_table', 'n int, alpha smallint, beta bool,'
                          ' gamma char(5), tau text, v varchar(3)')
        use_regtypes = self.db.use_regtypes
        regtypes = use_regtypes()
        self.assertEqual(regtypes, self.regtypes)
        use_regtypes(False)
        try:
            r = get_attnames("test_table")
            self.assertIsInstance(r, dict)
        finally:
            use_regtypes(regtypes)
        self.assertEqual(r, dict(
            n='int', alpha='int', beta='bool',
            gamma='text', tau='text', v='text'))

    def test_get_attnames_with_domains(self):
        query = self.db.query
        query('create domain pg_temp.test_dint as int')
        query('create domain pg_temp.test_dbytea as bytea')
        query('create domain pg_temp.test_ddbytea as pg_temp.test_dbytea')
        query('create domain pg_temp.test_dints as int[]')
        self.create_table(
            'test_table', 'i pg_temp.test_dint, b pg_temp.test_dbytea,'
            ' bb pg_temp.test_ddbytea, a pg_temp.test_dints')
        use_regtypes = self.db.use_regtypes
        regtypes = use_regtypes()
        use_regtypes(False)
        try:
            r = self.db.get_attnames('test_table')
            self.assertEqual(r, dict(i='int', b='bytea', bb='bytea',
                                     a='int[]'))
            use_regtypes(True)
            r = self.db.get_attnames('test_table')
            self.assertEqual(r, dict(i='test_dint', b='test_dbytea',
                                     bb='test_ddbytea', a='test_dints'))
        finally:
            use_regtypes(regtypes)
        self.assertEqual(r['i'].typtype, 'd')
        self.assertEqual(r['i'].simple, 'int')
        self.assertEqual(r['i'].base, 'integer')
        self.assertEqual(r['i'].base.oid, 23)
        self.assertEqual(r['b'].base.oid, 17)
        self.assertIs(r['bb'].base, r['b'].base)  # fully resolved
        self.assertEqual(r['a'].simple, 'int[]')
        self.assertEqual(r['a'].elem, 23)
        self.assertIsNone(r['a'].base.base)
        self.assertIsNone(self.db.get_attnames('test')['i4'].base)

    def test_get_attnames_is_cached(self):
        get_attnames = self.db.get_attnames
        int_type = 'integer' if self.regtypes else 'int'
        text_type = 'text'
        query = self.db.query
        self.create_table('test_table', 'col int')
        r = get_attnames("test_table")
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(col=int_type))
        query("alter table test_table alter column col type text")
        query("alter table test_table add column col2 int")
        r = get_attnames("test_table")
        self.assertEqual(r, dict(col=int_type))
        r = get_attnames("test_table", flush=True)
        self.assertEqual(r, dict(col=text_type, col2=int_type))
        query("alter table test_table drop column col2")
        r = get_attnames("test_table")
        self.assertEqual(r, dict(col=text_type, col2=int_type))
        r = get_attnames("test_table", flush=True)
        self.assertEqual(r, dict(col=text_type))
        query("alter table test_table drop column col")
        r = get_attnames("test_table")
        self.assertEqual(r, dict(col=text_type))
        r = get_attnames("test_table", flush=True)
        self.assertEqual(r, dict())

    def test_get_attnames_is_ordered(self):
        get_attnames = self.db.get_attnames
        r = get_attnames('test', flush=True)
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, {
                'i2': 'smallint', 'i4': 'integer', 'i8': 'bigint',
                'd': 'numeric', 'f4': 'real', 'f8': 'double precision',
                'm': 'money', 'v4': 'character varying',
                'c4': 'character', 't': 'text'})
        else:
            self.assertEqual(r, {
                'i2': 'int', 'i4': 'int', 'i8': 'int',
                'd': 'num', 'f4': 'float', 'f8': 'float', 'm': 'money',
                'v4': 'text', 'c4': 'text', 't': 'text'})
        r = ' '.join(list(r.keys()))
        self.assertEqual(r, 'i2 i4 i8 d f4 f8 m v4 c4 t')
        table = 'test table for get_attnames'
        self.create_table(
            table, 'n int, alpha smallint, v varchar(3),'
                   ' gamma char(5), tau text, beta bool')
        r = get_attnames(table)
        self.assertIsInstance(r, dict)
        if self.regtypes:
            self.assertEqual(r, {
                'n': 'integer', 'alpha': 'smallint',
                'v': 'character varying', 'gamma': 'character',
                'tau': 'text', 'beta': 'boolean'})
        else:
            self.assertEqual(r, {
                'n': 'int', 'alpha': 'int', 'v': 'text',
                'gamma': 'text', 'tau': 'text', 'beta': 'bool'})
        r = ' '.join(list(r.keys()))
        self.assertEqual(r, 'n alpha v gamma tau beta')

    def test_get_attnames_is_attr_dict(self):
        from pg.attrs import AttrDict
        get_attnames = self.db.get_attnames
        r = get_attnames('test', flush=True)
        self.assertIsInstance(r, AttrDict)
        if self.regtypes:
            self.assertEqual(r, AttrDict(
                i2='smallint', i4='integer', i8='bigint',
                d='numeric', f4='real', f8='double precision',
                m='money', v4='character varying',
                c4='character', t='text'))
        else:
            self.assertEqual(r, AttrDict(
                i2='int', i4='int', i8='int',
                d='num', f4='float', f8='float', m='money',
                v4='text', c4='text', t='text'))
        r = ' '.join(list(r.keys()))
        self.assertEqual(r, 'i2 i4 i8 d f4 f8 m v4 c4 t')
        table = 'test table for get_attnames'
        self.create_table(
            table, 'n int, alpha smallint, v varchar(3),'
                   ' gamma char(5), tau text, beta bool')
        r = get_attnames(table)
        self.assertIsInstance(r, AttrDict)
        if self.regtypes:
            self.assertEqual(r, AttrDict(
                n='integer', alpha='smallint',
                v='character varying', gamma='character',
                tau='text', beta='boolean'))
        else:
            self.assertEqual(r, AttrDict(
                n='int', alpha='int', v='text',
                gamma='text', tau='text', beta='bool'))
        r = ' '.join(list(r.keys()))
        self.assertEqual(r, 'n alpha v gamma tau beta')

    def test_get_generated(self):
        get_generated = self.db.get_generated
        server_version = self.db.server_version
        if server_version >= 100000:
            self.assertRaises(pg.ProgrammingError,
                              self.db.get_generated, 'does_not_exist')
            self.assertRaises(pg.ProgrammingError,
                              self.db.get_generated, 'has.too.many.dots')
        r = get_generated('test')
        self.assertIsInstance(r, frozenset)
        self.assertFalse(r)
        if server_version >= 100000:
            table = 'test_get_generated_1'
            self.create_table(
                table,
                'i int generated always as identity primary key,'
                ' j int generated always as identity,'
                ' k int generated by default as identity,'
                ' n serial, m int')
            r = get_generated(table)
            self.assertIsInstance(r, frozenset)
            self.assertEqual(r, {'i', 'j'})
        if server_version >= 120000:
            table = 'test_get_generated_2'
            self.create_table(
                table,
                'n int, m int generated always as (n + 3) stored,'
                ' i int generated always as identity,'
                ' j int generated by default as identity')
            r = get_generated(table)
            self.assertIsInstance(r, frozenset)
            self.assertEqual(r, {'m', 'i'})

    def test_get_generated_is_cached(self):
        server_version = self.db.server_version
        if server_version < 100000:
            self.skipTest("database does not support generated columns")
        get_generated = self.db.get_generated
        query = self.db.query
        table = 'test_get_generated_2'
        self.create_table(table, 'i int primary key')
        self.assertFalse(get_generated(table))
        query(f'alter table {table} alter column i'
              ' add generated always as identity')
        self.assertFalse(get_generated(table))
        self.assertEqual(get_generated(table, flush=True), {'i'})

    def test_has_table_privilege(self):
        can = self.db.has_table_privilege
        self.assertEqual(can('test'), True)
        self.assertEqual(can('test', 'select'), True)
        self.assertEqual(can('test', 'SeLeCt'), True)
        self.assertEqual(can('test', 'SELECT'), True)
        self.assertEqual(can('test', 'insert'), True)
        self.assertEqual(can('test', 'update'), True)
        self.assertEqual(can('test', 'delete'), True)
        self.assertRaises(pg.DataError, can, 'test', 'foobar')
        self.assertRaises(pg.ProgrammingError, can, 'table_does_not_exist')
        r = self.db.query(
            'select rolsuper FROM pg_roles'
            ' where rolname=current_user').getresult()[0][0]
        if not pg.get_bool():
            r = r == 't'
        if r:
            self.skipTest('must not be superuser')
        self.assertEqual(can('pg_views', 'select'), True)
        self.assertEqual(can('pg_views', 'delete'), False)

    def test_get(self):
        get = self.db.get
        query = self.db.query
        table = 'get_test_table'
        self.assertRaises(TypeError, get)
        self.assertRaises(TypeError, get, table)
        self.create_table(table, 'n integer, t text',
                         values=enumerate('xyz', start=1))
        self.assertRaises(pg.ProgrammingError, get, table, 2)
        r: Any = get(table, 2, 'n')
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(n=2, t='y'))
        r = get(table, 1, 'n')
        self.assertEqual(r, dict(n=1, t='x'))
        r = get(table, (3,), ('n',))
        self.assertEqual(r, dict(n=3, t='z'))
        r = get(table, 'y', 't')
        self.assertEqual(r, dict(n=2, t='y'))
        self.assertRaises(pg.DatabaseError, get, table, 4)
        self.assertRaises(pg.DatabaseError, get, table, 4, 'n')
        self.assertRaises(pg.DatabaseError, get, table, 'y')
        self.assertRaises(pg.DatabaseError, get, table, 2, 't')
        s: dict = dict(n=3)
        self.assertRaises(pg.ProgrammingError, get, table, s)
        r = get(table, s, 'n')
        self.assertIs(r, s)
        self.assertEqual(r, dict(n=3, t='z'))
        s.update(t='x')
        r = get(table, s, 't')
        self.assertIs(r, s)
        self.assertEqual(s, dict(n=1, t='x'))
        r = get(table, s, ('n', 't'))
        self.assertIs(r, s)
        self.assertEqual(r, dict(n=1, t='x'))
        query(f'alter table "{table}" alter n set not null')
        query(f'alter table "{table}" add primary key (n)')
        r = get(table, 2)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(n=2, t='y'))
        self.assertEqual(get(table, 1)['t'], 'x')
        self.assertEqual(get(table, 3)['t'], 'z')
        self.assertEqual(get(table + '*', 2)['t'], 'y')
        self.assertEqual(get(table + ' *', 2)['t'], 'y')
        self.assertRaises(KeyError, get, table, (2, 2))
        s = dict(n=3)
        r = get(table, s)
        self.assertIs(r, s)
        self.assertEqual(r, dict(n=3, t='z'))
        s.update(n=1)
        self.assertEqual(get(table, s)['t'], 'x')
        s.update(n=2)
        self.assertEqual(get(table, r)['t'], 'y')
        s.pop('n')
        self.assertRaises(KeyError, get, table, s)

    def test_get_with_keyname(self):
        get = self.db.get
        table = 'get_keyname_test_table'
        self.create_table(table, 'a int, b int, t text',
                          values=[(1, 1, 'x'), (1, 2, 'y')])
        self.assertRaises(pg.ProgrammingError, get, table, (1, 2))
        self.assertEqual(get(table, (1, 2), ('a', 'b'))['t'], 'y')
        self.assertEqual(get(table, [1, 2], ['a', 'b'])['t'], 'y')
        self.assertEqual(get(table, dict(a=1, b=1), ['a', 'b'])['t'], 'x')
        self.assertRaises(KeyError, get, table, dict(a=1), ('a', 'b'))
        self.assertRaises(TypeError, get, table, 1, 1)
        self.assertRaises(TypeError, get, table, (1, 2), (1, 2))

    def test_get_with_composite_key(self):
        get = self.db.get
        table = 'get_test_table_1'
        self.create_table(
            table, 'n integer primary key, t text',
            values=enumerate('abc', start=1))
        self.assertEqual(get(table, 2)['t'], 'b')
        self.assertEqual(get(table, 1, 'n')['t'], 'a')
        self.assertEqual(get(table, 2, ('n',))['t'], 'b')
        self.assertEqual(get(table, 3, ['n'])['t'], 'c')
        self.assertEqual(get(table, (2,), ('n',))['t'], 'b')
        self.assertEqual(get(table, 'b', 't')['n'], 2)
        self.assertEqual(get(table, ('a',), ('t',))['n'], 1)
        self.assertEqual(get(table, ['c'], ['t'])['n'], 3)
        table = 'get_test_table_2'
        self.create_table(
            table, 'n integer, m integer, t text, primary key (n, m)',
            values=[(n + 1, m + 1, chr(ord('a') + 2 * n + m))
                    for n in range(3) for m in range(2)])
        self.assertRaises(KeyError, get, table, 2)
        self.assertEqual(get(table, (1, 1))['t'], 'a')
        self.assertEqual(get(table, (1, 2))['t'], 'b')
        self.assertEqual(get(table, (2, 1))['t'], 'c')
        self.assertEqual(get(table, (1, 2), ('n', 'm'))['t'], 'b')
        self.assertEqual(get(table, (1, 2), ('m', 'n'))['t'], 'c')
        self.assertEqual(get(table, (3, 1), ('n', 'm'))['t'], 'e')
        self.assertEqual(get(table, (1, 3), ('m', 'n'))['t'], 'e')
        self.assertEqual(get(table, dict(n=2, m=2))['t'], 'd')
        self.assertEqual(get(table, dict(n=1, m=2), ('n', 'm'))['t'], 'b')
        self.assertEqual(get(table, dict(n=2, m=1), ['n', 'm'])['t'], 'c')
        self.assertEqual(get(table, dict(n=3, m=2), ('m', 'n'))['t'], 'f')

    def test_get_with_quoted_names(self):
        get = self.db.get
        table = 'test table for get()'
        self.create_table(
            table, '"Prime!" smallint primary key,'
                   ' "much space" integer, "Questions?" text',
            values=[(17, 1001, 'No!')])
        r = get(table, 17)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 17)
        self.assertEqual(r['much space'], 1001)
        self.assertEqual(r['Questions?'], 'No!')

    def test_get_with_oid_column(self):
        # the oid is a normal column since PostgreSQL 12
        self.assertIn('oid', self.db.get_attnames('pg_namespace'))
        r = self.db.get('pg_namespace', 'public', 'nspname')
        self.assertEqual(r['nspname'], 'public')
        self.assertIsInstance(r['oid'], int)
        self.assertNotIn('oid(pg_namespace)', r)
        self.assertEqual(
            self.db.get('pg_namespace', r['oid'], 'oid')['nspname'], 'public')

    def test_get_from_view(self):
        self.db.query('delete from test where i4=14')
        self.db.query('insert into test (i4, v4) values('
                      "14, 'abc4')")
        r = self.db.get('test_view', 14, 'i4')
        self.assertIn('v4', r)
        self.assertEqual(r['v4'], 'abc4')

    def test_get_little_bobby_tables(self):
        get = self.db.get
        query = self.db.query
        self.create_table(
            'test_students',
            'firstname varchar primary key, nickname varchar, grade char(2)',
            values=[("D'Arcy", 'Darcey', 'A+'), ('Sheldon', 'Moonpie', 'A+'),
                    ('Robert', 'Little Bobby Tables', 'D-')])
        r = get('test_students', 'Sheldon')
        self.assertEqual(r, dict(
            firstname="Sheldon", nickname='Moonpie', grade='A+'))
        r = get('test_students', 'Robert')
        self.assertEqual(r, dict(
            firstname="Robert", nickname='Little Bobby Tables', grade='D-'))
        r = get('test_students', "D'Arcy")
        self.assertEqual(r, dict(
            firstname="D'Arcy", nickname='Darcey', grade='A+'))
        try:
            get('test_students', "D' Arcy")
        except pg.DatabaseError as error:
            self.assertEqual(
                str(error),
                'No such record in test_students\nwhere "firstname" = $1\n'
                'with $1="D\' Arcy"')
        try:
            get('test_students', "Robert'); TRUNCATE TABLE test_students;--")
        except pg.DatabaseError as error:
            self.assertEqual(
                str(error),
                'No such record in test_students\nwhere "firstname" = $1\n'
                'with $1="Robert\'); TRUNCATE TABLE test_students;--"')
        q = "select * from test_students order by 1 limit 4"
        r = query(q).getresult()
        self.assertEqual(len(r), 3)
        self.assertEqual(r[1][2], 'D-')

    def test_insert(self):
        insert = self.db.insert
        query = self.db.query
        bool_on = pg.get_bool()
        decimal = pg.get_decimal()
        table = 'insert_test_table'
        self.create_table(
            table, 'i2 smallint, i4 integer, i8 bigint,'
                   ' d numeric, f4 real, f8 double precision, m money,'
                   ' v4 varchar(4), c4 char(4), t text,'
                   ' b boolean, ts timestamp')
        tests: list[dict | tuple[dict, dict]] = [
            dict(i2=None, i4=None, i8=None),
            (dict(i2='', i4='', i8=''), dict(i2=None, i4=None, i8=None)),
            (dict(i2=0, i4=0, i8=0), dict(i2=0, i4=0, i8=0)),
            dict(i2=42, i4=123456, i8=9876543210),
            dict(i2=2 ** 15 - 1, i4=2 ** 31 - 1, i8=2 ** 63 - 1),
            dict(d=None), (dict(d=''), dict(d=None)),
            dict(d=Decimal(0)), (dict(d=0), dict(d=Decimal(0))),
            dict(f4=None, f8=None), dict(f4=0, f8=0),
            (dict(f4='', f8=''), dict(f4=None, f8=None)),
            (dict(d=1234.5, f4=1234.5, f8=1234.5),
             dict(d=Decimal('1234.5'))),
            dict(d=Decimal('123.456789'), f4=12.375, f8=123.4921875),
            dict(d=Decimal('123456789.9876543212345678987654321')),
            dict(m=None), (dict(m=''), dict(m=None)),
            dict(m=Decimal('-1234.56')),
            (dict(m='-1234.56'), dict(m=Decimal('-1234.56'))),
            dict(m=Decimal('1234.56')), dict(m=Decimal('123456')),
            (dict(m='1234.56'), dict(m=Decimal('1234.56'))),
            (dict(m=1234.5), dict(m=Decimal('1234.5'))),
            (dict(m=-1234.5), dict(m=Decimal('-1234.5'))),
            (dict(m=123456), dict(m=Decimal('123456'))),
            (dict(m='1234567.89'), dict(m=Decimal('1234567.89'))),
            dict(b=None), (dict(b=''), dict(b=None)),
            dict(b='f'), dict(b='t'),
            (dict(b=0), dict(b='f')), (dict(b=1), dict(b='t')),
            (dict(b=False), dict(b='f')), (dict(b=True), dict(b='t')),
            (dict(b='0'), dict(b='f')), (dict(b='1'), dict(b='t')),
            (dict(b='n'), dict(b='f')), (dict(b='y'), dict(b='t')),
            (dict(b='no'), dict(b='f')), (dict(b='yes'), dict(b='t')),
            (dict(b='off'), dict(b='f')), (dict(b='on'), dict(b='t')),
            dict(v4=None, c4=None, t=None),
            (dict(v4='', c4='', t=''), dict(c4=' ' * 4)),
            dict(v4='1234', c4='1234', t='1234' * 10),
            dict(v4='abcd', c4='abcd', t='abcdefg'),
            (dict(v4='abc', c4='abc', t='abc'), dict(c4='abc ')),
            dict(ts=None), (dict(ts=''), dict(ts=None)),
            (dict(ts=0), dict(ts=None)), (dict(ts=False), dict(ts=None)),
            dict(ts='2012-12-21 00:00:00'),
            (dict(ts='2012-12-21'), dict(ts='2012-12-21 00:00:00')),
            dict(ts='2012-12-21 12:21:12'),
            dict(ts='2013-01-05 12:13:14'),
            dict(ts='current_timestamp')]
        for test in tests:
            if isinstance(test, dict):
                data: dict = test
                change: dict = {}
            else:
                data, change = test
            expect = data.copy()
            expect.update(change)
            if bool_on:
                b = expect.get('b')
                if b is not None:
                    expect['b'] = b == 't'
            if decimal is not Decimal:
                d = expect.get('d')
                if d is not None:
                    expect['d'] = decimal(d)
                m = expect.get('m')
                if m is not None:
                    expect['m'] = decimal(m)
            self.assertEqual(insert(table, data), data)
            data = dict(item for item in data.items()
                        if item[0] in expect)
            ts = expect.get('ts')
            if ts:
                if ts == 'current_timestamp':
                    ts = data['ts']
                    self.assertIsInstance(ts, datetime)
                    self.assertEqual(
                        ts.strftime('%Y-%m-%d'), strftime('%Y-%m-%d'))
                else:
                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                expect['ts'] = ts
            self.assertEqual(data, expect)
            data = query(f'select * from "{table}"').dictresult()[0]
            data = dict(item for item in data.items() if item[0] in expect)
            self.assertEqual(data, expect)
            query(f'truncate table "{table}"')

    def test_insert_with_quoted_names(self):
        insert = self.db.insert
        query = self.db.query
        table = 'test table for insert()'
        self.create_table(table, '"Prime!" smallint primary key,'
                                ' "much space" integer, "Questions?" text')
        r: Any = {'Prime!': 11, 'much space': 2002, 'Questions?': 'What?'}
        r = insert(table, r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 11)
        self.assertEqual(r['much space'], 2002)
        self.assertEqual(r['Questions?'], 'What?')
        r = query(f'select * from "{table}" limit 2').dictresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r['Prime!'], 11)
        self.assertEqual(r['much space'], 2002)
        self.assertEqual(r['Questions?'], 'What?')

    def test_insert_into_view(self):
        insert = self.db.insert
        query = self.db.query
        query("truncate table test")
        q = 'select * from test_view order by i4 limit 3'
        r: Any = query(q).getresult()
        self.assertEqual(r, [])
        r = dict(i4=1234, v4='abcd')
        insert('test', r)
        self.assertIsNone(r['i2'])
        self.assertEqual(r['i4'], 1234)
        self.assertIsNone(r['i8'])
        self.assertEqual(r['v4'], 'abcd')
        self.assertIsNone(r['c4'])
        r = query(q).getresult()
        self.assertEqual(r, [(1234, 'abcd')])
        r = dict(i4=5678, v4='efgh')
        insert('test_view', r)
        self.assertNotIn('i2', r)
        self.assertEqual(r['i4'], 5678)
        self.assertNotIn('i8', r)
        self.assertEqual(r['v4'], 'efgh')
        self.assertNotIn('c4', r)
        r = query(q).getresult()
        self.assertEqual(r, [(1234, 'abcd'), (5678, 'efgh')])

    def test_insert_with_generated_columns(self):
        insert = self.db.insert
        get = self.db.get
        server_version = self.db.server_version
        table = 'insert_test_table_2'
        table_def = 'i int not null'
        if server_version >= 100000:
            table_def += (
                ', a int generated always as identity'
                ', d int generated by default as identity primary key')
        else:
            table_def += ', a int not null default 1, d int primary key'
        if server_version >= 120000:
            table_def += ', j int generated always as (i + 7) stored'
        else:
            table_def += ', j int not null default 42'
        self.create_table(table, table_def)
        i, d = 35, 1001
        j = i + 7
        r = insert(table, {'i': i, 'd': d, 'a': 1, 'j': j})
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})
        r = get(table, d)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})

    def test_update(self):
        update = self.db.update
        query = self.db.query
        self.assertRaises(pg.ProgrammingError, update,
                          'test', i2=2, i4=4, i8=8)
        table = 'update_test_table'
        self.create_table(table, 'n integer primary key, t text',
                         values=enumerate('xyz', start=1))
        self.assertRaises(pg.DatabaseError, self.db.get, table, 4)
        r = self.db.get(table, 2)
        r['t'] = 'u'
        s = update(table, r)
        self.assertEqual(s, r)
        q = f'select t from "{table}" where n=2'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'u')

    def test_update_without_primary_key(self):
        update = self.db.update
        query = self.db.query
        self.assertRaises(pg.ProgrammingError, update,
                          'test', i2=2, i4=4, i8=8)
        table = 'update_test_table'
        self.create_table(table, 'n integer primary key, t text',
                         values=enumerate('xyz', start=1))
        r = self.db.get(table, 2)
        r['t'] = 'u'
        s = update(table, r)
        self.assertEqual(s, r)
        q = f'select t from "{table}" where n=2'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'u')

    def test_update_with_keyname(self):
        update = self.db.update
        query = self.db.query
        table = 'update_keyname_test_table'
        self.create_table(
            table, 'id int primary key, email text unique, name text',
            values=[(1, 'a@x', 'A'), (2, 'b@x', 'B')])
        q = f'select * from "{table}" order by id'
        r = update(table, dict(email='b@x', name='C'), 'email')
        self.assertEqual(r, dict(id=2, email='b@x', name='C'))
        # the primary key can be updated when using another key
        r = update(table, dict(email='a@x', id=3), keyname='email')
        self.assertEqual(r, dict(id=3, email='a@x', name='A'))
        self.assertEqual(
            query(q).getresult(), [(2, 'b@x', 'C'), (3, 'a@x', 'A')])
        # the key columns are not updated
        r = update(table, dict(id=2, email='c@x'), ['id', 'email'])
        self.assertEqual(r, dict(id=2, email='c@x'))
        self.assertEqual(
            query(q).getresult(), [(2, 'b@x', 'C'), (3, 'a@x', 'A')])
        # keyword arguments are merged into the row
        r = update(table, None, 'email', email='b@x', name='D')
        self.assertEqual(r, dict(id=2, email='b@x', name='D'))
        self.assertRaises(KeyError, update, table, dict(id=2), 'email')
        self.assertRaises(TypeError, update, table, dict(id=2), 2)

    def test_update_with_keyname_without_primary_key(self):
        update = self.db.update
        table = 'update_keyname_test_table_2'
        self.create_table(table, 'a int, b int, t text',
                          values=[(1, 1, 'x'), (1, 2, 'y')])
        self.assertRaises(
            pg.ProgrammingError, update, table, dict(a=1, b=2, t='z'))
        r = update(table, dict(a=1, b=2, t='z'), ('a', 'b'))
        self.assertEqual(r, dict(a=1, b=2, t='z'))
        self.assertEqual(self.db.query(
            f'select t from "{table}" order by b').getresult(),
            [('x',), ('z',)])

    def test_update_with_composite_key(self):
        update = self.db.update
        query = self.db.query
        table = 'update_test_table_1'
        self.create_table(table, 'n integer primary key, t text',
                         values=enumerate('abc', start=1))
        self.assertRaises(KeyError, update, table, dict(t='b'))
        s = dict(n=2, t='d')
        r = update(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'd')
        q = f'select t from "{table}" where n=2'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'd')
        s.update(dict(n=4, t='e'))
        r = update(table, s)
        self.assertEqual(r['n'], 4)
        self.assertEqual(r['t'], 'e')
        q = f'select t from "{table}" where n=2'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'd')
        q = f'select t from "{table}" where n=4'
        r = query(q).getresult()
        self.assertEqual(len(r), 0)
        query(f'drop table "{table}"')
        table = 'update_test_table_2'
        self.create_table(table,
                         'n integer, m integer, t text, primary key (n, m)',
                         values=[(n + 1, m + 1, chr(ord('a') + 2 * n + m))
                                 for n in range(3) for m in range(2)])
        self.assertRaises(KeyError, update, table, dict(n=2, t='b'))
        self.assertEqual(update(table,
                                dict(n=2, m=2, t='x'))['t'], 'x')
        q = f'select t from "{table}" where n=2 order by m'
        r = [r[0] for r in query(q).getresult()]
        self.assertEqual(r, ['c', 'x'])

    def test_update_with_quoted_names(self):
        update = self.db.update
        query = self.db.query
        table = 'test table for update()'
        self.create_table(table, '"Prime!" smallint primary key,'
                                ' "much space" integer, "Questions?" text',
                         values=[(13, 3003, 'Why!')])
        r: Any = {'Prime!': 13, 'much space': 7007, 'Questions?': 'When?'}
        r = update(table, r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 13)
        self.assertEqual(r['much space'], 7007)
        self.assertEqual(r['Questions?'], 'When?')
        r = query(f'select * from "{table}" limit 2').dictresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r['Prime!'], 13)
        self.assertEqual(r['much space'], 7007)
        self.assertEqual(r['Questions?'], 'When?')

    def test_update_with_generated_columns(self):
        update = self.db.update
        get = self.db.get
        query = self.db.query
        server_version = self.db.server_version
        table = 'update_test_table_2'
        table_def = 'i int not null'
        if server_version >= 100000:
            table_def += (
                ', a int generated always as identity'
                ', d int generated by default as identity primary key')
        else:
            table_def += ', a int not null default 1, d int primary key'
        if server_version >= 120000:
            table_def += ', j int generated always as (i + 7) stored'
        else:
            table_def += ', j int not null default 42'
        self.create_table(table, table_def)
        i, d = 35, 1001
        j = i + 7
        r: Any = query(f'insert into {table} (i, d) values ({i}, {d})')
        self.assertEqual(r, '1')
        r = get(table, d)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})
        r['i'] += 1
        r = update(table, r)
        i += 1
        if server_version >= 120000:
            j += 1
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})

    def test_upsert(self):
        upsert = self.db.upsert
        query = self.db.query
        self.assertRaises(pg.ProgrammingError, upsert,
                          'test', i2=2, i4=4, i8=8)
        table = 'upsert_test_table'
        self.create_table(table, 'n integer primary key, t text')
        s: dict = dict(n=1, t='x')
        r: Any = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['t'], 'x')
        s.update(n=2, t='y')
        r = upsert(table, s, **dict.fromkeys(s))
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'y')
        q = f'select n, t from "{table}" order by n limit 3'
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'y')])
        s.update(t='z')
        r = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'z')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'z')])
        s.update(t='n')
        r = upsert(table, s, t=False)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'z')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'z')])
        s.update(t='y')
        r = upsert(table, s, t=True)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'y')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'y')])
        s.update(t='n')
        r = upsert(table, s, t="included.t || '2'")
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'y2')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'y2')])
        s.update(t='y')
        r = upsert(table, s, t="excluded.t || '3'")
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'y3')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x'), (2, 'y3')])
        s.update(n=1, t='2')
        r = upsert(table, s, t="included.t || excluded.t")
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['t'], 'x2')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x2'), (2, 'y3')])
        # not existing columns should be ignored
        s = dict(m=3, u='z')
        r = upsert(table, s, oid='invalid')
        self.assertIs(r, s)
        s = dict(n=2)
        # do not modify columns missing in the dict
        r = upsert(table, s)
        self.assertIs(r, s)
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x2'), (2, 'y3')])

    def test_upsert_without_update(self):
        upsert = self.db.upsert
        table = 'upsert_test_table'
        self.create_table(table, 'n integer primary key, t text')
        r = upsert(table, dict(n=1, t='x'), t=False)
        self.assertEqual(r, dict(n=1, t='x'))
        output: list[str] = []
        self.db.debug = output.append
        r = upsert(table, dict(n=1, t='y'), t=False)
        self.assertEqual(r, dict(n=1, t='x'))
        self.assertIn('INSERT INTO', output[0])
        if self.db.server_version >= 190000:
            # the existing row is returned by the same statement
            self.assertIn(' DO select ', output[0])
            self.assertEqual(len(output), 1)
        else:
            # the existing row must be fetched with a separate query
            self.assertIn(' DO nothing ', output[0])
            self.assertEqual(len(output), 2)
            self.assertIn('SELECT', output[1])

    def test_upsert_with_keyname(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'upsert_keyname_test_table'
        self.create_table(
            table, 'id serial primary key, email text unique, name text')
        q = f'select * from "{table}" order by id'
        r = upsert(table, dict(email='a@x', name='A'), 'email')
        self.assertEqual(r, dict(id=1, email='a@x', name='A'))
        r = upsert(table, dict(email='a@x', name='B'), keyname='email')
        self.assertEqual(r, dict(id=1, email='a@x', name='B'))
        r = upsert(table, dict(email='b@x', name='C'), ['email'])
        self.assertEqual(r, dict(id=3, email='b@x', name='C'))
        self.assertEqual(
            query(q).getresult(), [(1, 'a@x', 'B'), (3, 'b@x', 'C')])
        # update rules are still given as keyword arguments
        r = upsert(table, dict(email='a@x', name='D'), 'email',
                   name="included.name || excluded.name")
        self.assertEqual(r, dict(id=1, email='a@x', name='BD'))
        # do nothing, the row is fetched with the keyname
        r = upsert(table, dict(email='b@x', name='E'), 'email', name=False)
        self.assertEqual(r, dict(id=3, email='b@x', name='C'))
        self.assertEqual(
            query(q).getresult(), [(1, 'a@x', 'BD'), (3, 'b@x', 'C')])
        # no unique index matches the keyname
        self.assertRaises(
            pg.ProgrammingError, upsert, table, dict(name='A'), 'name')
        self.assertRaises(TypeError, upsert, table, dict(name='A'), 1)

    def test_upsert_with_keyname_without_primary_key(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'upsert_keyname_test_table_2'
        self.create_table(table, 'a int, b int, t text, unique (a, b)')
        q = f'select * from "{table}" order by a, b'
        self.assertRaises(
            pg.ProgrammingError, upsert, table, dict(a=1, b=1, t='x'))
        r = upsert(table, dict(a=1, b=1, t='x'), ('a', 'b'))
        self.assertEqual(r, dict(a=1, b=1, t='x'))
        r = upsert(table, dict(a=1, b=2, t='y'), ('a', 'b'))
        self.assertEqual(r, dict(a=1, b=2, t='y'))
        r = upsert(table, dict(a=1, b=1, t='z'), ('b', 'a'))
        self.assertEqual(r, dict(a=1, b=1, t='z'))
        self.assertEqual(
            query(q).getresult(), [(1, 1, 'z'), (1, 2, 'y')])

    def test_upsert_with_composite_key(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'upsert_test_table_2'
        self.create_table(
            table, 'n integer, m integer, t text, primary key (n, m)')
        s: dict = dict(n=1, m=2, t='x')
        r: Any = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 2)
        self.assertEqual(r['t'], 'x')
        s.update(m=3, t='y')
        r = upsert(table, s, **dict.fromkeys(s))
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'y')
        q = f'select n, m, t from "{table}" order by n, m limit 3'
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'y')])
        s.update(t='z')
        r = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'z')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'z')])
        s.update(t='n')
        r = upsert(table, s, t=False)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'z')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'z')])
        s.update(t='n')
        r = upsert(table, s, t=True)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'n')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'n')])
        s.update(n=2, t='y')
        r = upsert(table, s, t="'z'")
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'y')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'n'), (2, 3, 'y')])
        s.update(n=1, t='m')
        r = upsert(table, s, t='included.t || excluded.t')
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        self.assertEqual(r['t'], 'nm')
        r = query(q).getresult()
        self.assertEqual(r, [(1, 2, 'x'), (1, 3, 'nm'), (2, 3, 'y')])

    def test_upsert_with_quoted_names(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'test table for upsert()'
        self.create_table(table, '"Prime!" smallint primary key,'
                                ' "much space" integer, "Questions?" text')
        s: dict = {'Prime!': 31, 'much space': 9009, 'Questions?': 'Yes.'}
        r: Any = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['Prime!'], 31)
        self.assertEqual(r['much space'], 9009)
        self.assertEqual(r['Questions?'], 'Yes.')
        q = f'select * from "{table}" limit 2'
        r = query(q).getresult()
        self.assertEqual(r, [(31, 9009, 'Yes.')])
        s.update({'Questions?': 'No.'})
        r = upsert(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['Prime!'], 31)
        self.assertEqual(r['much space'], 9009)
        self.assertEqual(r['Questions?'], 'No.')
        r = query(q).getresult()
        self.assertEqual(r, [(31, 9009, 'No.')])

    def test_upsert_with_generated_columns(self):
        upsert = self.db.upsert
        get = self.db.get
        server_version = self.db.server_version
        table = 'upsert_test_table_2'
        table_def = 'i int not null'
        if server_version >= 100000:
            table_def += (
                ', a int generated always as identity'
                ', d int generated by default as identity primary key')
        else:
            table_def += ', a int not null default 1, d int primary key'
        if server_version >= 120000:
            table_def += ', j int generated always as (i + 7) stored'
        else:
            table_def += ', j int not null default 42'
        self.create_table(table, table_def)
        i, d = 35, 1001
        j = i + 7
        r: Any = upsert(table, {'i': i, 'd': d, 'a': 1, 'j': j})
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})
        r['i'] += 1
        r = upsert(table, r)
        i += 1
        if server_version >= 120000:
            j += 1
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})
        r = get(table, d)
        self.assertEqual(r, {'a': 1, 'd': d, 'i': i, 'j': j})

    def test_clear(self):
        clear = self.db.clear
        f = False if pg.get_bool() else 'f'
        r: Any = clear('test')
        result = dict(
            i2=0, i4=0, i8=0, d=0, f4=0, f8=0, m=0, v4='', c4='', t='')
        self.assertEqual(r, result)
        table = 'clear_test_table'
        self.create_table(
            table, 'n integer, f float, b boolean, d date, t text')
        r = clear(table)
        result = dict(n=0, f=0, b=f, d='', t='')
        self.assertEqual(r, result)
        r['a'] = r['f'] = r['n'] = 1
        r['d'] = r['t'] = 'x'
        r['b'] = 't'
        r['oid'] = 1
        r = clear(table, r)
        result = dict(a=1, n=0, f=0, b=f, d='', t='', oid=1)
        self.assertEqual(r, result)

    def test_clear_with_quoted_names(self):
        clear = self.db.clear
        table = 'test table for clear()'
        self.create_table(
            table, '"Prime!" smallint primary key,'
                   ' "much space" integer, "Questions?" text')
        r = clear(table)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 0)
        self.assertEqual(r['much space'], 0)
        self.assertEqual(r['Questions?'], '')

    def test_delete(self):
        delete = self.db.delete
        query = self.db.query
        self.assertRaises(pg.ProgrammingError, delete,
                          'test', dict(i2=2, i4=4, i8=8))
        table = 'delete_test_table'
        self.create_table(table, 'n integer primary key, t text',
                          values=enumerate('xyz', start=1))
        self.assertRaises(pg.DatabaseError, self.db.get, table, 4)
        r: Any = self.db.get(table, 1)
        s: Any = delete(table, r)
        self.assertEqual(s, 1)
        r = self.db.get(table, 3)
        s = delete(table, r)
        self.assertEqual(s, 1)
        s = delete(table, r)
        self.assertEqual(s, 0)
        r = query(f'select * from "{table}"').dictresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        result = {'n': 2, 't': 'y'}
        self.assertEqual(r, result)
        r = self.db.get(table, 2)
        s = delete(table, r)
        self.assertEqual(s, 1)
        s = delete(table, r)
        self.assertEqual(s, 0)
        self.assertRaises(pg.DatabaseError, self.db.get, table, 2)
        # not existing columns should be ignored
        r.update(m=3, u='z', oid='invalid')
        s = delete(table, r)
        self.assertEqual(s, 0)

    def test_delete_with_keyname(self):
        delete = self.db.delete
        query = self.db.query
        table = 'delete_keyname_test_table'
        self.create_table(table, 'a int, b int, t text',
                          values=[(1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')])
        q = f'select t from "{table}" order by t'
        self.assertRaises(pg.ProgrammingError, delete, table, dict(a=1, b=1))
        self.assertRaises(KeyError, delete, table, dict(a=1), ('a', 'b'))
        self.assertRaises(TypeError, delete, table, dict(a=1), 1)
        self.assertEqual(delete(table, dict(a=1, b=2), ('a', 'b')), 1)
        self.assertEqual(delete(table, dict(a=1, b=2), ('a', 'b')), 0)
        self.assertEqual(query(q).getresult(), [('x',), ('z',)])
        self.assertEqual(delete(table, None, 't', t='z'), 1)
        self.assertEqual(query(q).getresult(), [('x',)])
        # the keyname should identify a unique row
        query(f'insert into "{table}" values (1, 3, \'y\')')
        self.assertEqual(delete(table, dict(a=1), keyname='a'), 2)
        self.assertEqual(query(q).getresult(), [])

    def test_delete_with_composite_key(self):
        query = self.db.query
        table = 'delete_test_table_1'
        self.create_table(table, 'n integer primary key, t text',
                         values=enumerate('abc', start=1))
        self.assertRaises(KeyError, self.db.delete, table, dict(t='b'))
        self.assertEqual(self.db.delete(table, dict(n=2)), 1)
        r: Any = query(f'select t from "{table}" where n=2').getresult()
        self.assertEqual(r, [])
        self.assertEqual(self.db.delete(table, dict(n=2)), 0)
        r = query(f'select t from "{table}" where n=3').getresult()[0][0]
        self.assertEqual(r, 'c')
        table = 'delete_test_table_2'
        self.create_table(
            table, 'n integer, m integer, t text, primary key (n, m)',
            values=[(n + 1, m + 1, chr(ord('a') + 2 * n + m))
                    for n in range(3) for m in range(2)])
        self.assertRaises(KeyError, self.db.delete, table, dict(n=2, t='b'))
        self.assertEqual(self.db.delete(table, dict(n=2, m=2)), 1)
        r = [r[0] for r in query(f'select t from "{table}" where n=2'
                                 ' order by m').getresult()]
        self.assertEqual(r, ['c'])
        self.assertEqual(self.db.delete(table, dict(n=2, m=2)), 0)
        r = [r[0] for r in query(f'select t from "{table}" where n=3'
                                 ' order by m').getresult()]
        self.assertEqual(r, ['e', 'f'])
        self.assertEqual(self.db.delete(table, dict(n=3, m=1)), 1)
        r = [r[0] for r in query(f'select t from "{table}" where n=3'
                                 f' order by m').getresult()]
        self.assertEqual(r, ['f'])

    def test_delete_with_quoted_names(self):
        delete = self.db.delete
        query = self.db.query
        table = 'test table for delete()'
        self.create_table(
            table, '"Prime!" smallint primary key,'
                   ' "much space" integer, "Questions?" text',
            values=[(19, 5005, 'Yes!')])
        r: Any = {'Prime!': 17}
        r = delete(table, r)
        self.assertEqual(r, 0)
        r = query(f'select count(*) from "{table}"').getresult()
        self.assertEqual(r[0][0], 1)
        r = {'Prime!': 19}
        r = delete(table, r)
        self.assertEqual(r, 1)
        r = query(f'select count(*) from "{table}"').getresult()
        self.assertEqual(r[0][0], 0)

    def test_delete_referenced(self):
        delete = self.db.delete
        query = self.db.query
        self.create_table(
            'test_parent', 'n smallint primary key', values=range(3))
        self.create_table(
            'test_child', 'n smallint primary key references test_parent',
            values=range(3))
        q = ("select (select count(*) from test_parent),"
             " (select count(*) from test_child)")
        self.assertEqual(query(q).getresult()[0], (3, 3))
        self.assertRaises(pg.IntegrityError,
                          delete, 'test_parent', None, n=2)
        self.assertRaises(pg.IntegrityError,
                          delete, 'test_parent *', None, n=2)
        r: Any = delete('test_child', None, n=2)
        self.assertEqual(r, 1)
        self.assertEqual(query(q).getresult()[0], (3, 2))
        r = delete('test_parent', None, n=2)
        self.assertEqual(r, 1)
        self.assertEqual(query(q).getresult()[0], (2, 2))
        self.assertRaises(pg.IntegrityError,
                          delete, 'test_parent', dict(n=0))
        self.assertRaises(pg.IntegrityError,
                          delete, 'test_parent *', dict(n=0))
        r = delete('test_child', dict(n=0))
        self.assertEqual(r, 1)
        self.assertEqual(query(q).getresult()[0], (2, 1))
        r = delete('test_child', dict(n=0))
        self.assertEqual(r, 0)
        r = delete('test_parent', dict(n=0))
        self.assertEqual(r, 1)
        self.assertEqual(query(q).getresult()[0], (1, 1))
        r = delete('test_parent', None, n=0)
        self.assertEqual(r, 0)
        q = "select n from test_parent natural join test_child limit 2"
        self.assertEqual(query(q).getresult(), [(1,)])

    def test_temp_crud(self):
        table = 'test_temp_table'
        self.create_table(table, "n int primary key, t varchar",
                          temporary=True)
        self.db.insert(table, dict(n=1, t='one'))
        self.db.insert(table, dict(n=2, t='too'))
        self.db.insert(table, dict(n=3, t='three'))
        r: Any = self.db.get(table, 2)
        self.assertEqual(r['t'], 'too')
        self.db.update(table, dict(n=2, t='two'))
        r = self.db.get(table, 2)
        self.assertEqual(r['t'], 'two')
        self.db.delete(table, r)
        r = self.db.query(f'select n, t from {table} order by 1').getresult()
        self.assertEqual(r, [(1, 'one'), (3, 'three')])

    def test_truncate(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, None)
        self.assertRaises(TypeError, truncate, 42)
        self.assertRaises(TypeError, truncate, dict(test_table=None))
        query = self.db.query
        self.create_table('test_table', 'n smallint',
                          temporary=False, values=[1] * 3)
        q = "select count(*) from test_table"
        r: Any = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate('test_table')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        for _i in range(3):
            query("insert into test_table values (1)")
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate('public.test_table')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        self.create_table('test_table_2', 'n smallint', temporary=True)
        for t in (list, tuple, set):
            for _i in range(3):
                query("insert into test_table values (1)")
                query("insert into test_table_2 values (2)")
            q = ("select (select count(*) from test_table),"
                 " (select count(*) from test_table_2)")
            r = query(q).getresult()[0]
            self.assertEqual(r, (3, 3))
            truncate(t(['test_table', 'test_table_2']))
            r = query(q).getresult()[0]
            self.assertEqual(r, (0, 0))

    def test_truncate_restart(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', restart='invalid')
        query = self.db.query
        self.create_table('test_table', 'n serial, t text')
        for _n in range(3):
            query("insert into test_table (t) values ('test')")
        q = "select count(n), min(n), max(n) from test_table"
        r: Any = query(q).getresult()[0]
        self.assertEqual(r, (3, 1, 3))
        truncate('test_table')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, None, None))
        for _n in range(3):
            query("insert into test_table (t) values ('test')")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 4, 6))
        truncate('test_table', restart=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, None, None))
        for _n in range(3):
            query("insert into test_table (t) values ('test')")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 1, 3))

    def test_truncate_cascade(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', cascade='invalid')
        query = self.db.query
        self.create_table('test_parent', 'n smallint primary key',
                         values=range(3))
        self.create_table('test_child',
                         'n smallint primary key references test_parent (n)',
                         values=range(3))
        q = ("select (select count(*) from test_parent),"
             " (select count(*) from test_child)")
        r: Any = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        self.assertRaises(pg.NotSupportedError, truncate, 'test_parent')
        truncate(['test_parent', 'test_child'])
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
            query(f"insert into test_parent (n) values ({n})")
            query(f"insert into test_child (n) values ({n})")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        truncate('test_parent', cascade=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
            query(f"insert into test_parent (n) values ({n})")
            query(f"insert into test_child (n) values ({n})")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        truncate('test_child')
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 0))
        self.assertRaises(pg.NotSupportedError, truncate, 'test_parent')
        truncate('test_parent', cascade=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))

    def test_truncate_only(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', only='invalid')
        query = self.db.query
        self.create_table('test_parent', 'n smallint')
        self.create_table('test_child', 'm smallint) inherits (test_parent')
        for _n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
        q = ("select (select count(*) from test_parent),"
             " (select count(*) from test_child)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3))
        truncate('test_parent')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for _n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3))
        truncate('test_parent*')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for _n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3))
        truncate('test_parent', only=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        truncate('test_parent', only=False)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        self.assertRaises(ValueError, truncate, 'test_parent*', only=True)
        truncate('test_parent*', only=False)
        self.create_table('test_parent_2', 'n smallint')
        self.create_table('test_child_2',
                          'm smallint) inherits (test_parent_2')
        for t in '', '_2':
            for _n in range(3):
                query(f"insert into test_parent{t} (n) values (1)")
                query(f"insert into test_child{t} (n, m) values (2, 3)")
        q = ("select (select count(*) from test_parent),"
             " (select count(*) from test_child),"
             " (select count(*) from test_parent_2),"
             " (select count(*) from test_child_2)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3, 6, 3))
        truncate(['test_parent', 'test_parent_2'], only=[False, True])
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0, 3, 3))
        truncate(['test_parent', 'test_parent_2'], only=False)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0, 0, 0))
        self.assertRaises(
            ValueError, truncate,
            ['test_parent*', 'test_child'], only=[True, False])
        truncate(['test_parent*', 'test_child'], only=[False, True])

    def test_truncate_quoted(self):
        truncate = self.db.truncate
        query = self.db.query
        table = "test table for truncate()"
        self.create_table(table, 'n smallint', temporary=False, values=[1] * 3)
        q = f'select count(*) from "{table}"'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate(table)
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        for _i in range(3):
            query(f'insert into "{table}" values (1)')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate(f'public."{table}"')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)

    # noinspection PyUnresolvedReferences
    def test_get_as_list(self):
        get_as_list = self.db.get_as_list
        self.assertRaises(TypeError, get_as_list)
        self.assertRaises(TypeError, get_as_list, None)
        query = self.db.query
        table = 'test_aslist'
        r: Any = query('select 1 as colname').namedresult()[0]
        self.assertIsInstance(r, tuple)
        named = hasattr(r, 'colname')
        names = [(1, 'Homer'), (2, 'Marge'),
                 (3, 'Bart'), (4, 'Lisa'), (5, 'Maggie')]
        self.create_table(
            table, 'id smallint primary key, name varchar', values=names)
        r = get_as_list(table)
        self.assertIsInstance(r, list)
        self.assertEqual(r, names)
        for t, n in zip(r, names, strict=True):
            self.assertIsInstance(t, tuple)
            self.assertEqual(t, n)
            if named:
                self.assertEqual(t.id, n[0])
                self.assertEqual(t.name, n[1])
                self.assertEqual(t._asdict(), dict(id=n[0], name=n[1]))
        r = get_as_list(table, what='name')
        self.assertIsInstance(r, list)
        expected: Any = sorted((row[1],) for row in names)
        self.assertEqual(r, expected)
        r = get_as_list(table, what='name, id')
        self.assertIsInstance(r, list)
        expected = sorted(tuple(reversed(row)) for row in names)
        self.assertEqual(r, expected)
        r = get_as_list(table, what=['name', 'id'])
        self.assertIsInstance(r, list)
        self.assertEqual(r, expected)
        r = get_as_list(table, where="name like 'Ba%'")
        self.assertIsInstance(r, list)
        self.assertEqual(r, names[2:3])
        r = get_as_list(table, what='name', where="name like 'Ma%'")
        self.assertIsInstance(r, list)
        self.assertEqual(r, [('Maggie',), ('Marge',)])
        r = get_as_list(
            table, what='name', where=["name like 'Ma%'", "name like '%r%'"])
        self.assertIsInstance(r, list)
        self.assertEqual(r, [('Marge',)])
        r = get_as_list(table, what='name', order='id')
        self.assertIsInstance(r, list)
        expected = [(row[1],) for row in names]
        self.assertEqual(r, expected)
        r = get_as_list(table, what=['name'], order=['id'])
        self.assertIsInstance(r, list)
        self.assertEqual(r, expected)
        r = get_as_list(table, what=['id', 'name'], order=['id', 'name'])
        self.assertIsInstance(r, list)
        self.assertEqual(r, names)
        r = get_as_list(table, what='id * 2 as num', order='id desc')
        self.assertIsInstance(r, list)
        expected = [(n,) for n in range(10, 0, -2)]
        self.assertEqual(r, expected)
        r = get_as_list(table, limit=2)
        self.assertIsInstance(r, list)
        self.assertEqual(r, names[:2])
        r = get_as_list(table, offset=3)
        self.assertIsInstance(r, list)
        self.assertEqual(r, names[3:])
        r = get_as_list(table, limit=1, offset=2)
        self.assertIsInstance(r, list)
        self.assertEqual(r, names[2:3])
        r = get_as_list(table, scalar=True)
        self.assertIsInstance(r, list)
        self.assertEqual(r, list(range(1, 6)))
        r = get_as_list(table, what='name', scalar=True)
        self.assertIsInstance(r, list)
        expected = sorted(row[1] for row in names)
        self.assertEqual(r, expected)
        r = get_as_list(table, what='name', limit=1, scalar=True)
        self.assertIsInstance(r, list)
        self.assertEqual(r, expected[:1])
        query(f'alter table "{table}" drop constraint "{table}_pkey"')
        self.assertRaises(KeyError, self.db.pkey, table, flush=True)
        names.insert(1, (1, 'Snowball'))
        query(f'insert into "{table}" values ($1, $2)', (1, 'Snowball'))
        r = get_as_list(table)
        self.assertIsInstance(r, list)
        self.assertEqual(r, names)
        r = get_as_list(table, what='name', where='id=1', scalar=True)
        self.assertIsInstance(r, list)
        self.assertEqual(r, ['Homer', 'Snowball'])
        # test with unordered query
        r = get_as_list(table, order=False)
        self.assertIsInstance(r, list)
        self.assertEqual(set(r), set(names))
        # test with arbitrary from clause
        from_table = f'(select lower(name) as n2 from "{table}") as t2'
        r = get_as_list(from_table)
        self.assertIsInstance(r, list)
        r = {row[0] for row in r}
        expected = {row[1].lower() for row in names}
        self.assertEqual(r, expected)
        r = get_as_list(from_table, order='n2', scalar=True)
        self.assertIsInstance(r, list)
        self.assertEqual(r, sorted(expected))
        r = get_as_list(from_table, order='n2', limit=1)
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 1)
        t = r[0]
        self.assertIsInstance(t, tuple)
        if named:
            self.assertEqual(t.n2, 'bart')
            self.assertEqual(t._asdict(), dict(n2='bart'))
        else:
            self.assertEqual(t, ('bart',))

    # noinspection PyUnresolvedReferences
    def test_table_names_must_be_str(self):
        table = 'test_str_names'
        self.create_table(table, 'n smallint primary key')
        db = self.db
        name = table.encode()
        # bytes names are only supported by the low-level methods
        for method, args in (
                (db.get, (name, 1)), (db.insert, (name, {'n': 1})),
                (db.update, (name, {'n': 1})), (db.upsert, (name, {'n': 1})),
                (db.delete, (name, {'n': 1})), (db.truncate, (name,)),
                (db.truncate, ([name],)), (db.get_attnames, (name,)),
                (db.pkey, (name,)), (db.pkeys, (name,))):
            with self.assertRaises(TypeError) as cm:
                method(*args)
            self.assertRegex(
                str(cm.exception), '^The table (name )?must be a string')
        db.db.inserttable(name, [(1,)], [b'n'])
        self.assertEqual(db.get(table, 1), {'n': 1})

    def test_get_as_dict(self):
        get_as_dict = self.db.get_as_dict
        self.assertRaises(TypeError, get_as_dict)
        self.assertRaises(TypeError, get_as_dict, None)
        # the test table has no primary key
        self.assertRaises(pg.ProgrammingError, get_as_dict, 'test')
        query = self.db.query
        table = 'test_asdict'
        r = query('select 1 as colname').namedresult()[0]
        self.assertIsInstance(r, tuple)
        named = hasattr(r, 'colname')
        colors = [(1, '#7cb9e8', 'Aero'), (2, '#b5a642', 'Brass'),
                  (3, '#b2ffff', 'Celeste'), (4, '#c19a6b', 'Desert')]
        self.create_table(
            table, 'id smallint primary key, rgb char(7), name varchar',
            values=colors)
        # keyname must be a column name or a sequence of names
        self.assertRaises(TypeError, get_as_dict, table, 3)
        self.assertRaises(TypeError, get_as_dict, table, dict(id=None))
        self.assertRaises(TypeError, get_as_dict, table, ['id', 3])
        # missing keyname in row
        self.assertRaises(KeyError, get_as_dict, table,
                          keyname='rgb', what='name')
        r = get_as_dict(table)
        self.assertIsInstance(r, dict)
        expected: Any = {row[0]: row[1:] for row in colors}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, int)
            self.assertIn(key, expected)
            row = r[key]
            self.assertIsInstance(row, tuple)
            t = expected[key]
            self.assertEqual(row, t)
            if named:
                self.assertEqual(row.rgb, t[0])
                self.assertEqual(row.name, t[1])
                self.assertEqual(row._asdict(), dict(rgb=t[0], name=t[1]))
        self.assertEqual(r.keys(), expected.keys())
        r = get_as_dict(table, keyname='rgb')
        self.assertIsInstance(r, dict)
        expected = {row[1]: (row[0], row[2])
                    for row in sorted(colors, key=itemgetter(1))}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, str)
            self.assertIn(key, expected)
            row = r[key]
            self.assertIsInstance(row, tuple)
            # noinspection PyTypeChecker
            t = expected[key]
            self.assertEqual(row, t)
            if named:
                self.assertEqual(row.id, t[0])
                self.assertEqual(row.name, t[1])
                self.assertEqual(row._asdict(), dict(id=t[0], name=t[1]))
        self.assertEqual(r.keys(), expected.keys())
        r = get_as_dict(table, keyname=['id', 'rgb'])
        self.assertIsInstance(r, dict)
        expected = {row[:2]: row[2:] for row in colors}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, tuple)
            self.assertIsInstance(key[0], int)
            self.assertIsInstance(key[1], str)
            if named:
                self.assertEqual(key, (key.id, key.rgb))
                self.assertEqual(key._fields, ('id', 'rgb'))
            row = r[key]
            self.assertIsInstance(row, tuple)
            self.assertIsInstance(row[0], str)
            # noinspection PyTypeChecker
            t = expected[key]
            self.assertEqual(row, t)
            if named:
                self.assertEqual(row.name, t[0])
                self.assertEqual(row._asdict(), dict(name=t[0]))
        self.assertEqual(r.keys(), expected.keys())
        r = get_as_dict(table, keyname=['id', 'rgb'], scalar=True)
        self.assertIsInstance(r, dict)
        expected = {row[:2]: row[2] for row in colors}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, tuple)
            row = r[key]
            self.assertIsInstance(row, str)
            # noinspection PyTypeChecker
            t = expected[key]
            self.assertEqual(row, t)
        self.assertEqual(r.keys(), expected.keys())
        r = get_as_dict(table, keyname='rgb', what=['rgb', 'name'],
                        scalar=True)
        self.assertIsInstance(r, dict)
        expected = {row[1]: row[2]
                    for row in sorted(colors, key=itemgetter(1))}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, str)
            row = r[key]
            self.assertIsInstance(row, str)
            # noinspection PyTypeChecker
            t = expected[key]
            self.assertEqual(row, t)
        self.assertEqual(r.keys(), expected.keys())
        r = get_as_dict(
            table, what='id, name', where="rgb like '#b%'", scalar=True)
        self.assertIsInstance(r, dict)
        expected = {row[0]: row[2] for row in colors[1:3]}
        self.assertEqual(r, expected)
        for key in r:
            self.assertIsInstance(key, int)
            row = r[key]
            self.assertIsInstance(row, str)
            t = expected[key]
            self.assertEqual(row, t)
        self.assertEqual(r.keys(), expected.keys())
        expected = r
        r = get_as_dict(
            table, what=['name', 'id'],
            where=['id > 1', 'id < 4', "rgb like '#b%'",
                   "name not like 'A%'", "name not like '%t'"], scalar=True)
        self.assertEqual(r, expected)
        r = get_as_dict(table, what='name, id', limit=2, offset=1, scalar=True)
        self.assertEqual(r, expected)
        r = get_as_dict(
            table, keyname=('id',), what=('name', 'id'),
            where=('id > 1', 'id < 4'), order=('id',), scalar=True)
        self.assertEqual(r, expected)
        r = get_as_dict(table, limit=1)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[1][1], 'Aero')
        r = get_as_dict(table, offset=3)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[4][1], 'Desert')
        r = get_as_dict(table, order='id desc')
        expected = {row[0]: row[1:] for row in reversed(colors)}
        self.assertEqual(r, expected)
        r = get_as_dict(table, where='id > 5')
        self.assertIsInstance(r, dict)
        self.assertEqual(len(r), 0)
        # test with unordered query
        expected = {row[0]: row[1:] for row in colors}
        r = get_as_dict(table, order=False)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, expected)
        self.assertNotIsInstance(self, dict)
        # test with arbitrary from clause
        from_table = f'(select id, lower(name) as n2 from "{table}") as t2'
        # primary key must be passed explicitly in this case
        self.assertRaises(pg.ProgrammingError, get_as_dict, from_table)
        r = get_as_dict(from_table, 'id')
        self.assertIsInstance(r, dict)
        expected = {row[0]: (row[2].lower(),) for row in colors}
        self.assertEqual(r, expected)
        # test without a primary key
        query(f'alter table "{table}" drop constraint "{table}_pkey"')
        self.assertRaises(KeyError, self.db.pkey, table, flush=True)
        self.assertRaises(pg.ProgrammingError, get_as_dict, table)
        r = get_as_dict(table, keyname='id')
        expected = {row[0]: row[1:] for row in colors}
        self.assertIsInstance(r, dict)
        self.assertEqual(r, expected)
        r = (1, '#007fff', 'Azure')
        query(f'insert into "{table}" values ($1, $2, $3)', r)
        # the last entry will win
        expected[1] = r[1:]
        r = get_as_dict(table, keyname='id')
        self.assertEqual(r, expected)

    def test_transaction(self):
        query = self.db.query
        self.create_table('test_table', 'n integer', temporary=False)
        self.db.begin()
        query("insert into test_table values (1)")
        query("insert into test_table values (2)")
        self.db.commit()
        self.db.begin()
        query("insert into test_table values (3)")
        query("insert into test_table values (4)")
        self.db.rollback()
        self.db.begin()
        query("insert into test_table values (5)")
        self.db.savepoint('before6')
        query("insert into test_table values (6)")
        self.db.rollback('before6')
        query("insert into test_table values (7)")
        self.db.commit()
        self.db.begin()
        self.db.savepoint('before8')
        query("insert into test_table values (8)")
        self.db.release('before8')
        self.assertRaises(pg.InternalError, self.db.rollback, 'before8')
        self.db.commit()
        self.db.start()
        query("insert into test_table values (9)")
        self.db.end()
        r = [r[0] for r in query(
            "select * from test_table order by 1").getresult()]
        self.assertEqual(r, [1, 2, 5, 7, 9])
        self.db.begin(mode='read only')
        self.assertRaises(pg.InternalError,
                          query, "insert into test_table values (0)")
        self.db.rollback()
        self.db.start(mode='Read Only')
        self.assertRaises(pg.InternalError,
                          query, "insert into test_table values (0)")
        self.db.abort()

    def test_transaction_aliases(self):
        self.assertEqual(self.db.begin, self.db.start)
        self.assertEqual(self.db.commit, self.db.end)
        self.assertEqual(self.db.rollback, self.db.abort)

    def test_context_manager(self):
        query = self.db.query
        self.create_table('test_table', 'n integer check(n>0)')
        with self.db:
            query("insert into test_table values (1)")
            query("insert into test_table values (2)")
        try:
            with self.db:
                query("insert into test_table values (3)")
                query("insert into test_table values (4)")
                raise ValueError('test transaction should rollback')
        except ValueError as error:
            self.assertEqual(str(error), 'test transaction should rollback')
        with self.db:
            query("insert into test_table values (5)")
        try:
            with self.db:
                query("insert into test_table values (6)")
                query("insert into test_table values (-1)")
        except pg.IntegrityError as error:
            self.assertIn('check', str(error))
        with self.db:
            query("insert into test_table values (7)")
        r = [r[0] for r in query(
            "select * from test_table order by 1").getresult()]
        self.assertEqual(r, [1, 2, 5, 7])

    def test_bytea(self):
        query = self.db.query
        self.create_table('bytea_test', 'n smallint primary key, data bytea')
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        r = self.db.escape_bytea(s)
        query('insert into bytea_test values(3, $1)', (r,))
        r = query('select * from bytea_test where n=3').getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 3)
        r = r[1]
        if pg.get_bytea_escaped():
            self.assertNotEqual(r, s)
            r = pg.unescape_bytea(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)

    def test_query_formatted_bytea(self):
        query_formatted = self.db.query_formatted
        unescape = pg.unescape_bytea if pg.get_bytea_escaped() else None
        self.create_table(
            'bytea_test', 'n smallint primary key, data bytea')
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        for n, inline in enumerate((False, True)):
            query_formatted('insert into bytea_test values (%s, %s)',
                            (n, s), inline=inline)
            r = query_formatted(
                'select data from bytea_test where data = %s',
                (s,), inline=inline).getresult()
            self.assertEqual(len(r), n + 1)
            r = r[n][0]
            if unescape:
                r = unescape(r)
            self.assertIsInstance(r, bytes)
            self.assertEqual(r, s)

    def test_query_formatted_bytea_is_binary(self):
        if pg.get_bytea_escaped():
            self.skipTest('bytea conversion is disabled')
        query_formatted = self.db.query_formatted
        data = bytes(range(256))  # including NUL and backslash
        for value in (data, pg.Bytea(data)):
            self.assertEqual(query_formatted(
                'select %s, pg_typeof(%s)::text', (value, value)
            ).getresult(), [(data, 'bytea')])
        self.assertEqual(query_formatted(
            'select %s', (b'\\x41',), ('bytea',)).getresult(), [(b'\\x41',)])

    def test_query_formatted_bytea_non_standard_strings(self):
        if pg.get_bytea_escaped():
            self.skipTest('bytea conversion is disabled')
        if self.db.server_version >= 190000:
            self.skipTest('non-standard strings are not supported')
        self.db.query('set standard_conforming_strings=off')
        self.create_table('bytea_test', 'n smallint primary key, data bytea')
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        # binary data is not escaped, so it is independent of the setting
        self.assertEqual(self.db.query_formatted(
            'select %s', (s,)).getresult(), [(s,)])
        r = self.db.insert('bytea_test', n=1, data=s)
        self.assertEqual(r['data'], s)
        self.assertEqual(self.db.get('bytea_test', 1)['data'], s)

    def test_query_formatted_bytea_array(self):
        if not pg.get_array() or pg.get_bytea_escaped():
            self.skipTest('array or bytea conversion is disabled')
        query_formatted = self.db.query_formatted
        data = [b'Hello, World!', b'', None, b'{a,b,c}', b'"Hi!"',
                b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"]
        for inline in (False, True):
            r = query_formatted(
                'select %s::bytea[]', (data,), inline=inline).getresult()
            self.assertEqual(r, [(data,)])

    def test_bytearray_and_memoryview_params(self):
        query_formatted = self.db.query_formatted
        values = (bytearray(b'ab'), bytearray(), memoryview(b'ab'))
        # values for json are passed to the JSON encoder (see below)
        types = ('bytea', 'text', 'int', 'bool', 'date', 'uuid',
                 'hstore', 'bytea[]', 'text[]', 'int[]', 'bool[]')
        for value in values:
            msg = f'Cannot adapt {type(value).__name__}'
            # passed directly to the connection
            self.assertRaisesRegex(
                TypeError, msg, self.db.query, 'select $1', value)
            # adapted, with and without types
            for params in ([value], [[value]], [[1, value]], [(1, value)],
                           [pg.Hstore(k=value)], {'v': value}):
                sql = ('select %(v)s' if isinstance(params, dict)
                       else 'select %s')
                self.assertRaisesRegex(
                    TypeError, msg, query_formatted, sql, params)
            for typ in types:
                param: Any = [value] if typ.endswith('[]') else value
                if typ == 'hstore':
                    param = {'k': value}
                self.assertRaisesRegex(
                    TypeError, msg, query_formatted,
                    'select %s', [param], [typ])
            # put into the SQL
            self.assertRaises(pg.InterfaceError, query_formatted,
                              'select %s', [value], inline=True)
        self.assertRaisesRegex(TypeError, 'memoryview', str,
                               pg.Hstore({memoryview(b'k'): 'v'}))
        self.assertRaisesRegex(
            pg.InterfaceError, 'Cannot adapt memoryview', query_formatted,
            'select %s', [pg.Hstore(k=memoryview(b'v'))], inline=True)
        self.create_table('test_table', 'n int, b bytea, f bool')
        for value in values:
            msg = f'Cannot adapt {type(value).__name__}'
            for column in 'nbf':
                self.assertRaisesRegex(
                    TypeError, msg, self.db.insert, 'test_table',
                    **{column: value})
        self.assertEqual(self.db.query(
            'select count(*) from test_table').getresult(), [(0,)])

    def test_bytearray_and_memoryview_with_hooks(self):
        query_formatted = self.db.query_formatted

        class MyByteArray(bytearray):
            def __pg_str__(self, typ):
                return self.decode('ascii')

        # objects with a __pg_str__ hook are adapted by the hook
        value = MyByteArray(b'abc')
        for types in (None, ['text']):
            self.assertEqual(query_formatted(
                'select %s', [value], types).getresult(), [('abc',)])
        # but not as elements of arrays, and their results are checked
        python_msg = 'Cannot adapt {}, convert it to bytes or Bytea'
        self.assertRaisesRegex(
            TypeError, python_msg.format('MyByteArray'),
            query_formatted, 'select %s', [[value]])

        class Hook:
            def __init__(self, value):
                self.value = value

            def __pg_str__(self, typ):
                return self.value

        for buffer in (bytearray(b'ab'), memoryview(b'ab')):
            msg = python_msg.format(type(buffer).__name__)
            for typ in ('uuid', 'text', 'int', 'bool', 'bytea'):
                with self.assertRaises(TypeError) as cm:
                    query_formatted('select %s', [Hook(buffer)], [typ])
                self.assertEqual(str(cm.exception), msg)
            # also for json arrays if the value is not a list
            with self.assertRaises(TypeError) as cm:
                query_formatted('select %s', [buffer], ['json[]'])
            self.assertEqual(str(cm.exception), msg)
            with self.assertRaises(pg.InterfaceError) as icm:
                query_formatted(
                    'select %s', [pg.Hstore(k=buffer)], inline=True)
            self.assertEqual(str(icm.exception), msg)
        # the JSON encoder may handle bytearray and memoryview objects
        self.assertRaisesRegex(
            TypeError, 'not JSON serializable', query_formatted,
            'select %s', [bytearray(b'ab')], ['json'])

        def encode_json(obj):
            return json.dumps(obj, default=lambda o: bytes(o).hex())

        self.db.encode_json = encode_json
        try:
            for buffer in (bytearray(b'ab'), memoryview(b'ab')):
                r = query_formatted('select %s::text', [buffer], ['json'])
                self.assertEqual(r.getresult(), [('"6162"',)])
                r = query_formatted('select %s::text', [[buffer]], ['json[]'])
                self.assertEqual(r.getresult(), [('{"\\"6162\\""}',)])
        finally:
            del self.db.encode_json

    def test_insert_update_get_bytea(self):
        query = self.db.query
        unescape = pg.unescape_bytea if pg.get_bytea_escaped() else None
        self.create_table('bytea_test', 'n smallint primary key, data bytea')
        # insert null value
        r = self.db.insert('bytea_test', n=0, data=None)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        self.assertIsNone(r['data'])
        s = b'None'
        r = self.db.update('bytea_test', n=0, data=s)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        r = r['data']
        if unescape:
            self.assertNotEqual(r, s)
            r = unescape(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        r = self.db.update('bytea_test', n=0, data=None)
        self.assertIsNone(r['data'])
        # insert as bytes
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        r = self.db.insert('bytea_test', n=5, data=s)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 5)
        self.assertIn('data', r)
        r = r['data']
        if unescape:
            self.assertNotEqual(r, s)
            r = unescape(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        # update as bytes
        s += b"and now even more \x00 nasty \t stuff!\f"
        r = self.db.update('bytea_test', n=5, data=s)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 5)
        self.assertIn('data', r)
        r = r['data']
        if unescape:
            self.assertNotEqual(r, s)
            r = unescape(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        r = query('select * from bytea_test where n=5').getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 5)
        r = r[1]
        if unescape:
            self.assertNotEqual(r, s)
            r = unescape(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        r = self.db.get('bytea_test', dict(n=5))
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 5)
        self.assertIn('data', r)
        r = r['data']
        if unescape:
            self.assertNotEqual(r, s)
            r = pg.unescape_bytea(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)

    def test_upsert_bytea(self):
        self.create_table('bytea_test', 'n smallint primary key, data bytea')
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        d = dict(n=7, data=s)
        d = self.db.upsert('bytea_test', d)
        self.assertIsInstance(d, dict)
        self.assertIn('n', d)
        self.assertEqual(d['n'], 7)
        self.assertIn('data', d)
        data = d['data']
        if pg.get_bytea_escaped():
            self.assertNotEqual(data, s)
            self.assertIsInstance(data, str)
            assert isinstance(data, str)  # type guard
            data = pg.unescape_bytea(data)
        self.assertIsInstance(data, bytes)
        self.assertEqual(data, s)
        d['data'] = None
        d = self.db.upsert('bytea_test', d)
        self.assertIsInstance(d, dict)
        self.assertIn('n', d)
        self.assertEqual(d['n'], 7)
        self.assertIn('data', d)
        self.assertIsNone(d['data'])

    def test_insert_get_json(self):
        self.create_table('json_test', 'n smallint primary key, data json')
        jsondecode = pg.get_jsondecode()
        # insert null value
        r = self.db.insert('json_test', n=0, data=None)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        self.assertIsNone(r['data'])
        r = self.db.get('json_test', 0)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        self.assertIsNone(r['data'])
        # insert JSON object
        data = {
            "id": 1, "name": "Foo", "price": 1234.5,
            "new": True, "note": None,
            "tags": ["Bar", "Eek"],
            "stock": {"warehouse": 300, "retail": 20}}
        r = self.db.insert('json_test', n=1, data=data)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 1)
        self.assertIn('data', r)
        r = r['data']
        if jsondecode is None:
            self.assertIsInstance(r, str)
            r = json.loads(r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, data)
        self.assertIsInstance(r['id'], int)
        self.assertIsInstance(r['name'], str)
        self.assertIsInstance(r['price'], float)
        self.assertIsInstance(r['new'], bool)
        self.assertIsInstance(r['tags'], list)
        self.assertIsInstance(r['stock'], dict)
        r = self.db.get('json_test', 1)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 1)
        self.assertIn('data', r)
        r = r['data']
        if jsondecode is None:
            self.assertIsInstance(r, str)
            r = json.loads(r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, data)
        self.assertIsInstance(r['id'], int)
        self.assertIsInstance(r['name'], str)
        self.assertIsInstance(r['price'], float)
        self.assertIsInstance(r['new'], bool)
        self.assertIsInstance(r['tags'], list)
        self.assertIsInstance(r['stock'], dict)
        # insert JSON object as text
        self.db.insert('json_test', n=2, data=json.dumps(data))
        q = "select data from json_test where n in (1, 2) order by n"
        r = self.db.query(q).getresult()
        self.assertEqual(len(r), 2)
        self.assertIsInstance(r[0][0], str if jsondecode is None else dict)
        self.assertEqual(r[0][0], r[1][0])

    def test_insert_get_jsonb(self):
        self.create_table('jsonb_test',
                         'n smallint primary key, data jsonb')
        jsondecode = pg.get_jsondecode()
        # insert null value
        r = self.db.insert('jsonb_test', n=0, data=None)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        self.assertIsNone(r['data'])
        r = self.db.get('jsonb_test', 0)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 0)
        self.assertIn('data', r)
        self.assertIsNone(r['data'])
        # insert JSON object
        data = {
            "id": 1, "name": "Foo", "price": 1234.5,
            "new": True, "note": None,
            "tags": ["Bar", "Eek"],
            "stock": {"warehouse": 300, "retail": 20}}
        r = self.db.insert('jsonb_test', n=1, data=data)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 1)
        self.assertIn('data', r)
        r = r['data']
        if jsondecode is None:
            self.assertIsInstance(r, str)
            r = json.loads(r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, data)
        self.assertIsInstance(r['id'], int)
        self.assertIsInstance(r['name'], str)
        self.assertIsInstance(r['price'], float)
        self.assertIsInstance(r['new'], bool)
        self.assertIsInstance(r['tags'], list)
        self.assertIsInstance(r['stock'], dict)
        r = self.db.get('jsonb_test', 1)
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 1)
        self.assertIn('data', r)
        r = r['data']
        if jsondecode is None:
            self.assertIsInstance(r, str)
            r = json.loads(r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r, data)
        self.assertIsInstance(r['id'], int)
        self.assertIsInstance(r['name'], str)
        self.assertIsInstance(r['price'], float)
        self.assertIsInstance(r['new'], bool)
        self.assertIsInstance(r['tags'], list)
        self.assertIsInstance(r['stock'], dict)

    def test_array(self):
        returns_arrays = pg.get_array()
        self.create_table(
            'arraytest',
            'id smallint, i2 smallint[], i4 integer[], i8 bigint[],'
            ' d numeric[], f4 real[], f8 double precision[], m money[],'
            ' b bool[], v4 varchar(4)[], c4 char(4)[], t text[]')
        r = self.db.get_attnames('arraytest')
        if self.regtypes:
            self.assertEqual(r, dict(
                id='smallint', i2='smallint[]', i4='integer[]', i8='bigint[]',
                d='numeric[]', f4='real[]', f8='double precision[]',
                m='money[]', b='boolean[]',
                v4='character varying[]', c4='character[]', t='text[]'))
        else:
            self.assertEqual(r, dict(
                id='int', i2='int[]', i4='int[]', i8='int[]',
                d='num[]', f4='float[]', f8='float[]', m='money[]',
                b='bool[]', v4='text[]', c4='text[]', t='text[]'))
        decimal = pg.get_decimal()
        if decimal is Decimal:
            long_decimal = decimal('123456789.123456789')
            odd_money = decimal('1234567891234567.89')
        else:
            long_decimal = decimal('12345671234.5')
            odd_money = decimal('1234567123.25')
        t, f = (True, False) if pg.get_bool() else ('t', 'f')
        data = dict(
            id=42, i2=[42, 1234, None, 0, -1],
            i4=[42, 123456789, None, 0, 1, -1],
            i8=[42, 123456789123456789, None, 0, 1, -1],
            d=[decimal(42), long_decimal, None,
               decimal(0), decimal(1), decimal(-1), -long_decimal],
            f4=[42.0, 1234.5, None, 0.0, 1.0, -1.0,
                float('inf'), float('-inf')],
            f8=[42.0, 12345671234.5, None, 0.0, 1.0, -1.0,
                float('inf'), float('-inf')],
            m=[decimal('42.00'), odd_money, None,
               decimal('0.00'), decimal('1.00'), decimal('-1.00'), -odd_money],
            b=[t, f, t, None, f, t, None, None, t],
            v4=['abc', '"Hi"', '', None], c4=['abc ', '"Hi"', '    ', None],
            t=['abc', 'Hello, World!', '"Hello, World!"', '', None])
        r = data.copy()
        self.db.insert('arraytest', r)
        if returns_arrays:
            self.assertEqual(r, data)
        else:
            self.assertEqual(r['i4'], '{42,123456789,NULL,0,1,-1}')
        self.db.insert('arraytest', r)
        r = self.db.get('arraytest', 42, 'id')
        if returns_arrays:
            self.assertEqual(r, data)
        else:
            self.assertEqual(r['i4'], '{42,123456789,NULL,0,1,-1}')
        r = self.db.query('select * from arraytest limit 1').dictresult()[0]
        if returns_arrays:
            self.assertEqual(r, data)
        else:
            self.assertEqual(r['i4'], '{42,123456789,NULL,0,1,-1}')

    def test_array_literal(self):
        insert = self.db.insert
        returns_arrays = pg.get_array()
        self.create_table('arraytest', 'i int[], t text[]')
        r = dict(i=[1, 2, 3], t=['a', 'b', 'c'])
        insert('arraytest', r)
        if returns_arrays:
            self.assertEqual(r['i'], [1, 2, 3])
            self.assertEqual(r['t'], ['a', 'b', 'c'])
        else:
            self.assertEqual(r['i'], '{1,2,3}')
            self.assertEqual(r['t'], '{a,b,c}')
        r = dict(i='{1,2,3}', t='{a,b,c}')
        self.db.insert('arraytest', r)
        if returns_arrays:
            self.assertEqual(r['i'], [1, 2, 3])
            self.assertEqual(r['t'], ['a', 'b', 'c'])
        else:
            self.assertEqual(r['i'], '{1,2,3}')
            self.assertEqual(r['t'], '{a,b,c}')
        Lit = pg.Literal  # noqa: N806
        r = dict(i=Lit("ARRAY[1, 2, 3]"), t=Lit("ARRAY['a', 'b', 'c']"))
        self.db.insert('arraytest', r)
        if returns_arrays:
            self.assertEqual(r['i'], [1, 2, 3])
            self.assertEqual(r['t'], ['a', 'b', 'c'])
        else:
            self.assertEqual(r['i'], '{1,2,3}')
            self.assertEqual(r['t'], '{a,b,c}')
        r = dict(i="1, 2, 3", t="'a', 'b', 'c'")
        self.assertRaises(pg.DataError, self.db.insert, 'arraytest', r)

    def test_array_of_ids(self):
        array_on = pg.get_array()
        self.create_table(
            'arraytest', 'i serial primary key, c cid[], o oid[], x xid[]')
        r = self.db.get_attnames('arraytest')
        if self.regtypes:
            self.assertEqual(r, dict(
                i='integer', c='cid[]', o='oid[]', x='xid[]'))
        else:
            self.assertEqual(r, dict(
                i='int', c='int[]', o='int[]', x='int[]'))
        data = dict(i=1, c=[11, 12, 13], o=[21, 22, 23], x=[31, 32, 33])
        r = data.copy()
        self.db.insert('arraytest', r)
        if array_on:
            self.assertEqual(r, data)
        else:
            self.assertEqual(r['o'], '{21,22,23}')
        self.db.get('arraytest', r)
        if array_on:
            self.assertEqual(r, data)
        else:
            self.assertEqual(r['o'], '{21,22,23}')

    def test_array_of_text(self):
        array_on = pg.get_array()
        self.create_table('arraytest', 'id serial primary key, data text[]')
        r = self.db.get_attnames('arraytest')
        self.assertEqual(r['data'], 'text[]')
        data = ['Hello, World!', '', None, '{a,b,c}', '"Hi!"',
                'null', 'NULL', 'Null', 'nulL',
                "It's all \\ kinds of\r nasty stuff!\n"]
        r = dict(data=data)
        self.db.insert('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'])
        self.assertEqual(r['data'], data)
        self.assertIsInstance(r['data'][1], str)
        self.assertIsNone(r['data'][2])
        r['data'] = None
        self.db.get('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'])
        self.assertEqual(r['data'], data)
        self.assertIsInstance(r['data'][1], str)
        self.assertIsNone(r['data'][2])

    # noinspection PyUnresolvedReferences
    def test_array_of_bytea(self):
        array_on = pg.get_array()
        bytea_escaped = pg.get_bytea_escaped()
        self.create_table('arraytest', 'id serial primary key, data bytea[]')
        r = self.db.get_attnames('arraytest')
        self.assertEqual(r['data'], 'bytea[]')
        data = [b'Hello, World!', b'', None, b'{a,b,c}', b'"Hi!"',
                b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"]
        r = dict(data=data)
        self.db.insert('arraytest', r)
        if array_on:
            self.assertIsInstance(r['data'], list)
        if array_on and not bytea_escaped:
            self.assertEqual(r['data'], data)
            self.assertIsInstance(r['data'][1], bytes)
            self.assertIsNone(r['data'][2])
        else:
            self.assertNotEqual(r['data'], data)
        r['data'] = None
        self.db.get('arraytest', r)
        if array_on:
            self.assertIsInstance(r['data'], list)
        if array_on and not bytea_escaped:
            self.assertEqual(r['data'], data)
            self.assertIsInstance(r['data'][1], bytes)
            self.assertIsNone(r['data'][2])
        else:
            self.assertNotEqual(r['data'], data)

    def test_array_of_json(self):
        self.create_table('arraytest', 'id serial primary key, data json[]')
        r = self.db.get_attnames('arraytest')
        self.assertEqual(r['data'], 'json[]')
        data = [dict(id=815, name='John Doe'), dict(id=816, name='Jane Roe')]
        array_on = pg.get_array()
        jsondecode = pg.get_jsondecode()
        r = dict(data=data)
        self.db.insert('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r['data'] = None
        self.db.get('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r = dict(data=[json.dumps(d) for d in data])
        self.db.insert('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r['data'] = None
        self.db.get('arraytest', r)
        # insert empty json values
        r = dict(data=['', None])
        self.db.insert('arraytest', r)
        r = r['data']
        if array_on:
            self.assertIsInstance(r, list)
            self.assertEqual(len(r), 2)
            self.assertIsNone(r[0])
            self.assertIsNone(r[1])
        else:
            self.assertEqual(r, '{NULL,NULL}')

    def test_array_of_jsonb(self):
        self.create_table('arraytest', 'id serial primary key, data jsonb[]')
        r = self.db.get_attnames('arraytest')
        self.assertEqual(r['data'], 'jsonb[]' if self.regtypes else 'json[]')
        data = [dict(id=815, name='John Doe'), dict(id=816, name='Jane Roe')]
        array_on = pg.get_array()
        jsondecode = pg.get_jsondecode()
        r = dict(data=data)
        self.db.insert('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r['data'] = None
        self.db.get('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r = dict(data=[json.dumps(d) for d in data])
        self.db.insert('arraytest', r)
        if not array_on:
            r['data'] = pg.cast_array(r['data'], jsondecode)
        if jsondecode is None:
            r['data'] = [json.loads(d) for d in r['data']]
        self.assertEqual(r['data'], data)
        r['data'] = None
        self.db.get('arraytest', r)
        # insert empty json values
        r = dict(data=['', None])
        self.db.insert('arraytest', r)
        r = r['data']
        if array_on:
            self.assertIsInstance(r, list)
            self.assertEqual(len(r), 2)
            self.assertIsNone(r[0])
            self.assertIsNone(r[1])
        else:
            self.assertEqual(r, '{NULL,NULL}')

    # noinspection PyUnresolvedReferences
    def test_deep_array(self):
        array_on = pg.get_array()
        self.create_table(
            'arraytest', 'id serial primary key, data text[][][]')
        r = self.db.get_attnames('arraytest')
        self.assertEqual(r['data'], 'text[]')
        data = [[['Hello, World!', '{a,b,c}', 'back\\slash']]]
        r = dict(data=data)
        self.db.insert('arraytest', r)
        if array_on:
            self.assertEqual(r['data'], data)
        else:
            self.assertTrue(r['data'].startswith('{{{"Hello,'))
        r['data'] = None
        self.db.get('arraytest', r)
        if array_on:
            self.assertEqual(r['data'], data)
        else:
            self.assertTrue(r['data'].startswith('{{{"Hello,'))

    # noinspection PyUnresolvedReferences
    def test_insert_update_get_record(self):
        query = self.db.query
        query('create type test_person_type as'
              ' (name varchar, age smallint, married bool,'
              ' weight real, salary money)')
        self.addCleanup(query, 'drop type test_person_type')
        self.create_table('test_person',
                         'id serial primary key, person test_person_type',
                         temporary=False)
        attnames = self.db.get_attnames('test_person')
        self.assertEqual(len(attnames), 2)
        self.assertIn('id', attnames)
        self.assertIn('person', attnames)
        person_typ = attnames['person']
        if self.regtypes:
            self.assertEqual(person_typ, 'test_person_type')
        else:
            self.assertEqual(person_typ, 'record')
        if self.regtypes:
            self.assertEqual(person_typ.attnames, dict(
                name='character varying', age='smallint',
                married='boolean', weight='real', salary='money'))
        else:
            self.assertEqual(person_typ.attnames, dict(
                name='text', age='int', married='bool',
                weight='float', salary='money'))
        decimal = pg.get_decimal()
        bool_class: type
        t: bool | str
        f: bool | str
        if pg.get_bool():
            bool_class = bool
            t, f = True, False
        else:
            bool_class = str
            t, f = 't', 'f'
        person: tuple = ('John Doe', 61, t, 99.5, decimal('93456.75'))
        r: Any = self.db.insert('test_person', None, person=person)
        self.assertEqual(r['id'], 1)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertEqual(p, person)
        self.assertEqual(p.name, 'John Doe')
        self.assertIsInstance(p.name, str)
        self.assertIsInstance(p.age, int)
        self.assertIsInstance(p.married, bool_class)
        self.assertIsInstance(p.weight, float)
        self.assertIsInstance(p.salary, decimal)
        person = ('Jane Roe', 59, f, 64.5, decimal('96543.25'))
        r['person'] = person
        self.db.update('test_person', r)
        self.assertEqual(r['id'], 1)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertEqual(p, person)
        self.assertEqual(p.name, 'Jane Roe')
        self.assertIsInstance(p.name, str)
        self.assertIsInstance(p.age, int)
        self.assertIsInstance(p.married, bool_class)
        self.assertIsInstance(p.weight, float)
        self.assertIsInstance(p.salary, decimal)
        r['person'] = None
        self.db.get('test_person', r)
        self.assertEqual(r['id'], 1)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertEqual(p, person)
        self.assertEqual(p.name, 'Jane Roe')
        self.assertIsInstance(p.name, str)
        self.assertIsInstance(p.age, int)
        self.assertIsInstance(p.married, bool_class)
        self.assertIsInstance(p.weight, float)
        self.assertIsInstance(p.salary, decimal)
        person = (None,) * 5
        r = self.db.insert('test_person', None, person=person)
        self.assertEqual(r['id'], 2)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertIsNone(p.name)
        self.assertIsNone(p.age)
        self.assertIsNone(p.married)
        self.assertIsNone(p.weight)
        self.assertIsNone(p.salary)
        r['person'] = None
        self.db.get('test_person', r)
        self.assertEqual(r['id'], 2)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertIsNone(p.name)
        self.assertIsNone(p.age)
        self.assertIsNone(p.married)
        self.assertIsNone(p.weight)
        self.assertIsNone(p.salary)
        r = self.db.insert('test_person', None, person=None)
        self.assertEqual(r['id'], 3)
        self.assertIsNone(r['person'])
        r['person'] = None
        self.db.get('test_person', r)
        self.assertEqual(r['id'], 3)
        self.assertIsNone(r['person'])

    # noinspection PyUnresolvedReferences
    def test_record_insert_bytea(self):
        query = self.db.query
        query('create type test_person_type as'
              ' (name text, picture bytea)')
        self.addCleanup(query, 'drop type test_person_type')
        self.create_table('test_person', 'person test_person_type',
                         temporary=False)
        person_typ = self.db.get_attnames('test_person')['person']
        self.assertEqual(person_typ.attnames,
                         dict(name='text', picture='bytea'))
        person = ('John Doe', b'O\x00ps\xff!')
        r = self.db.insert('test_person', None, person=person)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertEqual(p, person)
        self.assertEqual(p.name, 'John Doe')
        self.assertIsInstance(p.name, str)
        self.assertEqual(p.picture, person[1])
        self.assertIsInstance(p.picture, bytes)

    def test_record_insert_json(self):
        query = self.db.query
        query('create type test_person_type as (name text, data json)')
        self.addCleanup(query, 'drop type test_person_type')
        self.create_table('test_person', 'person test_person_type',
                         temporary=False)
        person_typ = self.db.get_attnames('test_person')['person']
        self.assertEqual(person_typ.attnames,
                         dict(name='text', data='json'))
        person = ('John Doe', dict(age=61, married=True, weight=99.5))
        r = self.db.insert('test_person', None, person=person)
        p = r['person']
        self.assertIsInstance(p, tuple)
        if pg.get_jsondecode() is None:
            # noinspection PyUnresolvedReferences
            p = p._replace(data=json.loads(p.data))
        self.assertEqual(p, person)
        self.assertEqual(p.name, 'John Doe')
        self.assertIsInstance(p.name, str)
        self.assertEqual(p.data, person[1])
        self.assertIsInstance(p.data, dict)

    def test_insert_get_with_domains(self):
        query = self.db.query
        query('create domain pg_temp.test_dint as int')
        query('create domain pg_temp.test_dbytea as bytea')
        query('create domain pg_temp.test_ddbytea as pg_temp.test_dbytea')
        query('create domain pg_temp.test_dints as int[]')
        self.create_table(
            'test_table', 'n int primary key, i pg_temp.test_dint,'
            ' b pg_temp.test_dbytea, bb pg_temp.test_ddbytea,'
            ' a pg_temp.test_dints')
        # check the stored values as text, independent of the cast options
        def stored():
            return self.db.query(
                "select i::text, encode(b, 'hex'), encode(bb, 'hex'),"
                " array_to_string(a, ',') from test_table").getresult()
        data = b'A\\101\x00\xff'
        r = self.db.insert('test_table', n=1, i=42, b=data, bb=data,
                           a=[1, 2])
        self.assertEqual(r['i'], 42)
        self.assertEqual(stored(), [
            ('42', '415c31303100ff', '415c31303100ff', '1,2')])
        r = self.db.update('test_table', n=1, b=b'\\x', bb=b'', a=[3])
        self.assertEqual(stored(), [('42', '5c78', '', '3')])
        if pg.get_array() and not pg.get_bytea_escaped():
            r = self.db.get('test_table', 1)
            self.assertEqual(r, dict(n=1, i=42, b=b'\\x', bb=b'', a=[3]))

    def test_arrays_of_domains(self):
        query = self.db.query
        query('create domain pg_temp.test_dint as int')
        query('create domain pg_temp.test_dbool as bool')
        query('create domain pg_temp.test_dbytea as bytea')
        query('create domain pg_temp.test_djson as jsonb')
        self.create_table(
            'test_table', 'n int primary key, ai pg_temp.test_dint[],'
            ' ab pg_temp.test_dbool[], ay pg_temp.test_dbytea[],'
            ' aj pg_temp.test_djson[]')
        regtypes = self.db.use_regtypes()
        self.db.use_regtypes(False)
        self.addCleanup(self.db.use_regtypes, regtypes)
        self.assertEqual(self.db.get_attnames('test_table'), dict(
            n='int', ai='int[]', ab='bool[]', ay='bytea[]', aj='json[]'))

        def stored():
            return self.db.query(
                "select array_to_string(ai, ','), array_to_string(ab, ','),"
                " array_to_string(array(select encode(b, 'hex')"
                " from unnest(ay) b), ','), array_to_string(aj, ';')"
                " from test_table").getresult()[0]

        value = dict(n=1, ai=[1, 2], ab=[True, False],
                     ay=[b'A\\101\x00\xff', b''],
                     aj=[{'a': 1}, pg.Json([1, 2])])
        self.db.insert('test_table', value)
        self.assertEqual(stored(), (
            '1,2', 't,f', '415c31303100ff,', '{"a": 1};[1, 2]'))
        if pg.get_array() and not pg.get_bytea_escaped() and pg.get_bool():
            r = self.db.get('test_table', 1)
            self.assertEqual(r['ai'], [1, 2])
            self.assertEqual(r['ab'], [True, False])
            self.assertEqual(r['ay'], [b'A\\101\x00\xff', b''])
            if pg.get_jsondecode():
                self.assertEqual(r['aj'], [{'a': 1}, [1, 2]])

    def test_record_with_domains(self):
        query = self.db.query
        query('create domain pg_temp.test_dint as int')
        query('create domain pg_temp.test_dbytea as bytea')
        query('create domain pg_temp.test_dints as int[]')
        query('create type pg_temp.test_rtype as (i pg_temp.test_dint,'
              ' b pg_temp.test_dbytea, a pg_temp.test_dints)')
        query('create domain pg_temp.test_drtype as pg_temp.test_rtype')
        self.create_table(
            'test_table', 'n int primary key, r pg_temp.test_rtype,'
            ' dr pg_temp.test_drtype')
        regtypes = self.db.use_regtypes()
        self.db.use_regtypes(False)
        self.addCleanup(self.db.use_regtypes, regtypes)
        attnames = self.db.get_attnames('test_table')
        self.assertEqual(attnames['r'].attnames,
                         dict(i='int', b='bytea', a='int[]'))
        self.assertEqual(attnames['dr'].simple, 'record')
        self.assertEqual(attnames['dr'].attnames,
                         dict(i='int', b='bytea', a='int[]'))
        value = (42, b'A\\101\x00\xff', [1, 2])
        r = self.db.insert('test_table', n=1, r=value, dr=value)
        self.assertEqual(r['r'], value)
        self.assertEqual(r['dr'], value)
        r = query('select r, dr from test_table').getresult()[0]
        self.assertEqual(r, (value, value))
        self.assertIsInstance(r[0].i, int)
        self.assertIsInstance(r[0].b, bytes)
        self.assertIsInstance(r[0].a, list)

    def test_oid8(self):
        if self.db.server_version < 190000:
            self.skipTest("the oid8 type needs PostgreSQL 19")
        query = self.db.query
        query('create type pg_temp.test_rtype as (o oid8, a oid8[])')
        self.create_table(
            'test_table', 'n oid8 primary key, a oid8[],'
            ' r pg_temp.test_rtype')
        regtypes = self.db.use_regtypes()
        self.db.use_regtypes(False)
        self.addCleanup(self.db.use_regtypes, regtypes)
        attnames = self.db.get_attnames('test_table')
        self.assertEqual(attnames['n'], 'int')
        self.assertEqual(attnames['a'], 'int[]')
        self.assertEqual(attnames['r'].attnames, dict(o='int', a='int[]'))
        big = 2**64 - 1  # oid8 is unsigned
        value = (big, [0, big])
        a = [big, 1] if pg.get_array() else f'{{{big},1}}'
        r = self.db.insert('test_table', n=big, a=[big, 1], r=value)
        self.assertEqual(r, dict(n=big, a=a, r=value))
        r = self.db.get('test_table', big)
        self.assertEqual(r, dict(n=big, a=a, r=value))
        # the fields of records are cast by the DB wrapper
        r = query('select r from test_table').getresult()[0][0]
        self.assertEqual(r, value)
        self.assertIsInstance(r.o, int)

    # noinspection PyUnresolvedReferences
    def test_record_literal(self):
        query = self.db.query
        query('create type test_person_type as'
              ' (name varchar, age smallint)')
        self.addCleanup(query, 'drop type test_person_type')
        self.create_table('test_person', 'person test_person_type',
                         temporary=False)
        person_typ = self.db.get_attnames('test_person')['person']
        if self.regtypes:
            self.assertEqual(person_typ, 'test_person_type')
        else:
            self.assertEqual(person_typ, 'record')
        if self.regtypes:
            self.assertEqual(person_typ.attnames,
                             dict(name='character varying', age='smallint'))
        else:
            self.assertEqual(person_typ.attnames,
                             dict(name='text', age='int'))
        person = pg.Literal("('John Doe', 61)")
        r = self.db.insert('test_person', None, person=person)
        p = r['person']
        self.assertIsInstance(p, tuple)
        self.assertEqual(p.name, 'John Doe')
        self.assertIsInstance(p.name, str)
        self.assertEqual(p.age, 61)
        self.assertIsInstance(p.age, int)

    def test_record_with_special_characters(self):
        query = self.db.query
        query('create type test_pair_type as (a text, b text)')
        self.addCleanup(query, 'drop type test_pair_type')
        typ = self.db.dbtypes['test_pair_type']
        q = 'select %s::test_pair_type'
        for value in (('a)b', 'c'), ('(', ')'), (' x ', 'y,z'),
                      ('"', '\\'), ('', None)):
            r = self.db.query_formatted(q, (value,), (typ,)).singlescalar()
            self.assertEqual(r, value)

    def test_record_with_undecodable_text(self):
        query = self.db.query
        # with SQL_ASCII, the server sends text without conversion
        query('set client_encoding=sql_ascii')
        query('create temp table test_person'
              ' (name text, tags varchar[], amount numeric, day date)')
        # chr(228) is non-ASCII in all server encodings
        raw = query('select chr(228)').singlescalar()
        self.assertIsInstance(raw, bytes)
        p = query("select row('w' || chr(228), array['x', chr(228)],"
                  " 1.5, '2026-11-21')::test_person").singlescalar()
        # undecodable text is returned as bytes, also inside records
        # (this raised SystemError before), but the other fields of the
        # record are still decoded and cast
        self.assertEqual(p, (b'w' + raw, ['x', raw],
                             Decimal('1.5'), date(2026, 11, 21)))
        self.assertEqual(p.name, b'w' + raw)

    def test_hstore_with_raw_bytes_as_text(self):
        query = self.db.query
        # with SQL_ASCII, the server sends text without conversion
        query('set client_encoding=sql_ascii')
        query('create type test_pair_type as (a text, b text)')
        self.addCleanup(query, 'drop type test_pair_type')
        typ = self.db.dbtypes['test_pair_type']
        # chr(228) is non-ASCII in all server encodings
        raw = query('select chr(228)').singlescalar()
        self.assertIsInstance(raw, bytes)
        h = pg.Hstore({b'k' + raw: b'v', 'x': 'y z'})
        expected = b'k' + raw + b'=>v,x=>"y z"'
        f = self.db.query_formatted
        # the raw bytes were sent with backslash escapes before
        self.assertEqual(f('select %s::text', (h,), ('text',)).singlescalar(),
                         expected)
        self.assertEqual(f('select (%s::text[])[1]', ([h],), ('text[]',)
                           ).singlescalar(), expected)
        self.assertEqual(f('select (%s::test_pair_type).a', ((h, 'z'),),
                           (typ,)).singlescalar(), expected)

    def test_write_back_undecodable_text(self):
        self.require_hstore()
        query = self.db.query
        # with SQL_ASCII, the server sends text without conversion
        query('set client_encoding=sql_ascii')
        query('create type test_item_type as (name text, tags text[])')
        self.addCleanup(query, 'drop type test_item_type')
        self.create_table(
            'test_items', 'n smallint primary key, h hstore, a text[],'
            ' i test_item_type, ia test_item_type[]', temporary=False)
        # chr(228) is non-ASCII in all server encodings
        raw = query('select chr(228)').singlescalar()
        self.assertIsInstance(raw, bytes)
        query("insert into test_items values (1,"
              " hstore('k' || chr(228), 'v' || chr(228)) || 'x=>y',"
              " array['a' || chr(228), 'b c'],"
              " row('w' || chr(228), array[chr(228), 'x']),"
              " array[row(chr(228), null)::test_item_type])")
        row = self.db.get('test_items', 1)
        self.assertEqual(row['h'], {b'k' + raw: b'v' + raw, 'x': 'y'})
        if pg.get_array():
            self.assertEqual(row['a'], [b'a' + raw, 'b c'])
            self.assertEqual(row['i'], (b'w' + raw, [raw, 'x']))
            self.assertEqual(row['ia'], [(raw, None)])
        # the raw text can be written back (this raised errors or
        # wrote the representation of the bytes objects before)
        row['n'] = 2
        r = self.db.insert('test_items', row)
        for key in 'h', 'a', 'i', 'ia':
            self.assertEqual(r[key], row[key])
        r['n'] = 3
        self.db.insert('test_items', r)
        r = self.db.get('test_items', 3)
        for key in 'h', 'a', 'i', 'ia':
            self.assertEqual(r[key], row[key])
        r = query('select count(distinct (h, a, i, ia)) from test_items')
        self.assertEqual(r.singlescalar(), 1)
        q = 'select count(*) from test_items where h=%s'
        h = pg.Hstore(row['h'])
        for inline in (False, True):
            r = self.db.query_formatted(q, (h,), inline=inline)
            self.assertEqual(r.singlescalar(), 3)
        r = self.db.query_formatted(q, (row['h'],), ('hstore',))
        self.assertEqual(r.singlescalar(), 3)
        q = 'select count(*) from test_items where a=%s'
        r = self.db.query_formatted(q, (row['a'],), ('text[]',))
        self.assertEqual(r.singlescalar(), 3)
        q = 'select count(*) from test_items where i=%s::test_item_type'
        r = self.db.query_formatted(
            q, (row['i'],), (self.db.dbtypes['test_item_type'],))
        self.assertEqual(r.singlescalar(), 3)

    def test_date(self):
        query = self.db.query
        for datestyle in (
                'ISO', 'Postgres, MDY', 'Postgres, DMY',
                'SQL, MDY', 'SQL, DMY', 'German'):
            self.db.set_parameter('datestyle', datestyle)
            d = date(2016, 3, 14)
            q = "select $1::date"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, date)
            self.assertEqual(r, d)
            q = "select '10000-08-01'::date, '0099-01-08 BC'::date"
            r = query(q).getresult()[0]
            self.assertIsInstance(r[0], date)
            self.assertIsInstance(r[1], date)
            self.assertEqual(r[0], date.max)
            self.assertEqual(r[1], date.min)
        q = "select 'infinity'::date, '-infinity'::date"
        r = query(q).getresult()[0]
        self.assertIsInstance(r[0], date)
        self.assertIsInstance(r[1], date)
        self.assertEqual(r[0], date.max)
        self.assertEqual(r[1], date.min)

    def test_time(self):
        query = self.db.query
        d = time(15, 9, 26)
        q = "select $1::time"
        r = query(q, (d,)).getresult()[0][0]
        self.assertIsInstance(r, time)
        self.assertEqual(r, d)
        d = time(15, 9, 26, 535897)
        q = "select $1::time"
        r = query(q, (d,)).getresult()[0][0]
        self.assertIsInstance(r, time)
        self.assertEqual(r, d)

    def test_timetz(self):
        query = self.db.query
        timezones = {'GMT': 0, 'Etc/GMT-1': 1, 'Etc/GMT-5': -5, 'UTC': 0}
        for timezone in sorted(timezones):
            tz = f'{timezones[timezone]:+03d}00'
            tzinfo = datetime.strptime(tz, '%z').tzinfo
            self.db.set_parameter('timezone', timezone)
            d = time(15, 9, 26, tzinfo=tzinfo)
            q = "select $1::timetz"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, time)
            self.assertEqual(r, d)
            d = time(15, 9, 26, 535897, tzinfo)
            q = "select $1::timetz"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, time)
            self.assertEqual(r, d)

    def test_timestamp(self):
        query = self.db.query
        for datestyle in ('ISO', 'Postgres, MDY', 'Postgres, DMY',
                          'SQL, MDY', 'SQL, DMY', 'German'):
            self.db.set_parameter('datestyle', datestyle)
            d = datetime(2016, 3, 14)
            q = "select $1::timestamp"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, datetime)
            self.assertEqual(r, d)
            d = datetime(2016, 3, 14, 15, 9, 26)
            q = "select $1::timestamp"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, datetime)
            self.assertEqual(r, d)
            d = datetime(2016, 3, 14, 15, 9, 26, 535897)
            q = "select $1::timestamp"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, datetime)
            self.assertEqual(r, d)
            q = ("select '10000-08-01 AD'::timestamp,"
                 " '0099-01-08 BC'::timestamp")
            r = query(q).getresult()[0]
            self.assertIsInstance(r[0], datetime)
            self.assertIsInstance(r[1], datetime)
            self.assertEqual(r[0], datetime.max)
            self.assertEqual(r[1], datetime.min)
        q = "select 'infinity'::timestamp, '-infinity'::timestamp"
        r = query(q).getresult()[0]
        self.assertIsInstance(r[0], datetime)
        self.assertIsInstance(r[1], datetime)
        self.assertEqual(r[0], datetime.max)
        self.assertEqual(r[1], datetime.min)

    def test_timestamptz(self):
        query = self.db.query
        timezones = {'GMT': 0, 'Etc/GMT-1': 1, 'Etc/GMT-5': -5, 'UTC': 0}
        for timezone in sorted(timezones):
            tz = f'{timezones[timezone]:+03d}00'
            tzinfo = datetime.strptime(tz, '%z').tzinfo
            self.db.set_parameter('timezone', timezone)
            for datestyle in ('ISO', 'Postgres, MDY', 'Postgres, DMY',
                              'SQL, MDY', 'SQL, DMY', 'German'):
                self.db.set_parameter('datestyle', datestyle)
                d = datetime(2016, 3, 14, tzinfo=tzinfo)
                q = "select $1::timestamptz"
                r = query(q, (d,)).getresult()[0][0]
                self.assertIsInstance(r, datetime)
                self.assertEqual(r, d)
                d = datetime(2016, 3, 14, 15, 9, 26, tzinfo=tzinfo)
                q = "select $1::timestamptz"
                r = query(q, (d,)).getresult()[0][0]
                self.assertIsInstance(r, datetime)
                self.assertEqual(r, d)
                d = datetime(2016, 3, 14, 15, 9, 26, 535897, tzinfo)
                q = "select $1::timestamptz"
                r = query(q, (d,)).getresult()[0][0]
                self.assertIsInstance(r, datetime)
                self.assertEqual(r, d)
                q = ("select '10000-08-01 AD'::timestamptz,"
                     " '0099-01-08 BC'::timestamptz")
                r = query(q).getresult()[0]
                self.assertIsInstance(r[0], datetime)
                self.assertIsInstance(r[1], datetime)
                self.assertEqual(r[0], datetime.max)
                self.assertEqual(r[1], datetime.min)
        q = "select 'infinity'::timestamptz, '-infinity'::timestamptz"
        r = query(q).getresult()[0]
        self.assertIsInstance(r[0], datetime)
        self.assertIsInstance(r[1], datetime)
        self.assertEqual(r[0], datetime.max)
        self.assertEqual(r[1], datetime.min)

    def test_interval(self):
        query = self.db.query
        for intervalstyle in (
                'sql_standard', 'postgres', 'postgres_verbose', 'iso_8601'):
            self.db.set_parameter('intervalstyle', intervalstyle)
            d = timedelta(3)
            q = "select $1::interval"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, timedelta)
            self.assertEqual(r, d)
            d = timedelta(-30)
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, timedelta)
            self.assertEqual(r, d)
            d = timedelta(hours=3, minutes=31, seconds=42, microseconds=5678)
            q = "select $1::interval"
            r = query(q, (d,)).getresult()[0][0]
            self.assertIsInstance(r, timedelta)
            self.assertEqual(r, d)

    def test_infinite_interval(self):
        if self.db.server_version < 170000:
            self.skipTest("infinite intervals need PostgreSQL 17")
        query = self.db.query
        for intervalstyle in (
                'sql_standard', 'postgres', 'postgres_verbose', 'iso_8601'):
            self.db.set_parameter('intervalstyle', intervalstyle)
            q = ("select 'infinity'::interval, '-infinity'::interval,"
                 " array['infinity', '-infinity', '1 day']::interval[]")
            r = query(q).getresult()[0]
            self.assertEqual(r[0], timedelta.max)
            self.assertEqual(r[1], timedelta.min)
            self.assertEqual(
                r[2], [timedelta.max, timedelta.min, timedelta(1)])
        # the extreme values are passed as finite intervals
        q = "select $1::interval, isfinite($1::interval)::int"
        for d in timedelta.max, timedelta.min:
            r = query(q, (d,)).getresult()[0]
            self.assertEqual(r, (d, 1))

    def test_date_and_time_arrays(self):
        dt = (date(2016, 3, 14), time(15, 9, 26))
        q = "select ARRAY[$1::date], ARRAY[$2::time]"
        r = self.db.query(q, dt).getresult()[0]
        self.assertIsInstance(r[0], list)
        self.assertEqual(r[0][0], dt[0])
        self.assertIsInstance(r[1], list)
        self.assertEqual(r[1][0], dt[1])

    def require_hstore(self):
        try:
            self.db.query("select 'k=>v'::hstore")
        except pg.DatabaseError:
            try:
                self.db.query("create extension hstore")
            except pg.DatabaseError:
                self.skipTest("hstore extension not enabled")

    def test_hstore(self):
        self.require_hstore()
        d = {'k': 'v', 'foo': 'bar', 'baz': 'whatever',
             '1a': 'anything at all', '2=b': 'value = 2', '3>c': 'value > 3',
             '4"c': 'value " 4', "5'c": "value ' 5", 'hello, world': '"hi!"',
             'None': None, 'NULL': 'NULL', 'empty': '',
             'back\\': '\\slash', '\\': '\\\\',
             'tab\tkey': 'line\nbreak', ' pad ': '\r\f\v',
             'v\vtab': 'null'}
        q = "select $1::hstore"
        r = self.db.query(q, (pg.Hstore(d),)).getresult()[0][0]
        self.assertIsInstance(r, dict)
        self.assertEqual(r, d)
        q = "select %s::hstore"
        r = self.db.query_formatted(
            q, (pg.Hstore(d),), inline=True).getresult()[0][0]
        self.assertIsInstance(r, dict)
        self.assertEqual(r, d)

    def test_insert_get_empty_hstore(self):
        self.require_hstore()
        self.create_table(
            'hstore_test', "n smallint primary key,"
            " data hstore not null default ''")
        for n, data in enumerate(({}, pg.Hstore(), '')):
            r = self.db.insert('hstore_test', n=n, data=data)
            self.assertEqual(r['data'], {})
            r = self.db.get('hstore_test', n)
            self.assertEqual(r['data'], {})
        r = self.db.update('hstore_test', n=0, data={'k': 'v'})
        self.assertEqual(r['data'], {'k': 'v'})
        r = self.db.update('hstore_test', n=0, data={})
        self.assertEqual(r['data'], {})
        self.assertRaises(
            pg.IntegrityError, self.db.insert, 'hstore_test', n=3, data=None)

    def test_uuid(self):
        d = UUID('{12345678-1234-5678-1234-567812345678}')
        q = 'select $1::uuid'
        r = self.db.query(q, (d,)).getresult()[0][0]
        self.assertIsInstance(r, UUID)
        self.assertEqual(r, d)

    def test_insert_get_hstore_array(self):
        self.require_hstore()
        self.create_table(
            'hstore_array_test', 'n smallint primary key, data hstore[]')
        query = self.db.query
        q = 'select data::text from hstore_array_test where n=$1'
        expected = r'{"\"k\"=>\"a b\"","\"x\"=>NULL","",NULL}'
        data: list = [pg.Hstore({'k': 'a b'}), {'x': None}, {}, None]
        self.db.insert('hstore_array_test', n=1, data=data)
        self.assertEqual(query(q, (1,)).getresult()[0][0], expected)
        r = self.db.get('hstore_array_test', 1)['data']
        if pg.get_array():
            self.assertEqual(r, [{'k': 'a b'}, {'x': None}, {}, None])
        data = [pg.Hstore({'k': 'a b'}), pg.Hstore({'x': None})]
        expected = r'{"\"k\"=>\"a b\"","\"x\"=>NULL"}'
        for inline in (False, True):
            r = self.db.query_formatted(
                'select %s::hstore[]::text', (data,), inline=inline)
            self.assertEqual(r.getresult()[0][0], expected)

    def test_insert_get_json_array_with_wrappers(self):
        self.create_table(
            'json_array_test', 'n smallint primary key, data json[]')
        query = self.db.query
        q = 'select data::text from json_array_test where n=$1'
        expected = r'{"{\"k\": \"v\"}","{}",0,false,NULL}'
        data: list = [pg.Json({'k': 'v'}), {}, 0, False, None]
        self.db.insert('json_array_test', n=1, data=data)
        self.assertEqual(query(q, (1,)).getresult()[0][0], expected)
        data = [pg.Json({'k': 'v'}), pg.Json({}), pg.Json(0)]
        expected = r'{"{\"k\": \"v\"}","{}",0}'
        for inline in (False, True):
            r = self.db.query_formatted(
                'select %s::json[]::text', (data,), inline=inline)
            self.assertEqual(r.getresult()[0][0], expected)

    def test_insert_get_uuid_array(self):
        self.create_table(
            'uuid_array_test', 'n smallint primary key, data uuid[]')
        query = self.db.query
        q = 'select data::text from uuid_array_test where n=$1'
        u = UUID('{12345678-1234-5678-1234-567812345678}')
        expected = '{12345678-1234-5678-1234-567812345678,NULL}'
        self.db.insert('uuid_array_test', n=1, data=[u, None])
        self.assertEqual(query(q, (1,)).getresult()[0][0], expected)
        r = self.db.get('uuid_array_test', 1)['data']
        if pg.get_array():
            self.assertEqual(r, [u, None])
        for inline in (False, True):
            r = self.db.query_formatted(
                'select %s::uuid[]::text', ([u, None],), inline=inline)
            self.assertEqual(r.getresult()[0][0], expected)
        r = self.db.query_formatted('select %s::text', (u,), inline=True)
        self.assertEqual(r.getresult()[0][0], str(u))

    def test_network_types(self):
        values: list[tuple[Any, str]] = [
            (IPv4Address('192.168.0.1'), 'inet'),
            (IPv6Address('2001:db8::1'), 'inet'),
            (IPv4Interface('192.168.0.1/24'), 'inet'),
            (IPv6Interface('2001:db8::1/64'), 'inet'),
            (IPv4Network('192.168.0.0/24'), 'cidr'),
            (IPv6Network('2001:db8::/32'), 'cidr')]
        query = self.db.query
        query_formatted = self.db.query_formatted
        for value, typ in values:
            with self.subTest(value=value):
                text = str(value)
                r = query(f'select $1::{typ}', (value,)).getresult()[0][0]
                self.assertEqual(r, text)
                for inline in (False, True):
                    r = query_formatted(
                        f'select %s::{typ}', (value,), inline=inline)
                    self.assertEqual(r.getresult()[0][0], text)
                r = query_formatted(
                    'select %s, pg_typeof(%s)::text', (value, value),
                    inline=True)
                self.assertEqual(r.getresult()[0], (text, typ))

    def test_network_type_arrays(self):
        a = IPv4Address('192.168.0.1')
        i = IPv6Interface('2001:db8::1/64')
        n = IPv4Network('192.168.0.0/24')
        expected = '{192.168.0.1,2001:db8::1/64,192.168.0.0/24,NULL}'
        for inline in (False, True):
            r = self.db.query_formatted(
                'select %s::inet[]::text', ([a, i, n, None],), inline=inline)
            self.assertEqual(r.getresult()[0][0], expected)
        r = self.db.query_formatted(
            'select %s::text, pg_typeof(%s)::text',
            ([a, i, n, None], [[n, None]]), inline=True)
        self.assertEqual(r.getresult()[0], (expected, 'cidr[]'))
        r = self.db.query_formatted(
            'select %s::text', ((a, n),), inline=True)
        self.assertEqual(r.getresult()[0][0], '(192.168.0.1,192.168.0.0/24)')

    def test_insert_get_network_types(self):
        self.create_table(
            'network_test',
            'n smallint primary key, i inet, c cidr, ia inet[], ca cidr[]')
        a = IPv4Address('192.168.0.1')
        i = IPv6Interface('2001:db8::1/64')
        n = IPv6Network('2001:db8::/32')
        r = self.db.insert(
            'network_test', n=1, i=i, c=n, ia=[a, i, None], ca=[n, None])
        self.assertEqual(r['i'], '2001:db8::1/64')
        self.assertEqual(r['c'], '2001:db8::/32')
        r = self.db.get('network_test', 1)
        self.assertEqual(r['i'], '2001:db8::1/64')
        self.assertEqual(r['c'], '2001:db8::/32')
        if pg.get_array():
            self.assertEqual(r['ia'], ['192.168.0.1', '2001:db8::1/64', None])
            self.assertEqual(r['ca'], ['2001:db8::/32', None])
        r = self.db.update('network_test', n=1, i=a, ia=[n])
        self.assertEqual(r['i'], '192.168.0.1')
        r = self.db.query(
            'select ia::text from network_test where i=$1', (a,))
        self.assertEqual(r.getresult(), [('{2001:db8::/32}',)])

    def test_network_types_as_ipaddress(self):
        from pg.cast import cast_cidr, cast_inet
        query = self.db.query
        q = ("select '192.168.0.1'::inet, '2001:db8::1/64'::inet,"
             " '192.168.0.0/24'::cidr,"
             " array['10.0.0.1'::inet, null], array['2001:db8::/32'::cidr]")
        r = query(q).getresult()[0]
        self.assertEqual(r[:3], ('192.168.0.1', '2001:db8::1/64',
                                 '192.168.0.0/24'))
        dbtypes = self.db.dbtypes
        dbtypes.set_typecast('inet', cast_inet)
        dbtypes.set_typecast('cidr', cast_cidr)
        self.addCleanup(dbtypes.reset_typecast, ['inet', 'cidr'])
        r = query(q).getresult()[0]
        self.assertEqual(r[:3], (
            IPv4Address('192.168.0.1'), IPv6Interface('2001:db8::1/64'),
            IPv4Network('192.168.0.0/24')))
        if pg.get_array():
            self.assertEqual(r[3:], (
                [IPv4Address('10.0.0.1'), None],
                [IPv6Network('2001:db8::/32')]))
        self.create_table('network_test', 'n smallint primary key,'
                          ' i inet, c cidr, ia inet[]')
        a = IPv4Address('192.168.0.1')
        i = IPv4Interface('192.168.0.1/24')
        n = IPv6Network('2001:db8::/32')
        r = self.db.insert('network_test', n=1, i=i, c=n, ia=[a, i])
        self.assertEqual(r['i'], i)
        self.assertEqual(r['c'], n)
        r = self.db.get('network_test', 1)
        self.assertEqual(r['i'], i)
        self.assertEqual(r['c'], n)
        if pg.get_array():
            self.assertEqual(r['ia'], [a, i])
        r = query('select network_test from network_test').getresult()[0][0]
        self.assertEqual(r.i, i)
        self.assertEqual(r.c, n)

    def test_insert_get_array_of_unknown_type(self):
        query = self.db.query
        query("create type test_mood as enum ('sad', 'ok', 'happy')")
        self.addCleanup(query, 'drop type test_mood')
        self.create_table('unknown_array_test',
                          'n smallint primary key, m test_mood[], i inet[]',
                          temporary=False)
        r = self.db.get_attnames('unknown_array_test')
        self.assertEqual(r['m'].simple, 'text[]')
        self.assertEqual(r['i'].simple, 'text[]')
        if self.regtypes:
            self.assertEqual(
                r, dict(n='smallint', m='test_mood[]', i='inet[]'))
        else:
            self.assertEqual(r, dict(n='int', m='text[]', i='text[]'))
        self.db.insert('unknown_array_test', n=1, m=['sad', None, 'happy'],
                       i=[IPv4Address('192.168.0.1'), '10.0.0.0/8'])
        r = query('select m::text, i::text from unknown_array_test')
        self.assertEqual(r.getresult(), [
            ('{sad,NULL,happy}', '{192.168.0.1,10.0.0.0/8}')])
        r = self.db.query_formatted(
            'select %s::test_mood[]::text', (['ok', 'sad'],), ('_test_mood',))
        self.assertEqual(r.getresult()[0][0], '{ok,sad}')

    def test_insert_get_record_array(self):
        query = self.db.query
        query('create type test_item_type as (n int, s text, a int[])')
        self.addCleanup(query, 'drop type test_item_type')
        self.create_table('record_array_test',
                          'n smallint primary key, data test_item_type[]',
                          temporary=False)
        attnames = self.db.get_attnames('record_array_test')
        typ = attnames['data']
        self.assertEqual(
            typ, 'test_item_type[]' if self.regtypes else 'record[]')
        self.assertEqual(typ.simple, 'record[]')
        self.assertEqual(typ.category, 'A')
        self.assertEqual(typ.elem, self.db.dbtypes['test_item_type'].oid)
        q = 'select data::text from record_array_test where n=$1'
        expected = r'{"(1,\"a, b\",\"{2,3}\")","(4,,)",NULL}'
        data: list = [(1, 'a, b', [2, 3]), (4, None, None), None]
        self.db.insert('record_array_test', n=1, data=data)
        self.assertEqual(query(q, (1,)).getresult()[0][0], expected)
        r = self.db.get('record_array_test', 1)['data']
        if pg.get_array():
            self.assertEqual(len(r), 3)
            self.assertEqual(r[0].s, 'a, b')
            self.assertEqual(r[1].n, 4)
            self.assertIsNone(r[2])
        self.assertRaises(TypeError, self.db.insert, 'record_array_test',
                          n=2, data=[(1, 'a')])
        for inline in (False, True):
            r = self.db.query_formatted(
                'select %s::test_item_type[]::text', (data[:2],),
                inline=inline)
            self.assertEqual(
                r.getresult()[0][0], r'{"(1,\"a, b\",\"{2,3}\")","(4,,)"}')

    def test_db_types_lookup(self):
        query = self.db.query
        query('create domain pg_temp."Test $2 Type" as int')
        dbtypes = self.db.dbtypes
        int4 = dbtypes[23]
        self.assertEqual(int4.pgtype, 'int4')
        for name in ('int4', '"int4"', 'pg_catalog.int4',
                     'pg_catalog."int4"'):
            self.assertIs(dbtypes[name], int4, name)
        typ = dbtypes['Test $2 Type']
        self.assertEqual(typ.pgtype, 'Test $2 Type')
        self.assertIs(dbtypes['"Test $2 Type"'], typ)
        self.assertIs(dbtypes['pg_temp."Test $2 Type"'], typ)
        self.assertIs(dbtypes['pg_temp.Test $2 Type'], typ)
        # names can also be passed as raw bytes
        self.assertIs(dbtypes[b'Test $2 Type'], typ)
        self.assertIs(dbtypes[b'pg_catalog.int4'], int4)
        # names are taken literally, they are not SQL type names
        for name in ('INT4', 'integer', 'int[]', 'pg_catalog.INT4',
                     'PG_CATALOG.int4', 'pg_catalog.int4[]', 'public.int4'):
            self.assertIsNone(dbtypes.get(name), name)

    def test_db_types_failing_lookup_in_transaction(self):
        query = self.db.query
        dbtypes = self.db.dbtypes
        # failing lookups must not abort the current transaction
        self.db.begin()
        try:
            for name in ('pg_temp.int4', 'test_no_such_type',
                         'pg_catalog.no_such_type', 'no_such_schema.int4',
                         'a.b.c', 'int[', '"int4', '', "it's", 'a$1',
                         b'a.b.c', b"it's", 0, 999999999, -1, 2 ** 32,
                         2 ** 64):
                self.assertIsNone(dbtypes.get(name), name)
                self.assertRaises(KeyError, dbtypes.__getitem__, name)
            for key in None, True, 1.5:
                self.assertRaises(TypeError, dbtypes.get, key)
            self.assertEqual(query('select 1').getresult(), [(1,)])
        finally:
            self.db.rollback()

    def test_db_types_internal_names_are_literal(self):
        # the names of the element types of arrays, which are used to look
        # up typecasts, must be taken literally, not as qualified names
        self.db.query('create domain pg_temp."pg_catalog.bool" as int')
        r = self.db.query(
            'select array[2]::pg_temp."pg_catalog.bool"[]').getresult()[0][0]
        self.assertEqual(r, [2])  # always cast in Python
        typ = self.db.dbtypes['"pg_catalog.bool"']
        self.assertEqual(typ.base.oid, 23)
        self.assertEqual(self.db.dbtypes.typecast('2', typ), 2)
        self.assertEqual(self.db.dbtypes.typecast('2', typ.base), 2)

    def test_db_types_outside_search_path(self):
        # typecasts for composite types, domains and arrays must be created
        # from the OIDs of the types, not from their names, since the types
        # may not be in the search path or their names may be ambiguous
        query = self.db.query
        dbtypes = self.db.dbtypes

        def get(what):
            return query(f'select {what}').getresult()[0]

        self.db.begin()
        try:
            query('create schema test_types_schema')
            query('create type test_types_schema.t as (a int, b text)')
            query('create domain test_types_schema.d as int')
            query('create type test_types_schema.o'
                  ' as (n test_types_schema.d, t test_types_schema.t)')
            query('create table test_types_schema.r'
                  ' (i int, t test_types_schema.t, ts test_types_schema.t[])')
            query("insert into test_types_schema.r values"
                  " (1, (2, 'b'), array[(3, 'c')]::test_types_schema.t[])")
            r = get("(1, 'a')::test_types_schema.t")[0]
            self.assertEqual(r, (1, 'a'))
            self.assertEqual(r._fields, ('a', 'b'))
            r = get("(4, (1, 'a'))::test_types_schema.o")[0]
            self.assertEqual(r, (4, (1, 'a')))
            self.assertEqual(r.t.b, 'a')
            r = get('* from test_types_schema.r')
            self.assertEqual(r[:2], (1, (2, 'b')))
            r = get('r from test_types_schema.r r')[0]
            self.assertEqual(r[:2], (1, (2, 'b')))
            self.assertEqual(r.ts, [(3, 'c')])
            self.assertEqual(
                get("array[(1, 'a')]::test_types_schema.t[]")[0], [(1, 'a')])
            self.assertEqual(get('array[3]::test_types_schema.d[]')[0], [3])
            # a different type with the same name in the search path
            query('create type pg_temp.t as (x text)')
            self.assertEqual(get("row('z')::pg_temp.t")[0], ('z',))
            self.assertEqual(get("(1, 'a')::test_types_schema.t")[0], (1, 'a'))
            self.assertEqual(get("row('z')::pg_temp.t")[0]._fields, ('x',))
            self.assertEqual(get(
                "array[(1, 'a')]::test_types_schema.t[],"
                " array[row('z')]::pg_temp.t[]"), ([(1, 'a')], [('z',)]))
            # typecasts set for the name of a type are used for all types
            # with that name and for fields and elements of that type
            dbtypes.set_typecast('t', lambda v: f'T{v}')
            try:
                self.assertEqual(
                    get("(4, (1, 'a'))::test_types_schema.o")[0],
                    (4, 'T(1,a)'))
                self.assertEqual(get("row('z')::pg_temp.t")[0], 'T(z)')
            finally:
                dbtypes.reset_typecast('t')
            self.assertEqual(
                get("(4, (1, 'a'))::test_types_schema.o")[0], (4, (1, 'a')))
            # types without typecast must not be looked up again and again
            query("create type test_types_schema.e as enum ('x', 'y')")
            sql = ("select 'x'::test_types_schema.e,"
                   " array['x', 'y']::test_types_schema.e[]"
                   " from generate_series(1, 3)")
            r = query(sql).getresult()
            self.assertEqual(r[2], ('x', ['x', 'y']))
            q = query(sql)
            with patch.object(self.db, 'query', wraps=query) as mock_query:
                self.assertEqual(q.getresult(), r)
            mock_query.assert_not_called()
        finally:
            self.db.rollback()

    def test_db_types_with_names_of_builtin_types(self):
        # the built-in default typecasts are only used for base types, not
        # for other types with the same name in another schema, but the
        # typecasts set for a type name are used for all types with that name
        query = self.db.query
        dbtypes = self.db.dbtypes
        s = 'test_types_schema'

        def get(what):
            return query(f'select {what}').getresult()[0]

        self.db.begin()
        try:
            query(f'create schema {s}')
            query(f'create type {s}.date as (a int, b text)')
            query(f'create type {s}.json as (a int)')
            query(f'create type {s}.bool as (a int)')
            query(f'create type {s}.text as (a int)')
            query(f'create type {s}.record as (a int, d date)')
            query(f"create type {s}.time as enum ('morning')")
            query(f'create type {s}.money as range (subtype = int4)')
            query(f'create domain {s}.interval as int')
            query(f'create domain {s}.timestamp as date')
            query(f'create domain {s}.float8 as {s}.date')
            query(f'create type {s}.o as'
                  f' (d {s}.date, i {s}.interval, t {s}.time)')
            r = get(f"'2020-01-02'::date, (1, 'x')::{s}.date")
            self.assertEqual(r, (date(2020, 1, 2), (1, 'x')))
            self.assertEqual(r[1]._fields, ('a', 'b'))
            self.assertEqual(get(f'row(1)::{s}.json')[0], (1,))
            self.assertEqual(get(f'row(1)::{s}.bool')[0], (1,))
            self.assertEqual(get(f'row(1)::{s}.text')[0], (1,))
            self.assertEqual(get(f"(1, '2020-01-02')::{s}.record")[0],
                             (1, date(2020, 1, 2)))
            self.assertEqual(get(f"'morning'::{s}.time, '10:00'::time"),
                             ('morning', time(10)))
            self.assertEqual(get(f"'[1,5)'::{s}.money")[0], '[1,5)')
            self.assertEqual(get(f'5::{s}.interval')[0], 5)
            self.assertEqual(get(f"(1, 'x')::{s}.float8")[0], (1, 'x'))
            self.assertEqual(get(
                f"array[(1, 'x')]::{s}.date[], array['morning']::{s}.time[],"
                f" array[5]::{s}.interval[],"
                f" array['2020-01-02']::{s}.timestamp[]"),
                ([(1, 'x')], ['morning'], [5], [date(2020, 1, 2)]))
            self.assertEqual(get(f"((1, 'x'), 5, 'morning')::{s}.o")[0],
                             ((1, 'x'), 5, 'morning'))
            self.assertEqual(dbtypes.typecast('(1,x)', f'{s}.date'), (1, 'x'))
            self.assertEqual(dbtypes.typecast('2020-01-02', 'date'),
                             date(2020, 1, 2))
            # typecasts set for the name apply to all types with that name
            dbtypes.set_typecast('date', lambda v: f'D{v}')
            try:
                self.assertEqual(get(
                    f"'2020-01-02'::date, (1, 'x')::{s}.date,"
                    f" array[(1, 'x')]::{s}.date[]"),
                    ('D2020-01-02', 'D(1,x)', ['D(1,x)']))
            finally:
                dbtypes.reset_typecast('date')
            self.assertEqual(get(f"'2020-01-02'::date, (1, 'x')::{s}.date"),
                             (date(2020, 1, 2), (1, 'x')))
            # parameters are adapted according to the type structure
            query(f'create table {s}.tab (id int primary key, d {s}.date,'
                  f' ds {s}.date[], t {s}.time, b {s}.json, i {s}.interval[])')
            attnames = self.db.get_attnames(f'{s}.tab')
            self.assertEqual({k: v.simple for k, v in attnames.items()}, dict(
                id='int', d='record', ds='record[]', t='text', b='record',
                i='int[]'))
            r = self.db.insert(f'{s}.tab', id=1, d=(1, 'x'), ds=[(2, 'y')],
                               t='morning', i=[5])
            self.assertEqual(r, dict(id=1, d=(1, 'x'), ds=[(2, 'y')],
                                     t='morning', b=None, i=[5]))
            self.assertEqual(self.db.get(f'{s}.tab', 1), r)
            self.assertEqual(self.db.get_as_dict(f'{s}.tab')[1][0], (1, 'x'))
        finally:
            self.db.rollback()

    def test_db_types_global_typecast_for_names_of_builtin_types(self):
        # a global typecast set for the name of a built-in type is used
        # for all types with that name, like in previous versions
        query = self.db.query
        cast = pg.get_typecast('uuid')
        pg.set_typecast('uuid', lambda v: f'U{v}')
        try:
            query('create type pg_temp.uuid as (a int)')
            self.assertEqual(query(
                "select '12345678-1234-5678-1234-567812345678'"
                "::pg_catalog.uuid, row(1)::pg_temp.uuid").getresult()[0],
                ('U12345678-1234-5678-1234-567812345678', 'U(1)'))
        finally:
            pg.set_typecast('uuid', cast)

    def test_db_types_global_typecast_for_type_without_typecast(self):
        # a global typecast set for a type without a typecast must be used
        # by the connection even after values of that type have been cast
        query = self.db.query
        query("create type pg_temp.test_mood as enum ('happy')")
        sql = ("select 'happy'::pg_temp.test_mood,"
               " array['happy']::pg_temp.test_mood[]")
        self.assertEqual(query(sql).getresult()[0], ('happy', ['happy']))
        pg.set_typecast('test_mood', str.upper)
        try:
            self.assertEqual(query(sql).getresult()[0], ('HAPPY', ['HAPPY']))
        finally:
            pg.set_typecast('test_mood', None)

    def test_db_types_records_with_invalid_python_names(self):
        # composite types with names that cannot be used for named tuples
        query = self.db.query
        query('create type pg_temp.test_keywords as ("class" int, "_x" int)')
        query('create type pg_temp."test type" as (a int)')
        r = query('select (1, 2)::pg_temp.test_keywords').getresult()[0][0]
        self.assertEqual(r, (1, 2))
        self.assertEqual(type(r).__name__, 'test_keywords')
        self.assertEqual(r._fields, ('_0', '_1'))
        r = query('select row(1)::pg_temp."test type"').getresult()[0][0]
        self.assertEqual(r, (1,))
        self.assertEqual(type(r).__name__, 'Record')
        self.assertEqual(r.a, 1)

    def test_db_types_typecast_with_simple_type(self):
        # type objects without OID are looked up by name
        dbtypes = self.db.dbtypes
        typ = self.db.adapter.simple_type('int4')
        self.assertIs(dbtypes.get_typecast(typ), int)
        self.assertEqual(dbtypes.typecast('42', typ), 42)

    def test_db_types_array_of_records(self):
        # the element type of arrays of anonymous records is a pseudo type
        r = self.db.query("select array[(1, 'a'), (2, 'b')]").getresult()
        self.assertEqual(r, [([('1', 'a'), ('2', 'b')],)])

    def test_db_types_global_typecast_for_array_name(self):
        # a global typecast set for the name of an array type is used
        query = self.db.query
        query("create type pg_temp.test_mood as enum ('happy')")
        pg.set_typecast('_test_mood', str.upper)
        try:
            r = query("select array['happy']::pg_temp.test_mood[]")
            self.assertEqual(r.getresult()[0][0], '{HAPPY}')
        finally:
            pg.set_typecast('_test_mood', None)

    def test_db_types_after_reopen_with_existing_connection(self):
        # the cast hook must be set again when reopening a DB object
        # that wraps an existing connection after closing it
        cnx = pg.connect(dbname, dbhost, dbport, user=dbuser, passwd=dbpasswd)
        try:
            db = pg.DB(cnx)
            db.close()
            db.reopen()
            self.assertEqual(cnx.get_cast_hook(), db.dbtypes.typecast)
            db.query('create type pg_temp.test_reopen as (a int)')
            r = db.query('select row(1)::pg_temp.test_reopen').getresult()
            self.assertEqual(r, [((1,),)])
        finally:
            cnx.close()

    def test_db_types_array_with_other_delimiter(self):
        # the elements of box arrays are separated by semicolons
        r = self.db.query("select array['(1,1),(0,0)'::box,"
                          " '(2,2),(1,1)'::box]").getresult()[0][0]
        self.assertEqual(r, ['(1,1),(0,0)', '(2,2),(1,1)'])

    def test_db_types_after_altering_composite_type(self):
        # typecasts for composite types must use the current fields
        # after flushing the attnames cache or resetting the typecasts
        query = self.db.query
        query('create type pg_temp.test_alter as (a int)')
        sql = 'select row(1, 2)::pg_temp.test_alter'
        query('alter type pg_temp.test_alter add attribute b int')
        self.assertEqual(query(sql).getresult()[0][0], (1, 2))
        query('alter type pg_temp.test_alter add attribute c int')
        self.db.get_attnames('pg_temp.test_alter', flush=True)
        sql = 'select row(1, 2, 3)::pg_temp.test_alter'
        r = query(sql).getresult()[0][0]
        self.assertEqual(r, (1, 2, 3))
        self.assertEqual(r._fields, ('a', 'b', 'c'))

    def test_db_types_after_reopen(self):
        # typecasts bound to the connection must use the new connection
        query = self.db.query
        sql = "select row(date '2020-01-02')::pg_temp.test_reopen"
        query('create type pg_temp.test_reopen as (d date)')
        self.assertEqual(query(sql).getresult()[0][0], (date(2020, 1, 2),))
        self.db.reopen()
        query('create type pg_temp.test_reopen as (d date)')
        self.assertEqual(query(sql).getresult()[0][0], (date(2020, 1, 2),))

    def test_db_types_typecast_with_qualified_names(self):
        # names are taken like the parts of table names
        dbtypes = self.db.dbtypes
        self.assertIs(dbtypes.get_typecast('pg_catalog.int4'), int)
        self.assertIs(dbtypes.get_typecast('pg_catalog."int4"'), int)
        self.assertEqual(dbtypes.typecast('42', 'pg_catalog.int4'), 42)
        self.db.query('create domain pg_temp."pg_catalog.bool" as int')
        cast = dbtypes.get_typecast('"pg_catalog.bool"')
        self.assertIs(cast, int)
        self.assertEqual(dbtypes.typecast('42', '"pg_catalog.bool"'), 42)

    def test_db_types_with_underscore_names(self):
        # only array types are cast as arrays, not types whose names only
        # start with an underscore, and the element types of arrays are
        # not derived from the names of the array types
        query = self.db.query

        def get(what):
            return query(f'select {what}').getresult()[0]

        query('create type pg_temp._q as (u int)')
        query('create type pg_temp.q as (v int)')  # array type is _q_1
        self.assertEqual(get('row(1)::pg_temp._q')[0], (1,))
        r = get('array[row(2)]::pg_temp.q[], array[row(3)]::pg_temp._q[]')
        self.assertEqual(r, ([(2,)], [(3,)]))
        self.assertEqual(r[0][0]._fields, ('v',))
        # these types have element types, but are not cast like arrays
        self.assertEqual(get("'1 2'::oidvector, 'ab'::name, '(1,2)'::point"),
                         ('1 2', 'ab', '(1,2)'))

    def test_db_types_typecasts_for_underscore_names(self):
        # setting or resetting a typecast for a name does not affect the
        # typecast for the name with an underscore, which can be the name
        # of an unrelated type, while arrays use the typecast for elements
        query = self.db.query
        dbtypes = self.db.dbtypes

        def get(what):
            return query(f'select {what}').getresult()[0]

        def cast_q(v):
            return f'q:{v}'

        def cast_uq(v):
            return f'_q:{v}'

        query('create type pg_temp._q as (u int)')
        query('create type pg_temp.q as (v int)')  # array type is _q_1
        sql = 'row(1)::pg_temp._q, array[row(2)]::pg_temp.q[]'
        dbtypes.set_typecast('_q', cast_uq)
        dbtypes.set_typecast('q', cast_q)
        self.assertEqual(get(sql), ('_q:(1)', ['q:(2)']))
        dbtypes.reset_typecast('q')
        self.assertEqual(get(sql), ('_q:(1)', [(2,)]))
        dbtypes.reset_typecast('_q')
        self.assertEqual(get(sql), ((1,), [(2,)]))
        # connections keep their cached copies of global typecasts,
        # so the local typecasts are reset after the global changes
        pg.set_typecast('_q', cast_uq)
        try:
            pg.set_typecast('q', cast_q)
            try:
                dbtypes.reset_typecast()
                self.assertEqual(get(sql), ('_q:(1)', ['q:(2)']))
            finally:
                pg.set_typecast('q', None)
            dbtypes.reset_typecast()
            self.assertEqual(get(sql), ('_q:(1)', [(2,)]))
        finally:
            pg.set_typecast('_q', None)
        dbtypes.reset_typecast()
        self.assertEqual(get(sql), ((1,), [(2,)]))

    def test_db_types_info(self):
        dbtypes = self.db.dbtypes
        self.assertIsInstance(dbtypes, dict)
        self.assertNotIn('numeric', dbtypes)
        typ = dbtypes['numeric']
        self.assertIn('numeric', dbtypes)
        self.assertEqual(typ, 'numeric' if self.regtypes else 'num')
        self.assertEqual(typ.oid, 1700)
        self.assertEqual(typ.pgtype, 'numeric')
        self.assertEqual(typ.regtype, 'numeric')
        self.assertEqual(typ.simple, 'num')
        self.assertEqual(typ.typlen, -1)
        self.assertEqual(typ.typtype, 'b')
        self.assertEqual(typ.category, 'N')
        self.assertEqual(typ.delim, ',')
        self.assertEqual(typ.relid, 0)
        self.assertEqual(typ.elem, 0)
        self.assertIs(dbtypes[1700], typ)
        typ = dbtypes['_numeric']
        self.assertEqual(typ, 'numeric[]' if self.regtypes else 'num[]')
        self.assertEqual(typ.category, 'A')
        self.assertEqual(typ.elem, 1700)
        self.assertNotIn('pg_type', dbtypes)
        typ = dbtypes['pg_type']
        self.assertIn('pg_type', dbtypes)
        self.assertEqual(typ, 'pg_type' if self.regtypes else 'record')
        self.assertIsInstance(typ.oid, int)
        self.assertEqual(typ.pgtype, 'pg_type')
        self.assertEqual(typ.regtype, 'pg_type')
        self.assertEqual(typ.simple, 'record')
        self.assertEqual(typ.typlen, -1)
        self.assertEqual(typ.typtype, 'c')
        self.assertEqual(typ.category, 'C')
        self.assertEqual(typ.delim, ',')
        self.assertNotEqual(typ.relid, 0)
        attnames = typ.attnames
        self.assertIsInstance(attnames, dict)
        # noinspection PyUnresolvedReferences
        self.assertIs(attnames, dbtypes.get_attnames('pg_type'))
        self.assertIn('typname', attnames)
        typname = attnames['typname']
        self.assertEqual(typname, 'name' if self.regtypes else 'text')
        self.assertEqual(typname.typlen, 64)  # base
        self.assertEqual(typname.typtype, 'b')  # base
        self.assertEqual(typname.category, 'S')  # string
        self.assertIn('typlen', attnames)
        typlen = attnames['typlen']
        self.assertEqual(typlen, 'smallint' if self.regtypes else 'int')
        self.assertEqual(typlen.typlen, 2)  # base
        self.assertEqual(typlen.typtype, 'b')  # base
        self.assertEqual(typlen.category, 'N')  # numeric

    # noinspection PyUnresolvedReferences
    def test_db_types_typecast(self):
        dbtypes = self.db.dbtypes
        self.assertIsInstance(dbtypes, dict)
        self.assertNotIn('int4', dbtypes)
        self.assertIs(dbtypes.get_typecast('int4'), int)
        self.assertEqual(dbtypes.typecast('42', 'int4'), 42)
        self.assertEqual(dbtypes.typecast('42', dbtypes['int4']), 42)
        dbtypes.set_typecast('int4', float)
        self.assertIs(dbtypes.get_typecast('int4'), float)
        dbtypes.reset_typecast('int4')
        self.assertIs(dbtypes.get_typecast('int4'), int)
        dbtypes.set_typecast('int4', float)
        self.assertIs(dbtypes.get_typecast('int4'), float)
        dbtypes.reset_typecast()
        self.assertIs(dbtypes.get_typecast('int4'), int)
        self.assertNotIn('circle', dbtypes)
        self.assertIsNone(dbtypes.get_typecast('circle'))
        squared_circle = lambda v: f'Squared Circle: {v}'  # noqa: E731
        dbtypes.set_typecast('circle', squared_circle)
        self.assertIs(dbtypes.get_typecast('circle'), squared_circle)
        r = self.db.query("select '0,0,1'::circle").getresult()[0][0]
        self.assertIn(718, dbtypes)
        self.assertEqual(r, 'Squared Circle: <(0,0),1>')
        self.assertEqual(
            dbtypes.typecast('Impossible', 'circle'),
            'Squared Circle: Impossible')
        r = self.db.query(
            "select array['<(0,0),1>'::circle]").getresult()[0][0]
        if pg.get_array():
            self.assertEqual(r, ['Squared Circle: <(0,0),1>'])
        dbtypes.reset_typecast('circle')
        self.assertIsNone(dbtypes.get_typecast('circle'))
        r = self.db.query(
            "select array['<(0,0),1>'::circle]").getresult()[0][0]
        if pg.get_array():  # array cast must also be reset
            self.assertEqual(r, ['<(0,0),1>'])

    def test_db_types_typecast_for_record_fields(self):
        query = self.db.query
        query('create type pg_temp.test_rtype as (c circle)')
        dbtypes = self.db.dbtypes
        cmd = "select row('<(0,0),1>')::pg_temp.test_rtype"
        self.assertEqual(query(cmd).getresult()[0][0], ('<(0,0),1>',))
        # record casts must pick up changed casts for their fields
        dbtypes.set_typecast('circle', lambda v: f'Circle: {v}')
        self.assertEqual(query(cmd).getresult()[0][0], ('Circle: <(0,0),1>',))
        dbtypes.reset_typecast('circle')
        self.assertEqual(query(cmd).getresult()[0][0], ('<(0,0),1>',))
        # a cast set for the composite type itself is kept
        dbtypes.set_typecast('test_rtype', lambda v: f'Record: {v}')
        dbtypes.set_typecast('circle', lambda v: f'Circle: {v}')
        self.assertEqual(query(cmd).getresult()[0][0], 'Record: ("<(0,0),1>")')
        dbtypes.reset_typecast()
        self.assertEqual(query(cmd).getresult()[0][0], ('<(0,0),1>',))

    def test_db_types_typecast_with_domains(self):
        query = self.db.query
        query('create domain pg_temp.test_dint as int')
        query('create domain pg_temp.test_ddint as pg_temp.test_dint')
        query('create type pg_temp.test_rtype as'
              ' (i pg_temp.test_dint, a pg_temp.test_dint[])')
        dbtypes = self.db.dbtypes
        self.assertEqual(dbtypes['test_dint'].base.oid, 23)
        # domains are cast like their base types
        self.assertIs(dbtypes.get_typecast('test_dint'), int)
        self.assertIs(dbtypes.get_typecast('test_ddint'), int)
        self.assertEqual(dbtypes.typecast('42', 'test_dint'), 42)
        cmd = ("select 1::pg_temp.test_dint, '{2}'::pg_temp.test_dint[],"
               " row(3, '{4}')::pg_temp.test_rtype")
        r = query(cmd).getresult()[0]
        self.assertEqual(r[0], 1)
        self.assertEqual(r[2], (3, [4]))
        if pg.get_array():
            self.assertEqual(r[1], [2])
        # unless a typecast has been set for the domain, which is used
        # for elements of arrays and fields of records, but not for
        # values, since PostgreSQL returns them with the base type
        dbtypes.set_typecast('test_dint', lambda v: f'<{v}>')
        self.assertEqual(dbtypes.typecast('42', 'test_dint'), '<42>')
        self.assertIs(dbtypes.get_typecast('test_ddint'), int)
        r = query(cmd).getresult()[0]
        self.assertEqual(r[0], 1)
        self.assertEqual(r[2][0], '<3>')
        if pg.get_array():
            self.assertEqual(r[1], ['<2>'])
            self.assertEqual(r[2][1], ['<4>'])
        dbtypes.reset_typecast('test_dint')
        self.assertIs(dbtypes.get_typecast('test_dint'), int)
        r = query(cmd).getresult()[0]
        self.assertEqual(r[2][0], 3)
        if pg.get_array():
            self.assertEqual(r[1], [2])

    def test_get_set_type_cast(self):
        get_typecast = pg.get_typecast
        set_typecast = pg.set_typecast
        dbtypes = self.db.dbtypes
        self.assertIsInstance(dbtypes, dict)
        self.assertNotIn('int4', dbtypes)
        self.assertNotIn('real', dbtypes)
        self.assertNotIn('bool', dbtypes)
        self.assertIs(get_typecast('int4'), int)
        self.assertIs(get_typecast('float4'), float)
        from pg.cast import cast_bool
        self.assertIs(get_typecast('bool'), cast_bool)
        cast_circle = get_typecast('circle')
        self.addCleanup(set_typecast, 'circle', cast_circle)
        squared_circle = lambda v: f'Squared Circle: {v}'  # noqa: E731
        self.assertNotIn('circle', dbtypes)
        set_typecast('circle', squared_circle)
        self.assertNotIn('circle', dbtypes)
        self.assertIs(get_typecast('circle'), squared_circle)
        r = self.db.query("select '0,0,1'::circle").getresult()[0][0]
        self.assertIn(718, dbtypes)  # cached by OID, not by bare name
        self.assertNotIn('circle', dbtypes)
        self.assertEqual(r, 'Squared Circle: <(0,0),1>')
        set_typecast('circle', cast_circle)
        self.assertIs(get_typecast('circle'), cast_circle)

    def test_notification_handler(self):
        # the notification handler itself is tested separately
        f = self.db.notification_handler
        callback = lambda arg_dict: None  # noqa: E731
        handler = f('test', callback)
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertIs(handler.db, self.db)
        self.assertEqual(handler.event, 'test')
        self.assertEqual(handler.stop_event, 'stop_test')
        self.assertIs(handler.callback, callback)
        self.assertIsInstance(handler.arg_dict, dict)
        self.assertEqual(handler.arg_dict, {})
        self.assertIsNone(handler.timeout)
        self.assertFalse(handler.listening)
        handler.close()
        self.assertIsNone(handler.db)
        self.db.reopen()
        self.assertIsNone(handler.db)
        handler = f('test2', callback, timeout=2)
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertIs(handler.db, self.db)
        self.assertEqual(handler.event, 'test2')
        self.assertEqual(handler.stop_event, 'stop_test2')
        self.assertIs(handler.callback, callback)
        self.assertIsInstance(handler.arg_dict, dict)
        self.assertEqual(handler.arg_dict, {})
        self.assertEqual(handler.timeout, 2)
        self.assertFalse(handler.listening)
        handler.close()
        self.assertIsNone(handler.db)
        self.db.reopen()
        self.assertIsNone(handler.db)
        arg_dict = {'testing': 3}
        handler = f('test3', callback, arg_dict=arg_dict)
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertIs(handler.db, self.db)
        self.assertEqual(handler.event, 'test3')
        self.assertEqual(handler.stop_event, 'stop_test3')
        self.assertIs(handler.callback, callback)
        self.assertIs(handler.arg_dict, arg_dict)
        self.assertEqual(arg_dict['testing'], 3)
        self.assertIsNone(handler.timeout)
        self.assertFalse(handler.listening)
        handler.close()
        self.assertIsNone(handler.db)
        self.db.reopen()
        self.assertIsNone(handler.db)
        handler = f('test4', callback, stop_event='stop4')
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertIs(handler.db, self.db)
        self.assertEqual(handler.event, 'test4')
        self.assertEqual(handler.stop_event, 'stop4')
        self.assertIs(handler.callback, callback)
        self.assertIsInstance(handler.arg_dict, dict)
        self.assertEqual(handler.arg_dict, {})
        self.assertIsNone(handler.timeout)
        self.assertFalse(handler.listening)
        handler.close()
        self.assertIsNone(handler.db)
        self.db.reopen()
        self.assertIsNone(handler.db)
        arg_dict = {'testing': 5}
        handler = f('test5', callback, arg_dict, 1.5, 'stop5')
        self.assertIsInstance(handler, pg.NotificationHandler)
        self.assertIs(handler.db, self.db)
        self.assertEqual(handler.event, 'test5')
        self.assertEqual(handler.stop_event, 'stop5')
        self.assertIs(handler.callback, callback)
        self.assertIs(handler.arg_dict, arg_dict)
        self.assertEqual(arg_dict['testing'], 5)
        self.assertEqual(handler.timeout, 1.5)
        self.assertFalse(handler.listening)
        handler.close()
        self.assertIsNone(handler.db)
        self.db.reopen()
        self.assertIsNone(handler.db)

    def test_inserttable_from_query(self):
        # use inserttable() to copy from one table to another
        query = self.db.query
        self.create_table('test_table_from', 'n integer, t timestamp')
        self.create_table('test_table_to', 'n integer, t timestamp')
        for i in range(1, 4):
            query("insert into test_table_from values ($1, now())", i)
        n = self.db.inserttable(
            'test_table_to', query("select n, t::text from test_table_from"))
        data_from = query("select * from test_table_from").getresult()
        data_to = query("select * from test_table_to").getresult()
        self.assertEqual(n, 3)
        self.assertEqual([row[0] for row in data_from], [1, 2, 3])
        self.assertEqual(data_from, data_to)

    def test_inserttable_with_freeze(self):
        # use inserttable() with freeze and table created in same transaction
        query = self.db.query
        values = [(i,) for i in range(1, 4)]
        self.db.begin()
        self.create_table('test_table_freeze', 'n integer')
        self.db.inserttable('test_table_freeze', values, freeze=True)
        self.db.commit()
        r = query("select * from test_table_freeze").getresult()
        self.assertEqual(r, values)

    def test_inserttable_with_keywords(self):
        values = [(i,) for i in range(1, 4)]
        self.create_table('test_table_keywords', 'n integer')
        inserttable = self.db.inserttable
        self.assertEqual(inserttable(
            table='test_table_keywords', values=values, columns=['n']), 3)
        r = self.db.query("select * from test_table_keywords").getresult()
        self.assertEqual(r, values)
        # freeze is a keyword-only parameter like in the connection
        self.assertRaises(TypeError, inserttable,
                          'test_table_keywords', values, None, True)

    def test_inserttable_with_freeze_no_transaction(self):
        # use inserttable() with freeze and table created before transaction
        values = [(i,) for i in range(1, 4)]
        self.create_table('test_table_freeze', 'n integer')
        self.assertRaises(ValueError, self.db.inserttable,
                          'test_table_freeze', values, freeze=True)


class TestDBClassNonStdOpts(TestDBClass):
    """Test the methods of the DB class with non-standard global options."""

    saved_options: ClassVar[dict[str, Any]] = {}

    @classmethod
    def setUpClass(cls):
        cls.saved_options.clear()
        cls.set_option('decimal', float)
        not_bool = not pg.get_bool()
        cls.set_option('bool', not_bool)
        not_array = not pg.get_array()
        cls.set_option('array', not_array)
        not_bytea_escaped = not pg.get_bytea_escaped()
        cls.set_option('bytea_escaped', not_bytea_escaped)
        cls.set_option('jsondecode', None)
        db = DB()
        cls.regtypes = not db.use_regtypes()
        db.close()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.reset_option('jsondecode')
        cls.reset_option('bool')
        cls.reset_option('array')
        cls.reset_option('bytea_escaped')
        cls.reset_option('decimal')

    @classmethod
    def set_option(cls, option, value):
        # noinspection PyUnresolvedReferences
        cls.saved_options[option] = getattr(pg, 'get_' + option)()
        return getattr(pg, 'set_' + option)(value)

    @classmethod
    def reset_option(cls, option):
        # noinspection PyUnresolvedReferences
        return getattr(pg, 'set_' + option)(cls.saved_options[option])


class TestDBClassAdapter(unittest.TestCase):
    """Test the adapter object associated with the DB class."""

    def setUp(self):
        self.db = DB()
        self.adapter = self.db.adapter

    def tearDown(self):
        with suppress(pg.InternalError):
            self.db.close()

    def test_guess_simple_type(self):
        f = self.adapter.guess_simple_type
        self.assertEqual(f(pg.Bytea(b'test')), 'bytea')
        self.assertEqual(f(b'test'), 'bytea')
        self.assertEqual(f('string'), 'text')
        self.assertEqual(f([b'a', b'b']), 'bytea[]')
        self.assertEqual(f([[[b'abc']]]), 'bytea[]')
        self.assertEqual(f(True), 'bool')
        self.assertEqual(f(3), 'int')
        self.assertEqual(f(2.75), 'float')
        self.assertEqual(f(Decimal('4.25')), 'num')
        self.assertEqual(f(date(2016, 1, 30)), 'date')
        self.assertEqual(f([1, 2, 3]), 'int[]')
        self.assertEqual(f([[[123]]]), 'int[]')
        self.assertEqual(f(['a', 'b', 'c']), 'text[]')
        self.assertEqual(f([[['abc']]]), 'text[]')
        self.assertEqual(f([False, True]), 'bool[]')
        self.assertEqual(f([[[False]]]), 'bool[]')
        self.assertEqual(f([pg.Json([1])]), 'json[]')
        self.assertEqual(f([pg.Hstore({'k': 'v'})]), 'hstore[]')
        self.assertEqual(f([UUID(int=1)]), 'uuid[]')
        self.assertEqual(f([(1, 'a')]), 'record[]')
        r = f(('string', True, 3, 2.75, [1], [False], b'bytes'))
        self.assertEqual(r, 'record')
        self.assertEqual(list(r.attnames.values()), [
            'text', 'bool', 'int', 'float', 'int[]', 'bool[]', 'bytea'])

    def test_guess_simple_type_of_bytes_subclass(self):
        class MyBytes(bytes):
            pass

        f = self.adapter.guess_simple_type
        self.assertEqual(f(MyBytes(b'test')), 'bytea')

    def test_adapt_query_typed_list(self):
        format_query = self.adapter.format_query
        self.assertRaises(TypeError, format_query, '%s,%s', (1, 2), ('int2',))
        self.assertRaises(
            TypeError, format_query, '%s,%s', (1,), ('int2', 'int2'))
        values: list | tuple = (3, 7.5, 'hello', True)
        types: list | tuple = ('int4', 'float4', 'text', 'bool')
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [3, 7.5, 'hello', 't'])
        types = ('bool', 'bool', 'bool', 'bool')
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, ['t', 't', 'f', 't'])
        values = ('2016-01-30', 'current_date')
        types = ('date', 'date')
        sql, params = format_query("values(%s,%s)", values, types)
        self.assertEqual(sql, 'values($1,current_date)')
        self.assertEqual(params, ['2016-01-30'])
        values = ([1, 2, 3], ['a', 'b', 'c'])
        types = ('_int4', '_text')
        sql, params = format_query("%s::int4[],%s::text[]", values, types)
        self.assertEqual(sql, '$1::int4[],$2::text[]')
        self.assertEqual(params, ['{1,2,3}', '{a,b,c}'])
        types = ('_bool', '_bool')
        sql, params = format_query("%s::bool[],%s::bool[]", values, types)
        self.assertEqual(sql, '$1::bool[],$2::bool[]')
        self.assertEqual(params, ['{t,t,t}', '{f,f,f}'])
        values = [(3, 7.5, 'hello', True, [123], ['abc'])]
        t = self.adapter.simple_type
        typ = t('record')
        from pg.attrs import AttrDict
        typ._get_attnames = lambda _self: AttrDict(
            i=t('int'), f=t('float'),
            t=t('text'), b=t('bool'),
            i3=t('int[]'), t3=t('text[]'))
        types = [typ]
        sql, params = format_query('select %s', values, types)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(3,7.5,hello,t,{123},{abc})'])
        values = [(0, -3.25, '', False, [0], [''])]
        sql, params = format_query('select %s', values, types)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(0,-3.25,"",f,{0},"{\\"\\"}")'])

    def test_adapt_query_typed_list_with_python_types(self):
        format_query = self.adapter.format_query
        values: tuple = (3, 7.5, 'hello', True)
        types: list = [int, float, str, bool]
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [3, 7.5, 'hello', 't'])
        values = ([1, 2, 3], ['a', 'b', 'c'], [True, False])
        # builtin generic types
        types = [list[int], list[str], list[bool]]
        sql, params = format_query("select %s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3')
        self.assertEqual(params, ['{1,2,3}', '{a,b,c}', '{t,f}'])
        # legacy generic types from the typing module
        types = [List[int], List[str], List[bool]]  # noqa: UP006
        sql, params = format_query("select %s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3')
        self.assertEqual(params, ['{1,2,3}', '{a,b,c}', '{t,f}'])

    def test_adapt_query_typed_list_with_types_as_string(self):
        format_query = self.adapter.format_query
        self.assertRaises(TypeError, format_query, '%s,%s', (1, 2), 'int2')
        self.assertRaises(
            TypeError, format_query, '%s,%s', (1,), 'int2 int2')
        values = (3, 7.5, 'hello', True)
        types = 'int4 float4 text bool'  # pass types as string
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [3, 7.5, 'hello', 't'])

    def test_adapt_query_typed_list_with_types_as_classes(self):
        format_query = self.adapter.format_query
        self.assertRaises(TypeError, format_query, '%s,%s', (1, 2), (int,))
        self.assertRaises(
            TypeError, format_query, '%s,%s', (1,), (int, int))
        values = (3, 7.5, 'hello', True)
        types = (int, float, str, bool)  # pass types as classes
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [3, 7.5, 'hello', 't'])

    def test_adapt_query_typed_list_with_json(self):
        format_query = self.adapter.format_query
        value: Any = {'test': [1, "it's fine", 3]}
        sql, params = format_query("select %s", (value,), 'json')
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"test": [1, "it\'s fine", 3]}'])
        value = pg.Json({'test': [1, "it's fine", 3]})
        sql, params = format_query("select %s", (value,), 'json')
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"test": [1, "it\'s fine", 3]}'])
        value = {'test': [1, "it's fine", 3]}
        sql, params = format_query("select %s", [value], [pg.Json])
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"test": [1, "it\'s fine", 3]}'])

    def test_adapt_query_typed_list_with_empty_json(self):
        format_query = self.adapter.format_query
        values: Any = [None, 0, False, '', [], {}]
        types = ('json',) * 6
        sql, params = format_query("select %s,%s,%s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4,$5,$6')
        self.assertEqual(params, [None, '0', 'false', '', '[]', '{}'])

    def test_adapt_query_typed_with_hstore(self):
        format_query = self.adapter.format_query
        value: Any = {'one': "it's fine", 'two': 2}
        sql, params = format_query("select %s", (value,), 'hstore')
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['one=>"it\'s fine\",two=>2'])
        value = pg.Hstore({'one': "it's fine", 'two': 2})
        sql, params = format_query("select %s", (value,), 'hstore')
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['one=>"it\'s fine\",two=>2'])
        value = pg.Hstore({'one': "it's fine", 'two': 2})
        sql, params = format_query("select %s", [value], [pg.Hstore])
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['one=>"it\'s fine\",two=>2'])

    def test_adapt_query_typed_with_empty_hstore(self):
        format_query = self.adapter.format_query
        values: Any = [None, {}, pg.Hstore(), '']
        types = ('hstore',) * 4
        sql, params = format_query("select %s,%s,%s,%s", values, types)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [None, '', '', ''])

    def test_adapt_query_typed_with_uuid(self):
        format_query = self.adapter.format_query
        value: Any = '12345678-1234-5678-1234-567812345678'
        sql, params = format_query("select %s", (value,), 'uuid')
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['12345678-1234-5678-1234-567812345678'])
        value = UUID('{12345678-1234-5678-1234-567812345678}')
        sql, params = format_query("select %s", (value,), 'uuid')
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['12345678-1234-5678-1234-567812345678'])
        value = UUID('{12345678-1234-5678-1234-567812345678}')
        sql, params = format_query("select %s", (value,))
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['12345678-1234-5678-1234-567812345678'])

    def test_adapt_query_untyped_with_network(self):
        format_query = self.adapter.format_query
        a = IPv4Address('192.168.0.1')
        n = IPv6Network('2001:db8::/32')
        sql, params = format_query("select %s, %s, %s", (a, n, [a, n, None]))
        self.assertEqual(sql, "select $1, $2, $3")
        self.assertEqual(params, [a, n, '{192.168.0.1,2001:db8::/32,null}'])

    def test_adapt_query_typed_with_network(self):
        format_query = self.adapter.format_query
        a = IPv4Interface('192.168.0.1/24')
        n = IPv4Network('192.168.0.0/24')
        sql, params = format_query(
            "select %s, %s, %s, %s", (a, n, [a, None], [[n]]),
            ('inet', 'cidr', 'inet[]', '_cidr'))
        self.assertEqual(sql, "select $1, $2, $3, $4")
        self.assertEqual(params, [
            a, n, '{192.168.0.1/24,null}', '{{192.168.0.0/24}}'])

    def test_adapt_query_typed_with_array_type_names(self):
        format_query = self.adapter.format_query
        values: Any = [[[1, 2], [3, None]], ['a', 'b c'], [True, None]]
        sql, params = format_query(
            "select %s,%s,%s", values, ('int[]', 'text[]', 'bool[][]'))
        self.assertEqual(sql, 'select $1,$2,$3')
        self.assertEqual(params, ['{{1,2},{3,null}}', '{a,"b c"}', '{t,null}'])
        sql, params = format_query(
            "select %s", ([{'k': 'v'}],), ('hstore[]',))
        self.assertEqual(params, ['{k=>v}'])
        sql, params = format_query("select %s", ([(1, 'a')],), ('record[]',))
        self.assertEqual(params, ['{"(1,a)"}'])

    def test_adapt_query_typed_with_json_array(self):
        format_query = self.adapter.format_query
        value: Any = [pg.Json({'k': 'v'}), {}, 0, False, None, '']
        sql, params = format_query("select %s", (value,), ('_json',))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, [
            '{"{\\"k\\": \\"v\\"}","{}",0,false,null,null}'])

    def test_adapt_query_typed_with_hstore_array(self):
        format_query = self.adapter.format_query
        value: Any = [pg.Hstore({'k': 'a b'}), {'x': None}, {}, None]
        sql, params = format_query("select %s", (value,), ('_hstore',))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"k=>\\"a b\\"",x=>NULL,"",null}'])

    def test_adapt_query_typed_with_uuid_array(self):
        format_query = self.adapter.format_query
        value: Any = [
            UUID(int=1), None, '00000000-0000-0000-0000-000000000002']
        sql, params = format_query("select %s", (value,), ('_uuid',))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, [
            '{00000000-0000-0000-0000-000000000001,null,'
            '00000000-0000-0000-0000-000000000002}'])

    def test_adapt_query_typed_dict(self):
        format_query = self.adapter.format_query
        self.assertRaises(
            TypeError, format_query,
            '%s,%s', dict(i1=1, i2=2), dict(i1='int2'))
        values: dict = dict(i=3, f=7.5, t='hello', b=True)
        types: dict = dict(i='int4', f='float4', t='text', b='bool')
        sql, params = format_query(
            "select %(i)s,%(f)s,%(t)s,%(b)s", values, types)
        self.assertEqual(sql, 'select $3,$2,$4,$1')
        self.assertEqual(params, ['t', 7.5, 3, 'hello'])
        types = dict(i='bool', f='bool', t='bool', b='bool')
        sql, params = format_query(
            "select %(i)s,%(f)s,%(t)s,%(b)s", values, types)
        self.assertEqual(sql, 'select $3,$2,$4,$1')
        self.assertEqual(params, ['t', 't', 't', 'f'])
        values = dict(d1='2016-01-30', d2='current_date')
        types = dict(d1='date', d2='date')
        sql, params = format_query("values(%(d1)s,%(d2)s)", values, types)
        self.assertEqual(sql, 'values($1,current_date)')
        self.assertEqual(params, ['2016-01-30'])
        values = dict(i=[1, 2, 3], t=['a', 'b', 'c'])
        types = dict(i='_int4', t='_text')
        sql, params = format_query(
            "%(i)s::int4[],%(t)s::text[]", values, types)
        self.assertEqual(sql, '$1::int4[],$2::text[]')
        self.assertEqual(params, ['{1,2,3}', '{a,b,c}'])
        types = dict(i='_bool', t='_bool')
        sql, params = format_query(
            "%(i)s::bool[],%(t)s::bool[]", values, types)
        self.assertEqual(sql, '$1::bool[],$2::bool[]')
        self.assertEqual(params, ['{t,t,t}', '{f,f,f}'])
        values = dict(record=(3, 7.5, 'hello', True, [123], ['abc']))
        t = self.adapter.simple_type
        typ = t('record')
        from pg.attrs import AttrDict
        typ._get_attnames = lambda _self: AttrDict(
            i=t('int'), f=t('float'),
            t=t('text'), b=t('bool'),
            i3=t('int[]'), t3=t('text[]'))
        types = dict(record=typ)
        sql, params = format_query('select %(record)s', values, types)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(3,7.5,hello,t,{123},{abc})'])
        values = dict(record=(0, -3.25, '', False, [0], ['']))
        sql, params = format_query('select %(record)s', values, types)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(0,-3.25,"",f,{0},"{\\"\\"}")'])

    def test_adapt_query_typed_anonymous_record(self):
        adapter = self.adapter
        dbtypes = self.db.dbtypes
        get_simple_name = adapter.get_simple_name
        self.assertEqual(get_simple_name('record'), 'record')
        self.assertEqual(get_simple_name('_record'), 'record[]')
        self.assertEqual(get_simple_name('record[]'), 'record[]')
        self.assertEqual(dbtypes['record'].simple, 'record')
        self.assertEqual(dbtypes['_record'].simple, 'record[]')
        format_query = adapter.format_query
        value = (1, 'a b', None, [1, 2], True)
        literal = '(1,a b,,"{1,2}",t)'
        # the types of the fields are guessed like for untyped tuples
        self.assertEqual(adapter.adapt(value), literal)
        for typ in 'record', dbtypes['record']:
            sql, params = format_query('select %s', (value,), (typ,))
            self.assertEqual(sql, 'select $1')
            self.assertEqual(params, [literal])
        values = [(1, 'a b'), None, ('x',)]
        literal = '{"(1,a b)",null,(x)}'
        for typ in '_record', 'record[]', dbtypes['_record']:
            sql, params = format_query('select %s', (values,), (typ,))
            self.assertEqual(sql, 'select $1')
            self.assertEqual(params, [literal])
        query = self.db.query
        query('create temporary table test_anonymous_record'
              ' (i int, t text)')
        q = self.db.query_formatted(
            'select %s::test_anonymous_record,'
            ' %s::test_anonymous_record[]',
            ((1, 'a b'), [(2, 'c'), (3, None)]), 'record _record')
        r = q.getresult()[0]
        self.assertEqual(r[0], (1, 'a b'))
        self.assertEqual(r[1], [(2, 'c'), (3, None)])

    def test_adapt_query_untyped_list(self):
        format_query = self.adapter.format_query
        values: list | tuple = (3, 7.5, 'hello', True)
        sql, params = format_query("select %s,%s,%s,%s", values)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [3, 7.5, 'hello', 't'])
        values = [date(2016, 1, 30), 'current_date']
        sql, params = format_query("values(%s,%s)", values)
        self.assertEqual(sql, 'values($1,$2)')
        self.assertEqual(params, values)
        values = ([1, 2, 3], ['a', 'b', 'c'], [True, False, True])
        sql, params = format_query("%s,%s,%s", values)
        self.assertEqual(sql, "$1,$2,$3")
        self.assertEqual(params, ['{1,2,3}', '{a,b,c}', '{t,f,t}'])
        values = ([[1, 2], [3, 4]], [['a', 'b'], ['c', 'd']],
                  [[True, False], [False, True]])
        sql, params = format_query("%s,%s,%s", values)
        self.assertEqual(sql, "$1,$2,$3")
        self.assertEqual(params, [
            '{{1,2},{3,4}}', '{{a,b},{c,d}}', '{{t,f},{f,t}}'])
        values = [(3, 7.5, 'hello', True, [123], ['abc'])]
        sql, params = format_query('select %s', values)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(3,7.5,hello,t,{123},{abc})'])
        values = [(0, -3.25, '', False, [0], [''])]
        sql, params = format_query('select %s', values)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(0,-3.25,"",f,{0},"{\\"\\"}")'])

    def test_adapt_query_untyped_list_with_json(self):
        format_query = self.adapter.format_query
        value = pg.Json({'test': [1, "it's fine", 3]})
        sql, params = format_query("select %s", (value,))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"test": [1, "it\'s fine", 3]}'])

    def test_adapt_query_untyped_with_hstore(self):
        format_query = self.adapter.format_query
        value = pg.Hstore({'one': "it's fine", 'two': 2})
        sql, params = format_query("select %s", (value,))
        self.assertEqual(sql, "select $1")
        self.assertEqual(params, ['one=>"it\'s fine\",two=>2'])

    def test_adapt_query_untyped_with_plain_dict(self):
        format_query = self.adapter.format_query
        value = {'one': 1}
        for inline in False, True:
            for values in ((value,), ([value],), ((1, value),),
                           ([(1, value)],)):
                with self.assertRaises(pg.InterfaceError) as cm:
                    format_query("select %s", values, inline=inline)
                msg = str(cm.exception)
                self.assertIn("type <class 'dict'>", msg)
                self.assertIn('wrap dicts with Json() or Hstore()', msg)
        self.assertRaises(
            pg.InterfaceError, self.adapter.guess_simple_type, value)
        self.assertRaises(pg.InterfaceError, self.adapter.adapt, value)
        # with a type, plain dicts can be passed
        sql, params = format_query("select %s", (value,), ('json',))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['{"one": 1}'])
        sql, params = format_query("select %s", (value,), ('hstore',))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['one=>1'])

    def test_adapt_query_untyped_with_pg_str_dict(self):
        class MyDict(dict):
            def __pg_str__(self, typ: Any) -> str:
                return 'my dict'

        format_query = self.adapter.format_query
        sql, params = format_query("select %s", (MyDict(one=1),))
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['my dict'])

    def test_adapt_query_untyped_with_arrays(self):
        format_query = self.adapter.format_query
        values: Any = [
            [pg.Json({'k': 'v'}), None], [pg.Hstore({'k': 'v'}), None],
            [UUID(int=1), None], [(1, 'a b', None), (2, '"', [3, 4])]]
        sql, params = format_query("select %s,%s,%s,%s", values)
        self.assertEqual(sql, 'select $1,$2,$3,$4')
        self.assertEqual(params, [
            '{"{\\"k\\": \\"v\\"}",null}', '{k=>v,null}',
            '{00000000-0000-0000-0000-000000000001,null}',
            '{"(1,a b,)","(2,\\"\\\\\\"\\",\\"{3,4}\\")"}'])

    def test_adapt_query_typed_with_raw_bytes(self):
        format_query = self.adapter.format_query
        values: Any = [b'raw'] * 8
        types = ('text', 'int', 'bool', 'date', 'json', 'hstore', 'uuid',
                 'int[]')
        sql, params = format_query(
            ','.join(['%s'] * 8), values, types)
        self.assertEqual(sql, ','.join(f'${n}' for n in range(1, 9)))
        self.assertEqual(params, values)  # raw text is passed unchanged
        self.assertFalse(any(isinstance(p, pg.Bytea) for p in params))
        sql, params = format_query('%s', (b'raw',), ('bytea',))
        self.assertEqual(params, [b'raw'])  # binary data is sent as binary
        self.assertIsInstance(params[0], pg.Bytea)
        sql, params = format_query('%s', ([b'raw', b'a b', b'', None, 'x'],),
                                   ('text[]',))
        self.assertEqual(params, [b'{raw,"a b","",null,x}'])
        sql, params = format_query('%s', ([b'1', 2],), ('int[]',))
        self.assertEqual(params, [b'{1,2}'])
        sql, params = format_query('%s', ([[b'f', True]],), ('bool[]',))
        self.assertEqual(params, [b'{{f,t}}'])
        sql, params = format_query('%s', ([b'"\\', 'x'],), ('text[]',))
        self.assertEqual(params, [b'{"\\"\\\\",x}'])
        sql, params = format_query(
            '%s', ({b'k': b'a b', 'x': None, b'n': b'"'},), ('hstore',))
        self.assertEqual(params, [b'k=>"a b",x=>NULL,n=>\\"'])
        sql, params = format_query(
            '%s', ([{b'k': b'v'}, {'x': 'y'}],), ('hstore[]',))
        self.assertEqual(params, [b'{k=>v,x=>y}'])
        sql, params = format_query(
            '%s', ([pg.Hstore({b'k': b'a,b'})],), ('hstore[]',))
        self.assertEqual(params, [b'{"k=>\\"a,b\\""}'])

    def test_adapt_query_with_bytea_params(self):
        format_query = self.adapter.format_query
        data = b'\0\\\xff'
        # binary data for bytea parameters is passed as Bytea
        for types in (None, ('bytea',)):
            sql, params = format_query('%s', (data,), types)
            self.assertEqual(sql, '$1')
            self.assertEqual(params, [data])
            self.assertIsInstance(params[0], pg.Bytea)
        value = pg.Bytea(data)
        sql, params = format_query('%s', (value,))
        self.assertIs(params[0], value)
        # Bytea marks binary data also for other types
        sql, params = format_query('%s', (value,), ('text',))
        self.assertIs(params[0], value)
        # text for bytea parameters is still escaped
        sql, params = format_query('%s', ('ab',), ('bytea',))
        self.assertEqual(params, ['\\x6162'])
        # nested binary data is adapted as text
        sql, params = format_query('%s', ((1, data),))
        self.assertEqual(params, [b'(1,"\\\\x005cff")'])
        self.assertNotIsInstance(params[0], pg.Bytea)
        sql, params = format_query('%s', (data,), inline=True)
        self.assertEqual(sql, "'\\x005cff'::bytea")
        self.assertEqual(params, [])

    def test_adapt_query_typed_hstore_with_raw_bytes_as_text(self):
        format_query = self.adapter.format_query
        h = pg.Hstore({b'k\xfc': b'v', 'x': 'y z'})
        # text parameters are passed on and converted by the C extension
        params = format_query('%s', (h,), ('text',))[1]
        self.assertEqual(params, [h])
        # the raw bytes were converted with backslash escapes before
        params = format_query('%s', ([h, 'a'],), ('text[]',))[1]
        self.assertEqual(params, [b'{"k\xfc=>v,x=>\\"y z\\"",a}'])
        params = format_query('%s', ([h],), ('record[]',))[1]
        self.assertEqual(params, [b'{"k\xfc=>v,x=>\\"y z\\""}'])

    def test_adapt_query_inline_with_raw_bytes(self):
        format_query = self.adapter.format_query
        h = pg.Hstore({b'k': b'a b', 'x': "it's"})
        sql, params = format_query('select %s, %s', (h, 1), inline=True)
        self.assertEqual(
            sql, b'select \'k=>"a b",x=>it\'\'s\'::hstore, 1')
        self.assertEqual(params, [])
        sql, params = format_query(
            'select %(h)s, %(x)s', {'h': [h], 'x': 'y'}, inline=True)
        self.assertEqual(
            sql, b'select ARRAY[\'k=>"a b",x=>it\'\'s\'::hstore], \'y\'')
        self.assertEqual(str(h), 'k=>"a b",x=>it\'s')
        h = pg.Hstore({b'\xfc': b'\xfc'})
        self.assertEqual(str(h), '\\xfc=>\\xfc')

    def test_adapt_query_untyped_with_bytes(self):
        format_query = self.adapter.format_query
        value = b"it's \\ \x00\xff"
        sql, params = format_query("select %s, %s", (value, [b'a', b'']))
        self.assertEqual(sql, "select $1, $2")
        # binary data is sent as binary, arrays of it as escaped text
        self.assertEqual(params, [value, b'{\\\\x61,\\\\x}'])
        self.assertIsInstance(params[0], pg.Bytea)
        self.assertNotIsInstance(params[1], pg.Bytea)

    def test_adapt_query_untyped_dict(self):
        format_query = self.adapter.format_query
        values: dict = dict(i=3, f=7.5, t='hello', b=True)
        sql, params = format_query(
            "select %(i)s,%(f)s,%(t)s,%(b)s", values)
        self.assertEqual(sql, 'select $3,$2,$4,$1')
        self.assertEqual(params, ['t', 7.5, 3, 'hello'])
        values = dict(d1='2016-01-30', d2='current_date')
        sql, params = format_query("values(%(d1)s,%(d2)s)", values)
        self.assertEqual(sql, 'values($1,$2)')
        self.assertEqual(params, [values['d1'], values['d2']])
        values = dict(i=[1, 2, 3], t=['a', 'b', 'c'], b=[True, False, True])
        sql, params = format_query("%(i)s,%(t)s,%(b)s", values)
        self.assertEqual(sql, "$2,$3,$1")
        self.assertEqual(params, ['{t,f,t}', '{1,2,3}', '{a,b,c}'])
        values = dict(
            i=[[1, 2], [3, 4]], t=[['a', 'b'], ['c', 'd']],
            b=[[True, False], [False, True]])
        sql, params = format_query("%(i)s,%(t)s,%(b)s", values)
        self.assertEqual(sql, "$2,$3,$1")
        self.assertEqual(params, [
            '{{t,f},{f,t}}', '{{1,2},{3,4}}', '{{a,b},{c,d}}'])
        values = dict(record=(3, 7.5, 'hello', True, [123], ['abc']))
        sql, params = format_query('select %(record)s', values)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(3,7.5,hello,t,{123},{abc})'])
        values = dict(record=(0, -3.25, '', False, [0], ['']))
        sql, params = format_query('select %(record)s', values)
        self.assertEqual(sql, 'select $1')
        self.assertEqual(params, ['(0,-3.25,"",f,{0},"{\\"\\"}")'])

    def test_adapt_query_inline_list(self):
        format_query = self.adapter.format_query
        values: list | tuple = (3, 7.5, 'hello', True)
        sql, params = format_query("select %s,%s,%s,%s", values, inline=True)
        self.assertEqual(sql, "select 3,7.5,'hello',true")
        self.assertEqual(params, [])
        values = [date(2016, 1, 30), 'current_date']
        sql, params = format_query("values(%s,%s)", values, inline=True)
        self.assertEqual(sql, "values('2016-01-30','current_date')")
        self.assertEqual(params, [])
        values = ([1, 2, 3], ['a', 'b', 'c'], [True, False, True])
        sql, params = format_query("%s,%s,%s", values, inline=True)
        self.assertEqual(
            sql, "ARRAY[1,2,3],ARRAY['a','b','c'],ARRAY[true,false,true]")
        self.assertEqual(params, [])
        values = ([[1, 2], [3, 4]], [['a', 'b'], ['c', 'd']],
                  [[True, False], [False, True]])
        sql, params = format_query("%s,%s,%s", values, inline=True)
        self.assertEqual(
            sql, "ARRAY[[1,2],[3,4]],ARRAY[['a','b'],['c','d']],"
                 "ARRAY[[true,false],[false,true]]")
        self.assertEqual(params, [])
        values = [(3, 7.5, 'hello', True, [123], ['abc'])]
        sql, params = format_query('select %s', values, inline=True)
        self.assertEqual(
            sql, "select (3,7.5,'hello',true,ARRAY[123],ARRAY['abc'])")
        self.assertEqual(params, [])
        values = [(0, -3.25, '', False, [0], [''])]
        sql, params = format_query('select %s', values, inline=True)
        self.assertEqual(
            sql, "select (0,-3.25,'',false,ARRAY[0],ARRAY[''])")
        self.assertEqual(params, [])

    def test_adapt_query_inline_list_with_json(self):
        format_query = self.adapter.format_query
        value = pg.Json({'test': [1, "it's fine", 3]})
        sql, params = format_query("select %s", (value,), inline=True)
        self.assertEqual(
            sql, "select '{\"test\": [1, \"it''s fine\", 3]}'::json")
        self.assertEqual(params, [])

    def test_adapt_query_inline_list_with_hstore(self):
        format_query = self.adapter.format_query
        value = pg.Hstore({'one': "it's fine", 'two': 2})
        sql, params = format_query("select %s", (value,), inline=True)
        self.assertEqual(
            sql, "select 'one=>\"it''s fine\",two=>2'::hstore")
        self.assertEqual(params, [])

    def test_adapt_query_inline_list_with_uuid(self):
        format_query = self.adapter.format_query
        value = UUID(int=1)
        sql, params = format_query(
            "select %s, %s, %s", (value, [value], (value, 1)), inline=True)
        u = "'00000000-0000-0000-0000-000000000001'::uuid"
        self.assertEqual(sql, f"select {u}, ARRAY[{u}], ({u},1)")
        self.assertEqual(params, [])

    def test_adapt_query_inline_list_with_network(self):
        format_query = self.adapter.format_query
        values = (
            IPv4Address('192.168.0.1'), IPv6Address('2001:db8::1'),
            IPv4Interface('192.168.0.1/24'), IPv6Interface('2001:db8::1/64'),
            IPv4Network('192.168.0.0/24'), IPv6Network('2001:db8::/32'))
        sql, params = format_query(
            "select %s, %s, %s, %s, %s, %s", values, inline=True)
        self.assertEqual(
            sql, "select '192.168.0.1'::inet, '2001:db8::1'::inet,"
            " '192.168.0.1/24'::inet, '2001:db8::1/64'::inet,"
            " '192.168.0.0/24'::cidr, '2001:db8::/32'::cidr")
        self.assertEqual(params, [])
        a, n = values[0], values[4]
        sql, params = format_query(
            "select %s, %s", ([[a, None], [n, a]], (a, 1)), inline=True)
        self.assertEqual(
            sql, "select ARRAY[['192.168.0.1'::inet,NULL],"
            "['192.168.0.0/24'::cidr,'192.168.0.1'::inet]],"
            " ('192.168.0.1'::inet,1)")
        self.assertEqual(params, [])
        # the scope id of an IPv6 address may contain quotes
        sql, params = format_query(
            "select %s", (IPv6Address("fe80::1%it's"),), inline=True)
        self.assertEqual(sql, "select 'fe80::1%it''s'::inet")
        self.assertEqual(params, [])

    def test_adapt_query_inline_list_with_bytes(self):
        format_query = self.adapter.format_query
        value = b"it's \\ \x00\xff"
        sql, params = format_query(
            "select %s, %s", (value, [b'a', b'']), inline=True)
        self.assertEqual(
            sql, "select '\\x69742773205c2000ff'::bytea,"
            " ARRAY['\\x61'::bytea,'\\x'::bytea]")
        self.assertEqual(params, [])
        # the values are returned as bytes, also in untyped contexts
        self.assertEqual(self.db.query(sql).getresult(),
                         [(value, [b'a', b''])])

    def test_adapt_query_inline_dict(self):
        format_query = self.adapter.format_query
        values: dict = dict(i=3, f=7.5, t='hello', b=True)
        sql, params = format_query(
            "select %(i)s,%(f)s,%(t)s,%(b)s", values, inline=True)
        self.assertEqual(sql, "select 3,7.5,'hello',true")
        self.assertEqual(params, [])
        values = dict(d1='2016-01-30', d2='current_date')
        sql, params = format_query(
            "values(%(d1)s,%(d2)s)", values, inline=True)
        self.assertEqual(sql, "values('2016-01-30','current_date')")
        self.assertEqual(params, [])
        values = dict(i=[1, 2, 3], t=['a', 'b', 'c'], b=[True, False, True])
        sql, params = format_query("%(i)s,%(t)s,%(b)s", values, inline=True)
        self.assertEqual(
            sql, "ARRAY[1,2,3],ARRAY['a','b','c'],ARRAY[true,false,true]")
        self.assertEqual(params, [])
        values = dict(
            i=[[1, 2], [3, 4]], t=[['a', 'b'], ['c', 'd']],
            b=[[True, False], [False, True]])
        sql, params = format_query("%(i)s,%(t)s,%(b)s", values, inline=True)
        self.assertEqual(
            sql, "ARRAY[[1,2],[3,4]],ARRAY[['a','b'],['c','d']],"
                 "ARRAY[[true,false],[false,true]]")
        self.assertEqual(params, [])
        values = dict(record=(3, 7.5, 'hello', True, [123], ['abc']))
        sql, params = format_query('select %(record)s', values, inline=True)
        self.assertEqual(
            sql, "select (3,7.5,'hello',true,ARRAY[123],ARRAY['abc'])")
        self.assertEqual(params, [])
        values = dict(record=(0, -3.25, '', False, [0], ['']))
        sql, params = format_query('select %(record)s', values, inline=True)
        self.assertEqual(
            sql, "select (0,-3.25,'',false,ARRAY[0],ARRAY[''])")
        self.assertEqual(params, [])

    def test_adapt_query_with_pg_repr(self):
        format_query = self.adapter.format_query
        self.assertRaises(TypeError, format_query, '%s', object(), inline=True)

        class TestObject:
            # noinspection PyMethodMayBeStatic
            def __pg_repr__(self):
                return "'adapted'"

        sql, params = format_query('select %s', [TestObject()], inline=True)
        self.assertEqual(sql, "select 'adapted'")
        self.assertEqual(params, [])
        sql, params = format_query('select %s', [[TestObject()]], inline=True)
        self.assertEqual(sql, "select ARRAY['adapted']")
        self.assertEqual(params, [])


class TestSchemas(unittest.TestCase):
    """Test correct handling of schemas (namespaces)."""

    cls_set_up = False

    @classmethod
    def setUpClass(cls):
        db = DB()
        query = db.query
        for num_schema in range(5):
            if num_schema:
                schema = f"s{num_schema}"
                query(f"drop schema if exists {schema} cascade")
                try:
                    query(f"create schema {schema}")
                except pg.ProgrammingError as e:
                    raise RuntimeError(
                        "The test user cannot create schemas.\n"
                        f"Grant create on database {dbname} to the user"
                        " for running these tests.") from e
            else:
                schema = "public"
                query(f"drop table if exists {schema}.t")
                query(f"drop table if exists {schema}.t{num_schema}")
            query(f"create table {schema}.t"
                  f" as select 1 as n, {num_schema} as d")
            query(f"create table {schema}.t{num_schema}"
                  f" as select 1 as n, {num_schema} as d")
        db.close()
        cls.cls_set_up = True

    @classmethod
    def tearDownClass(cls):
        db = DB()
        query = db.query
        for num_schema in range(5):
            if num_schema:
                schema = f"s{num_schema}"
                query(f"drop schema {schema} cascade")
            else:
                schema = "public"
                query(f"drop table {schema}.t")
                query(f"drop table {schema}.t{num_schema}")
        db.close()

    def setUp(self):
        self.assertTrue(self.cls_set_up)
        self.db = DB()

    def tearDown(self):
        self.doCleanups()
        self.db.close()

    def test_get_tables(self):
        tables = self.db.get_tables()
        for num_schema in range(5):
            schema = 's' + str(num_schema) if num_schema else 'public'
            for t in (schema + '.t',
                      schema + '.t' + str(num_schema)):
                self.assertIn(t, tables)

    def test_get_attnames(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        result = {'d': 'int', 'n': 'int'}
        r = get_attnames("t")
        self.assertEqual(r, result)
        r = get_attnames("s4.t4")
        self.assertEqual(r, result)
        query("drop table if exists s3.t3m")
        self.addCleanup(query, "drop table s3.t3m")
        query("create table s3.t3m as select 1 as m")
        result_m = {'m': 'int'}
        r = get_attnames("s3.t3m")
        self.assertEqual(r, result_m)
        query("set search_path to s1,s3")
        r = get_attnames("t3")
        self.assertEqual(r, result)
        r = get_attnames("t3m")
        self.assertEqual(r, result_m)

    def test_get(self):
        get = self.db.get
        query = self.db.query
        PrgError = pg.ProgrammingError  # noqa: N806
        self.assertEqual(get("t", 1, 'n')['d'], 0)
        self.assertEqual(get("t0", 1, 'n')['d'], 0)
        self.assertEqual(get("public.t", 1, 'n')['d'], 0)
        self.assertEqual(get("public.t0", 1, 'n')['d'], 0)
        self.assertRaises(PrgError, get, "public.t1", 1, 'n')
        self.assertEqual(get("s1.t1", 1, 'n')['d'], 1)
        self.assertEqual(get("s3.t", 1, 'n')['d'], 3)
        query("set search_path to s2,s4")
        self.assertRaises(PrgError, get, "t1", 1, 'n')
        self.assertEqual(get("t4", 1, 'n')['d'], 4)
        self.assertRaises(PrgError, get, "t3", 1, 'n')
        self.assertEqual(get("t", 1, 'n')['d'], 2)
        self.assertEqual(get("s3.t3", 1, 'n')['d'], 3)
        query("set search_path to s1,s3")
        self.assertRaises(PrgError, get, "t2", 1, 'n')
        self.assertEqual(get("t3", 1, 'n')['d'], 3)
        self.assertRaises(PrgError, get, "t4", 1, 'n')
        self.assertEqual(get("t", 1, 'n')['d'], 1)
        self.assertEqual(get("s4.t4", 1, 'n')['d'], 4)

    def test_qualified_names_with_special_characters(self):
        db = self.db
        query = db.query
        query('drop schema if exists "s.5" cascade')
        query('create schema "s.5"')
        self.addCleanup(query, 'drop schema "s.5" cascade')
        tables = {  # table name as passed: as written in SQL
            's1.User': 's1."User"',
            's1."my.table"': 's1."my.table"',
            '"s.5"."q""t"': '"s.5"."q""t"'}
        for table in tables.values():
            query(f'create table {table} (n int primary key, d int)')
            if table.startswith('s1.'):
                self.addCleanup(query, f'drop table {table}')
        for name, table in tables.items():
            with self.subTest(name=name):
                self.assertEqual(db.pkey(name), 'n')
                self.assertEqual(
                    db.get_attnames(name), {'n': 'int', 'd': 'int'})
                self.assertEqual(db.get_generated(name), set())
                self.assertTrue(db.has_table_privilege(name, 'insert'))
                db.insert(name, n=1, d=2)
                self.assertEqual(db.get(name, 1)['d'], 2)
                db.update(name, n=1, d=3)
                db.upsert(name, {'n': 2, 'd': 4})
                q = f'select n, d from {table} order by n'
                self.assertEqual(query(q).getresult(), [(1, 3), (2, 4)])
                self.assertEqual(db.delete(name, n=1), 1)
                db.truncate(name)
                self.assertEqual(query(q).getresult(), [])
        # the names returned by get_tables() can be passed back
        names = [t for t in db.get_tables() if t in tables.values()]
        self.assertEqual(sorted(names), sorted(tables.values()))
        for name in names:
            self.assertEqual(db.pkey(name), 'n')

    def test_qualified_names_are_not_folded(self):
        get = self.db.get
        self.assertEqual(get("public.t", 1, 'n')['d'], 0)
        self.assertEqual(get('"public"."t"', 1, 'n')['d'], 0)
        self.assertRaises(pg.ProgrammingError, get, "public.T", 1, 'n')
        self.assertRaises(pg.ProgrammingError, get, "PUBLIC.t", 1, 'n')

    def test_qualified_names_share_cache(self):
        db = self.db
        self.assertEqual(db.get_attnames("s1.t1", flush=True),
                         {'n': 'int', 'd': 'int'})
        attnames = db._attnames
        self.assertEqual(len(attnames), 1)
        for name in '"s1".t1', 's1."t1"', '"s1"."t1"':
            self.assertIs(db.get_attnames(name), attnames['"s1"."t1"'])
        self.assertEqual(len(attnames), 1)

    def test_malformed_qualified_names(self):
        db = self.db
        for name in 's1.', '.t', 's1..t1', '"s1".""', '"s1', 's1."t1':
            with self.subTest(name=name):
                self.assertRaises(ValueError, db.pkey, name)
                self.assertRaises(ValueError, db.get_attnames, name)
                self.assertRaises(ValueError, db.get, name, 1)
                self.assertRaises(ValueError, db.insert, name, n=1)
                self.assertRaises(ValueError, db.truncate, name)
        self.assertRaises(TypeError, db.pkey, None)
        self.assertRaises(TypeError, db.get_attnames, None)

    def test_relation_oid_as_table(self):
        db = self.db
        oid = db.query("select 's1.t1'::regclass::oid").singlescalar()
        self.assertIsInstance(oid, int)
        self.assertEqual(db.get_attnames(oid), {'n': 'int', 'd': 'int'})
        self.assertEqual(db.get_generated(oid), set())
        self.assertRaises(KeyError, db.pkey, oid)  # table has no pkey

    def test_query_information_schema(self):
        q = "column_name"
        if self.db.server_version < 110000:
            q += "::text"  # old version does not have sql_identifier array
        q = f"select array_agg({q}) from information_schema.columns"
        q += " where table_schema in ('s1', 's2', 's3', 's4')"
        r = self.db.query(q).onescalar()
        self.assertIsInstance(r, list)
        self.assertEqual(set(r), set(['d', 'n'] * 8))


class TestDebug(unittest.TestCase):
    """Test the debug attribute of the DB class."""

    def setUp(self):
        self.db = DB()
        self.query = self.db.query
        self.debug = self.db.debug  # type: ignore[method-assign]
        self.output = StringIO()
        self.stdout, sys.stdout = sys.stdout, self.output

    def tearDown(self):
        sys.stdout = self.stdout
        self.output.close()
        self.db.debug = debug
        self.db.close()

    def get_output(self):
        return self.output.getvalue()

    def send_queries(self):
        self.db.query("select 1")
        self.db.query("select 2")

    def test_debug_default(self):
        if debug:
            self.assertEqual(self.db.debug, debug)
        else:
            self.assertIsNone(self.db.debug)

    def test_debug_is_false(self):
        self.db.debug = False
        self.send_queries()
        self.assertEqual(self.get_output(), "")

    def test_debug_is_true(self):
        self.db.debug = True
        self.send_queries()
        self.assertEqual(self.get_output(), "select 1\nselect 2\n")

    def test_debug_is_string(self):
        self.db.debug = "Test with string: %s."
        self.send_queries()
        self.assertEqual(
            self.get_output(),
            "Test with string: select 1.\nTest with string: select 2.\n")

    def test_debug_is_file_like(self):
        with tempfile.TemporaryFile('w+') as debug_file:
            self.db.debug = debug_file
            self.send_queries()
            debug_file.seek(0)
            output = debug_file.read()
            self.assertEqual(output, "select 1\nselect 2\n")
            self.assertEqual(self.get_output(), "")

    def test_debug_is_callable(self):
        output: list[str] = []
        self.db.debug = output.append
        self.db.query("select 1")
        self.db.query("select 2")
        self.assertEqual(output, ["select 1", "select 2"])
        self.assertEqual(self.get_output(), "")

    def test_debug_send_query(self):
        output: list[str] = []
        self.db.debug = output.append
        self.db.send_query("select 1").getresult()
        self.db.send_query("select $1::int", 2).getresult()
        self.assertEqual(output, ["select 1", "select $1::int\n(2,)"])

    def test_debug_multiple_args(self):
        output: list[str] = []
        self.db.debug = output.append
        args = ['Error', 42, {1: 'a', 2: 'b'}, [3, 5, 7]]
        self.db._do_debug(*args)
        self.assertEqual(output, ['\n'.join(str(arg) for arg in args)])
        self.assertEqual(self.get_output(), "")


class TestMemoryLeaks(unittest.TestCase):
    """Test that the DB class does not leak memory."""

    def get_leaks(self, fut: Callable):
        ids: set = set()
        objs: list = []
        add_ids = ids.update
        gc.collect()
        objs[:] = gc.get_objects()
        add_ids(id(obj) for obj in objs)
        fut()
        gc.collect()
        objs[:] = gc.get_objects()
        objs[:] = [obj for obj in objs if id(obj) not in ids]
        self.assertEqual(len(objs), 0)

    def test_leaks_with_close(self):
        def fut():
            db = DB()
            db.query("select $1::int as r", 42).dictresult()
            db.close()

        self.get_leaks(fut)

    def test_leaks_without_close(self):
        def fut():
            db = DB()
            db.query("select $1::int as r", 42).dictresult()

        self.get_leaks(fut)


if __name__ == '__main__':
    unittest.main()
