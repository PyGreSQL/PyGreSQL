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
from contextlib import suppress
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import StringIO
from operator import itemgetter
from time import strftime
from typing import Any, Callable, ClassVar
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
        self.assertIsNone(query.getresult())
        self.assertIsNone(query.getresult())


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
            'cancel', 'clear', 'close', 'commit',
            'date_format', 'db', 'dbname', 'dbtypes',
            'debug', 'decode_json', 'delete',
            'delete_prepared', 'describe_prepared',
            'encode_json', 'end', 'endcopy', 'error',
            'escape_bytea', 'escape_identifier',
            'escape_literal', 'escape_string',
            'fileno',
            'get', 'get_as_dict', 'get_as_list',
            'get_attnames', 'get_cast_hook', 'get_databases',
            'get_generated', 'get_notice_receiver',
            'get_parameter', 'get_relations', 'get_tables',
            'getline', 'getlo', 'getnotify',
            'has_table_privilege', 'host',
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
        db = pg.DB(db2)  # type: ignore
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
    supports_oids = False

    @classmethod
    def setUpClass(cls):
        db = DB()
        cls.supports_oids = db.server_version < 120000
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
                    temporary=True, oids=None, values=None):
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
        with_oids = 'with oids' if oids else (
            'without oids' if self.supports_oids else '')
        cmd_parts = ['create', temporary, table]
        if as_query:
            cmd_parts.extend([with_oids, definition])
        else:
            cmd_parts.extend([definition, with_oids])
        cmd = ' '.join(cmd_parts)
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
        f('standard_conforming_strings', 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f('datestyle', 'ISO, DMY')
        self.assertEqual(g('datestyle'), 'ISO, DMY')
        f(['standard_conforming_strings', 'datestyle'], ['on', 'ISO, DMY'])
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, DMY')
        f(['escape_string_warning', 'standard_conforming_strings'], 'off')
        self.assertEqual(g('escape_string_warning'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f(('standard_conforming_strings', 'datestyle'), ('on', 'ISO, YMD'))
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')
        f(('escape_string_warning', 'standard_conforming_strings'), 'off')
        self.assertEqual(g('escape_string_warning'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f({'escape_string_warning', 'standard_conforming_strings'}, 'on')
        self.assertEqual(g('escape_string_warning'), 'on')
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertRaises(
            ValueError, f,
            {'escape_string_warning', 'standard_conforming_strings'},
            ['off', 'on'])
        f({'escape_string_warning', 'standard_conforming_strings'},
          ['off', 'off'])
        self.assertEqual(g('escape_string_warning'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f({'standard_conforming_strings': 'on', 'datestyle': 'ISO, YMD'})
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')

    def test_reset_parameter(self):
        db = DB()
        f = db.set_parameter
        g = db.get_parameter
        r = g('escape_string_warning')
        self.assertIn(r, ('on', 'off'))
        esw, not_esw = r, 'off' if r == 'on' else 'on'
        r = g('standard_conforming_strings')
        self.assertIn(r, ('on', 'off'))
        scs, not_scs = r, 'off' if r == 'on' else 'on'
        f('escape_string_warning', not_esw)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('escape_string_warning'), not_esw)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f('escape_string_warning')
        f('standard_conforming_strings', None)
        self.assertEqual(g('escape_string_warning'), esw)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('escape_string_warning', not_esw)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('escape_string_warning'), not_esw)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f(['escape_string_warning', 'standard_conforming_strings'], None)
        self.assertEqual(g('escape_string_warning'), esw)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('escape_string_warning', not_esw)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('escape_string_warning'), not_esw)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f(('escape_string_warning', 'standard_conforming_strings'))
        self.assertEqual(g('escape_string_warning'), esw)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('escape_string_warning', not_esw)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('escape_string_warning'), not_esw)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f({'escape_string_warning', 'standard_conforming_strings'})
        self.assertEqual(g('escape_string_warning'), esw)
        self.assertEqual(g('standard_conforming_strings'), scs)
        db.close()

    def test_reset_parameter_all(self):
        db = DB()
        f = db.set_parameter
        self.assertRaises(ValueError, f, 'all', 0)
        self.assertRaises(ValueError, f, 'all', 'off')
        g = db.get_parameter
        r = g('escape_string_warning')
        self.assertIn(r, ('on', 'off'))
        dwi, not_dwi = r, 'off' if r == 'on' else 'on'
        r = g('standard_conforming_strings')
        self.assertIn(r, ('on', 'off'))
        scs, not_scs = r, 'off' if r == 'on' else 'on'
        f('escape_string_warning', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('escape_string_warning'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f('all')
        self.assertEqual(g('escape_string_warning'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)
        db.close()

    def test_set_parameter_local(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.db.begin()
        f('standard_conforming_strings', 'off', local=True)
        self.assertEqual(g('standard_conforming_strings'), 'off')
        self.db.end()
        self.assertEqual(g('standard_conforming_strings'), 'on')

    def test_set_parameter_session(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.db.begin()
        f('standard_conforming_strings', 'off', local=False)
        self.assertEqual(g('standard_conforming_strings'), 'off')
        self.db.end()
        self.assertEqual(g('standard_conforming_strings'), 'off')

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
                         temporary=True, oids=False, values=values)
        r = self.db.query(f'select t from "{table}" order by n').getresult()
        r = ', '.join(row[0] for row in r)
        self.assertEqual(r, "Hello, World!")

    def test_create_table_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        table = 'test hello world'
        values = [(2, "World!"), (1, "Hello")]
        self.create_table(table, "n smallint, t varchar",
                         temporary=True, oids=True, values=values)
        r = self.db.query(f'select t from "{table}" order by n').getresult()
        r = ', '.join(row[0] for row in r)
        self.assertEqual(r, "Hello, World!")
        r = self.db.query(f'select oid from "{table}" limit 1').getresult()
        self.assertIsInstance(r[0][0], int)

    def test_query(self):
        query = self.db.query
        table = 'test_table'
        self.create_table(table, "n integer", oids=False)
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

    def test_query_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        query = self.db.query
        table = 'test_table'
        self.create_table(table, "n integer", oids=True)
        q = "insert into test_table values (1)"
        r = query(q)
        self.assertIsInstance(r, int)
        q = "insert into test_table select 2"
        r = query(q)
        self.assertIsInstance(r, int)
        oid = r
        q = "select oid from test_table where n=2"
        r = query(q).getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r, oid)
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
        self.create_table('test_table', 'n1 integer, n2 integer', oids=False)
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

    def test_get_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        get = self.db.get
        query = self.db.query
        table = 'get_with_oid_test_table'
        self.create_table(table, 'n integer, t text', oids=True,
                         values=enumerate('xyz', start=1))
        self.assertRaises(pg.ProgrammingError, get, table, 2)
        self.assertRaises(KeyError, get, table, {}, 'oid')
        r = get(table, 2, 'n')
        qoid = f'oid({table})'
        self.assertIn(qoid, r)
        oid = r[qoid]
        self.assertIsInstance(oid, int)
        result = {'t': 'y', 'n': 2, qoid: oid}
        self.assertEqual(r, result)
        r = get(table, oid, 'oid')
        self.assertEqual(r, result)
        r = get(table, dict(oid=oid))
        self.assertEqual(r, result)
        r = get(table, dict(oid=oid), 'oid')
        self.assertEqual(r, result)
        r = get(table, {qoid: oid})
        self.assertEqual(r, result)
        r = get(table, {qoid: oid}, 'oid')
        self.assertEqual(r, result)
        self.assertEqual(get(table + '*', 2, 'n'), r)
        self.assertEqual(get(table + ' *', 2, 'n'), r)
        self.assertEqual(get(table, oid, 'oid')['t'], 'y')
        self.assertEqual(get(table, 1, 'n')['t'], 'x')
        self.assertEqual(get(table, 3, 'n')['t'], 'z')
        self.assertEqual(get(table, 2, 'n')['t'], 'y')
        self.assertRaises(pg.DatabaseError, get, table, 4, 'n')
        r['n'] = 3
        self.assertEqual(get(table, r, 'n')['t'], 'z')
        self.assertEqual(get(table, 1, 'n')['t'], 'x')
        self.assertEqual(get(table, r, 'oid')['t'], 'z')
        query(f'alter table "{table}" alter n set not null')
        query(f'alter table "{table}" add primary key (n)')
        self.assertEqual(get(table, 3)['t'], 'z')
        self.assertEqual(get(table, 1)['t'], 'x')
        self.assertEqual(get(table, 2)['t'], 'y')
        r['n'] = 1
        self.assertEqual(get(table, r)['t'], 'x')
        r['n'] = 3
        self.assertEqual(get(table, r)['t'], 'z')
        r['n'] = 2
        self.assertEqual(get(table, r)['t'], 'y')
        r = get(table, oid, 'oid')
        self.assertEqual(r, result)
        r = get(table, dict(oid=oid))
        self.assertEqual(r, result)
        r = get(table, dict(oid=oid), 'oid')
        self.assertEqual(r, result)
        r = get(table, {qoid: oid})
        self.assertEqual(r, result)
        r = get(table, {qoid: oid}, 'oid')
        self.assertEqual(r, result)
        r = get(table, dict(oid=oid, n=1))
        self.assertEqual(r['n'], 1)
        self.assertNotEqual(r[qoid], oid)
        r = get(table, dict(oid=oid, t='z'), 't')
        self.assertEqual(r['n'], 3)
        self.assertNotEqual(r[qoid], oid)

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

    def test_insert_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        insert = self.db.insert
        query = self.db.query
        self.create_table('test_table', 'n int', oids=True)
        self.assertRaises(pg.ProgrammingError, insert, 'test_table', m=1)
        r = insert('test_table', n=1)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 1)
        self.assertNotIn('oid', r)
        qoid = 'oid(test_table)'
        self.assertIn(qoid, r)
        oid = r[qoid]
        self.assertEqual(sorted(r.keys()), ['n', qoid])
        r = insert('test_table', n=2, oid=oid)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 2)
        self.assertIn(qoid, r)
        self.assertNotEqual(r[qoid], oid)
        self.assertNotIn('oid', r)
        r = insert('test_table', None, n=3)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 3)
        s = r
        r = insert('test_table', r)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 3)
        r = insert('test_table *', r)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 3)
        r = insert('test_table', r, n=4)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 4)
        self.assertNotIn('oid', r)
        self.assertIn(qoid, r)
        oid = r[qoid]
        r = insert('test_table', r, n=5, oid=oid)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 5)
        self.assertIn(qoid, r)
        self.assertNotEqual(r[qoid], oid)
        self.assertNotIn('oid', r)
        r['oid'] = oid = r[qoid]
        r = insert('test_table', r, n=6)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 6)
        self.assertIn(qoid, r)
        self.assertNotEqual(r[qoid], oid)
        self.assertNotIn('oid', r)
        q = 'select n from test_table order by 1 limit 9'
        r = ' '.join(str(row[0]) for row in query(q).getresult())
        self.assertEqual(r, '1 2 3 3 3 4 5 6')
        query("truncate table test_table")
        query("alter table test_table add unique (n)")
        r = insert('test_table', dict(n=7))
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 7)
        self.assertRaises(pg.IntegrityError, insert, 'test_table', r)
        r['n'] = 6
        self.assertRaises(pg.IntegrityError, insert, 'test_table', r, n=7)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 7)
        r['n'] = 6
        r = insert('test_table', r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 6)
        r = ' '.join(str(row[0]) for row in query(q).getresult())
        self.assertEqual(r, '6 7')

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

    def test_update_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        update = self.db.update
        get = self.db.get
        query = self.db.query
        self.create_table('test_table', 'n int', oids=True, values=[1])
        s = get('test_table', 1, 'n')
        self.assertIsInstance(s, dict)
        self.assertEqual(s['n'], 1)
        s['n'] = 2
        r = update('test_table', s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        qoid = 'oid(test_table)'
        self.assertIn(qoid, r)
        self.assertNotIn('oid', r)
        self.assertEqual(sorted(r.keys()), ['n', qoid])
        r['n'] = 3
        oid = r.pop(qoid)
        r = update('test_table', r, oid=oid)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 3)
        r.pop(qoid)
        self.assertRaises(pg.ProgrammingError, update, 'test_table', r)
        s = get('test_table', 3, 'n')
        self.assertIsInstance(s, dict)
        self.assertEqual(s['n'], 3)
        s.pop('n')
        r = update('test_table', s)
        oid = r.pop(qoid)
        self.assertEqual(r, {})
        q = "select n from test_table limit 2"
        r = query(q).getresult()
        self.assertEqual(r, [(3,)])
        query("insert into test_table values (1)")
        self.assertRaises(pg.ProgrammingError,
                          update, 'test_table', dict(oid=oid, n=4))
        r = update('test_table', dict(n=4), oid=oid)
        self.assertEqual(r['n'], 4)
        r = update('test_table *', dict(n=5), oid=oid)
        self.assertEqual(r['n'], 5)
        query("alter table test_table add column m int")
        query("alter table test_table add primary key (n)")
        self.assertIn('m', self.db.get_attnames('test_table', flush=True))
        self.assertEqual('n', self.db.pkey('test_table', flush=True))
        s = dict(n=1, m=4)
        r = update('test_table', s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 4)
        s = dict(m=7)
        r = update('test_table', s, n=5)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 5)
        self.assertEqual(r['m'], 7)
        q = "select n, m from test_table order by 1 limit 3"
        r = query(q).getresult()
        self.assertEqual(r, [(1, 4), (5, 7)])
        s = dict(m=9, oid=oid)
        self.assertRaises(KeyError, update, 'test_table', s)
        r = update('test_table', s, oid=oid)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 5)
        self.assertEqual(r['m'], 9)
        s = dict(n=1, m=3, oid=oid)
        r = update('test_table', s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        r = query(q).getresult()
        self.assertEqual(r, [(1, 3), (5, 9)])
        s.update(n=4, m=7)
        r = update('test_table', s, oid=oid)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 4)
        self.assertEqual(r['m'], 7)
        r = query(q).getresult()
        self.assertEqual(r, [(1, 3), (4, 7)])

    def test_update_without_oid(self):
        update = self.db.update
        query = self.db.query
        self.assertRaises(pg.ProgrammingError, update,
                          'test', i2=2, i4=4, i8=8)
        table = 'update_test_table'
        self.create_table(table, 'n integer primary key, t text', oids=False,
                         values=enumerate('xyz', start=1))
        r = self.db.get(table, 2)
        r['t'] = 'u'
        s = update(table, r)
        self.assertEqual(s, r)
        q = f'select t from "{table}" where n=2'
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'u')

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
        # not existing columns and oid parameter should be ignored
        s = dict(m=3, u='z')
        r = upsert(table, s, oid='invalid')
        self.assertIs(r, s)
        s = dict(n=2)
        # do not modify columns missing in the dict
        r = upsert(table, s)
        self.assertIs(r, s)
        r = query(q).getresult()
        self.assertEqual(r, [(1, 'x2'), (2, 'y3')])

    def test_upsert_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        upsert = self.db.upsert
        get = self.db.get
        query = self.db.query
        self.create_table('test_table', 'n int', oids=True, values=[1])
        self.assertRaises(pg.ProgrammingError,
                          upsert, 'test_table', dict(n=2))
        r: Any = get('test_table', 1, 'n')
        self.assertIsInstance(r, dict)
        self.assertEqual(r['n'], 1)
        qoid = 'oid(test_table)'
        self.assertIn(qoid, r)
        self.assertNotIn('oid', r)
        oid = r[qoid]
        self.assertRaises(pg.ProgrammingError,
                          upsert, 'test_table', dict(n=2, oid=oid))
        query("alter table test_table add column m int")
        query("alter table test_table add primary key (n)")
        self.assertIn('m', self.db.get_attnames('test_table', flush=True))
        self.assertEqual('n', self.db.pkey('test_table', flush=True))
        s = dict(n=2)
        r = upsert('test_table', s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertIsNone(r['m'])
        q = query("select n, m from test_table order by n limit 3")
        self.assertEqual(q.getresult(), [(1, None), (2, None)])
        r['oid'] = oid
        r = upsert('test_table', r)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertIsNone(r['m'])
        self.assertIn(qoid, r)
        self.assertNotIn('oid', r)
        self.assertNotEqual(r[qoid], oid)
        r['m'] = 7
        r = upsert('test_table', r)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['m'], 7)
        r.update(n=1, m=3)
        r = upsert('test_table', r)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        q = query("select n, m from test_table order by n limit 3")
        self.assertEqual(q.getresult(), [(1, 3), (2, 7)])
        r = upsert('test_table', r, oid='invalid')
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        r['m'] = 5
        r = upsert('test_table', r, m=False)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 3)
        r['m'] = 5
        r = upsert('test_table', r, m=True)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['m'], 5)
        r.update(n=2, m=1)
        r = upsert('test_table', r, m='included.m')
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['m'], 7)
        r['m'] = 9
        r = upsert('test_table', r, m='excluded.m')
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['m'], 9)
        r['m'] = 8
        r = upsert('test_table *', r, m='included.m + 1')
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['m'], 10)
        q = query("select n, m from test_table order by n limit 3")
        self.assertEqual(q.getresult(), [(1, 5), (2, 10)])

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
                         oids=False, values=enumerate('xyz', start=1))
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
        # not existing columns and oid parameter should be ignored
        r.update(m=3, u='z', oid='invalid')
        s = delete(table, r)
        self.assertEqual(s, 0)

    def test_delete_with_oids(self):
        if not self.supports_oids:
            self.skipTest("database does not support tables with oids")
        delete = self.db.delete
        get = self.db.get
        query = self.db.query
        self.create_table('test_table', 'n int', oids=True, values=range(1, 7))
        r: Any = dict(n=3)
        self.assertRaises(pg.ProgrammingError, delete, 'test_table', r)
        s: Any = get('test_table', 1, 'n')
        qoid = 'oid(test_table)'
        self.assertIn(qoid, s)
        r = delete('test_table', s)
        self.assertEqual(r, 1)
        r = delete('test_table', s)
        self.assertEqual(r, 0)
        q = "select min(n),count(n) from test_table"
        self.assertEqual(query(q).getresult()[0], (2, 5))
        oid = get('test_table', 2, 'n')[qoid]
        s = dict(oid=oid, n=2)
        self.assertRaises(pg.ProgrammingError, delete, 'test_table', s)
        r = delete('test_table', None, oid=oid)
        self.assertEqual(r, 1)
        r = delete('test_table', None, oid=oid)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (3, 4))
        s = dict(oid=oid, n=2)
        oid = get('test_table', 3, 'n')[qoid]
        self.assertRaises(pg.ProgrammingError, delete, 'test_table', s)
        r = delete('test_table', s, oid=oid)
        self.assertEqual(r, 1)
        r = delete('test_table', s, oid=oid)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (4, 3))
        s = get('test_table', 4, 'n')
        r = delete('test_table *', s)
        self.assertEqual(r, 1)
        r = delete('test_table *', s)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (5, 2))
        oid = get('test_table', 5, 'n')[qoid]
        s = {qoid: oid, 'm': 4}
        r = delete('test_table', s, m=6)
        self.assertEqual(r, 1)
        r = delete('test_table *', s)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (6, 1))
        query("alter table test_table add column m int")
        query("alter table test_table add primary key (n)")
        self.assertIn('m', self.db.get_attnames('test_table', flush=True))
        self.assertEqual('n', self.db.pkey('test_table', flush=True))
        for i in range(5):
            query(f"insert into test_table values ({i + 1}, {i + 2})")
        s = dict(m=2)
        self.assertRaises(KeyError, delete, 'test_table', s)
        s = dict(m=2, oid=oid)
        self.assertRaises(KeyError, delete, 'test_table', s)
        r = delete('test_table', dict(m=2), oid=oid)
        self.assertEqual(r, 0)
        oid = get('test_table', 1, 'n')[qoid]
        s = dict(oid=oid)
        self.assertRaises(KeyError, delete, 'test_table', s)
        r = delete('test_table', s, oid=oid)
        self.assertEqual(r, 1)
        r = delete('test_table', s, oid=oid)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (2, 5))
        s = get('test_table', 2, 'n')
        del s['n']
        r = delete('test_table', s)
        self.assertEqual(r, 1)
        r = delete('test_table', s)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (3, 4))
        r = delete('test_table', n=3)
        self.assertEqual(r, 1)
        r = delete('test_table', n=3)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (4, 3))
        r = delete('test_table', None, n=4)
        self.assertEqual(r, 1)
        r = delete('test_table', None, n=4)
        self.assertEqual(r, 0)
        self.assertEqual(query(q).getresult()[0], (5, 2))
        s = dict(n=6)
        r = delete('test_table', s, n=5)
        self.assertEqual(r, 1)
        r = delete('test_table', s, n=5)
        self.assertEqual(r, 0)
        s = get('test_table', 6, 'n')
        self.assertEqual(s['n'], 6)
        s['n'] = 7
        r = delete('test_table', s)
        self.assertEqual(r, 1)
        self.assertEqual(query(q).getresult()[0], (None, 0))

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
        for t, n in zip(r, names):
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
        # keyname must be string, list or tuple
        self.assertRaises(KeyError, get_as_dict, table, 3)
        self.assertRaises(KeyError, get_as_dict, table, dict(id=None))
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
                         oids=False, temporary=False)
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

    def test_date_and_time_arrays(self):
        dt = (date(2016, 3, 14), time(15, 9, 26))
        q = "select ARRAY[$1::date], ARRAY[$2::time]"
        r = self.db.query(q, dt).getresult()[0]
        self.assertIsInstance(r[0], list)
        self.assertEqual(r[0][0], dt[0])
        self.assertIsInstance(r[1], list)
        self.assertEqual(r[1][0], dt[1])

    def test_hstore(self):
        try:
            self.db.query("select 'k=>v'::hstore")
        except pg.DatabaseError:
            try:
                self.db.query("create extension hstore")
            except pg.DatabaseError:
                self.skipTest("hstore extension not enabled")
        d = {'k': 'v', 'foo': 'bar', 'baz': 'whatever',
             '1a': 'anything at all', '2=b': 'value = 2', '3>c': 'value > 3',
             '4"c': 'value " 4', "5'c": "value ' 5", 'hello, world': '"hi!"',
             'None': None, 'NULL': 'NULL', 'empty': ''}
        q = "select $1::hstore"
        r = self.db.query(q, (pg.Hstore(d),)).getresult()[0][0]
        self.assertIsInstance(r, dict)
        self.assertEqual(r, d)

    def test_uuid(self):
        d = UUID('{12345678-1234-5678-1234-567812345678}')
        q = 'select $1::uuid'
        r = self.db.query(q, (d,)).getresult()[0][0]
        self.assertIsInstance(r, UUID)
        self.assertEqual(r, d)

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
        self.assertIs(dbtypes[1700], typ)
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
        self.assertIn('circle', dbtypes)
        self.assertEqual(r, 'Squared Circle: <(0,0),1>')
        self.assertEqual(
            dbtypes.typecast('Impossible', 'circle'),
            'Squared Circle: Impossible')
        dbtypes.reset_typecast('circle')
        self.assertIsNone(dbtypes.get_typecast('circle'))

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
        self.assertIn('circle', dbtypes)
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
        self.assertEqual(f('string'), 'text')
        self.assertEqual(f(b'string'), 'text')
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
        r = f(('string', True, 3, 2.75, [1], [False]))
        self.assertEqual(r, 'record')
        self.assertEqual(list(r.attnames.values()), [
            'text', 'bool', 'int', 'float', 'int[]', 'bool[]'])

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
    with_oids = ""

    @classmethod
    def setUpClass(cls):
        db = DB()
        cls.with_oids = "with oids" if db.server_version < 120000 else ""
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
            query(f"create table {schema}.t {cls.with_oids}"
                  f" as select 1 as n, {num_schema} as d")
            query(f"create table {schema}.t{num_schema} {cls.with_oids}"
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
        if self.with_oids:
            result['oid'] = 'int'
        r = get_attnames("t")
        self.assertEqual(r, result)
        r = get_attnames("s4.t4")
        self.assertEqual(r, result)
        query("drop table if exists s3.t3m")
        self.addCleanup(query, "drop table s3.t3m")
        query(f"create table s3.t3m {self.with_oids} as select 1 as m")
        result_m = {'m': 'int'}
        if self.with_oids:
            result_m['oid'] = 'int'
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

    def test_munging(self):
        get = self.db.get
        query = self.db.query
        r = get("t", 1, 'n')
        if self.with_oids:
            self.assertIn('oid(t)', r)
        else:
            self.assertNotIn('oid(t)', r)
        query("set search_path to s2")
        r = get("t2", 1, 'n')
        if self.with_oids:
            self.assertIn('oid(t2)', r)
        else:
            self.assertNotIn('oid(t2)', r)
        query("set search_path to s3")
        r = get("t", 1, 'n')
        if self.with_oids:
            self.assertIn('oid(t)', r)
        else:
            self.assertNotIn('oid(t)', r)

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
        self.debug = self.db.debug  # type: ignore
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
