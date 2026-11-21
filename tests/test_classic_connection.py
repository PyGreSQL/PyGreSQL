#!/usr/bin/python

"""Test the classic PyGreSQL interface.

Sub-tests for the low-level connection object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.
"""

from __future__ import annotations

import ctypes
import datetime
import gc
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
import warnings
import weakref
from collections import namedtuple
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from decimal import Decimal
from typing import Any
from unittest import mock

import pg  # the module under test

from .config import dbhost, dbname, dbpasswd, dbport, dbuser

windows = os.name == 'nt'

# There is a known a bug in libpq under Windows which can cause
# the interface to crash when calling PQhost():
do_not_ask_for_host = windows
do_not_ask_for_host_reason = 'libpq issue on Windows'


def connect():
    """Create a basic pg connection to the test database."""
    # noinspection PyArgumentList
    connection = pg.connect(dbname, dbhost, dbport,
                            user=dbuser, passwd=dbpasswd)
    connection.query("set client_min_messages=warning")
    return connection


class MallInfo2(ctypes.Structure):
    """Memory allocation information as returned by glibc."""

    _fields_ = [(name, ctypes.c_size_t) for name in (
        'arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
        'fsmblks', 'uordblks', 'fordblks', 'keepcost')]


def get_mallinfo2() -> Callable[[], MallInfo2] | None:
    """Get the mallinfo2() function of glibc if available."""
    try:
        func = ctypes.CDLL(None).mallinfo2
    except (AttributeError, OSError, TypeError):  # not glibc 2.33 or newer
        return None
    func.argtypes = []
    func.restype = MallInfo2
    return func


mallinfo2 = get_mallinfo2()


def connect_nowait():
    """Start a basic pg connection in a non-blocking manner."""
    # noinspection PyArgumentList
    return pg.connect(dbname, dbhost, dbport,
                      user=dbuser, passwd=dbpasswd, nowait=True)


class TestCanConnect(unittest.TestCase):
    """Test whether a basic connection to PostgreSQL is possible."""

    def test_can_connect(self):
        try:
            connection = connect()
            rc = connection.poll()
        except pg.Error as error:
            self.fail(f'Cannot connect to database {dbname}:\n{error}')
        self.assertEqual(rc, pg.POLLING_OK)
        self.assertIs(connection.is_non_blocking(), False)
        connection.set_non_blocking(True)
        self.assertIs(connection.is_non_blocking(), True)
        connection.set_non_blocking(False)
        self.assertIs(connection.is_non_blocking(), False)
        try:
            connection.close()
        except pg.Error:
            self.fail('Cannot close the database connection')

    def test_can_connect_with_bytes(self):
        # bytes were not accepted as parameters before
        connection = pg.connect(
            dbname.encode(), dbhost.encode() if dbhost else None, dbport,
            user=dbuser.encode() if dbuser else None,
            passwd=dbpasswd.encode() if dbpasswd else None)
        try:
            self.assertEqual(connection.db, dbname)
        finally:
            connection.close()

    def test_can_connect_with_default_port(self):
        # None was not accepted as port before
        connection = pg.connect(
            dbname, dbhost, None, user=dbuser, passwd=dbpasswd)
        connection.close()

    def test_connect_with_bad_parameters(self):
        self.assertRaises(TypeError, pg.connect, 1)
        self.assertRaises(TypeError, pg.connect, dbname, port='5432')
        self.assertRaises(ValueError, pg.connect, 'db\0name')
        self.assertRaises(ValueError, pg.connect, b'db\0name')
        self.assertRaises(ValueError, pg.connect, dbname, port=-2)
        self.assertRaises(ValueError, pg.connect, dbname, port=65536)

    def test_can_connect_no_wait(self):
        try:
            connection = connect_nowait()
            rc = connection.poll()
            self.assertIn(rc, (pg.POLLING_READING, pg.POLLING_WRITING))
            while rc not in (pg.POLLING_OK, pg.POLLING_FAILED):
                rc = connection.poll()
        except pg.Error as error:
            self.fail(f'Cannot connect to database {dbname}:\n{error}')
        self.assertEqual(rc, pg.POLLING_OK)
        self.assertIs(connection.is_non_blocking(), False)
        connection.set_non_blocking(True)
        self.assertIs(connection.is_non_blocking(), True)
        connection.set_non_blocking(False)
        self.assertIs(connection.is_non_blocking(), False)
        try:
            connection.close()
        except pg.Error:
            self.fail('Cannot close the database connection')


class TestConnectObject(unittest.TestCase):
    """Test existence of basic pg connection methods."""

    def setUp(self):
        self.connection = connect()

    def tearDown(self):
        with suppress(pg.InternalError):
            self.connection.close()

    def is_method(self, attribute):
        """Check if given attribute on the connection is a method."""
        if do_not_ask_for_host and attribute == 'host':
            return False
        return callable(getattr(self.connection, attribute))

    def test_class_name(self):
        self.assertEqual(self.connection.__class__.__name__, 'Connection')

    def test_module_name(self):
        self.assertEqual(self.connection.__class__.__module__, 'pg')

    def test_str(self):
        r = str(self.connection)
        self.assertTrue(r.startswith('<pg.Connection object'), r)

    def test_repr(self):
        r = repr(self.connection)
        self.assertTrue(r.startswith('<pg.Connection object'), r)

    def test_all_connect_attributes(self):
        attributes = [
            'backend_pid', 'db', 'encoding', 'error', 'host', 'options',
            'port', 'protocol_version', 'server_version',
            'socket', 'ssl_attributes', 'ssl_in_use', 'status', 'user']
        connection_attributes = [
            a for a in dir(self.connection)
            if not a.startswith('__') and not self.is_method(a)]
        self.assertEqual(attributes, connection_attributes)

    def test_all_connect_methods(self):
        methods = [
            'cancel', 'close', 'date_format', 'describe_prepared',
            'endcopy', 'escape_bytea', 'escape_identifier',
            'escape_literal', 'escape_string', 'fileno', 'get_cast_hook',
            'get_notice_receiver', 'getline', 'getlo', 'getnotify',
            'inserttable', 'is_non_blocking', 'locreate', 'loimport',
            'parameter', 'poll', 'prepare', 'putline',
            'query', 'query_prepared', 'reset', 'send_query',
            'set_cast_hook', 'set_non_blocking', 'set_notice_receiver',
            'source', 'transaction']
        connection_methods = [
            a for a in dir(self.connection)
            if not a.startswith('__') and self.is_method(a)]
        self.assertEqual(methods, connection_methods)

    def test_attribute_db(self):
        self.assertEqual(self.connection.db, dbname)

    def test_attribute_error(self):
        error = self.connection.error
        self.assertTrue(not error or 'krb5_' in error)

    @unittest.skipIf(do_not_ask_for_host, do_not_ask_for_host_reason)
    def test_attribute_host(self):
        host = dbhost if dbhost and not dbhost.startswith('/') else 'localhost'
        self.assertIsInstance(self.connection.host, str)
        self.assertEqual(self.connection.host, host)

    def test_attribute_options(self):
        no_options = ''
        self.assertEqual(self.connection.options, no_options)

    def test_attribute_port(self):
        def_port = 5432
        self.assertIsInstance(self.connection.port, int)
        self.assertEqual(self.connection.port, dbport or def_port)

    def test_attribute_protocol_version(self):
        protocol_version = self.connection.protocol_version
        self.assertIsInstance(protocol_version, int)
        self.assertTrue(2 <= protocol_version < 4)

    def test_attribute_server_version(self):
        server_version = self.connection.server_version
        self.assertIsInstance(server_version, int)
        self.assertGreaterEqual(server_version, 120000)  # >= 12.0
        self.assertLess(server_version, 200000)  # < 20.0

    def test_attribute_encoding(self):
        query = self.connection.query
        encoding = self.connection.encoding
        self.assertIsInstance(encoding, str)
        client_encoding = query('show client_encoding').getresult()[0][0]
        try:
            for name, codec in [
                    ('utf8', 'utf-8'), ('latin1', 'latin-1'),
                    ('latin9', 'iso8859-15'), ('win1252', 'cp1252'),
                    ('iso_8859_5', 'iso8859-5'), ('sql_ascii', 'ascii')]:
                try:
                    query(f'set client_encoding={name}')
                except pg.DatabaseError:  # conversion not supported
                    continue
                self.assertEqual(self.connection.encoding, codec)
        finally:
            query(f'set client_encoding={client_encoding}')
        self.assertEqual(self.connection.encoding, encoding)

    def test_attribute_socket(self):
        socket = self.connection.socket
        self.assertIsInstance(socket, int)
        self.assertGreaterEqual(socket, 0)

    def test_attribute_backend_pid(self):
        backend_pid = self.connection.backend_pid
        self.assertIsInstance(backend_pid, int)
        self.assertGreaterEqual(backend_pid, 1)

    def test_attribute_ssl_in_use(self):
        ssl_in_use = self.connection.ssl_in_use
        self.assertIsInstance(ssl_in_use, bool)
        self.assertFalse(ssl_in_use)

    def test_attribute_ssl_attributes(self):
        ssl_attributes = self.connection.ssl_attributes
        self.assertIsInstance(ssl_attributes, dict)
        if ssl_attributes:
            self.assertEqual(ssl_attributes, {
                'cipher': None, 'compression': None, 'key_bits': None,
                'library': None, 'protocol': None})

    def test_attribute_status(self):
        status_ok = 1
        self.assertIsInstance(self.connection.status, int)
        self.assertEqual(self.connection.status, status_ok)

    def test_attribute_user(self):
        no_user = 'Deprecated facility'
        user = self.connection.user
        self.assertTrue(user)
        self.assertIsInstance(user, str)
        self.assertNotEqual(user, no_user)

    def test_method_query(self):
        query = self.connection.query
        query("select 1+1")
        query("select 1+$1", (1,))
        query("select 1+$1+$2", (2, 3))
        query("select 1+$1+$2", [2, 3])

    def test_method_query_empty(self):
        self.assertRaises(ValueError, self.connection.query, '')

    def test_method_send_query_single(self):
        query = self.connection.send_query
        for q, args, result in (
                ("select 1+1 as a", (), 2),
                ("select 1+$1 as a", ((1,),), 2),
                ("select 1+$1+$2 as a", ((2, 3),), 6)):
            pgq = query(q, *args)
            self.assertEqual(self.connection.transaction(), pg.TRANS_ACTIVE)
            self.assertEqual(pgq.getresult()[0][0], result)
            self.assertEqual(self.connection.transaction(), pg.TRANS_ACTIVE)
            self.assertIsNone(pgq.getresult())
            self.assertEqual(self.connection.transaction(), pg.TRANS_IDLE)

            pgq = query(q, *args)
            self.assertEqual(pgq.namedresult()[0].a, result)
            self.assertIsNone(pgq.namedresult())

            pgq = query(q, *args)
            self.assertEqual(pgq.dictresult()[0]['a'], result)
            self.assertIsNone(pgq.dictresult())

    def test_method_send_query_multiple(self):
        query = self.connection.send_query

        pgq = query("select 1+1; select 'pg';")
        self.assertEqual(pgq.getresult()[0][0], 2)
        self.assertEqual(pgq.getresult()[0][0], 'pg')
        self.assertIsNone(pgq.getresult())

        pgq = query("select 1+1 as a; select 'pg' as a;")
        self.assertEqual(pgq.namedresult()[0].a, 2)
        self.assertEqual(pgq.namedresult()[0].a, 'pg')
        self.assertIsNone(pgq.namedresult())

        pgq = query("select 1+1 as a; select 'pg' as a;")
        self.assertEqual(pgq.dictresult()[0]['a'], 2)
        self.assertEqual(pgq.dictresult()[0]['a'], 'pg')
        self.assertIsNone(pgq.dictresult())

    def test_method_send_query_empty(self):
        query = self.connection.send_query('')
        self.assertRaises(ValueError, query.getresult)

    def test_method_send_query_fields_after_error(self):
        query = self.connection.send_query("select 1 as a; select 1/0")
        self.assertEqual(query.getresult(), [(1,)])
        self.assertEqual(query.listfields(), ('a',))
        self.assertRaises(pg.DataError, query.getresult)
        # the fields of the previous result were kept, which crashed
        self.assertEqual(query.listfields(), ())
        self.assertEqual(query.fieldinfo(), ())
        self.assertRaises(ValueError, query.fieldname, 0)
        self.assertEqual(len(query), 0)

    def test_all_query_members(self):
        query = self.connection.query("select true where false")
        members = [
            'dictiter', 'dictresult', 'fieldinfo', 'fieldname', 'fieldnum',
            'getresult', 'listfields', 'memsize', 'namediter', 'namedresult',
            'one', 'onedict', 'onenamed', 'onescalar',
            'scalariter', 'scalarresult',
            'single', 'singledict', 'singlenamed', 'singlescalar']
        # noinspection PyUnresolvedReferences
        if pg.get_pqlib_version() < 120000:
            members.remove('memsize')
        query_members = [
            a for a in dir(query)
            if not a.startswith('__')]
        self.assertEqual(members, query_members)

    def test_method_endcopy(self):
        with suppress(OSError):
            self.connection.endcopy()

    def test_method_close(self):
        self.connection.close()
        try:
            self.connection.reset()
        except (pg.Error, TypeError):
            pass
        else:
            self.fail('Reset should give an error for a closed connection')
        self.assertRaises(pg.InternalError, self.connection.close)
        try:
            self.connection.query('select 1')
        except (pg.Error, TypeError):
            pass
        else:
            self.fail('Query should give an error for a closed connection')
        self.connection = connect()

    def test_escape_methods_after_close(self):
        connection = self.connection
        methods = [connection.escape_literal, connection.escape_identifier,
                   connection.escape_string, connection.escape_bytea]
        connection.close()
        for method in methods:
            for arg in ('x', b'x'):
                with self.assertRaises(TypeError) as cm:
                    method(arg)
                self.assertEqual(str(cm.exception), 'Connection is not valid')
        self.connection = connect()

    def test_escape_literal_and_identifier_with_invalid_bytes(self):
        connection = self.connection
        connection.query("set client_encoding=utf8")
        invalid = '€'.encode()[:2]  # incomplete multibyte character
        for method in connection.escape_literal, connection.escape_identifier:
            with self.assertRaises(pg.DataError) as cm:
                method(invalid)
            self.assertIn('multibyte character', str(cm.exception))
            self.assertEqual(method('€'.encode()), method('€').encode())

    def test_method_reset(self):
        query = self.connection.query
        # check that client encoding gets reset
        encoding = query('show client_encoding').getresult()[0][0].upper()
        changed_encoding = 'LATIN1' if encoding == 'UTF8' else 'UTF8'
        self.assertNotEqual(encoding, changed_encoding)
        self.connection.query(f"set client_encoding={changed_encoding}")
        new_encoding = query('show client_encoding').getresult()[0][0].upper()
        self.assertEqual(new_encoding, changed_encoding)
        self.connection.reset()
        new_encoding = query('show client_encoding').getresult()[0][0].upper()
        self.assertNotEqual(new_encoding, changed_encoding)
        self.assertEqual(new_encoding, encoding)

    def test_method_cancel(self):
        r = self.connection.cancel()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)

    def test_cancel_long_running_thread(self):
        errors = []

        def sleep():
            try:
                self.connection.query('select pg_sleep(5)').getresult()
            except pg.DatabaseError as error:
                errors.append(str(error))

        thread = threading.Thread(target=sleep)
        t1 = time.time()
        thread.start()  # run the query
        while 1:  # make sure the query is really running
            time.sleep(0.1)
            if thread.is_alive() or time.time() - t1 > 5:
                break
        r = self.connection.cancel()  # cancel the running query
        thread.join()  # wait for the thread to end
        t2 = time.time()

        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)  # return code should be 1
        self.assertLessEqual(t2 - t1, 3)  # time should be under 3 seconds
        self.assertTrue(errors)

    def test_method_file_no(self):
        r = self.connection.fileno()
        self.assertIsInstance(r, int)
        self.assertGreaterEqual(r, 0)

    def test_method_transaction(self):
        transaction = self.connection.transaction
        self.assertRaises(TypeError, transaction, None)
        self.assertEqual(transaction(), pg.TRANS_IDLE)
        self.connection.query('begin')
        self.assertEqual(transaction(), pg.TRANS_INTRANS)
        self.connection.query('rollback')
        self.assertEqual(transaction(), pg.TRANS_IDLE)

    def test_method_parameter(self):
        parameter = self.connection.parameter
        query = self.connection.query
        self.assertRaises(TypeError, parameter)
        r = parameter('this server setting does not exist')
        self.assertIsNone(r)
        s = query('show server_version').getresult()[0][0]
        self.assertIsNotNone(s)
        r = parameter('server_version')
        self.assertEqual(r, s)
        s = query('show server_encoding').getresult()[0][0]
        self.assertIsNotNone(s)
        r = parameter('server_encoding')
        self.assertEqual(r, s)
        s = query('show client_encoding').getresult()[0][0]
        self.assertIsNotNone(s)
        r = parameter('client_encoding')
        self.assertEqual(r, s)
        s = query('show server_encoding').getresult()[0][0]
        self.assertIsNotNone(s)
        r = parameter('server_encoding')
        self.assertEqual(r, s)

    def test_get_cast_hook(self):
        self.assertIsNone(self.connection.get_cast_hook())

    def test_set_cast_hook(self):
        f = self.connection.set_cast_hook
        self.assertRaises(TypeError, f, 42)
        self.assertRaises(TypeError, f, 'invalid')
        self.assertIsNone(f(lambda value, oid: value))
        self.assertIsNone(f(None))

    def test_set_and_get_cast_hook(self):
        h = lambda value, oid: value  # noqa: E731
        self.assertIsNone(self.connection.set_cast_hook(h))
        for _i in range(10):
            self.assertIs(self.connection.get_cast_hook(), h)
        self.assertIsNone(self.connection.set_cast_hook(None))
        self.assertIsNone(self.connection.get_cast_hook())

    def test_cast_hook(self):
        calls = []

        def cast_hook(value, oid):
            calls.append((value, oid))
            return f'cast {value}'

        self.connection.set_cast_hook(cast_hook)
        del cast_hook  # the connection must keep its own reference
        for _i in range(10):
            self.assertTrue(callable(self.connection.get_cast_hook()))
        query = self.connection.query
        r = query("select '(1,2)'::point, 42").getresult()
        self.assertEqual(r, [('cast (1,2)', 42)])
        self.assertEqual(calls, [('(1,2)', 600)])  # 600 is the point type


class TestLostConnection(unittest.TestCase):
    """Test a connection that has been lost or is not yet established."""

    def setUp(self):
        self.c = connect()
        other = connect()
        try:
            other.query(f'select pg_terminate_backend({self.c.backend_pid})')
        finally:
            other.close()

    def tearDown(self):
        self.c.close()

    def test_lost_connection(self):
        c = self.c
        # the first query that notices the lost connection
        # raised a ProgrammingError before
        self.assertRaises(pg.OperationalError, c.query, 'select 1')
        self.assertEqual(c.status, 0)
        # the following raised a LookupError (unknown encoding) before
        for method, args in (
                (c.query, ('select 1',)), (c.query, (b'select 1',)),
                (c.query, ('select $1', (1,))),
                (c.send_query, ('select 1',)),
                (c.query_prepared, ('q',)),
                (c.prepare, ('q', 'select 1')), (c.describe_prepared, ('q',)),
                (c.escape_literal, ('a',)), (c.escape_identifier, ('a',)),
                (c.escape_string, ('a',)), (c.escape_bytea, ('a',)),
                (c.inserttable, ('t', [(1,)])),
                (c.source().execute, ('select 1',)),
                (c.source().execute, (b'select 1',))):
            with self.subTest(method=method.__name__, args=args):
                with self.assertRaises(pg.OperationalError) as cm:
                    method(*args)
                if method not in (c.prepare, c.describe_prepared):
                    self.assertEqual(
                        str(cm.exception), 'Connection has been lost')
        # escaping bytes does not need the client encoding
        self.assertEqual(c.escape_literal(b"a'b"), b"'a''b'")

    def test_connection_not_established(self):
        c = connect_nowait()
        try:
            for query in 'select 1', b'select 1':
                with self.assertRaises(pg.OperationalError) as cm:
                    c.query(query)
                self.assertEqual(
                    str(cm.exception), 'Connection is not established')
        finally:
            c.close()


class TestSimpleQueries(unittest.TestCase):
    """Test simple queries via a basic pg connection."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.doCleanups()
        self.c.close()

    def test_class_name(self):
        r = self.c.query("select 1")
        self.assertEqual(r.__class__.__name__, 'Query')

    def test_module_name(self):
        r = self.c.query("select 1")
        self.assertEqual(r.__class__.__module__, 'pg')

    def test_str(self):
        q = ("select 1 as a, 'hello' as h, 'w' as world"
             " union select 2, 'xyz', 'uvw'")
        r = self.c.query(q)
        self.assertEqual(
            str(r),
            'a|  h  |world\n'
            '-+-----+-----\n'
            '1|hello|w    \n'
            '2|xyz  |uvw  \n'
            '(2 rows)')

    def test_repr(self):
        r = repr(self.c.query("select 1"))
        self.assertTrue(r.startswith('<pg.Query object'), r)

    def test_select0(self):
        q = "select 0"
        self.c.query(q)

    def test_select0_semicolon(self):
        q = "select 0;"
        self.c.query(q)

    def test_select_dot_semicolon(self):
        q = "select .;"
        self.assertRaises(pg.DatabaseError, self.c.query, q)

    def test_getresult(self):
        q = "select 0"
        result = [(0,)]
        r = self.c.query(q).getresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, tuple)
        self.assertIsInstance(v[0], int)
        self.assertEqual(r, result)

    def test_getresult_long(self):
        q = "select 9876543210"
        result = 9876543210
        self.assertIsInstance(result, int)
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, int)
        self.assertEqual(v, result)

    def test_getresult_decimal(self):
        q = "select 98765432109876543210"
        result = Decimal(98765432109876543210)
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, Decimal)
        self.assertEqual(v, result)

    def test_getresult_string(self):
        result = 'Hello, world!'
        q = f"select '{result}'"
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_getresult_async(self):
        q = "select 0"
        result = [(0,)]
        query = self.c.send_query(q)
        r = query.getresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, tuple)
        self.assertIsInstance(v[0], int)
        self.assertEqual(r, result)
        self.assertIsNone(query.getresult())

    def test_dictresult(self):
        q = "select 0 as alias0"
        result = [{'alias0': 0}]
        r = self.c.query(q).dictresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, dict)
        self.assertIsInstance(v['alias0'], int)
        self.assertEqual(r, result)

    def test_dictresult_long(self):
        q = "select 9876543210 as longjohnsilver"
        result = 9876543210
        self.assertIsInstance(result, int)
        v = self.c.query(q).dictresult()[0]['longjohnsilver']
        self.assertIsInstance(v, int)
        self.assertEqual(v, result)

    def test_dictresult_decimal(self):
        q = "select 98765432109876543210 as longjohnsilver"
        result = Decimal(98765432109876543210)
        v = self.c.query(q).dictresult()[0]['longjohnsilver']
        self.assertIsInstance(v, Decimal)
        self.assertEqual(v, result)

    def test_dictresult_string(self):
        result = 'Hello, world!'
        q = f"select '{result}' as greeting"
        v = self.c.query(q).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresult_async(self):
        q = "select 0 as alias0"
        result = [{'alias0': 0}]
        query = self.c.send_query(q)
        r = query.dictresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, dict)
        self.assertIsInstance(v['alias0'], int)
        self.assertEqual(r, result)
        self.assertIsNone(query.dictresult())

    def test_namedresult(self):
        q = "select 0 as alias0"
        result = [(0,)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('alias0',))
        self.assertEqual(v.alias0, 0)

    def test_namedresult_with_good_fieldnames(self):
        q = 'select 1 as snake_case_alias, 2 as "CamelCaseAlias"'
        result = [(1, 2)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('snake_case_alias', 'CamelCaseAlias'))

    def test_namedresult_with_bad_fieldnames(self):
        t = namedtuple('Bad', ['?'] * 6, rename=True)  # type: ignore[misc]
        # noinspection PyUnresolvedReferences
        fields = t._fields
        q = ('select 3 as "0alias", 4 as _alias, 5 as "alias$", 6 as "alias?",'
             ' 7 as "kebap-case-alias", 8 as break, 9 as and_a_good_one')
        result = [tuple(range(3, 10))]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields[:6], fields)
        self.assertEqual(v._fields[6], 'and_a_good_one')

    def test_namedresult_async(self):
        q = "select 0 as alias0"
        query = self.c.send_query(q)
        result = [(0,)]
        r = query.namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('alias0',))
        self.assertEqual(v.alias0, 0)
        self.assertIsNone(query.namedresult())
        self.assertIsNone(query.namedresult())

    def test_list_fields_after_second_get_result_async(self):
        q = "select 1 as one"
        query = self.c.send_query(q)
        self.assertEqual(query.getresult(), [(1,)])
        self.assertEqual(query.listfields(), ('one',))
        self.assertIsNone(query.getresult())
        self.assertEqual(query.listfields(), ())
        self.assertIsNone(query.getresult())
        self.assertEqual(query.listfields(), ())

    def test_get3_cols(self):
        q = "select 1,2,3"
        result = [(1, 2, 3)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def test_get3_dict_cols(self):
        q = "select 1 as a,2 as b,3 as c"
        result = [dict(a=1, b=2, c=3)]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def test_get3_named_cols(self):
        q = "select 1 as a,2 as b,3 as c"
        result = [(1, 2, 3)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('a', 'b', 'c'))
        self.assertEqual(v.b, 2)

    def test_get3_rows(self):
        q = "select 3 union select 1 union select 2 order by 1"
        result = [(1,), (2,), (3,)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def test_get3_dict_rows(self):
        q = ("select 3 as alias3"
             " union select 1 union select 2 order by 1")
        result = [{'alias3': 1}, {'alias3': 2}, {'alias3': 3}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def test_get3_named_rows(self):
        q = ("select 3 as alias3"
             " union select 1 union select 2 order by 1")
        result = [(1,), (2,), (3,)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        for v in r:
            self.assertEqual(v._fields, ('alias3',))

    def test_dictresult_names(self):
        q = "select 'MixedCase' as MixedCaseAlias"
        result = [{'mixedcasealias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)
        q = "select 'MixedCase' as \"MixedCaseAlias\""
        result = [{'MixedCaseAlias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def test_namedresult_names(self):
        q = "select 'MixedCase' as MixedCaseAlias"
        result = [('MixedCase',)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('mixedcasealias',))
        self.assertEqual(v.mixedcasealias, 'MixedCase')
        q = "select 'MixedCase' as \"MixedCaseAlias\""
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('MixedCaseAlias',))
        self.assertEqual(v.MixedCaseAlias, 'MixedCase')

    def test_big_getresult(self):
        num_cols = 100
        num_rows = 100
        q = "select " + ','.join(map(str, range(num_cols)))
        q = ' union all '.join((q,) * num_rows)
        r = self.c.query(q).getresult()
        result = [tuple(range(num_cols))] * num_rows
        self.assertEqual(r, result)

    def test_listfields(self):
        q = ('select 0 as a, 0 as b, 0 as c,'
             ' 0 as c, 0 as b, 0 as a,'
             ' 0 as lowercase, 0 as UPPERCASE,'
             ' 0 as MixedCase, 0 as "MixedCase",'
             ' 0 as a_long_name_with_underscores,'
             ' 0 as "A long name with Blanks"')
        r = self.c.query(q).listfields()
        self.assertIsInstance(r, tuple)
        result = ('a', 'b', 'c', 'c', 'b', 'a',
                  'lowercase', 'uppercase', 'mixedcase', 'MixedCase',
                  'a_long_name_with_underscores', 'A long name with Blanks')
        self.assertEqual(r, result)

    def test_fieldname(self):
        q = "select 0 as z, 0 as a, 0 as x, 0 as y"
        r = self.c.query(q).fieldname(2)
        self.assertEqual(r, 'x')
        r = self.c.query(q).fieldname(3)
        self.assertEqual(r, 'y')

    def test_fieldname_out_of_range(self):
        q = self.c.query("select 0 as x")
        self.assertRaises(ValueError, q.fieldname, 1)
        # negative numbers crashed before
        self.assertRaises(ValueError, q.fieldname, -1)

    def test_fieldnum(self):
        q = "select 1 as x"
        self.assertRaises(ValueError, self.c.query(q).fieldnum, 'y')
        q = "select 1 as x"
        r = self.c.query(q).fieldnum('x')
        self.assertIsInstance(r, int)
        self.assertEqual(r, 0)
        q = "select 0 as z, 0 as a, 0 as x, 0 as y"
        r = self.c.query(q).fieldnum('x')
        self.assertIsInstance(r, int)
        self.assertEqual(r, 2)
        r = self.c.query(q).fieldnum('y')
        self.assertIsInstance(r, int)
        self.assertEqual(r, 3)
        r = self.c.query(q).fieldnum(b'y')
        self.assertIsInstance(r, int)
        self.assertEqual(r, 3)
        self.assertRaises(TypeError, self.c.query(q).fieldnum, 3)
        # names were truncated at NUL characters
        self.assertRaises(ValueError, self.c.query(q).fieldnum, 'y\0z')
        self.assertRaises(ValueError, self.c.query(q).fieldinfo, b'y\0z')

    def test_fieldnum_mixed_case(self):
        q = self.c.query('select 1 as "Xy", 2 as xy2')
        self.assertEqual(q.listfields(), ('Xy', 'xy2'))
        # names that are not quoted are converted to lower case, so this
        # failed before, now names are also compared with the field names
        self.assertEqual(q.fieldnum('Xy'), 0)
        self.assertEqual(q.fieldinfo('Xy')[0], 'Xy')
        self.assertEqual(q.fieldnum('"Xy"'), 0)
        self.assertEqual(q.fieldnum('XY2'), 1)
        self.assertRaises(ValueError, q.fieldnum, 'xy')
        self.assertRaises(ValueError, q.fieldnum, '"xy"')
        self.assertEqual(q.fieldnum(b'Xy'), 0)
        s = self.c.source()
        s.execute('select 1 as "Xy"')
        self.assertEqual(s.fieldinfo('Xy'), (0, 'Xy', 23, 4, -1))

    def test_fieldnum_names_differing_in_case(self):
        q = self.c.query('select 1 as a, 2 as "A", 3 as "B", 4 as b')
        names = q.listfields()
        self.assertEqual(names, ('a', 'A', 'B', 'b'))
        # the names from listfields() are always found
        self.assertEqual([q.fieldnum(name) for name in names], [0, 1, 2, 3])
        self.assertEqual([q.fieldinfo(name)[0] for name in names], list(names))
        self.assertEqual(q.fieldnum('"A"'), 1)
        self.assertEqual(q.fieldnum('"b"'), 3)
        self.assertEqual(q.fieldnum(b'B'), 2)

    def test_field_info_name(self):
        q = ('select true as FooBar, 42::smallint as "FooBar",'
             ' 4.2::numeric(4,2) as foo_bar, \'baz\'::char(3) as "Foo Bar"')
        f = self.c.query(q).fieldinfo
        result = (('foobar', 16, 1, -1), ('FooBar', 21, 2, -1),
                  ('foo_bar', 1700, -1, ((4 << 16) | 2) + 4),
                  ('Foo Bar', 1042, -1, 3 + 4))
        r = f()
        self.assertIsInstance(r, tuple)
        self.assertEqual(len(r), 4)
        self.assertEqual(r, result)
        for field_num, info in enumerate(result):
            field_name = info[0]
            if field_num > 0:
                field_name = f'"{field_name}"'
            r = f(field_name)
            self.assertIsInstance(r, tuple)
            self.assertEqual(len(r), 4)
            self.assertEqual(r, info)
            r = f(field_num)
            self.assertIsInstance(r, tuple)
            self.assertEqual(len(r), 4)
            self.assertEqual(r, info)
        self.assertRaises(IndexError, f, 'foobaz')
        self.assertRaises(IndexError, f, '"Foobar"')
        self.assertRaises(IndexError, f, -1)
        self.assertRaises(IndexError, f, 4)

    def test_len(self):
        q = "select 1 where false"
        self.assertEqual(len(self.c.query(q)), 0)
        q = ("select 1 as a, 2 as b, 3 as c, 4 as d"
             " union select 5 as a, 6 as b, 7 as c, 8 as d")
        self.assertEqual(len(self.c.query(q)), 2)
        q = ("select 1 union select 2 union select 3"
             " union select 4 union select 5 union select 6")
        self.assertEqual(len(self.c.query(q)), 6)

    def test_query(self):
        query = self.c.query
        query("drop table if exists test_table")
        self.addCleanup(query, "drop table test_table")
        q = "create table test_table (n integer)"
        r = query(q)
        self.assertIsNone(r)
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
        if self.c.server_version >= 120000:
            self.skipTest("database does not support tables with oids")
        query = self.c.query
        query("drop table if exists test_table")
        self.addCleanup(query, "drop table test_table")
        q = "create table test_table (n integer) with oids"
        r = query(q)
        self.assertIsNone(r)
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
        self.assertIsInstance(r, int)
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

    def test_mem_size(self):
        # noinspection PyUnresolvedReferences
        if pg.get_pqlib_version() < 120000:
            self.skipTest("pqlib does not support memsize()")
        query = self.c.query
        q = query("select repeat('foo!', 8)")
        size = q.memsize()
        self.assertIsInstance(size, int)
        self.assertGreaterEqual(size, 32)
        self.assertLess(size, 8000)
        q = query("select repeat('foo!', 2000)")
        size = q.memsize()
        self.assertGreaterEqual(size, 8000)
        self.assertLess(size, 16000)


class TestUnicodeQueries(unittest.TestCase):
    """Test unicode strings as queries via a basic pg connection."""

    def setUp(self):
        self.c = connect()
        self.c.query('set client_encoding=utf8')

    def tearDown(self):
        self.c.close()

    def test_getresul_ascii(self):
        result = 'Hello, world!'
        cmd = f"select '{result}'"
        v = self.c.query(cmd).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresul_ascii(self):
        result = 'Hello, world!'
        cmd = f"select '{result}' as greeting"
        v = self.c.query(cmd).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_getresult_utf8(self):
        result = 'Hello, wörld & мир!'
        cmd = f"select '{result}'"
        # pass the query as unicode
        try:
            v = self.c.query(cmd).getresult()[0][0]
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support utf8")
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode()
        v = self.c.query(cmd_bytes).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresult_utf8(self):
        result = 'Hello, wörld & мир!'
        cmd = f"select '{result}' as greeting"
        try:
            v = self.c.query(cmd).dictresult()[0]['greeting']
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support utf8")
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode()
        v = self.c.query(cmd_bytes).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_getresult_latin1(self):
        try:
            self.c.query('set client_encoding=latin1')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        result = 'Hello, wörld!'
        cmd = f"select '{result}'"
        v = self.c.query(cmd).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('latin1')
        v = self.c.query(cmd_bytes).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresult_latin1(self):
        try:
            self.c.query('set client_encoding=latin1')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        result = 'Hello, wörld!'
        cmd = f"select '{result}' as greeting"
        v = self.c.query(cmd).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('latin1')
        v = self.c.query(cmd_bytes).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_getresult_cyrillic(self):
        try:
            self.c.query('set client_encoding=iso_8859_5')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support cyrillic")
        result = 'Hello, мир!'
        cmd = f"select '{result}'"
        v = self.c.query(cmd).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('cyrillic')
        v = self.c.query(cmd_bytes).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresult_cyrillic(self):
        try:
            self.c.query('set client_encoding=iso_8859_5')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support cyrillic")
        result = 'Hello, мир!'
        cmd = f"select '{result}' as greeting"
        v = self.c.query(cmd).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('cyrillic')
        v = self.c.query(cmd_bytes).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_getresult_latin9(self):
        try:
            self.c.query('set client_encoding=latin9')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin9")
        result = 'smœrebrœd with pražská šunka (pay in ¢, £, €, or ¥)'
        cmd = f"select '{result}'"
        v = self.c.query(cmd).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('latin9')
        v = self.c.query(cmd_bytes).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_dictresult_latin9(self):
        try:
            self.c.query('set client_encoding=latin9')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin9")
        result = 'smœrebrœd with pražská šunka (pay in ¢, £, €, or ¥)'
        cmd = f"select '{result}' as menu"
        v = self.c.query(cmd).dictresult()[0]['menu']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)
        cmd_bytes = cmd.encode('latin9')
        v = self.c.query(cmd_bytes).dictresult()[0]['menu']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def test_client_encodings(self):
        # the PostgreSQL names of these encodings were passed to Python,
        # which does not know many of them (e.g. WIN1252, KOI8R, SJIS);
        # JOHAB with Hangul does not work, the server rejects the input
        if self.c.parameter('server_encoding') != 'UTF8':
            self.skipTest("server can only convert some encodings")
        query = self.c.query
        for encoding, char in (
                ('BIG5', '中'), ('EUC_CN', '中'), ('EUC_JIS_2004', 'あ'),
                ('EUC_JP', 'あ'), ('EUC_KR', '한'), ('GB18030', '中'),
                ('GBK', '中'), ('ISO_8859_5', 'ж'), ('ISO_8859_6', 'ع'),
                ('ISO_8859_7', 'λ'), ('ISO_8859_8', 'א'), ('JOHAB', '中'),
                ('KOI8R', 'ж'), ('KOI8U', 'ї'), ('LATIN1', 'ä'),
                ('LATIN2', 'ő'), ('LATIN3', 'ĝ'), ('LATIN4', 'ā'),
                ('LATIN5', 'ğ'), ('LATIN6', 'ŋ'), ('LATIN7', 'ą'),
                ('LATIN8', 'ŵ'), ('LATIN9', '€'), ('LATIN10', 'ș'),
                ('SHIFT_JIS_2004', 'あ'), ('SJIS', 'あ'), ('UHC', '한'),
                ('UTF8', '€'), ('WIN866', 'ж'), ('WIN874', 'ก'),
                ('WIN1250', 'ő'), ('WIN1251', 'ж'), ('WIN1252', '€'),
                ('WIN1253', 'λ'), ('WIN1254', 'ğ'), ('WIN1255', 'א'),
                ('WIN1256', 'ع'), ('WIN1257', 'ą'), ('WIN1258', 'ư')):
            with self.subTest(encoding=encoding):
                query(f'set client_encoding={encoding}'.encode())
                code = ord(char)
                r = query(f"select chr({code}), '{char}' = chr({code}),"
                          f" $1::text = chr({code})", (char,)).getresult()
                self.assertEqual(r, [(char, True, True)])
                r = query(f'select 1 as "{char}"').listfields()
                self.assertEqual(r, (char,))

    def test_client_encoding_without_codec(self):
        # EUC_TW is not supported by Python, so it is handled like SQL_ASCII
        try:
            self.c.query('set client_encoding=euc_tw')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support euc_tw")
        query = self.c.query
        # a warning is issued once when the client encoding is used
        with self.assertWarns(UnicodeWarning) as cm:
            r = query("select 'abc', $1::text", ('def',)).getresult()
        self.assertEqual(r, [('abc', 'def')])
        self.assertIn('EUC_TW is not supported', str(cm.warning))
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            self.assertRaises(UnicodeEncodeError, query, "select '中'")
            self.assertRaises(
                UnicodeEncodeError, query, 'select $1', ('中',))
            if self.c.parameter('server_encoding') == 'UTF8':
                r = query('select chr(20013)').singlescalar()
                self.assertEqual(r, b'\xc4\xe3')  # 中 in EUC_TW
                r = query(b'select $1::text = chr(20013)', (r,)
                          ).singlescalar()
                self.assertIs(r, True)
                # field names that are not valid UTF-8 contain escapes,
                # they could not be passed back before
                q = query(b'select 1 as "\xc4\xe3"')
                self.assertEqual(q.listfields(), ('\\xc4\\xe3',))
                self.assertEqual(q.fieldnum('\\xc4\\xe3'), 0)
            self.assertEqual(self.c.escape_literal("a'b"), "'a''b'")
            self.assertRaises(
                UnicodeEncodeError, self.c.escape_literal, '中')
            # the warning is issued again after the encoding changed
            query('set client_encoding=utf8')
            query("select 'abc'")
            query('set client_encoding=euc_tw')
            self.assertRaises(UnicodeWarning, query, "select 'abc'")
        # the warning can also be filtered or turned into an error
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.assertEqual(query("select 'abc'").singlescalar(), 'abc')

    def test_field_names_latin1(self):
        try:
            self.c.query('set client_encoding=latin1')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        # field names were decoded as UTF-8 (and namedresult crashed)
        q = self.c.query('select 1 as "größe", \'wär\'::text as "ä"')
        self.assertEqual(q.listfields(), ('größe', 'ä'))
        self.assertEqual(q.fieldname(0), 'größe')
        self.assertEqual(q.fieldnum('größe'), 0)
        self.assertEqual(q.fieldnum('"ä"'), 1)
        self.assertEqual(q.fieldnum('größe'.encode('latin1')), 0)
        self.assertEqual(q.fieldinfo('größe'), ('größe', 23, 4, -1))
        self.assertEqual(q.fieldinfo()[1], ('ä', 25, -1, -1))
        self.assertEqual(q.dictresult(), [{'größe': 1, 'ä': 'wär'}])
        self.assertEqual(q.namedresult()[0]._fields, ('größe', 'ä'))
        # the query was also decoded as UTF-8 when printing it
        self.assertEqual(str(q).splitlines()[0], 'größe| ä ')
        self.assertEqual(str(q).splitlines()[2], '    1|wär')


class TestServerStringsLatin1(unittest.TestCase):
    """Test strings from the server with the client encoding LATIN1."""

    def setUp(self):
        self.c = connect()
        try:
            self.c.query('set client_encoding=latin1')
        except (pg.DataError, pg.NotSupportedError):
            self.c.close()
            self.skipTest("database does not support latin1")

    def tearDown(self):
        self.c.close()

    def test_error_attribute(self):
        with self.assertRaises(pg.InternalError):
            self.c.query("do $$ begin raise exception 'wärme'; end $$")
        # this raised UnicodeDecodeError before
        self.assertIn('wärme', self.c.error)

    def test_parameter(self):
        if self.c.server_version < 180000:
            self.skipTest('search_path is reported since PostgreSQL 18')
        self.c.query('set search_path = "schäma"')
        # this raised UnicodeDecodeError before
        self.assertEqual(self.c.parameter('search_path'), '"schäma"')

    def test_getnotify(self):
        query = self.c.query
        query('listen "kanäl"')
        query("notify \"kanäl\", 'wärme'")
        # this raised UnicodeDecodeError before
        self.assertEqual(self.c.getnotify(),
                         ('kanäl', self.c.backend_pid, 'wärme'))

    def test_getline(self):
        self.c.query("copy (values ('wärme'), ('')) to stdout")
        # this raised UnicodeDecodeError before
        self.assertEqual(self.c.getline(), 'wärme')
        self.assertEqual(self.c.getline(), '')
        self.assertIsNone(self.c.getline())

    def test_putdata_error_messages(self):
        self.c.query("create temporary table test_copy (n int)")
        source = self.c.source()
        source.execute("copy test_copy from stdin")
        self.assertIsNone(source.putdata('wärme\n'))
        # these raised UnicodeDecodeError before
        with self.assertRaises(OSError) as cm:
            source.putdata(None)
        self.assertIn('"wärme"', str(cm.exception))
        source.execute("copy test_copy from stdin")
        with self.assertRaises(OSError) as cm:
            source.putdata(ValueError('wärme'))
        self.assertEqual(str(cm.exception), 'wärme')


class TestStringsSentToServer(unittest.TestCase):
    """Test encoding and checking strings that are sent to the server."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def set_latin1(self):
        try:
            self.c.query('set client_encoding=latin1')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")

    def test_query_with_nul(self):
        query = self.c.query

        class S:
            def __str__(self):
                return 'a\0b'

        # these were silently truncated before
        self.assertRaises(ValueError, query, 'select 1\0select 2')
        self.assertRaises(ValueError, query, b'select 1\0select 2')
        self.assertRaises(ValueError, query, 'select $1', ['a\0b'])
        self.assertRaises(ValueError, query, 'select $1', [b'a\0b'])
        self.assertRaises(ValueError, query, 'select $1', [S()])
        self.assertRaises(ValueError, self.c.send_query, 'select 1\0')
        self.assertRaises(
            ValueError, self.c.send_query, 'select $1', ['a\0b'])
        self.assertRaises(ValueError, self.c.query_prepared, 'q\0x')
        self.c.prepare('q', 'select $1::text')
        self.assertRaises(ValueError, self.c.query_prepared, 'q', ['a\0b'])
        source = self.c.source()
        self.assertRaises(ValueError, source.execute, 'select 1\0select 2')
        self.assertEqual(query('select $1::text', ['ab']).singlescalar(), 'ab')

    def test_escape_with_nul(self):
        # these were silently truncated before
        for escape in (self.c.escape_string, self.c.escape_literal,
                       self.c.escape_identifier, pg.escape_string):
            self.assertRaises(ValueError, escape, 'a\0b')
            self.assertRaises(ValueError, escape, b'a\0b')
        self.assertEqual(self.c.escape_bytea(b'a\0b'), b'\\x610062')
        db = pg.DB(self.c)
        self.assertRaises(
            ValueError, db.query_formatted, 'select %s', ['a\0b'],
            inline=True)

    def test_query_with_params_changed_by_str(self):
        params: list = []

        class S:
            def __str__(self) -> str:
                params.clear()
                gc.collect()
                return 'x'

        params.extend([S()] + [object() for _i in range(2000)])
        # this crashed before (the converted items were freed)
        self.assertRaises(pg.DatabaseError, self.c.query, 'select 1', params)

    def test_query_with_params_from_iterator(self):
        # this crashed before
        q = self.c.query('select $1::text', (p for p in [['abc']]))
        self.assertEqual(q.getresult(), [('abc',)])

    def test_query_with_other_params_latin1(self):
        self.set_latin1()

        class S:
            def __init__(self, s: str) -> None:
                self.s = s

            def __str__(self) -> str:
                return self.s

        query = self.c.query
        # these were encoded as UTF-8 before
        self.assertEqual(
            query('select $1::text', [S('wärme')]).singlescalar(), 'wärme')
        self.assertRaises(
            UnicodeEncodeError, query, 'select $1::text', [S('мир')])

    def test_prepare_latin1(self):
        self.set_latin1()
        prepare, describe = self.c.prepare, self.c.describe_prepared
        query = self.c.query_prepared
        # the name and the query were encoded as UTF-8 before,
        # but query_prepared() used the client encoding
        self.assertIsNone(prepare('größe', 'select \'wärme\' as "ä"'))
        self.assertEqual(query('größe').getresult(), [('wärme',)])
        self.assertEqual(describe('größe').listfields(), ('ä',))
        self.assertIsNone(prepare(b'kl\xe4in', b"select 'k\xe4lte'"))
        self.assertEqual(query('kläin').singlescalar(), 'kälte')
        self.assertEqual(describe(b'kl\xe4in').listfields(), ('?column?',))
        self.assertRaises(UnicodeEncodeError, prepare, 'мир', 'select 1')
        self.assertRaises(UnicodeEncodeError, prepare, 'q', "select 'мир'")
        self.assertRaises(UnicodeEncodeError, describe, 'мир')

    def test_prepare_with_nul(self):
        prepare, describe = self.c.prepare, self.c.describe_prepared
        # these were silently truncated before
        self.assertRaises(ValueError, prepare, 'q\0x', 'select 1')
        self.assertRaises(ValueError, prepare, 'q', 'select 1\0select 2')
        self.assertRaises(ValueError, describe, 'q\0x')
        self.assertRaises(TypeError, prepare, 'q')
        self.assertRaises(TypeError, prepare, 'q', 1)
        self.assertRaises(TypeError, describe, 1)

    def test_putline_latin1(self):
        self.set_latin1()
        query, putline = self.c.query, self.c.putline
        query('create temporary table test_copy (t text)')
        query('copy test_copy from stdin')
        try:
            # this was encoded as UTF-8 before
            putline('wärme\n')
            putline(b'k\xe4lte\n')
            self.assertRaises(UnicodeEncodeError, putline, 'мир\n')
            self.assertRaises(TypeError, putline, 1)
        finally:
            self.c.endcopy()
        r = query('select t from test_copy').getresult()
        self.assertEqual(sorted(r), [('kälte',), ('wärme',)])

    def test_putdata_error_is_always_sent(self):
        self.set_latin1()
        self.c.query('create temporary table test_copy (n int)')
        source = self.c.source()
        source.execute('copy test_copy from stdin')
        # characters that cannot be encoded raised UnicodeEncodeError before
        # and the copy was not aborted, NUL characters truncated the message
        with self.assertRaises(OSError) as cm:
            source.putdata(ValueError('wärme ошибка\0x'))
        escaped = 'ошибка'.encode('ascii', 'backslashreplace').decode()
        self.assertEqual(str(cm.exception), f'wärme {escaped}\\x00x')
        self.assertEqual(self.c.transaction(), pg.TRANS_IDLE)

    def test_parameter_name(self):
        parameter = self.c.parameter
        encoding = parameter('client_encoding')
        self.assertIsNotNone(encoding)
        # bytes were not accepted before
        self.assertEqual(parameter(b'client_encoding'), encoding)
        self.assertIsNone(parameter('größe'))
        self.assertRaises(ValueError, parameter, 'client_encoding\0x')
        self.assertRaises(TypeError, parameter, 1)

    def test_connect_with_defaults(self):
        old = (pg.get_defbase(), pg.get_defhost(),
               pg.get_defport(), pg.get_defuser())

        def restore_defaults():
            pg.set_defbase(old[0])
            pg.set_defhost(old[1])
            pg.set_defport(old[2])
            pg.set_defuser(old[3])
            pg.set_defpasswd(None)

        self.addCleanup(restore_defaults)
        pg.set_defbase(dbname)
        pg.set_defhost(dbhost)
        pg.set_defport(dbport)
        pg.set_defuser(dbuser)
        pg.set_defpasswd(dbpasswd)
        # this raised SystemError before
        c = pg.connect()
        try:
            self.assertEqual(c.db, dbname)
            self.assertEqual(c.query('select 1').singlescalar(), 1)
        finally:
            c.close()

    def test_connect_with_bytes_defaults(self):
        old = (pg.get_defbase(), pg.get_defhost(),
               pg.get_defport(), pg.get_defuser())

        def restore_defaults():
            pg.set_defbase(old[0])
            pg.set_defhost(old[1])
            pg.set_defport(old[2])
            pg.set_defuser(old[3])
            pg.set_defpasswd(None)

        self.addCleanup(restore_defaults)
        # bytes were not accepted as defaults before
        pg.set_defbase(dbname.encode())
        pg.set_defhost(dbhost.encode() if dbhost else None)
        pg.set_defport(dbport)
        pg.set_defuser(dbuser.encode() if dbuser else None)
        pg.set_defpasswd(dbpasswd.encode() if dbpasswd else None)
        c = pg.connect()
        try:
            self.assertEqual(c.db, dbname)
        finally:
            c.close()


class TestUndecodableText(unittest.TestCase):
    """Test text that cannot be decoded with the client encoding."""

    def setUp(self):
        self.c = connect()
        # with SQL_ASCII, the server sends text without conversion
        self.c.query('set client_encoding=sql_ascii')
        # chr(228) is non-ASCII in all server encodings
        self.raw = self.c.query("select chr(228)").singlescalar()
        self.assertIsInstance(self.raw, bytes)
        # names and messages are decoded as UTF-8 with escapes instead
        self.text = self.raw.decode('utf-8', 'backslashreplace')

    def tearDown(self):
        self.c.close()

    def test_text_as_bytes(self):
        # this raised SystemError before
        raw = self.raw
        q = "select 'abc' as a, 'w' || chr(228) || 'r' as b"
        self.assertEqual(self.c.query(q).getresult(),
                         [('abc', b'w' + raw + b'r')])
        self.assertEqual(self.c.query(q).dictresult(),
                         [{'a': 'abc', 'b': b'w' + raw + b'r'}])
        self.assertEqual(self.c.query(q).namedresult(),
                         [('abc', b'w' + raw + b'r')])
        self.assertEqual(list(self.c.query(q)), [('abc', b'w' + raw + b'r')])
        q = "select ('w' || chr(228))::varchar, (chr(228) || 'r')::name"
        self.assertEqual(self.c.query(q).getresult(),
                         [(b'w' + raw, raw + b'r')])

    def test_text_array_as_bytes(self):
        raw = self.raw
        q = "select array['abc', 'w' || chr(228) || 'r', null]"
        self.assertEqual(self.c.query(q).singlescalar(),
                         ['abc', b'w' + raw + b'r', None])

    def test_json_as_bytes(self):
        jsondecode = pg.get_jsondecode()
        self.addCleanup(pg.set_jsondecode, jsondecode)
        decoded = []

        def decode(s: str | bytes) -> list:
            decoded.append(s)
            return decoded

        pg.set_jsondecode(decode)
        q = "select json_build_array(chr(228))"
        self.assertIs(self.c.query(q).singlescalar(), decoded)
        self.assertEqual(decoded, [b'["' + self.raw + b'"]'])

    def test_cast_hook_gets_bytes(self):
        received = []

        def cast_hook(value: Any, pgtype: int) -> Any:
            received.append((value, pgtype))
            return value

        self.c.set_cast_hook(cast_hook)
        q = "select row('w' || chr(228))::record"
        self.assertEqual(self.c.query(q).singlescalar(),
                         b'(w' + self.raw + b')')
        self.assertEqual(received, [(b'(w' + self.raw + b')', 2249)])

    def test_error_message(self):
        q = "do $$ begin raise exception 'w%r', chr(228); end $$"
        with self.assertRaises(pg.InternalError) as cm:
            # this raised SystemError before
            self.c.query(q)
        error = cm.exception
        self.assertIsInstance(error.args[0], str)
        self.assertIn(f'w{self.text}r', error.args[0])
        self.assertEqual(error.sqlstate, 'P0001')

    def test_field_names(self):
        raw, text = self.raw, self.text
        q = self.c.query(b'select 1 as "w' + raw + b'r", 2 as b')
        # this raised SystemError before
        self.assertEqual(q.listfields(), (f'w{text}r', 'b'))
        self.assertEqual(q.fieldname(0), f'w{text}r')
        self.assertEqual(q.fieldnum(b'"w' + raw + b'r"'), 0)
        self.assertEqual(q.fieldinfo(0), (f'w{text}r', 23, 4, -1))
        self.assertEqual(q.fieldinfo()[0], (f'w{text}r', 23, 4, -1))
        self.assertEqual(q.dictresult(), [{f'w{text}r': 1, 'b': 2}])
        self.assertEqual(q.namedresult(), [(1, 2)])
        # names are looked up like they are decoded, i.e. with UTF-8
        # instead of SQL_ASCII, and also compared with the decoded names,
        # so the names from listfields() can be used even if they are not
        # valid UTF-8 and contain escapes (this failed before)
        self.assertEqual(q.fieldnum(f'w{text}r'), 0)
        self.assertEqual(q.fieldinfo(f'w{text}r')[0], f'w{text}r')
        if self.c.parameter('server_encoding') == 'UTF8':
            self.assertEqual(q.fieldnum(f'"w{text}r"'), 0)
            self.assertEqual(q.fieldinfo(f'"w{text}r"')[0], f'w{text}r')
        else:  # quoted names with escapes cannot be found
            self.assertRaises(ValueError, q.fieldnum, f'"w{text}r"')
        self.assertRaises(ValueError, q.fieldnum, '"wör"')

    def test_error_attribute(self):
        q = "do $$ begin raise exception 'w%r', chr(228); end $$"
        with self.assertRaises(pg.InternalError):
            self.c.query(q)
        # this raised UnicodeDecodeError before with non-UTF8 servers
        self.assertIn(f'w{self.text}r', self.c.error)

    def test_parameter(self):
        if self.c.server_version < 180000:
            self.skipTest('search_path is reported since PostgreSQL 18')
        self.c.query(b'set search_path = "s' + self.raw + b'"')
        # this raised UnicodeDecodeError before with non-UTF8 servers
        self.assertEqual(self.c.parameter('search_path'), f'"s{self.text}"')

    def test_getnotify(self):
        query = self.c.query
        channel = b'"k' + self.raw + b'"'
        query(b'listen ' + channel)
        query("select pg_notify('k' || chr(228), 'w' || chr(228))")
        # the channel is a name, the payload is returned like text values
        self.assertEqual(self.c.getnotify(), (
            f'k{self.text}', self.c.backend_pid, b'w' + self.raw))

    def test_getline(self):
        self.c.query("copy (select 'w' || chr(228)) to stdout")
        # this returned str or raised UnicodeDecodeError before
        self.assertEqual(self.c.getline(), b'w' + self.raw)
        self.assertIsNone(self.c.getline())

    def test_getdata(self):
        source = self.c.source()
        source.execute("copy (select 'w' || chr(228)) to stdout")
        # this raised UnicodeDecodeError before
        self.assertEqual(source.getdata(True), b'w' + self.raw + b'\n')
        self.assertEqual(source.getdata(True), 1)

    def test_str(self):
        q = self.c.query("select 'w' || chr(228) || 'r' as a")
        self.assertEqual(str(q).splitlines()[2], f'w{self.text}r')

    def test_source(self):
        raw, text = self.raw, self.text
        s = self.c.source()
        s.execute(b"select 'w' || chr(228) || 'r' as \"" + raw + b'"')
        self.assertEqual(s.listinfo(), ((0, text, 25, -1, -1),))
        self.assertEqual(s.fieldinfo(b'"' + raw + b'"'),
                         (0, text, 25, -1, -1))
        self.assertEqual(s.fieldinfo(text), (0, text, 25, -1, -1))
        # this raised SystemError before
        self.assertEqual(s.fetch(1), [(b'w' + raw + b'r',)])
        s.movefirst()
        # this raised UnicodeDecodeError before
        self.assertEqual(s.field(0), b'w' + raw + b'r')


class TestSourceFields(unittest.TestCase):
    """Test the field methods of source objects."""

    def setUp(self):
        self.c = connect()
        self.c.query('set client_encoding=utf8')
        self.s = self.c.source()

    def tearDown(self):
        self.s.close()
        self.c.close()

    def test_listinfo(self):
        self.s.execute("select 1 as a, 'b'::text as \"MixedCase\"")
        self.assertEqual(self.s.listinfo(), (
            (0, 'a', 23, 4, -1), (1, 'MixedCase', 25, -1, -1)))

    def test_fieldinfo(self):
        self.s.execute("select 1 as a, 'b'::text as \"MixedCase\"")
        fieldinfo = self.s.fieldinfo
        self.assertEqual(fieldinfo(0), (0, 'a', 23, 4, -1))
        self.assertEqual(fieldinfo(1), (1, 'MixedCase', 25, -1, -1))
        # looking up fields by name did not work before
        self.assertEqual(fieldinfo('a'), (0, 'a', 23, 4, -1))
        self.assertEqual(fieldinfo('"MixedCase"'),
                         (1, 'MixedCase', 25, -1, -1))
        self.assertEqual(fieldinfo(b'a'), (0, 'a', 23, 4, -1))
        self.assertRaises(ValueError, fieldinfo, 'b')
        self.assertRaises(ValueError, fieldinfo, 'a\0b')
        self.assertRaises(ValueError, fieldinfo, 2)
        self.assertRaises(TypeError, fieldinfo, 1.0)

    def test_field(self):
        self.s.execute("select 1 as a, 'b'::text as \"MixedCase\"")
        self.assertEqual(self.s.fetch(1), [('1', 'b')])
        self.s.movefirst()
        field = self.s.field
        self.assertEqual(field(0), '1')
        self.assertEqual(field('a'), '1')
        self.assertEqual(field('"MixedCase"'), 'b')
        self.assertEqual(field(b'a'), '1')
        self.assertRaises(ValueError, field, 'c')
        # after the last row, field() crashed before
        self.s.movenext()
        self.assertRaises(IndexError, field, 0)
        self.s.execute("select null as a")
        self.assertEqual(field(0), None)  # was an empty string
        self.s.execute("select 1 as a where false")
        self.assertRaises(IndexError, field, 0)
        self.s.movelast()
        self.assertRaises(IndexError, field, 0)


class TestFakeServer(unittest.TestCase):
    """Test the handling of unusual server responses with a fake server."""

    @staticmethod
    def message(kind: bytes, payload: bytes = b'') -> bytes:
        """Create a message of the frontend/backend protocol."""
        return kind + (len(payload) + 4).to_bytes(4, 'big') + payload

    def serve(self, server: socket.socket, response: bytes) -> None:
        """Accept one connection and send the response to all queries."""
        message = self.message
        conn = server.accept()[0]
        with conn, conn.makefile('rb') as f:
            conn.settimeout(5)
            size = int.from_bytes(f.read(4), 'big')
            f.read(size - 4)  # skip the startup message
            params = b''.join(
                message(b'S', name + b'\0' + value + b'\0')
                for name, value in (
                    (b'server_version', b'19.0'),
                    (b'server_encoding', b'UTF8'),
                    (b'client_encoding', b'UTF8'),
                    (b'DateStyle', b'ISO, MDY'),
                    (b'integer_datetimes', b'on'),
                    (b'standard_conforming_strings', b'on')))
            conn.sendall(message(b'R', bytes(4)) + params
                         + message(b'K', bytes(8)) + message(b'Z', b'I'))
            while True:
                kind = f.read(1)
                if not kind or kind == b'X':  # closed or terminated
                    break
                size = int.from_bytes(f.read(4), 'big')
                f.read(size - 4)
                if kind == b'Q':  # simple query
                    conn.sendall(response)

    def connect(self, response: bytes) -> pg.Connection:
        """Connect to a fake server sending the given query response."""
        server = socket.create_server(('127.0.0.1', 0))
        self.addCleanup(server.close)
        server.settimeout(5)
        port = server.getsockname()[1]
        thread = threading.Thread(
            target=self.serve, args=(server, response), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        connection = pg.connect(
            f'host=127.0.0.1 port={port} dbname=test user=test'
            ' sslmode=disable gssencmode=disable connect_timeout=5')
        self.addCleanup(connection.close)
        return connection

    def test_results_after_error(self):
        message = self.message
        error = message(b'E', b'SERROR\0VERROR\0C42000\0Mboom\0\0')
        done = message(b'C', b'SELECT 0\0')
        connection = self.connect(
            error + done + done + message(b'Z', b'I'))
        query = connection.send_query("select 1")
        # this crashed before when more than one result followed the error
        self.assertRaises(pg.ProgrammingError, query.getresult)
        # the remaining results have been consumed, the query is invalid
        self.assertRaises(TypeError, query.getresult)
        self.assertEqual(connection.transaction(), pg.TRANS_IDLE)


class TestParamQueries(unittest.TestCase):
    """Test queries with parameters via a basic pg connection."""

    def setUp(self):
        self.c = connect()
        self.c.query('set client_encoding=utf8')

    def tearDown(self):
        self.c.close()

    def test_query_with_none_param(self):
        self.assertRaises(TypeError, self.c.query, "select $1", None)
        self.assertRaises(TypeError, self.c.query, "select $1+$2", None, None)
        self.assertEqual(
            self.c.query("select $1::integer", (None,)).getresult(), [(None,)])
        self.assertEqual(
            self.c.query("select $1::text", [None]).getresult(), [(None,)])
        self.assertEqual(
            self.c.query("select $1::text", [[None]]).getresult(), [(None,)])

    def test_query_with_bool_params(self, bool_enabled=None):
        query = self.c.query
        bool_enabled_default = None
        if bool_enabled is not None:
            bool_enabled_default = pg.get_bool()
            pg.set_bool(bool_enabled)
        try:
            bool_on = bool_enabled or bool_enabled is None
            v_false, v_true = (False, True) if bool_on else ('f', 't')
            r_false, r_true = [(v_false,)], [(v_true,)]
            self.assertEqual(query("select false").getresult(), r_false)
            self.assertEqual(query("select true").getresult(), r_true)
            q = "select $1::bool"
            self.assertEqual(query(q, (None,)).getresult(), [(None,)])
            self.assertEqual(query(q, ('f',)).getresult(), r_false)
            self.assertEqual(query(q, ('t',)).getresult(), r_true)
            self.assertEqual(query(q, ('false',)).getresult(), r_false)
            self.assertEqual(query(q, ('true',)).getresult(), r_true)
            self.assertEqual(query(q, ('n',)).getresult(), r_false)
            self.assertEqual(query(q, ('y',)).getresult(), r_true)
            self.assertEqual(query(q, (0,)).getresult(), r_false)
            self.assertEqual(query(q, (1,)).getresult(), r_true)
            self.assertEqual(query(q, (False,)).getresult(), r_false)
            self.assertEqual(query(q, (True,)).getresult(), r_true)
        finally:
            if bool_enabled_default is not None:
                pg.set_bool(bool_enabled_default)

    def test_query_with_bool_params_not_default(self):
        self.test_query_with_bool_params(bool_enabled=not pg.get_bool())

    def test_query_with_int_params(self):
        query = self.c.query
        self.assertEqual(query("select 1+1").getresult(), [(2,)])
        self.assertEqual(query("select 1+$1", (1,)).getresult(), [(2,)])
        self.assertEqual(query("select 1+$1", [1]).getresult(), [(2,)])
        self.assertEqual(query("select $1::integer", (2,)).getresult(), [(2,)])
        self.assertEqual(query("select $1::text", (2,)).getresult(), [('2',)])
        self.assertEqual(
            query("select 1+$1::numeric", [1]).getresult(), [(Decimal('2'),)])
        self.assertEqual(
            query("select 1, $1::integer", (2,)).getresult(), [(1, 2)])
        self.assertEqual(
            query("select 1 union select $1::integer", (2,)).getresult(),
            [(1,), (2,)])
        self.assertEqual(
            query("select $1::integer+$2", (1, 2)).getresult(), [(3,)])
        self.assertEqual(
            query("select $1::integer+$2", [1, 2]).getresult(), [(3,)])
        self.assertEqual(
            query("select 0+$1+$2+$3+$4+$5+$6", list(range(6))).getresult(),
            [(15,)])

    def test_query_with_str_params(self):
        query = self.c.query
        self.assertEqual(
            query("select $1||', world!'", ('Hello',)).getresult(),
            [('Hello, world!',)])
        self.assertEqual(
            query("select $1||', world!'", ['Hello']).getresult(),
            [('Hello, world!',)])
        self.assertEqual(
            query("select $1||', '||$2||'!'", ('Hello', 'world')).getresult(),
            [('Hello, world!',)])
        self.assertEqual(
            query("select $1::text", ('Hello, world!',)).getresult(),
            [('Hello, world!',)])
        self.assertEqual(
            query("select $1::text,$2::text", ('Hello', 'world')).getresult(),
            [('Hello', 'world')])
        self.assertEqual(
            query("select $1::text,$2::text", ['Hello', 'world']).getresult(),
            [('Hello', 'world')])
        self.assertEqual(
            query("select $1::text union select $2::text",
                  ('Hello', 'world')).getresult(),
            [('Hello',), ('world',)])
        try:
            query("select 'wörld'")
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest('database does not support utf8')
        self.assertEqual(
            query("select $1||', '||$2||'!'",
                  ('Hello', 'w\xc3\xb6rld')).getresult(),
            [('Hello, w\xc3\xb6rld!',)])

    def test_query_with_unicode_params(self):
        query = self.c.query
        try:
            query('set client_encoding=utf8')
            self.assertEqual(
                query("select 'wörld'").getresult()[0][0], 'wörld')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support utf8")
        self.assertEqual(
            query("select $1||', '||$2||'!'", ('Hello', 'wörld')).getresult(),
            [('Hello, wörld!',)])

    def test_query_with_unicode_params_latin1(self):
        query = self.c.query
        try:
            query('set client_encoding=latin1')
            self.assertEqual(
                query("select 'wörld'").getresult()[0][0], 'wörld')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        r = query("select $1||', '||$2||'!'", ('Hello', 'wörld')).getresult()
        self.assertEqual(r, [('Hello, wörld!',)])
        self.assertRaises(
            UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', 'мир'))
        query('set client_encoding=iso_8859_1')
        r = query(
            "select $1||', '||$2||'!'", ('Hello', 'wörld')).getresult()
        self.assertEqual(r, [('Hello, wörld!',)])
        self.assertRaises(
            UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', 'мир'))
        query('set client_encoding=sql_ascii')
        self.assertRaises(
            UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', 'wörld'))

    def test_query_with_unicode_params_cyrillic(self):
        query = self.c.query
        try:
            query('set client_encoding=iso_8859_5')
            self.assertEqual(
                query("select 'мир'").getresult()[0][0], 'мир')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support cyrillic")
        self.assertRaises(
            UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', 'wörld'))
        r = query(
            "select $1||', '||$2||'!'", ('Hello', 'мир')).getresult()
        self.assertEqual(r, [('Hello, мир!',)])
        query('set client_encoding=sql_ascii')
        self.assertRaises(
            UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', 'мир!'))

    def test_query_with_mixed_params(self):
        self.assertEqual(
            self.c.query(
                "select $1+2,$2||', world!'", (1, 'Hello')).getresult(),
            [(3, 'Hello, world!')])
        self.assertEqual(
            self.c.query(
                "select $1::integer,$2::date,$3::text",
                (4711, None, 'Hello!')).getresult(),
            [(4711, None, 'Hello!')])

    def test_query_with_duplicate_params(self):
        self.assertRaises(
            pg.ProgrammingError, self.c.query, "select $1+$1", (1,))
        self.assertRaises(
            pg.ProgrammingError, self.c.query, "select $1+$1", (1, 2))

    def test_query_with_zero_params(self):
        self.assertEqual(
            self.c.query("select 1+1", []).getresult(), [(2,)])

    def test_query_with_garbage(self):
        garbage = r"'\{}+()-#[]oo324"
        self.assertEqual(
            self.c.query("select $1::text AS garbage",
                         (garbage,)).dictresult(),
            [{'garbage': garbage}])


class TestPreparedQueries(unittest.TestCase):
    """Test prepared queries via a basic pg connection."""

    def setUp(self):
        self.c = connect()
        self.c.query('set client_encoding=utf8')

    def tearDown(self):
        self.c.close()

    def test_empty_prepared_statement(self):
        self.c.prepare('', '')
        self.assertRaises(ValueError, self.c.query_prepared, '')

    def test_invalid_prepared_statement(self):
        self.assertRaises(pg.ProgrammingError, self.c.prepare, '', 'bad')

    def test_duplicate_prepared_statement(self):
        self.assertIsNone(self.c.prepare('q', 'select 1'))
        self.assertRaises(pg.ProgrammingError, self.c.prepare, 'q', 'select 2')

    def test_non_existent_prepared_statement(self):
        self.assertRaises(
            pg.OperationalError, self.c.query_prepared, 'does-not-exist')

    def test_unnamed_query_without_params(self):
        self.assertIsNone(self.c.prepare('', "select 'anon'"))
        self.assertEqual(self.c.query_prepared('').getresult(), [('anon',)])
        self.assertEqual(self.c.query_prepared('').getresult(), [('anon',)])

    def test_named_query_without_params(self):
        self.assertIsNone(self.c.prepare('hello', "select 'world'"))
        self.assertEqual(
            self.c.query_prepared('hello').getresult(), [('world',)])

    def test_multiple_named_queries_without_params(self):
        self.assertIsNone(self.c.prepare('query17', "select 17"))
        self.assertIsNone(self.c.prepare('query42', "select 42"))
        self.assertEqual(self.c.query_prepared('query17').getresult(), [(17,)])
        self.assertEqual(self.c.query_prepared('query42').getresult(), [(42,)])

    def test_unnamed_query_with_params(self):
        self.assertIsNone(self.c.prepare('', "select $1 || ', ' || $2"))
        self.assertEqual(
            self.c.query_prepared('', ['hello', 'world']).getresult(),
            [('hello, world',)])
        self.assertIsNone(self.c.prepare('', "select 1+ $1 + $2 + $3"))
        self.assertEqual(
            self.c.query_prepared('', [17, -5, 29]).getresult(), [(42,)])

    def test_multiple_named_queries_with_params(self):
        self.assertIsNone(self.c.prepare('q1', "select $1 || '!'"))
        self.assertIsNone(self.c.prepare('q2', "select $1 || '-' || $2"))
        self.assertEqual(
            self.c.query_prepared('q1', ['hello']).getresult(),
            [('hello!',)])
        self.assertEqual(
            self.c.query_prepared('q2', ['he', 'lo']).getresult(),
            [('he-lo',)])

    def test_describe_non_existent_query(self):
        self.assertRaises(
            pg.OperationalError, self.c.describe_prepared, 'does-not-exist')

    def test_describe_unnamed_query(self):
        self.c.prepare('', "select 1::int, 'a'::char")
        r = self.c.describe_prepared('')
        self.assertEqual(r.listfields(), ('int4', 'bpchar'))

    def test_describe_named_query(self):
        self.c.prepare('myquery', "select 1 as first, 2 as second")
        r = self.c.describe_prepared('myquery')
        self.assertEqual(r.listfields(), ('first', 'second'))

    def test_describe_multiple_named_queries(self):
        self.c.prepare('query1', "select 1::int")
        self.c.prepare('query2', "select 1::int, 2::int")
        r = self.c.describe_prepared('query1')
        self.assertEqual(r.listfields(), ('int4',))
        r = self.c.describe_prepared('query2')
        self.assertEqual(r.listfields(), ('int4', 'int4'))


class TestQueryResultTypes(unittest.TestCase):
    """Test proper result types via a basic pg connection."""

    def setUp(self):
        self.c = connect()
        self.c.query('set client_encoding=utf8')
        self.c.query("set datestyle='ISO,YMD'")
        self.c.query("set timezone='UTC'")

    def tearDown(self):
        self.c.close()

    def assert_proper_cast(self, value, pgtype, pytype):
        q = f'select $1::{pgtype}'
        try:
            r = self.c.query(q, (value,)).getresult()[0][0]
        except pg.ProgrammingError as e:
            if pgtype in ('json', 'jsonb'):
                self.skipTest('database does not support json')
            self.fail(str(e))
        # noinspection PyUnboundLocalVariable
        self.assertIsInstance(r, pytype)
        if isinstance(value, str) and (
                not value or ' ' in value or '{' in value):
            value = f'"{value}"'
        value = f'{{{value}}}'
        r = self.c.query(q + '[]', (value,)).getresult()[0][0]
        if pgtype.startswith(('date', 'time', 'interval')):
            # arrays of these are casted by the DB wrapper only
            self.assertEqual(r, value)
        else:
            self.assertIsInstance(r, list)
            self.assertEqual(len(r), 1)
            self.assertIsInstance(r[0], pytype)

    def test_int(self):
        self.assert_proper_cast(0, 'int', int)
        self.assert_proper_cast(0, 'smallint', int)
        self.assert_proper_cast(0, 'oid', int)
        self.assert_proper_cast(0, 'cid', int)
        self.assert_proper_cast(0, 'xid', int)

    def test_long(self):
        self.assert_proper_cast(0, 'bigint', int)

    def test_float(self):
        self.assert_proper_cast(0, 'float', float)
        self.assert_proper_cast(0, 'real', float)
        self.assert_proper_cast(0, 'double precision', float)
        self.assert_proper_cast('infinity', 'float', float)

    def test_numeric(self):
        decimal = pg.get_decimal()
        self.assert_proper_cast(decimal(0), 'numeric', decimal)
        self.assert_proper_cast(decimal(0), 'decimal', decimal)

    def test_money(self):
        decimal = pg.get_decimal()
        self.assert_proper_cast(decimal('0'), 'money', decimal)

    def test_bool(self):
        bool_type = bool if pg.get_bool() else str
        self.assert_proper_cast('f', 'bool', bool_type)

    def test_date(self):
        self.assert_proper_cast('1956-01-31', 'date', str)
        self.assert_proper_cast('10:20:30', 'interval', str)
        self.assert_proper_cast('08:42:15', 'time', str)
        self.assert_proper_cast('08:42:15+00', 'timetz', str)
        self.assert_proper_cast('1956-01-31 08:42:15', 'timestamp', str)
        self.assert_proper_cast('1956-01-31 08:42:15+00', 'timestamptz', str)

    def test_text(self):
        self.assert_proper_cast('', 'text', str)
        self.assert_proper_cast('', 'char', str)
        self.assert_proper_cast('', 'bpchar', str)
        self.assert_proper_cast('', 'varchar', str)

    def test_bytea(self):
        self.assert_proper_cast('', 'bytea', bytes)

    def test_json(self):
        self.assert_proper_cast('{}', 'json', dict)


class TestQueryIterator(unittest.TestCase):
    """Test the query operating as an iterator."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def test_len(self):
        r = self.c.query("select generate_series(3,7)")
        self.assertEqual(len(r), 5)

    def test_get_item(self):
        r = self.c.query("select generate_series(7,9)")
        self.assertEqual(r[0], (7,))
        self.assertEqual(r[1], (8,))
        self.assertEqual(r[2], (9,))

    def test_get_item_with_negative_index(self):
        r = self.c.query("select generate_series(7,9)")
        self.assertEqual(r[-1], (9,))
        self.assertEqual(r[-2], (8,))
        self.assertEqual(r[-3], (7,))

    def test_get_item_out_of_range(self):
        r = self.c.query("select generate_series(7,9)")
        self.assertRaises(IndexError, r.__getitem__, 3)

    def test_iterate(self):
        r = self.c.query("select generate_series(3,5)")
        self.assertNotIsInstance(r, (list, tuple))
        self.assertIsInstance(r, Iterable)
        self.assertEqual(list(r), [(3,), (4,), (5,)])
        # noinspection PyUnresolvedReferences
        self.assertIsInstance(r[1], tuple)

    def test_iterate_twice(self):
        r = self.c.query("select generate_series(3,5)")
        for _i in range(2):
            self.assertEqual(list(r), [(3,), (4,), (5,)])

    def test_iterate_two_columns(self):
        r = self.c.query("select 1,2 union select 3,4")
        self.assertIsInstance(r, Iterable)
        self.assertEqual(list(r), [(1, 2), (3, 4)])

    def test_next(self):
        r = self.c.query("select generate_series(7,9)")
        self.assertEqual(next(r), (7,))
        self.assertEqual(next(r), (8,))
        self.assertEqual(next(r), (9,))
        self.assertRaises(StopIteration, next, r)

    def test_contains(self):
        r = self.c.query("select generate_series(7,9)")
        self.assertIn((8,), r)
        self.assertNotIn((5,), r)

    def test_dict_iterate(self):
        r = self.c.query("select generate_series(3,5) as n").dictiter()
        self.assertNotIsInstance(r, (list, tuple))
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [dict(n=3), dict(n=4), dict(n=5)])
        self.assertIsInstance(r[1], dict)

    def test_dict_iterate_two_columns(self):
        r = self.c.query(
            "select 1 as one, 2 as two"
            " union select 3 as one, 4 as two").dictiter()
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [dict(one=1, two=2), dict(one=3, two=4)])

    def test_dict_next(self):
        r = self.c.query("select generate_series(7,9) as n").dictiter()
        self.assertEqual(next(r), dict(n=7))
        self.assertEqual(next(r), dict(n=8))
        self.assertEqual(next(r), dict(n=9))
        self.assertRaises(StopIteration, next, r)

    def test_dict_contains(self):
        r = self.c.query("select generate_series(7,9) as n").dictiter()
        self.assertIn(dict(n=8), r)
        self.assertNotIn(dict(n=5), r)

    def test_named_iterate(self):
        r = self.c.query("select generate_series(3,5) as number").namediter()
        self.assertNotIsInstance(r, (list, tuple))
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [(3,), (4,), (5,)])
        self.assertIsInstance(r[1], tuple)
        self.assertEqual(r[1]._fields, ('number',))
        self.assertEqual(r[1].number, 4)

    def test_named_iterate_two_columns(self):
        r = self.c.query(
            "select 1 as one, 2 as two"
            " union select 3 as one, 4 as two").namediter()
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [(1, 2), (3, 4)])
        self.assertEqual(r[0]._fields, ('one', 'two'))
        self.assertEqual(r[0].one, 1)
        self.assertEqual(r[1]._fields, ('one', 'two'))
        self.assertEqual(r[1].two, 4)

    def test_named_next(self):
        r = self.c.query("select generate_series(7,9) as number").namediter()
        self.assertEqual(next(r), (7,))
        self.assertEqual(next(r), (8,))
        n = next(r)
        self.assertEqual(n._fields, ('number',))
        self.assertEqual(n.number, 9)
        self.assertRaises(StopIteration, next, r)

    def test_named_contains(self):
        r = self.c.query("select generate_series(7,9)").namediter()
        self.assertIn((8,), r)
        self.assertNotIn((5,), r)

    def test_scalar_iterate(self):
        r = self.c.query("select generate_series(3,5)").scalariter()
        self.assertNotIsInstance(r, (list, tuple))
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [3, 4, 5])
        self.assertIsInstance(r[1], int)

    def test_scalar_iterate_two_columns(self):
        r = self.c.query("select 1, 2 union select 3, 4").scalariter()
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [1, 3])

    def test_scalar_next(self):
        r = self.c.query("select generate_series(7,9)").scalariter()
        self.assertEqual(next(r), 7)
        self.assertEqual(next(r), 8)
        self.assertEqual(next(r), 9)
        self.assertRaises(StopIteration, next, r)

    def test_scalar_contains(self):
        r = self.c.query("select generate_series(7,9)").scalariter()
        self.assertIn(8, r)
        self.assertNotIn(5, r)


class TestQueryOneSingleScalar(unittest.TestCase):
    """Test the query methods for getting single rows and columns."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def test_one_with_empty_query(self):
        q = self.c.query("select 0 where false")
        self.assertIsNone(q.one())

    def test_one_with_single_row(self):
        q = self.c.query("select 1, 2")
        r = q.one()
        self.assertIsInstance(r, tuple)
        self.assertEqual(r, (1, 2))
        self.assertEqual(q.one(), None)

    def test_one_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        self.assertEqual(q.one(), (1, 2))
        self.assertEqual(q.one(), (3, 4))
        self.assertEqual(q.one(), None)

    def test_one_dict_with_empty_query(self):
        q = self.c.query("select 0 where false")
        self.assertIsNone(q.onedict())

    def test_one_dict_with_single_row(self):
        q = self.c.query("select 1 as one, 2 as two")
        r = q.onedict()
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(one=1, two=2))
        self.assertEqual(q.onedict(), None)

    def test_one_dict_with_two_rows(self):
        q = self.c.query(
            "select 1 as one, 2 as two union select 3 as one, 4 as two")
        self.assertEqual(q.onedict(), dict(one=1, two=2))
        self.assertEqual(q.onedict(), dict(one=3, two=4))
        self.assertEqual(q.onedict(), None)

    def test_one_named_with_empty_query(self):
        q = self.c.query("select 0 where false")
        self.assertIsNone(q.onenamed())

    def test_one_named_with_single_row(self):
        q = self.c.query("select 1 as one, 2 as two")
        r = q.onenamed()
        self.assertEqual(r._fields, ('one', 'two'))
        self.assertEqual(r.one, 1)
        self.assertEqual(r.two, 2)
        self.assertEqual(r, (1, 2))
        self.assertEqual(q.onenamed(), None)

    def test_one_named_with_two_rows(self):
        q = self.c.query(
            "select 1 as one, 2 as two union select 3 as one, 4 as two")
        r = q.onenamed()
        self.assertEqual(r._fields, ('one', 'two'))
        self.assertEqual(r.one, 1)
        self.assertEqual(r.two, 2)
        self.assertEqual(r, (1, 2))
        r = q.onenamed()
        self.assertEqual(r._fields, ('one', 'two'))
        self.assertEqual(r.one, 3)
        self.assertEqual(r.two, 4)
        self.assertEqual(r, (3, 4))
        self.assertEqual(q.onenamed(), None)

    def test_one_scalar_with_empty_query(self):
        q = self.c.query("select 0 where false")
        self.assertIsNone(q.onescalar())

    def test_one_scalar_with_single_row(self):
        q = self.c.query("select 1, 2")
        r = q.onescalar()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)
        self.assertEqual(q.onescalar(), None)

    def test_one_scalar_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        self.assertEqual(q.onescalar(), 1)
        self.assertEqual(q.onescalar(), 3)
        self.assertEqual(q.onescalar(), None)

    def test_single_with_empty_query(self):
        q = self.c.query("select 0 where false")
        try:
            q.single()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.NoResultError)
        self.assertEqual(str(r), 'No result found')

    def test_single_with_single_row(self):
        q = self.c.query("select 1, 2")
        r = q.single()
        self.assertIsInstance(r, tuple)
        self.assertEqual(r, (1, 2))
        r = q.single()
        self.assertIsInstance(r, tuple)
        self.assertEqual(r, (1, 2))

    def test_single_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        try:
            q.single()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.MultipleResultsError)
        self.assertEqual(str(r), 'Multiple results found')

    def test_single_dict_with_empty_query(self):
        q = self.c.query("select 0 where false")
        try:
            q.singledict()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.NoResultError)
        self.assertEqual(str(r), 'No result found')

    def test_single_dict_with_single_row(self):
        q = self.c.query("select 1 as one, 2 as two")
        r = q.singledict()
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(one=1, two=2))
        r = q.singledict()
        self.assertIsInstance(r, dict)
        self.assertEqual(r, dict(one=1, two=2))

    def test_single_dict_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        try:
            q.singledict()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.MultipleResultsError)
        self.assertEqual(str(r), 'Multiple results found')

    def test_single_named_with_empty_query(self):
        q = self.c.query("select 0 where false")
        try:
            q.singlenamed()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.NoResultError)
        self.assertEqual(str(r), 'No result found')

    def test_single_named_with_single_row(self):
        q = self.c.query("select 1 as one, 2 as two")
        r: Any = q.singlenamed()
        self.assertEqual(r._fields, ('one', 'two'))
        self.assertEqual(r.one, 1)
        self.assertEqual(r.two, 2)
        self.assertEqual(r, (1, 2))
        r = q.singlenamed()
        self.assertEqual(r._fields, ('one', 'two'))
        self.assertEqual(r.one, 1)
        self.assertEqual(r.two, 2)
        self.assertEqual(r, (1, 2))

    def test_single_named_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        try:
            q.singlenamed()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.MultipleResultsError)
        self.assertEqual(str(r), 'Multiple results found')

    def test_single_scalar_with_empty_query(self):
        q = self.c.query("select 0 where false")
        try:
            q.singlescalar()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.NoResultError)
        self.assertEqual(str(r), 'No result found')

    def test_single_scalar_with_single_row(self):
        q = self.c.query("select 1, 2")
        r = q.singlescalar()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)
        r = q.singlescalar()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)

    def test_single_scalar_with_two_rows(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        try:
            q.singlescalar()
        except pg.InvalidResultError as e:
            r: Any = e
        else:
            r = None
        self.assertIsInstance(r, pg.MultipleResultsError)
        self.assertEqual(str(r), 'Multiple results found')

    def test_scalar_result(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        r = q.scalarresult()
        self.assertIsInstance(r, list)
        self.assertEqual(r, [1, 3])

    def test_scalar_iter(self):
        q = self.c.query("select 1, 2 union select 3, 4")
        r = q.scalariter()
        self.assertNotIsInstance(r, (list, tuple))
        self.assertIsInstance(r, Iterable)
        r = list(r)
        self.assertEqual(r, [1, 3])


class TestInserttable(unittest.TestCase):
    """Test inserttable method."""

    cls_set_up = False
    has_encoding = False

    @classmethod
    def setUpClass(cls):
        c = connect()
        c.query("drop table if exists test cascade")
        c.query("create table test ("
                "i2 smallint, i4 integer, i8 bigint,"
                "b boolean, dt date, ti time, ts timestamp, td interval,"
                "d numeric, f4 real, f8 double precision, m money,"
                "c char(1), v4 varchar(4), c4 char(4), t text)")
        # Check whether the test database uses SQL_ASCII - this means
        # that it does not consider encoding when calculating lengths.
        c.query("set client_encoding=utf8")
        try:
            c.query("select 'ä'")
        except (pg.DataError, pg.NotSupportedError):
            cls.has_encoding = False
        else:
            cls.has_encoding = c.query(
                "select length('ä') - length('a')").getresult()[0][0] == 0
        c.close()
        cls.cls_set_up = True

    @classmethod
    def tearDownClass(cls):
        c = connect()
        c.query("drop table test cascade")
        c.close()

    def setUp(self):
        self.assertTrue(self.cls_set_up)
        self.c = connect()
        self.c.query("set client_encoding=utf8")
        self.c.query("set datestyle='ISO,YMD'")
        self.c.query("set lc_monetary='C'")

    def tearDown(self):
        self.c.query("truncate table test")
        self.c.close()

    data: Sequence[tuple] = [
        (-1, -1, -1, True,
         '1492-10-12', '08:30:00', '1492-10-12 08:30:00', '-3 days',
         -1.2345, -1.75, -1.875, '-1.25', '-', 'r?', '!u', 'xyz'),
        (0, 0, 0, False,
         '1607-04-14', '09:00:00', '1607-04-14 09:00:00', '7 days',
         0.0, 0.0, 0.0, '0.0', ' ', '0123', '4567', '890'),
        (1, 1, 1, True,
         '1801-03-04', '03:45:00', '1801-03-04 03:45:00', '3 mons',
         1.23456, 1.75, 1.875, '1.25', 'x', 'bc', 'cdef', 'g'),
        (2, 2, 2, False,
         '1903-12-17', '11:22:00', '1903-12-17 11:22:00', '1 year',
         2.345678, 2.25, 2.125, '2.75', 'y', 'q', 'ijk', 'mnop\nstux!')]

    @classmethod
    def db_len(cls, s, encoding):
        # noinspection PyUnresolvedReferences
        if cls.has_encoding:
            s = s if isinstance(s, str) else s.decode(encoding)
        else:
            s = s.encode(encoding) if isinstance(s, str) else s
        return len(s)

    def get_back(self, encoding='utf-8'):
        """Convert boolean and decimal values back."""
        data = []
        for row in self.c.query("select * from test order by 1").getresult():
            self.assertIsInstance(row, tuple)
            row = list(row)
            if row[0] is not None:  # smallint
                self.assertIsInstance(row[0], int)
            if row[1] is not None:  # integer
                self.assertIsInstance(row[1], int)
            if row[2] is not None:  # bigint
                self.assertIsInstance(row[2], int)
            if row[3] is not None:  # boolean
                self.assertIsInstance(row[3], bool)
            if row[4] is not None:  # date
                self.assertIsInstance(row[4], str)
                self.assertTrue(row[4].replace('-', '').isdigit())
            if row[5] is not None:  # time
                self.assertIsInstance(row[5], str)
                self.assertTrue(row[5].replace(':', '').isdigit())
            if row[6] is not None:  # timestamp
                self.assertIsInstance(row[6], str)
                parts = row[6].split(' ')
                self.assertEqual(len(parts), 2)
                self.assertTrue(parts[0].replace('-', '').isdigit())
                self.assertTrue(parts[1].replace(':', '').isdigit())
            if row[7] is not None:  # interval
                self.assertIsInstance(row[7], str)
            if row[8] is not None:  # numeric
                self.assertIsInstance(row[8], Decimal)
                row[8] = float(row[8])
            if row[9] is not None:  # real
                self.assertIsInstance(row[9], float)
            if row[10] is not None:  # double precision
                self.assertIsInstance(row[10], float)
                row[10] = float(row[10])
            if row[11] is not None:  # money
                self.assertIsInstance(row[11], Decimal)
                row[11] = str(float(row[11]))
            if row[12] is not None:  # char(1)
                self.assertIsInstance(row[12], str)
                self.assertEqual(self.db_len(row[12], encoding), 1)
            if row[13] is not None:  # varchar(4)
                self.assertIsInstance(row[13], str)
                self.assertLessEqual(self.db_len(row[13], encoding), 4)
            if row[14] is not None:  # char(4)
                self.assertIsInstance(row[14], str)
                self.assertEqual(self.db_len(row[14], encoding), 4)
                row[14] = row[14].rstrip()
            if row[15] is not None:  # text
                self.assertIsInstance(row[15], str)
            row = tuple(row)
            data.append(row)
        return data

    def test_inserttable1_row(self):
        data = self.data[2:3]
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable4_rows(self):
        data = self.data
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable_from_tuple_of_lists(self):
        data = tuple(list(row) for row in self.data)
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_with_different_row_sizes(self):
        data = [*self.data[:-1], (self.data[-1][:-1],)]
        try:
            self.c.inserttable('test', data)
        except TypeError as e:
            self.assertIn(
                'second arg must contain sequences of the same size', str(e))
        else:
            self.assertFalse('expected an error')

    def test_inserttable_from_setof_tuples(self):
        data = {row for row in self.data}
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_from_dict_as_interable(self):
        data = {row: None for row in self.data}
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_from_dict_keys(self):
        data = {row: None for row in self.data}
        keys = data.keys()
        self.c.inserttable('test', keys)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_from_dict_values(self):
        data = {i: row for i, row in enumerate(self.data)}
        values = data.values()
        self.c.inserttable('test', values)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_from_generator_of_tuples(self):
        data = (row for row in self.data)
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_from_list_of_sets(self):
        data = [set(row) for row in self.data]
        try:
            self.c.inserttable('test', data)
        except TypeError as e:
            self.assertIn(
                'second argument must contain tuples or lists', str(e))
        else:
            self.assertFalse('expected an error')

    def test_inserttable_multiple_rows(self):
        num_rows = 100
        data = list(self.data[2:3]) * num_rows
        self.c.inserttable('test', data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def test_inserttable_multiple_calls(self):
        num_rows = 10
        data = self.data[2:3]
        for _i in range(num_rows):
            self.c.inserttable('test', data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def test_inserttable_null_values(self):
        data = [(None,) * 16] * 100
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable_no_column(self):
        data = [()] * 10
        self.c.inserttable('test', data, [])
        self.assertEqual(self.get_back(), [])

    def test_inserttable_datetime_adapt(self):
        data = [(datetime.date(1999, 1, 2), datetime.time(11, 12, 13),
                 datetime.datetime(1999, 1, 2, 11, 12, 13),
                 datetime.timedelta(days=123))]
        self.c.inserttable('test', data, ['dt', 'ti', 'ts', 'td'])
        back = [row[4:8] for row in self.get_back()]
        self.assertEqual(back, [(
            '1999-01-02', '11:12:13', '1999-01-02 11:12:13', '123 days')])

    def test_inserttable_only_one_column(self):
        data: list[tuple] = [(42,)] * 50
        self.c.inserttable('test', data, ['i4'])
        data = [tuple([42 if i == 1 else None for i in range(16)])] * 50
        self.assertEqual(self.get_back(), data)

    def test_inserttable_only_two_columns(self):
        data: list[tuple] = [(bool(i % 2), i * .5) for i in range(20)]
        self.c.inserttable('test', data, ('b', 'f4'))
        # noinspection PyTypeChecker
        data = [(None,) * 3 + (bool(i % 2),) + (None,) * 5 + (i * .5,)
                + (None,) * 6 for i in range(20)]
        self.assertEqual(self.get_back(), data)

    def test_inserttable_with_dotted_table_name(self):
        data = self.data
        self.c.inserttable('public.test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable_with_bytes_as_table_name(self):
        data = self.data
        self.c.inserttable(b'test', data[:2])
        self.c.inserttable(b'public.test', data[2:])
        self.assertEqual(self.get_back(), data)

    def test_inserttable_with_invalid_table_name_type(self):
        for table in 42, None, ['test']:
            with self.assertRaises(TypeError) as cm:
                self.c.inserttable(table, self.data)
            self.assertIn('expects a string', str(cm.exception))
        self.assert_ready()

    def test_inserttable_with_invalid_table_name(self):
        data = [(42,)]
        # check that the table name is not inserted unescaped
        # (this would pass otherwise since there is a column named i4)
        try:
            self.c.inserttable('test (i4)', data)
        except ValueError as e:
            self.assertIn('relation "test (i4)" does not exist', str(e))
        else:
            self.assertFalse('expected an error')
        # make sure that it works if parameters are passed properly
        self.c.inserttable('test', data, ['i4'])

    def test_inserttable_with_invalid_data_type(self):
        try:
            self.c.inserttable('test', 42)
        except TypeError as e:
            self.assertIn('expects an iterable as second argument', str(e))
        else:
            self.assertFalse('expected an error')

    def test_inserttable_with_invalid_column_name(self):
        data = [(2, 4)]
        # check that the column names are not inserted unescaped
        # (this would pass otherwise since there are columns i2 and i4)
        try:
            self.c.inserttable('test', data, ['i2,i4'])
        except ValueError as e:
            self.assertIn(
                'column "i2,i4" of relation "test" does not exist', str(e))
        else:
            self.assertFalse('expected an error')
        # make sure that it works if parameters are passed properly
        self.c.inserttable('test', data, ['i2', 'i4'])

    def test_inserttable_with_invalid_colum_list(self):
        data = self.data
        try:
            self.c.inserttable('test', data, 'invalid')
        except TypeError as e:
            self.assertIn(
                'expects a tuple or a list as third argument', str(e))
        else:
            self.assertFalse('expected an error')

    def test_inserttable_with_huge_list_of_column_names(self):
        data = self.data
        # try inserting data with a huge list of column names
        cols = ['very_long_column_name'] * 2000
        # Should raise a value error because the column does not exist
        self.assertRaises(ValueError, self.c.inserttable, 'test', data, cols)
        # double the size, should not overflow buffer nor raise memory error
        cols *= 2
        self.assertRaises(ValueError, self.c.inserttable, 'test', data, cols)

    def test_inserttable_with_out_of_range_data(self):
        # try inserting data out of range for the column type
        # Should raise a value error because of smallint out of range
        self.assertRaises(
            ValueError, self.c.inserttable, 'test', [[33000]], ['i2'])

    def test_inserttable_max_values(self):
        data = [(2 ** 15 - 1, 2 ** 31 - 1, 2 ** 31 - 1, True,
                 '2999-12-31', '11:59:59', '2999-12-31 23:59:59', '9999 years',
                 1e99, 1.0 + 1.0 / 32, 1.0 + 1.0 / 32, None,
                 "1", "1234", "1234", "1234" * 100)]
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable_byte_values(self):
        try:
            self.c.query("select '€', 'käse', 'сыр', 'pont-l''évêque'")
        except pg.DataError:
            self.skipTest("database does not support utf8")
        # non-ascii chars do not fit in char(1) when there is no encoding
        c = '€' if self.has_encoding else '$'
        row_unicode = (
            0, 0, 0, False,
            '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '00:00:00',
            0.0, 0.0, 0.0, '0.0',
            c, 'bäd', 'bäd', "käse сыр pont-l'évêque")
        row_bytes = tuple(
            s.encode() if isinstance(s, str) else s
            for s in row_unicode)
        data_bytes = [row_bytes] * 2
        self.c.inserttable('test', data_bytes)
        data_unicode = [row_unicode] * 2
        self.assertEqual(self.get_back(), data_unicode)

    def test_inserttable_unicode_utf8(self):
        try:
            self.c.query("select '€', 'käse', 'сыр', 'pont-l''évêque'")
        except pg.DataError:
            self.skipTest("database does not support utf8")
        # non-ascii chars do not fit in char(1) when there is no encoding
        c = '€' if self.has_encoding else '$'
        row_unicode = (
            0, 0, 0, False,
            '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '00:00:00',
            0.0, 0.0, 0.0, '0.0',
            c, 'bäd', 'bäd', "käse сыр pont-l'évêque")
        data = [row_unicode] * 2
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)

    def test_inserttable_unicode_latin1(self):
        try:
            self.c.query("set client_encoding=latin1")
            self.c.query("select '¥'")
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        # non-ascii chars do not fit in char(1) when there is no encoding
        c = '€' if self.has_encoding else '$'
        row_unicode: tuple = (
            0, 0, 0, False,
            '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '00:00:00',
            0.0, 0.0, 0.0, '0.0',
            c, 'bäd', 'bäd', "for käse and pont-l'évêque pay in €")
        data = [row_unicode]
        # cannot encode € sign with latin1 encoding
        self.assertRaises(UnicodeEncodeError, self.c.inserttable, 'test', data)
        row_unicode = tuple(
            s.replace('€', '¥') if isinstance(s, str) else s
            for s in row_unicode)
        data = [row_unicode] * 2
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back('latin1'), data)

    def test_inserttable_unicode_latin9(self):
        try:
            self.c.query("set client_encoding=latin9")
            self.c.query("select '€'")
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin9")
            return
        # non-ascii chars do not fit in char(1) when there is no encoding
        c = '€' if self.has_encoding else '$'
        row_unicode = (
            0, 0, 0, False,
            '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '00:00:00',
            0.0, 0.0, 0.0, '0.0',
            c, 'bäd', 'bäd', "for käse and pont-l'évêque pay in €")
        data = [row_unicode] * 2
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back('latin9'), data)

    def test_inserttable_no_encoding(self):
        self.c.query("set client_encoding=sql_ascii")
        # non-ascii chars do not fit in char(1) when there is no encoding
        c = '€' if self.has_encoding else '$'
        row_unicode = (
            0, 0, 0, False,
            '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '00:00:00',
            0.0, 0.0, 0.0, '0.0',
            c, 'bäd', 'bäd', "for käse and pont-l'évêque pay in €")
        data = [row_unicode]
        # cannot encode non-ascii unicode without a specific encoding
        self.assertRaises(UnicodeEncodeError, self.c.inserttable, 'test', data)

    def test_inserttable_from_query(self):
        data = self.c.query(
            "select 2::int2 as i2, 4::int4 as i4, 8::int8 as i8, true as b,"
            "null as dt, null as ti, null as ts, null as td, null as d,"
            "4.5::float as float4, 8.5::float8 as f8,"
            "null as m, 'c' as c, 'v4' as v4, null as c4, 'text' as text")
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), [
            (2, 4, 8, True, None, None, None, None, None, 4.5, 8.5,
             None, 'c', 'v4', None, 'text')])

    def test_inserttable_special_chars(self):
        class S:
            def __repr__(self):
                return s

        class I(int):  # noqa: E742
            def __str__(self):
                return s

        s = '1\'2"3\b4\f5\n6\r7\t8\b9\\0'
        s1 = s.encode('ascii')
        s2 = S()
        s3 = I()
        data = [(t,) for t in (s, s1, s2, s3)]
        self.c.inserttable('test', data, ['t'])
        self.assertEqual(
            self.c.query('select t from test').getresult(), [(s,)] * 4)

    def assert_ready(self):
        """Check that the connection is ready for the next command."""
        q = self.c.send_query("select 1")
        self.assertEqual(q.getresult(), [(1,)])
        self.assertIsNone(q.getresult())

    def assert_failed(self, error, data, columns=None, message=None):
        """Check that inserttable() fails without inserting any rows."""
        with self.assertRaises(error) as cm:
            if columns is None:
                self.c.inserttable('test', data)
            else:
                self.c.inserttable('test', data, columns)
        if message:
            self.assertIn(message, str(cm.exception))
        self.assert_ready()
        self.assertEqual(self.get_back(), [])

    def test_inserttable_ready_after_success(self):
        self.c.inserttable('test', self.data)
        self.assert_ready()
        self.assertEqual(self.get_back(), self.data)

    def test_inserttable_ready_after_errors(self):
        self.assert_failed(ValueError, [(1,), ('x',)], ['i4'],
                           'invalid input syntax for type integer')
        self.assert_failed(TypeError, [(1,), (2, 3)], ['i4'],
                           'sequences of the same size')
        self.assert_failed(TypeError, [(1,), 2], ['i4'],
                           'must contain tuples or lists')

    def test_inserttable_with_iteration_error(self):
        def rows():
            yield 1,
            raise ZeroDivisionError

        self.assert_failed(ZeroDivisionError, rows(), ['i4'])

    def test_inserttable_with_unencodable_strings(self):
        self.c.query("set client_encoding=sql_ascii")
        for i in range(100):
            # a new string that is only referenced by the row
            row = (f'€{i}',)
            refcount = sys.getrefcount(row[0])
            self.assertRaises(
                UnicodeEncodeError, self.c.inserttable, 'test', [row], ['t'])
            self.assertEqual(sys.getrefcount(row[0]), refcount)
        self.assert_ready()
        self.assertEqual(self.get_back(), [])

    def test_inserttable_with_failing_str_and_repr(self):
        class I(int):  # noqa: E742
            def __str__(self):
                raise ZeroDivisionError

        class R:
            def __repr__(self):
                raise ZeroDivisionError

        class S:
            def __repr__(self):
                return '\udc80'  # lone surrogate, cannot be encoded

        self.assert_failed(ZeroDivisionError, [(1,), (I(2),)], ['i4'])
        self.assert_failed(ZeroDivisionError, [('a',), (R(),)], ['t'])
        self.assert_failed(UnicodeEncodeError, [('a',), (S(),)], ['t'])

    def test_inserttable_with_nul_characters(self):
        message = 'must not contain NUL characters'
        self.assert_failed(ValueError, [('a',), (b'b\0c',)], ['t'], message)
        self.assert_failed(ValueError, [('a',), ('b\0c',)], ['t'], message)

    def test_inserttable_with_nul_characters_in_names(self):
        message = 'must not contain NUL'
        # the column name must not be truncated to the existing column t
        self.assert_failed(ValueError, [('a',)], [b't\0x'], message)
        self.assert_failed(ValueError, [('a',)], ['t\0x'], message)
        with self.assertRaises(ValueError) as cm:
            self.c.inserttable('test\0x', [('a',)], ['t'])
        self.assertIn(message, str(cm.exception))
        self.assert_ready()

    def test_inserttable_with_invalid_bytes_in_names(self):
        invalid = '€'.encode()[:2]  # incomplete multibyte character
        self.assert_failed(ValueError, [('a',)], [invalid],
                           'multibyte character')

    def test_inserttable_with_rows_changed_while_converting(self):
        class R:
            def __init__(self, row):
                self.row = row

            def __repr__(self):
                self.row.clear()
                return 'changed'

        row: list = []
        row.extend([R(row), 'x'])
        self.c.inserttable('test', [row], ['t', 'v4'])
        self.assertEqual(row, [])
        self.assertEqual(
            self.c.query("select t, v4 from test").getresult(),
            [('changed', 'x')])

    def test_inserttable_with_columns_iterating_differently(self):
        class L(list):
            def __iter__(self):
                return iter(['t'])

        class T(tuple):
            __slots__ = ()

            def __iter__(self):
                return iter(['t'])

        for columns in L(['t', 'v4']), T(['t', 'v4']):
            self.c.inserttable('test', [('a', 'b')], columns)
        self.assertEqual(
            self.c.query("select t, v4 from test").getresult(),
            [('a', 'b')] * 2)

    def test_inserttable_with_rows_failing_to_get_length(self):
        class L(list):
            def __len__(self):
                raise ZeroDivisionError

        self.assertRaises(ZeroDivisionError, self.c.inserttable,
                          'test', L([('a',)]), ['t'])
        self.assert_ready()
        self.assertEqual(self.get_back(), [])

    def test_inserttable_with_latin1_names_and_values(self):
        try:
            self.c.query("set client_encoding=latin1")
            self.c.query('create temporary table "täble"'
                         ' ("cölumn" text, "zähler" int)')
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest("database does not support latin1")
        # table and column names and the repr of other objects
        # must also be encoded with the client encoding
        self.c.inserttable('täble', [('käse',), (['käse'],)], ['cölumn'])
        self.assertEqual(
            self.c.query('select "cölumn" from "täble"').getresult(),
            [('käse',), ("['käse']",)])
        # error messages must be decoded with the client encoding
        with self.assertRaises(ValueError) as cm:
            self.c.inserttable('täble', [('käse',)], ['spalte_ä'])
        self.assertIn('"spalte_ä"', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            self.c.inserttable('täble', [('käse',)], ['zähler'])
        self.assertIn('"käse"', str(cm.exception))
        # names can also be passed as bytes in the server encoding
        # when they cannot be encoded with the client encoding
        server_encoding = self.c.parameter('server_encoding')
        if server_encoding not in ('UTF8', 'LATIN1', 'SQL_ASCII'):
            return
        encoding = 'utf8' if server_encoding == 'UTF8' else 'latin1'
        self.c.query("set client_encoding=sql_ascii")
        self.assertRaises(UnicodeEncodeError, self.c.inserttable,
                          'täble', [(b'k',)], ['cölumn'])
        self.assertRaises(UnicodeEncodeError, self.c.inserttable,
                          'täble'.encode(encoding), [(b'k',)], ['cölumn'])
        self.c.inserttable(
            'täble'.encode(encoding), [(b'k',)], ['cölumn'.encode(encoding)])
        self.c.query("set client_encoding=latin1")
        self.assertEqual(self.c.query(
            'select "cölumn" from "täble" where "cölumn" = \'k\'').getresult(),
            [('k',)])

    def test_insert_table_big_row_size(self):
        # inserting rows with a size exceeding 64k bytes should work
        t = '*' * 75000
        data = [(t,)]
        self.c.inserttable('test', data, ['t'])
        self.assertEqual(
            self.c.query('select t from test').getresult(), data)

    def test_insert_table_small_int_overflow(self):
        rest_row = self.data[2][1:]
        data = [(32000, *rest_row)]
        self.c.inserttable('test', data)
        self.assertEqual(self.get_back(), data)
        data = [(33000, *rest_row)]
        try:
            self.c.inserttable('test', data)
        except ValueError as e:
            self.assertIn(
                'value "33000" is out of range for type smallint', str(e))
        else:
            self.assertFalse('expected an error since value is out of range')

    def test_insert_table_with_freeze_false(self):
        data = self.data
        self.c.inserttable('test', data, freeze=False)
        self.assertEqual(self.get_back(), data)

    def test_insert_table_with_freeze_true_without_truncate(self):
        try:
            self.c.inserttable('test', self.data, freeze=True)
        except ValueError as e:
            self.assertIn('cannot perform COPY FREEZE', str(e))
        else:
            self.assertFalse('expected an error since table was not truncated')

    def test_insert_table_with_freeze_true_with_truncate(self):
        data = self.data
        self.c.query("begin")
        self.c.query('truncate table test')
        self.c.inserttable('test', data, freeze=True)
        self.c.query('commit')
        self.assertEqual(self.get_back(), data)


class TestDirectSocketAccess(unittest.TestCase):
    """Test copy command with direct socket access."""

    cls_set_up = False

    @classmethod
    def setUpClass(cls):
        c = connect()
        c.query("drop table if exists test cascade")
        c.query("create table test (i int, v varchar(16))")
        c.close()
        cls.cls_set_up = True

    @classmethod
    def tearDownClass(cls):
        c = connect()
        c.query("drop table test cascade")
        c.close()

    def setUp(self):
        self.assertTrue(self.cls_set_up)
        self.c = connect()
        self.c.query("set client_encoding=utf8")

    def tearDown(self):
        self.c.query("truncate table test")
        self.c.close()

    def test_putline(self):
        putline = self.c.putline
        query = self.c.query
        data = list(enumerate(["apple", "pear", "plum", "cherry", "banana"]))
        query("copy test from stdin")
        try:
            for i, v in data:
                putline(f"{i}\t{v}\n")
        finally:
            self.c.endcopy()
        r = query("select * from test").getresult()
        self.assertEqual(r, data)

    def test_putline_bytes_and_unicode(self):
        putline = self.c.putline
        query = self.c.query
        try:
            query("select 'käse+würstel'")
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest('database does not support utf8')
        query("copy test from stdin")
        try:
            putline("47\tkäse\n".encode())
            putline("35\twürstel\n")
        finally:
            self.c.endcopy()
        r = query("select * from test").getresult()
        self.assertEqual(r, [(47, 'käse'), (35, 'würstel')])

    def test_getline(self):
        getline = self.c.getline
        query = self.c.query
        data = list(enumerate([
            "apple", "banana", "pear", "plum", "strawberry"]))
        n = len(data)
        self.c.inserttable('test', data)
        query("copy test to stdout")
        try:
            for i in range(n + 1):
                v = getline()
                if i < n:
                    # noinspection PyStringFormat
                    self.assertEqual(v, '{}\t{}'.format(*data[i]))
                elif i == n:
                    self.assertIsNone(v)
        finally:
            with suppress(OSError):
                self.c.endcopy()

    def test_getline_bytes_and_unicode(self):
        getline = self.c.getline
        query = self.c.query
        try:
            query("select 'käse+würstel'")
        except (pg.DataError, pg.NotSupportedError):
            self.skipTest('database does not support utf8')
        data = [(54, 'käse'.encode()), (73, 'würstel')]
        self.c.inserttable('test', data)
        query("copy test to stdout")
        try:
            v = getline()
            self.assertIsInstance(v, str)
            self.assertEqual(v, '54\tkäse')
            v = getline()
            self.assertIsInstance(v, str)
            self.assertEqual(v, '73\twürstel')
            self.assertIsNone(getline())
        finally:
            with suppress(OSError):
                self.c.endcopy()

    def test_parameter_checks(self):
        self.assertRaises(TypeError, self.c.putline)
        self.assertRaises(TypeError, self.c.getline, 'invalid')
        self.assertRaises(TypeError, self.c.endcopy, 'invalid')


class TestCopyData(unittest.TestCase):
    """Test copy command with the putdata() and getdata() methods."""

    def setUp(self):
        self.c = connect()
        self.c.query("create temporary table test_copy (n int)")
        self.source = self.c.source()

    def tearDown(self):
        self.c.close()

    def get_table(self):
        return self.c.query("select n from test_copy order by n").getresult()

    def assert_no_memory_leak(self, func):
        if mallinfo2 is None:
            self.skipTest('needs glibc 2.33 or newer')
        for _i in range(10):
            func()  # warm-up
        n = 100
        leaked = []
        # use the minimum of several runs, since a leak shows up in every
        # run, while unrelated one-time allocations would not
        for _run in range(3):
            before = mallinfo2().uordblks
            for _i in range(n):
                func()
            leaked.append(mallinfo2().uordblks - before)
        # libpq results have more than 100 bytes
        self.assertLess(min(leaked), 100 * n, f'{leaked} bytes leaked')

    def copy_from(self):
        source = self.source
        source.execute("copy test_copy from stdin")
        self.assertIsNone(source.putdata(b'1\n'))
        self.assertIsNone(source.putdata('2\n'))
        self.assertEqual(source.putdata(None), 2)

    def copy_from_aborted(self):
        source = self.source
        source.execute("copy test_copy from stdin")
        self.assertIsNone(source.putdata(b'1\n'))
        with self.assertRaises(OSError) as cm:
            source.putdata(ValueError('Copy aborted by the client'))
        self.assertEqual(str(cm.exception), 'Copy aborted by the client')

    def copy_to(self):
        source = self.source
        source.execute("copy (select generate_series(1, 2)) to stdout")
        self.assertEqual(source.getdata(), b'1\n')
        self.assertEqual(source.getdata(True), '2\n')
        self.assertEqual(source.getdata(), 2)

    def test_putdata(self):
        self.copy_from()
        self.assertEqual(self.get_table(), [(1,), (2,)])

    def test_putdata_with_exception(self):
        self.copy_from_aborted()
        self.assertEqual(self.get_table(), [])

    def test_putdata_with_exception_that_cannot_be_converted(self):
        class BadError(Exception):
            def __str__(self):
                raise RuntimeError('Cannot convert to string')

        source = self.source
        source.execute("copy test_copy from stdin")
        self.assertRaises(RuntimeError, source.putdata, BadError())
        # the copy operation is still in progress
        self.assertEqual(source.putdata(None), 0)

    def test_putdata_with_invalid_argument(self):
        source = self.source
        source.execute("copy test_copy from stdin")
        self.assertRaises(TypeError, source.putdata, 42)
        self.assertEqual(source.putdata(None), 0)

    def test_putdata_without_copy(self):
        self.assertRaises(OSError, self.source.putdata, b'1\n')
        self.source.execute("select 1")
        self.assertRaises(OSError, self.source.putdata, None)

    def test_getdata(self):
        self.copy_to()

    def test_getdata_with_invalid_argument(self):
        self.assertRaises(TypeError, self.source.getdata, 'invalid')

    def test_getdata_without_copy(self):
        self.assertRaises(OSError, self.source.getdata)
        self.source.execute("select 1")
        self.assertRaises(OSError, self.source.getdata)

    def test_connection_is_idle_after_copy(self):
        for copy in self.copy_from, self.copy_from_aborted, self.copy_to:
            copy()
            self.assertEqual(self.c.transaction(), pg.TRANS_IDLE)
            query = self.c.send_query("select 1")
            self.assertEqual(query.getresult(), [(1,)])
            self.assertIsNone(query.getresult())

    def test_putdata_with_failing_statement_after_copy(self):
        source = self.source
        source.execute("copy test_copy from stdin; select 1/0")
        self.assertIsNone(source.putdata(b'1\n'))
        with self.assertRaises(OSError) as cm:
            source.putdata(None)
        self.assertIn('division by zero', str(cm.exception))
        # the failing statement has also rolled back the copy
        self.assertEqual(self.get_table(), [])

    def test_getdata_with_failing_statement_after_copy(self):
        source = self.source
        source.execute("copy (select 1) to stdout; select 1/0")
        self.assertEqual(source.getdata(), b'1\n')
        with self.assertRaises(OSError) as cm:
            source.getdata()
        self.assertIn('division by zero', str(cm.exception))

    def test_multiple_copy_operations(self):
        source = self.source
        source.execute("copy test_copy from stdin;"
                       " copy test_copy to stdout; copy test_copy from stdin")
        self.assertIsNone(source.putdata(b'1\n'))
        self.assertEqual(source.putdata(None), 1)
        self.assertEqual(source.getdata(), b'1\n')
        self.assertEqual(source.getdata(), 1)
        self.assertIsNone(source.putdata(b'2\n'))
        self.assertEqual(source.putdata(None), 1)
        self.assertRaises(OSError, source.putdata, None)
        self.assertEqual(self.c.transaction(), pg.TRANS_IDLE)
        self.assertEqual(self.get_table(), [(1,), (2,)])

    def test_putdata_memory_leak(self):
        self.assert_no_memory_leak(self.copy_from)

    def test_putdata_with_exception_memory_leak(self):
        self.assert_no_memory_leak(self.copy_from_aborted)

    def test_getdata_memory_leak(self):
        self.assert_no_memory_leak(self.copy_to)


class TestNotificatons(unittest.TestCase):
    """Test notification support."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.doCleanups()
        self.c.close()

    def test_get_notify(self):
        getnotify = self.c.getnotify
        query = self.c.query
        self.assertIsNone(getnotify())
        query('listen test_notify')
        try:
            self.assertIsNone(self.c.getnotify())
            query("notify test_notify")
            r = getnotify()
            self.assertIsInstance(r, tuple)
            self.assertEqual(len(r), 3)
            self.assertIsInstance(r[0], str)
            self.assertIsInstance(r[1], int)
            self.assertIsInstance(r[2], str)
            self.assertEqual(r[0], 'test_notify')
            self.assertEqual(r[2], '')
            self.assertIsNone(self.c.getnotify())
            query("notify test_notify, 'test_payload'")
            r = getnotify()
            self.assertIsInstance(r, tuple)
            self.assertEqual(len(r), 3)
            self.assertIsInstance(r[0], str)
            self.assertIsInstance(r[1], int)
            self.assertIsInstance(r[2], str)
            self.assertEqual(r[0], 'test_notify')
            self.assertEqual(r[2], 'test_payload')
            self.assertIsNone(getnotify())
        finally:
            query('unlisten test_notify')

    def test_get_notice_receiver(self):
        self.assertIsNone(self.c.get_notice_receiver())

    def test_set_notice_receiver(self):
        self.assertRaises(TypeError, self.c.set_notice_receiver, 42)
        self.assertRaises(TypeError, self.c.set_notice_receiver, 'invalid')
        self.assertIsNone(self.c.set_notice_receiver(lambda notice: None))
        self.assertIsNone(self.c.set_notice_receiver(None))

    def test_set_and_get_notice_receiver(self):
        r = lambda notice: None  # noqa: E731
        self.assertIsNone(self.c.set_notice_receiver(r))
        self.assertIs(self.c.get_notice_receiver(), r)
        self.assertIsNone(self.c.set_notice_receiver(None))
        self.assertIsNone(self.c.get_notice_receiver())

    def test_get_notice_receiver_repeatedly(self):
        r = lambda notice: None  # noqa: E731
        self.c.set_notice_receiver(r)
        for _i in range(10):
            self.assertIs(self.c.get_notice_receiver(), r)

    def test_notice_receiver_is_kept(self):
        received = []
        self.c.set_notice_receiver(
            lambda notice: received.append(notice.primary))
        # the connection must keep its own reference to the receiver
        for _i in range(10):
            self.assertTrue(callable(self.c.get_notice_receiver()))
        self.c.query("do $$ begin raise warning 'Bilbo was here!'; end $$")
        self.assertEqual(received, ['Bilbo was here!'])

    def test_notice_receiver(self):
        self.addCleanup(self.c.query, 'drop function bilbo_notice();')
        self.c.query('''create function bilbo_notice() returns void AS $$
            begin
                raise warning 'Bilbo was here!';
            end;
            $$ language plpgsql''')
        received = {}

        def notice_receiver(notice):
            for attr in dir(notice):
                if attr.startswith('__'):
                    continue
                value = getattr(notice, attr)
                if isinstance(value, str):
                    value = value.replace('WARNUNG', 'WARNING')
                received[attr] = value

        self.c.set_notice_receiver(notice_receiver)
        self.c.query('select bilbo_notice()')
        self.assertEqual(received, dict(
            pgcnx=self.c, message='WARNING:  Bilbo was here!\n',
            severity='WARNING', primary='Bilbo was here!',
            detail=None, hint=None))

    def test_notice_can_be_kept(self):
        notices: list[Any] = []
        connections: list[Any] = []

        def receiver(notice):
            notices.append(notice)
            connections.append(notice.pgcnx)

        self.c.set_notice_receiver(receiver)
        self.c.query("""do $$ begin raise warning 'Bilbo was here!'
            using detail = 'In the Shire.', hint = 'Look for him.'; end $$""")
        self.assertEqual(len(notices), 1)
        self.assertEqual(connections, [self.c])
        notice = notices[0]
        self.assertEqual(
            {attr for attr in dir(notice) if not attr.startswith('_')},
            {'pgcnx', 'message', 'severity', 'primary', 'detail', 'hint'})
        # the fields must still be valid after the receiver has returned
        self.assertEqual(notice.primary, 'Bilbo was here!')
        self.assertEqual(notice.detail, 'In the Shire.')
        self.assertEqual(notice.hint, 'Look for him.')
        severity = notice.severity
        self.assertIn(severity, ('WARNING', 'WARNUNG'))
        message = notice.message
        self.assertTrue(message.startswith(f'{severity}:  Bilbo was here!\n'))
        self.assertEqual(str(notice), message)
        # but the connection is only available inside the receiver
        self.assertIsNone(notice.pgcnx)
        # the fields are read-only
        with self.assertRaises(AttributeError):
            notice.primary = 'Frodo was here!'

    def test_notice_str(self):
        received = []
        self.c.set_notice_receiver(lambda notice: received.append(str(notice)))
        self.c.query("do $$ begin raise warning 'Bilbo was here!'; end $$")
        self.assertEqual(len(received), 1)
        self.assertTrue(received[0].endswith(':  Bilbo was here!\n'))

    def test_notice_receiver_raising_exception(self):
        unraisable: list[Any] = []

        def receiver(notice):
            raise ValueError('Bilbo is gone!')

        self.c.set_notice_receiver(receiver)
        with mock.patch.object(sys, 'unraisablehook', unraisable.append):
            r = self.c.query(
                "do $$ begin raise warning 'Bilbo was here!'; end $$")
        # the exception must not break the query
        self.assertIsNone(r)
        self.assertEqual(self.c.query('select 1').getresult(), [(1,)])
        # but it must be reported as unraisable
        self.assertEqual(len(unraisable), 1)
        self.assertIs(unraisable[0].exc_type, ValueError)
        self.assertEqual(str(unraisable[0].exc_value), 'Bilbo is gone!')
        self.assertIs(unraisable[0].object, receiver)

    def test_notice_receiver_replacing_itself(self):
        received = []

        def other_receiver(notice):
            received.append(f'other: {notice.primary}')

        def receiver(notice):
            received.append(notice.primary)
            notice.pgcnx.set_notice_receiver(other_receiver)

        self.c.set_notice_receiver(receiver)
        del receiver  # the connection has the only reference now
        self.c.query("do $$ begin raise warning 'Bilbo was here!';"
                     " raise warning 'Frodo was here!'; end $$")
        self.assertEqual(
            received, ['Bilbo was here!', 'other: Frodo was here!'])
        self.assertIs(self.c.get_notice_receiver(), other_receiver)

    def test_notice_receiver_with_pending_exception(self):
        # libpq can call the notice receiver while an exception is pending,
        # e.g. when inserttable() sends the remaining data after the
        # iterable has raised an error; this exception must not get lost
        query = self.c.query
        query("create temporary table test_notice_pending (t text)")
        # use notices, since warnings would also go to the server log
        query("""create function pg_temp.test_notice_pending()
            returns trigger language plpgsql as $$ begin
            raise notice '%', repeat('x', 1000); return new; end $$""")
        query("""create trigger test_notice_pending
            before insert on test_notice_pending for each row
            execute function pg_temp.test_notice_pending()""")
        query("set client_min_messages to notice")
        # libpq only reads the notices while it is blocked sending data,
        # and passes them to the receiver when it sends the next data, so
        # make the send buffer small to ensure that this happens
        with socket.socket(fileno=os.dup(self.c.fileno())) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        raised = False  # set when the iterable raises the error
        received_after_error = 0

        def receiver(notice):
            nonlocal received_after_error
            if raised:
                received_after_error += 1

        self.c.set_notice_receiver(receiver)
        row = ('y' * 1000,)

        def rows():
            nonlocal raised
            # inserttable() sends the data in chunks of 128 kB
            for _i in range(300):
                yield row
            raised = True
            raise RuntimeError('Bilbo is gone!')

        for _i in range(5):
            raised = False
            with self.assertRaises(RuntimeError) as cm:
                self.c.inserttable('test_notice_pending', rows())
            self.assertEqual(str(cm.exception), 'Bilbo is gone!')
            raised = False
            # the COPY must not have been completed
            self.assertEqual(query(
                "select count(*) from test_notice_pending").getresult(),
                [(0,)])
        # make sure that the test really covered the situation
        self.assertGreater(received_after_error, 0)

    def test_notice_receiver_with_undecodable_message(self):
        encoding = self.c.parameter('server_encoding')
        self.c.query("set client_encoding to sql_ascii")
        self.addCleanup(self.c.query, "reset client_encoding")
        received = []
        self.c.set_notice_receiver(
            lambda notice: received.append(notice.primary))
        self.c.query("do $$ begin raise warning 'w%r', chr(228); end $$")
        # the message is decoded as UTF-8 with escapes as a fallback
        self.assertEqual(received, [
            'wär' if encoding == 'UTF8' else 'w\\xe4r'])

    def test_notice_receiver_replaced_in_del(self):
        c = self.c
        received = []

        class Receiver:
            def __call__(self, notice):
                pass

            def __del__(self):
                # the old receiver must already have been replaced
                received.append(c.get_notice_receiver())

        def other_receiver(notice):
            pass

        c.set_notice_receiver(Receiver())
        c.set_notice_receiver(other_receiver)
        self.assertEqual(received, [other_receiver])
        c.set_notice_receiver(Receiver())
        c.set_notice_receiver(None)
        self.assertEqual(received, [other_receiver, None])

    def test_set_notice_receiver_none_restores_default(self):
        query = self.c.query
        self.c.set_notice_receiver(lambda notice: None)
        self.c.set_notice_receiver(None)
        # the default receiver of libpq prints the message to stderr
        with tempfile.TemporaryFile() as f:
            stderr = os.dup(2)
            try:
                sys.stderr.flush()
                os.dup2(f.fileno(), 2)
                query("do $$ begin raise warning 'Bilbo was here!'; end $$")
            finally:
                os.dup2(stderr, 2)
                os.close(stderr)
            f.seek(0)
            output = f.read()
        self.assertIn(b'Bilbo was here!', output)

    def test_notice_receiver_released_with_connection(self):
        class Receiver:
            def __call__(self, notice):
                pass

        for close in (True, False):
            with self.subTest(close=close):
                c = connect()
                receiver = Receiver()
                ref = weakref.ref(receiver)
                c.set_notice_receiver(receiver)
                del receiver
                self.assertIsNotNone(ref())
                if close:
                    c.close()
                else:
                    del c
                self.assertIsNone(ref())

    def test_notice_receiver_in_reference_cycle(self):
        class Holder:
            def __init__(self):
                self.c = connect()
                self.c.set_notice_receiver(self.receive)

            def receive(self, notice):
                pass

        holder = Holder()
        ref = weakref.ref(holder)
        # the connection holds the bound method, which holds the holder
        del holder
        gc.collect()
        self.assertIsNone(ref())

    def test_notice_receiver_with_client_encoding(self):
        encoding = self.c.parameter('server_encoding')
        if encoding not in ('UTF8', 'SQL_ASCII', 'LATIN1'):
            self.skipTest(f'cannot convert {encoding} to LATIN1')
        self.c.query("set client_encoding to latin1")
        self.addCleanup(self.c.query, "reset client_encoding")
        received = []
        self.c.set_notice_receiver(
            lambda notice: received.append(notice.primary))
        self.c.query("do $$ begin raise warning 'Bilbo wär hier!'; end $$")
        self.assertEqual(received, ['Bilbo wär hier!'])


class TestConfigFunctions(unittest.TestCase):
    """Test the functions for changing default settings.

    To test the effect of most of these functions, we need a database
    connection.  That's why they are covered in this test module.
    """

    def setUp(self):
        self.c = connect()
        self.c.query("set client_encoding=utf8")
        self.c.query('set bytea_output=hex')
        self.c.query("set lc_monetary='C'")

    def tearDown(self):
        self.c.close()

    def test_get_decimal_point(self):
        point = pg.get_decimal_point()
        # error if a parameter is passed
        self.assertRaises(TypeError, pg.get_decimal_point, point)
        self.assertIsInstance(point, str)
        self.assertEqual(point, '.')  # the default setting
        pg.set_decimal_point(',')
        try:
            r = pg.get_decimal_point()
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertEqual(r, ',')
        pg.set_decimal_point("'")
        try:
            r = pg.get_decimal_point()
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertEqual(r, "'")
        pg.set_decimal_point('')
        try:
            r = pg.get_decimal_point()
        finally:
            pg.set_decimal_point(point)
        self.assertIsNone(r)
        pg.set_decimal_point(None)
        try:
            r = pg.get_decimal_point()
        finally:
            pg.set_decimal_point(point)
        self.assertIsNone(r)

    def test_set_decimal_point(self):
        d = Decimal
        point = pg.get_decimal_point()
        self.assertRaises(TypeError, pg.set_decimal_point)
        # error if decimal point is not a string
        self.assertRaises(TypeError, pg.set_decimal_point, 0)
        # error if more than one decimal point passed
        self.assertRaises(TypeError, pg.set_decimal_point, '.', ',')
        self.assertRaises(TypeError, pg.set_decimal_point, '.,')
        # error if decimal point is not a punctuation character
        self.assertRaises(TypeError, pg.set_decimal_point, '0')
        query = self.c.query
        # check that money values are interpreted as decimal values
        # only if decimal_point is set, and that the result is correct
        # only if it is set suitable for the current lc_monetary setting
        select_money = "select '34.25'::money"
        proper_money = d('34.25')
        bad_money = d('3425')
        en_locales = 'en', 'en_US', 'en_US.utf8', 'en_US.UTF-8'
        en_money = '$34.25', '$ 34.25', '34.25$', '34.25 $', '34.25 Dollar'
        de_locales = 'de', 'de_DE', 'de_DE.utf8', 'de_DE.UTF-8'
        de_money = (
            '34,25€', '34,25 €', '€34,25', '€ 34,25',
            'EUR34,25', 'EUR 34,25', '34,25 EUR', '34,25 Euro', '34,25 DM')
        # first try with English localization (using the point)
        for lc in en_locales:
            try:
                query(f"set lc_monetary='{lc}'")
            except pg.DataError:
                pass
            else:
                break
        else:
            self.skipTest("cannot set English money locale")
        try:
            query(select_money)
        except (pg.DataError, pg.ProgrammingError):
            # this can happen if the currency signs cannot be
            # converted using the encoding of the test database
            self.skipTest("database does not support English money")
        pg.set_decimal_point(None)
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertIn(r, en_money)
        pg.set_decimal_point('')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertIn(r, en_money)
        pg.set_decimal_point('.')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, d)
        self.assertEqual(r, proper_money)
        pg.set_decimal_point(',')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, d)
        self.assertEqual(r, bad_money)
        pg.set_decimal_point("'")
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, d)
        self.assertEqual(r, bad_money)
        # then try with German localization (using the comma)
        for lc in de_locales:
            try:
                query(f"set lc_monetary='{lc}'")
            except pg.DataError:
                pass
            else:
                break
        else:
            self.skipTest("cannot set German money locale")
        select_money = select_money.replace('.', ',')
        try:
            query(select_money)
        except (pg.DataError, pg.ProgrammingError):
            self.skipTest("database does not support German money")
        pg.set_decimal_point(None)
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertIn(r, de_money)
        pg.set_decimal_point('')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, str)
        self.assertIn(r, de_money)
        pg.set_decimal_point(',')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertIsInstance(r, d)
        self.assertEqual(r, proper_money)
        pg.set_decimal_point('.')
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertEqual(r, bad_money)
        pg.set_decimal_point("'")
        try:
            r = query(select_money).getresult()[0][0]
        finally:
            pg.set_decimal_point(point)
        self.assertEqual(r, bad_money)

    def test_get_decimal(self):
        decimal_class = pg.get_decimal()
        # error if a parameter is passed
        self.assertRaises(TypeError, pg.get_decimal, decimal_class)
        self.assertIs(decimal_class, Decimal)  # the default setting
        pg.set_decimal(int)
        try:
            r = pg.get_decimal()
        finally:
            pg.set_decimal(decimal_class)
        self.assertIs(r, int)
        r = pg.get_decimal()
        self.assertIs(r, decimal_class)

    def test_set_decimal(self):
        decimal_class = pg.get_decimal()
        # error if no parameter is passed
        self.assertRaises(TypeError, pg.set_decimal)
        query = self.c.query
        try:
            r = query("select 3425::numeric")
        except pg.DatabaseError:
            self.skipTest('database does not support numeric')
        r = r.getresult()[0][0]
        self.assertIsInstance(r, decimal_class)
        self.assertEqual(r, decimal_class('3425'))
        r = query("select 3425::numeric")
        pg.set_decimal(int)
        try:
            r = r.getresult()[0][0]
        finally:
            pg.set_decimal(decimal_class)
        self.assertNotIsInstance(r, decimal_class)
        self.assertIsInstance(r, int)
        self.assertEqual(r, 3425)

    def test_get_bool(self):
        use_bool = pg.get_bool()
        # error if a parameter is passed
        self.assertRaises(TypeError, pg.get_bool, use_bool)
        self.assertIsInstance(use_bool, bool)
        self.assertIs(use_bool, True)  # the default setting
        pg.set_bool(False)
        try:
            r = pg.get_bool()
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, bool)
        self.assertIs(r, False)
        pg.set_bool(True)
        try:
            r = pg.get_bool()
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, bool)
        self.assertIs(r, True)
        pg.set_bool(0)
        try:
            r = pg.get_bool()
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, bool)
        self.assertIs(r, False)
        pg.set_bool(1)
        try:
            r = pg.get_bool()
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, bool)
        self.assertIs(r, True)

    def test_set_bool(self):
        use_bool = pg.get_bool()
        # error if no parameter is passed
        self.assertRaises(TypeError, pg.set_bool)
        query = self.c.query
        try:
            r = query("select true::bool")
        except pg.ProgrammingError:
            self.skipTest('database does not support bool')
        r = r.getresult()[0][0]
        self.assertIsInstance(r, bool)
        self.assertEqual(r, True)
        pg.set_bool(False)
        try:
            r = query("select true::bool").getresult()[0][0]
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, str)
        self.assertEqual(r, 't')
        pg.set_bool(True)
        try:
            r = query("select true::bool").getresult()[0][0]
        finally:
            pg.set_bool(use_bool)
        self.assertIsInstance(r, bool)
        self.assertIs(r, True)

    def test_get_byte_escaped(self):
        bytea_escaped = pg.get_bytea_escaped()
        # error if a parameter is passed
        self.assertRaises(TypeError, pg.get_bytea_escaped, bytea_escaped)
        self.assertIsInstance(bytea_escaped, bool)
        self.assertIs(bytea_escaped, False)  # the default setting
        pg.set_bytea_escaped(True)
        try:
            r = pg.get_bytea_escaped()
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, bool)
        self.assertIs(r, True)
        pg.set_bytea_escaped(False)
        try:
            r = pg.get_bytea_escaped()
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, bool)
        self.assertIs(r, False)
        pg.set_bytea_escaped(1)
        try:
            r = pg.get_bytea_escaped()
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, bool)
        self.assertIs(r, True)
        pg.set_bytea_escaped(0)
        try:
            r = pg.get_bytea_escaped()
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, bool)
        self.assertIs(r, False)

    def test_set_bytea_escaped(self):
        bytea_escaped = pg.get_bytea_escaped()
        # error if no parameter is passed
        self.assertRaises(TypeError, pg.set_bytea_escaped)
        query = self.c.query
        try:
            r = query("select 'data'::bytea")
        except pg.ProgrammingError:
            self.skipTest('database does not support bytea')
        r = r.getresult()[0][0]
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'data')
        pg.set_bytea_escaped(True)
        try:
            r = query("select 'data'::bytea").getresult()[0][0]
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '\\x64617461')
        pg.set_bytea_escaped(False)
        try:
            r = query("select 'data'::bytea").getresult()[0][0]
        finally:
            pg.set_bytea_escaped(bytea_escaped)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'data')

    def test_change_row_factory_cache_size(self):
        cache = pg.RowCache
        queries = ['select 1 as a, 2 as b, 3 as c', 'select 123 as abc']
        query = self.c.query
        for maxsize in (None, 0, 1, 2, 3, 10, 1024):
            cache.change_size(maxsize)
            for _i in range(3):
                for q in queries:
                    r = query(q).namedresult()[0]
                    if q.endswith('abc'):
                        self.assertEqual(r, (123,))
                        self.assertEqual(r._fields, ('abc',))
                    else:
                        self.assertEqual(r, (1, 2, 3))
                        self.assertEqual(r._fields, ('a', 'b', 'c'))
            info = cache.row_factory.cache_info()
            self.assertEqual(info.maxsize, maxsize)
            self.assertEqual(info.hits + info.misses, 6)
            self.assertEqual(info.hits,
                             0 if maxsize is not None and maxsize < 2 else 4)


class TestStandaloneEscapeFunctions(unittest.TestCase):
    """Test pg escape functions.

    The libpq interface memorizes some parameters of the last opened
    connection that influence the result of these functions.  Therefore
    we need to open a connection with fixed parameters prior to testing
    in order to ensure that the tests always run under the same conditions.
    That's why these tests are included in this test module.
    """

    cls_set_up = False
    std_strings = False

    @classmethod
    def setUpClass(cls):
        db = connect()
        query = db.query
        query('set client_encoding=sql_ascii')
        # PostgreSQL 19 does not support non-standard strings any more
        cls.std_strings = db.server_version >= 190000
        scs = 'on' if cls.std_strings else 'off'
        query(f'set standard_conforming_strings={scs}')
        query('set bytea_output=escape')
        db.close()
        cls.cls_set_up = True

    def test_escape_string(self):
        self.assertTrue(self.cls_set_up)
        f = pg.escape_string
        b = f(b'plain')
        self.assertIsInstance(b, bytes)
        self.assertEqual(b, b'plain')
        s = f('plain')
        self.assertIsInstance(s, str)
        self.assertEqual(s, 'plain')
        b = f("das is' käse".encode())
        self.assertIsInstance(b, bytes)
        self.assertEqual(b, "das is'' käse".encode())
        s = f("that's cheesy")
        self.assertIsInstance(s, str)
        self.assertEqual(s, "that''s cheesy")
        s = f(r"It's bad to have a \ inside.")
        if self.std_strings:
            self.assertEqual(s, r"It''s bad to have a \ inside.")
        else:
            self.assertEqual(s, r"It''s bad to have a \\ inside.")

    def test_escape_bytea(self):
        self.assertTrue(self.cls_set_up)
        f = pg.escape_bytea
        b = f(b'plain')
        self.assertIsInstance(b, bytes)
        self.assertEqual(b, b'plain')
        s = f('plain')
        self.assertIsInstance(s, str)
        self.assertEqual(s, 'plain')
        b = f("das is' käse".encode())
        self.assertIsInstance(b, bytes)
        if self.std_strings:
            self.assertEqual(b, b"das is'' k\\303\\244se")
        else:
            self.assertEqual(b, b"das is'' k\\\\303\\\\244se")
        s = f("that's cheesy")
        self.assertIsInstance(s, str)
        self.assertEqual(s, "that''s cheesy")
        b = f(b'O\x00ps\xff!')
        if self.std_strings:
            self.assertEqual(b, b'O\\000ps\\377!')
        else:
            self.assertEqual(b, b'O\\\\000ps\\\\377!')


if __name__ == '__main__':
    unittest.main()
