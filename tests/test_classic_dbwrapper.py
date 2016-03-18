#! /usr/bin/python
# -*- coding: utf-8 -*-

"""Test the classic PyGreSQL interface.

Sub-tests for the DB wrapper object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.

"""

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest
import os

import sys

import pg  # the module under test

from decimal import Decimal

# check whether the "with" statement is supported
no_with = sys.version_info[:2] < (2, 5)

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
# The current user must have create schema privilege on the database.
dbname = 'unittest'
dbhost = None
dbport = 5432

debug = False  # let DB wrapper print debugging output

try:
    from LOCAL_PyGreSQL import *
except ImportError:
    pass

windows = os.name == 'nt'

# There is a known a bug in libpq under Windows which can cause
# the interface to crash when calling PQhost():
do_not_ask_for_host = windows
do_not_ask_for_host_reason = 'libpq issue on Windows'


def DB():
    """Create a DB wrapper object connecting to the test database."""
    db = pg.DB(dbname, dbhost, dbport)
    if debug:
        db.debug = debug
    db.query("set client_min_messages=warning")
    return db


class TestDBClassBasic(unittest.TestCase):
    """Test existence of the DB class wrapped pg connection methods."""

    def setUp(self):
        self.db = DB()

    def tearDown(self):
        try:
            self.db.close()
        except pg.InternalError:
            pass

    def testAllDBAttributes(self):
        attributes = [
            'abort',
            'begin',
            'cancel', 'clear', 'close', 'commit',
            'db', 'dbname', 'debug', 'delete',
            'end', 'endcopy', 'error',
            'escape_bytea', 'escape_identifier',
            'escape_literal', 'escape_string',
            'fileno',
            'get', 'get_attnames', 'get_databases',
            'get_notice_receiver', 'get_parameter',
            'get_relations', 'get_tables',
            'getline', 'getlo', 'getnotify',
            'has_table_privilege', 'host',
            'insert', 'inserttable',
            'locreate', 'loimport',
            'notification_handler',
            'options',
            'parameter', 'pkey', 'port',
            'protocol_version', 'putline',
            'query',
            'release', 'reopen', 'reset', 'rollback',
            'savepoint', 'server_version',
            'set_notice_receiver', 'set_parameter',
            'source', 'start', 'status',
            'transaction', 'truncate', 'tty',
            'unescape_bytea', 'update',
            'use_regtypes', 'user',
        ]
        if self.db.server_version < 90000:  # PostgreSQL < 9.0
            attributes.remove('escape_identifier')
            attributes.remove('escape_literal')
        db_attributes = [a for a in dir(self.db)
            if not a.startswith('_')]
        self.assertEqual(attributes, db_attributes)

    def testAttributeDb(self):
        self.assertEqual(self.db.db.db, dbname)

    def testAttributeDbname(self):
        self.assertEqual(self.db.dbname, dbname)

    def testAttributeError(self):
        error = self.db.error
        self.assertTrue(not error or 'krb5_' in error)
        self.assertEqual(self.db.error, self.db.db.error)

    @unittest.skipIf(do_not_ask_for_host, do_not_ask_for_host_reason)
    def testAttributeHost(self):
        def_host = 'localhost'
        host = self.db.host
        self.assertIsInstance(host, str)
        self.assertEqual(host, dbhost or def_host)
        self.assertEqual(host, self.db.db.host)

    def testAttributeOptions(self):
        no_options = ''
        options = self.db.options
        self.assertEqual(options, no_options)
        self.assertEqual(options, self.db.db.options)

    def testAttributePort(self):
        def_port = 5432
        port = self.db.port
        self.assertIsInstance(port, int)
        self.assertEqual(port, dbport or def_port)
        self.assertEqual(port, self.db.db.port)

    def testAttributeProtocolVersion(self):
        protocol_version = self.db.protocol_version
        self.assertIsInstance(protocol_version, int)
        self.assertTrue(2 <= protocol_version < 4)
        self.assertEqual(protocol_version, self.db.db.protocol_version)

    def testAttributeServerVersion(self):
        server_version = self.db.server_version
        self.assertIsInstance(server_version, int)
        self.assertTrue(70400 <= server_version < 100000)
        self.assertEqual(server_version, self.db.db.server_version)

    def testAttributeStatus(self):
        status_ok = 1
        status = self.db.status
        self.assertIsInstance(status, int)
        self.assertEqual(status, status_ok)
        self.assertEqual(status, self.db.db.status)

    def testAttributeTty(self):
        def_tty = ''
        tty = self.db.tty
        self.assertIsInstance(tty, str)
        self.assertEqual(tty, def_tty)
        self.assertEqual(tty, self.db.db.tty)

    def testAttributeUser(self):
        no_user = 'Deprecated facility'
        user = self.db.user
        self.assertTrue(user)
        self.assertIsInstance(user, str)
        self.assertNotEqual(user, no_user)
        self.assertEqual(user, self.db.db.user)

    def testMethodEscapeLiteral(self):
        if self.db.server_version < 90000:  # PostgreSQL < 9.0
            self.skipTest('Escaping functions not supported')
        self.assertEqual(self.db.escape_literal(''), "''")

    def testMethodEscapeIdentifier(self):
        if self.db.server_version < 90000:  # PostgreSQL < 9.0
            self.skipTest('Escaping functions not supported')
        self.assertEqual(self.db.escape_identifier(''), '""')

    def testMethodEscapeString(self):
        self.assertEqual(self.db.escape_string(''), '')

    def testMethodEscapeBytea(self):
        self.assertEqual(self.db.escape_bytea('').replace(
            '\\x', '').replace('\\', ''), '')

    def testMethodUnescapeBytea(self):
        self.assertEqual(self.db.unescape_bytea(''), '')

    def testMethodQuery(self):
        query = self.db.query
        query("select 1+1")
        query("select 1+$1+$2", 2, 3)
        query("select 1+$1+$2", (2, 3))
        query("select 1+$1+$2", [2, 3])
        query("select 1+$1", 1)

    def testMethodQueryEmpty(self):
        self.assertRaises(ValueError, self.db.query, '')

    def testMethodQueryProgrammingError(self):
        try:
            self.db.query("select 1/0")
        except pg.ProgrammingError, error:
            self.assertEqual(error.sqlstate, '22012')

    def testMethodEndcopy(self):
        try:
            self.db.endcopy()
        except IOError:
            pass

    def testMethodClose(self):
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

    def testMethodReset(self):
        con = self.db.db
        self.db.reset()
        self.assertIs(self.db.db, con)
        self.db.query("select 1+1")
        self.db.close()
        self.assertRaises(pg.InternalError, self.db.reset)

    def testMethodReopen(self):
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

    def testExistingConnection(self):
        db = pg.DB(self.db.db)
        self.assertEqual(self.db.db, db.db)
        self.assertTrue(db.db)
        db.close()
        self.assertTrue(db.db)
        db.reopen()
        self.assertTrue(db.db)
        db.close()
        self.assertTrue(db.db)
        db = pg.DB(self.db)
        self.assertEqual(self.db.db, db.db)
        db = pg.DB(db=self.db.db)
        self.assertEqual(self.db.db, db.db)

        class DB2:
            pass

        db2 = DB2()
        db2._cnx = self.db.db
        db = pg.DB(db2)
        self.assertEqual(self.db.db, db.db)


class TestDBClass(unittest.TestCase):
    """Test the methods of the DB class wrapped pg connection."""

    cls_set_up = False

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
        query = self.db.query
        query('set client_encoding=utf8')
        query('set standard_conforming_strings=on')
        query("set lc_monetary='C'")
        query("set datestyle='ISO,YMD'")
        try:
            query('set bytea_output=hex')
        except pg.ProgrammingError:  # PostgreSQL < 9.0
            pass

    def tearDown(self):
        self.db.close()

    def testEscapeLiteral(self):
        if self.db.server_version < 90000:  # PostgreSQL < 9.0
            self.skipTest('Escaping functions not supported')
        f = self.db.escape_literal
        self.assertEqual(f("plain"), "'plain'")
        self.assertEqual(f("that's k\xe4se"), "'that''s k\xe4se'")
        self.assertEqual(f(r"It's fine to have a \ inside."),
            r" E'It''s fine to have a \\ inside.'")
        self.assertEqual(f('No "quotes" must be escaped.'),
            "'No \"quotes\" must be escaped.'")

    def testEscapeIdentifier(self):
        if self.db.server_version < 90000:  # PostgreSQL < 9.0
            self.skipTest('Escaping functions not supported')
        f = self.db.escape_identifier
        self.assertEqual(f("plain"), '"plain"')
        self.assertEqual(f("that's k\xe4se"), '"that\'s k\xe4se"')
        self.assertEqual(f(r"It's fine to have a \ inside."),
            '"It\'s fine to have a \\ inside."')
        self.assertEqual(f('All "quotes" must be escaped.'),
            '"All ""quotes"" must be escaped."')

    def testEscapeString(self):
        f = self.db.escape_string
        self.assertEqual(f("plain"), "plain")
        self.assertEqual(f("that's k\xe4se"), "that''s k\xe4se")
        self.assertEqual(f(r"It's fine to have a \ inside."),
            r"It''s fine to have a \ inside.")

    def testEscapeBytea(self):
        f = self.db.escape_bytea
        # note that escape_byte always returns hex output since PostgreSQL 9.0,
        # regardless of the bytea_output setting
        if self.db.server_version < 90000:
            self.assertEqual(f("plain"), r"plain")
            self.assertEqual(f("that's k\xe4se"), r"that''s k\344se")
            self.assertEqual(f('O\x00ps\xff!'), r"O\000ps\377!")
        else:
            self.assertEqual(f("plain"), r"\x706c61696e")
            self.assertEqual(f("that's k\xe4se"), r"\x746861742773206be47365")
            self.assertEqual(f('O\x00ps\xff!'), r"\x4f007073ff21")

    def testUnescapeBytea(self):
        f = self.db.unescape_bytea
        self.assertEqual(f("plain"), "plain")
        self.assertEqual(f("that's k\\344se"), "that's k\xe4se")
        self.assertEqual(f(r'O\000ps\377!'), 'O\x00ps\xff!')
        self.assertEqual(f(r"\\x706c61696e"), r"\x706c61696e")
        self.assertEqual(f(r"\\x746861742773206be47365"),
            r"\x746861742773206be47365")
        self.assertEqual(f(r"\\x4f007073ff21"), r"\x4f007073ff21")

    def testQuote(self):
        f = self.db._quote
        self.assertEqual(f(None, None), 'NULL')
        self.assertEqual(f(None, 'int'), 'NULL')
        self.assertEqual(f(None, 'float'), 'NULL')
        self.assertEqual(f(None, 'num'), 'NULL')
        self.assertEqual(f(None, 'money'), 'NULL')
        self.assertEqual(f(None, 'bool'), 'NULL')
        self.assertEqual(f(None, 'date'), 'NULL')
        self.assertEqual(f('', 'int'), 'NULL')
        self.assertEqual(f('', 'float'), 'NULL')
        self.assertEqual(f('', 'num'), 'NULL')
        self.assertEqual(f('', 'money'), 'NULL')
        self.assertEqual(f('', 'bool'), 'NULL')
        self.assertEqual(f('', 'date'), 'NULL')
        self.assertEqual(f('', 'text'), "''")
        self.assertEqual(f(0, 'int'), '0')
        self.assertEqual(f(0, 'num'), '0')
        self.assertEqual(f(1, 'int'), '1')
        self.assertEqual(f(1, 'num'), '1')
        self.assertEqual(f(-1, 'int'), '-1')
        self.assertEqual(f(-1, 'num'), '-1')
        self.assertEqual(f(123456789, 'int'), '123456789')
        self.assertEqual(f(123456987, 'num'), '123456987')
        self.assertEqual(f(1.23654789, 'num'), '1.23654789')
        self.assertEqual(f(12365478.9, 'num'), '12365478.9')
        self.assertEqual(f('123456789', 'num'), '123456789')
        self.assertEqual(f('1.23456789', 'num'), '1.23456789')
        self.assertEqual(f('12345678.9', 'num'), '12345678.9')
        self.assertEqual(f(123, 'money'), '123')
        self.assertEqual(f('123', 'money'), '123')
        self.assertEqual(f(123.45, 'money'), '123.45')
        self.assertEqual(f('123.45', 'money'), '123.45')
        self.assertEqual(f(123.454, 'money'), '123.454')
        self.assertEqual(f('123.454', 'money'), '123.454')
        self.assertEqual(f(123.456, 'money'), '123.456')
        self.assertEqual(f('123.456', 'money'), '123.456')
        self.assertEqual(f('f', 'bool'), "'f'")
        self.assertEqual(f('F', 'bool'), "'f'")
        self.assertEqual(f('false', 'bool'), "'f'")
        self.assertEqual(f('False', 'bool'), "'f'")
        self.assertEqual(f('FALSE', 'bool'), "'f'")
        self.assertEqual(f(0, 'bool'), "'f'")
        self.assertEqual(f('0', 'bool'), "'f'")
        self.assertEqual(f('-', 'bool'), "'f'")
        self.assertEqual(f('n', 'bool'), "'f'")
        self.assertEqual(f('N', 'bool'), "'f'")
        self.assertEqual(f('no', 'bool'), "'f'")
        self.assertEqual(f('off', 'bool'), "'f'")
        self.assertEqual(f('t', 'bool'), "'t'")
        self.assertEqual(f('T', 'bool'), "'t'")
        self.assertEqual(f('true', 'bool'), "'t'")
        self.assertEqual(f('True', 'bool'), "'t'")
        self.assertEqual(f('TRUE', 'bool'), "'t'")
        self.assertEqual(f(1, 'bool'), "'t'")
        self.assertEqual(f(2, 'bool'), "'t'")
        self.assertEqual(f(-1, 'bool'), "'t'")
        self.assertEqual(f(0.5, 'bool'), "'t'")
        self.assertEqual(f('1', 'bool'), "'t'")
        self.assertEqual(f('y', 'bool'), "'t'")
        self.assertEqual(f('Y', 'bool'), "'t'")
        self.assertEqual(f('yes', 'bool'), "'t'")
        self.assertEqual(f('on', 'bool'), "'t'")
        self.assertEqual(f('01.01.2000', 'date'), "'01.01.2000'")
        self.assertEqual(f(123, 'text'), "'123'")
        self.assertEqual(f(1.23, 'text'), "'1.23'")
        self.assertEqual(f('abc', 'text'), "'abc'")
        self.assertEqual(f("ab'c", 'text'), "'ab''c'")
        self.assertEqual(f('ab\\c', 'text'), "'ab\\c'")
        self.assertEqual(f("a\\b'c", 'text'), "'a\\b''c'")
        self.db.query('set standard_conforming_strings=off')
        self.assertEqual(f('ab\\c', 'text'), "'ab\\\\c'")
        self.assertEqual(f("a\\b'c", 'text'), "'a\\\\b''c'")

    def testGetParameter(self):
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
        r = f(set(['bytea_output', 'lc_monetary']))
        self.assertIsInstance(r, dict)
        self.assertEqual(r, {'bytea_output': 'hex', 'lc_monetary': 'C'})
        r = f(set(['Bytea_Output', ' LC_Monetary ']))
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

    def testGetParameterServerVersion(self):
        r = self.db.get_parameter('server_version_num')
        self.assertIsInstance(r, str)
        s = self.db.server_version
        self.assertIsInstance(s, int)
        self.assertEqual(r, str(s))

    def testGetParameterAll(self):
        f = self.db.get_parameter
        r = f('all')
        self.assertIsInstance(r, dict)
        self.assertEqual(r['standard_conforming_strings'], 'on')
        self.assertEqual(r['lc_monetary'], 'C')
        self.assertEqual(r['DateStyle'], 'ISO, YMD')
        self.assertEqual(r['bytea_output'], 'hex')

    def testSetParameter(self):
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
        f(['default_with_oids', 'standard_conforming_strings'], 'off')
        self.assertEqual(g('default_with_oids'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f(('standard_conforming_strings', 'datestyle'), ('on', 'ISO, YMD'))
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')
        f(('default_with_oids', 'standard_conforming_strings'), 'off')
        self.assertEqual(g('default_with_oids'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f(set(['default_with_oids', 'standard_conforming_strings']), 'on')
        self.assertEqual(g('default_with_oids'), 'on')
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertRaises(ValueError, f, set([ 'default_with_oids',
            'standard_conforming_strings']), ['off', 'on'])
        f(set(['default_with_oids', 'standard_conforming_strings']),
            ['off', 'off'])
        self.assertEqual(g('default_with_oids'), 'off')
        self.assertEqual(g('standard_conforming_strings'), 'off')
        f({'standard_conforming_strings': 'on', 'datestyle': 'ISO, YMD'})
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.assertEqual(g('datestyle'), 'ISO, YMD')

    def testResetParameter(self):
        db = DB()
        f = db.set_parameter
        g = db.get_parameter
        r = g('default_with_oids')
        self.assertIn(r, ('on', 'off'))
        dwi, not_dwi = r, r == 'on' and 'off' or 'on'
        r = g('standard_conforming_strings')
        self.assertIn(r, ('on', 'off'))
        scs, not_scs = r, r == 'on' and 'off' or 'on'
        f('default_with_oids', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('default_with_oids'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f('default_with_oids')
        f('standard_conforming_strings', None)
        self.assertEqual(g('default_with_oids'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('default_with_oids', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('default_with_oids'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f(['default_with_oids', 'standard_conforming_strings'], None)
        self.assertEqual(g('default_with_oids'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('default_with_oids', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('default_with_oids'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f(('default_with_oids', 'standard_conforming_strings'))
        self.assertEqual(g('default_with_oids'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)
        f('default_with_oids', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('default_with_oids'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f(set(['default_with_oids', 'standard_conforming_strings']), None)
        self.assertEqual(g('default_with_oids'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)

    def testResetParameterAll(self):
        db = DB()
        f = db.set_parameter
        self.assertRaises(ValueError, f, 'all', 0)
        self.assertRaises(ValueError, f, 'all', 'off')
        g = db.get_parameter
        r = g('default_with_oids')
        self.assertIn(r, ('on', 'off'))
        dwi, not_dwi = r, r == 'on' and 'off' or 'on'
        r = g('standard_conforming_strings')
        self.assertIn(r, ('on', 'off'))
        scs, not_scs = r, r == 'on' and 'off' or 'on'
        f('default_with_oids', not_dwi)
        f('standard_conforming_strings', not_scs)
        self.assertEqual(g('default_with_oids'), not_dwi)
        self.assertEqual(g('standard_conforming_strings'), not_scs)
        f('all')
        self.assertEqual(g('default_with_oids'), dwi)
        self.assertEqual(g('standard_conforming_strings'), scs)

    def testSetParameterLocal(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.db.begin()
        f('standard_conforming_strings', 'off', local=True)
        self.assertEqual(g('standard_conforming_strings'), 'off')
        self.db.end()
        self.assertEqual(g('standard_conforming_strings'), 'on')

    def testSetParameterSession(self):
        f = self.db.set_parameter
        g = self.db.get_parameter
        self.assertEqual(g('standard_conforming_strings'), 'on')
        self.db.begin()
        f('standard_conforming_strings', 'off', local=False)
        self.assertEqual(g('standard_conforming_strings'), 'off')
        self.db.end()
        self.assertEqual(g('standard_conforming_strings'), 'off')

    def testQuery(self):
        query = self.db.query
        query("drop table if exists test_table")
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
        self.assertEqual(r, oid)
        q = "insert into test_table select 3 union select 4 union select 5"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '3')
        q = "update test_table set n=4 where n<5"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '4')
        q = "delete from test_table"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '5')
        query("drop table test_table")

    def testMultipleQueries(self):
        self.assertEqual(self.db.query(
            "create temporary table test_multi (n integer);"
            "insert into test_multi values (4711);"
            "select n from test_multi").getresult()[0][0], 4711)

    def testQueryWithParams(self):
        query = self.db.query
        query("drop table if exists test_table")
        q = "create table test_table (n1 integer, n2 integer) with oids"
        query(q)
        q = "insert into test_table values ($1, $2)"
        r = query(q, (1, 2))
        self.assertIsInstance(r, int)
        r = query(q, [3, 4])
        self.assertIsInstance(r, int)
        r = query(q, [5, 6])
        self.assertIsInstance(r, int)
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
        query("drop table test_table")

    def testEmptyQuery(self):
        self.assertRaises(ValueError, self.db.query, '')

    def testQueryProgrammingError(self):
        try:
            self.db.query("select 1/0")
        except pg.ProgrammingError, error:
            self.assertEqual(error.sqlstate, '22012')

    def testPkey(self):
        query = self.db.query
        for n in range(4):
            query("drop table if exists pkeytest%d" % n)
        query("create table pkeytest0 ("
            "a smallint)")
        query("create table pkeytest1 ("
            "b smallint primary key)")
        query("create table pkeytest2 ("
            "c smallint, d smallint primary key)")
        query("create table pkeytest3 ("
            "e smallint, f smallint, g smallint,"
            " h smallint, i smallint,"
            " primary key (f,h))")
        pkey = self.db.pkey
        self.assertRaises(KeyError, pkey, 'pkeytest0')
        self.assertEqual(pkey('pkeytest1'), 'b')
        self.assertEqual(pkey('pkeytest2'), 'd')
        self.assertEqual(pkey('pkeytest3'), frozenset('fh'))
        self.assertEqual(pkey('pkeytest0', 'none'), 'none')
        self.assertEqual(pkey('pkeytest0'), 'none')
        pkey(None, {'t': 'a', 'n.t': 'b'})
        self.assertEqual(pkey('t'), 'a')
        self.assertEqual(pkey('n.t'), 'b')
        self.assertRaises(KeyError, pkey, 'pkeytest0')
        for n in range(4):
            query("drop table pkeytest%d" % n)

    def testGetDatabases(self):
        databases = self.db.get_databases()
        self.assertIn('template0', databases)
        self.assertIn('template1', databases)
        self.assertNotIn('not existing database', databases)
        self.assertIn('postgres', databases)
        self.assertIn(dbname, databases)

    def testGetTables(self):
        get_tables = self.db.get_tables
        result1 = get_tables()
        self.assertIsInstance(result1, list)
        for t in result1:
            t = t.split('.', 1)
            self.assertGreaterEqual(len(t), 2)
            if len(t) > 2:
                self.assertTrue(t[1].startswith('"'))
            t = t[0]
            self.assertNotEqual(t, 'information_schema')
            self.assertFalse(t.startswith('pg_'))
        tables = ('"A very Special Name"',
            '"A_MiXeD_quoted_NaMe"', 'a1', 'a2',
            'A_MiXeD_NaMe', '"another special name"',
            'averyveryveryveryveryveryverylongtablename',
            'b0', 'b3', 'x', 'xx', 'xXx', 'y', 'z')
        for t in tables:
            self.db.query('drop table if exists %s' % t)
            self.db.query("create table %s"
                " as select 0" % t)
        result3 = get_tables()
        result2 = []
        for t in result3:
            if t not in result1:
                result2.append(t)
        result3 = []
        for t in tables:
            if not t.startswith('"'):
                t = t.lower()
            result3.append('public.' + t)
        self.assertEqual(result2, result3)
        for t in result2:
            self.db.query('drop table %s' % t)
        result2 = get_tables()
        self.assertEqual(result2, result1)

    def testGetSystemTables(self):
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

    def testGetRelations(self):
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

    def testGetAttnames(self):
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'does_not_exist')
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'has.too.many.dots')
        attributes = self.db.get_attnames('test')
        self.assertIsInstance(attributes, dict)
        self.assertEqual(attributes, dict(
            i2='int', i4='int', i8='int', d='num',
            f4='float', f8='float', m='money',
            v4='text', c4='text', t='text'))
        for table in ('attnames_test_table', 'test table for attnames'):
            self.db.query('drop table if exists "%s"' % table)
            self.db.query('create table "%s" ('
                ' a smallint, b integer, c bigint,'
                ' e numeric, f float, f2 double precision, m money,'
                ' x smallint, y smallint, z smallint,'
                ' Normal_NaMe smallint, "Special Name" smallint,'
                ' t text, u char(2), v varchar(2),'
                ' primary key (y, u)) with oids' % table)
            attributes = self.db.get_attnames(table)
            result = {'a': 'int', 'c': 'int', 'b': 'int',
                'e': 'num', 'f': 'float', 'f2': 'float', 'm': 'money',
                'normal_name': 'int', 'Special Name': 'int',
                'u': 'text', 't': 'text', 'v': 'text',
                'y': 'int', 'x': 'int', 'z': 'int', 'oid': 'int'}
            self.assertEqual(attributes, result)
            self.db.query('drop table "%s"' % table)

    def testGetSystemRelations(self):
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

    def testHasTablePrivilege(self):
        can = self.db.has_table_privilege
        self.assertEqual(can('test'), True)
        self.assertEqual(can('test', 'select'), True)
        self.assertEqual(can('test', 'SeLeCt'), True)
        self.assertEqual(can('test', 'SELECT'), True)
        self.assertEqual(can('test', 'insert'), True)
        self.assertEqual(can('test', 'update'), True)
        self.assertEqual(can('test', 'delete'), True)
        self.assertRaises(pg.ProgrammingError, can, 'test', 'foobar')
        self.assertRaises(pg.ProgrammingError, can, 'table_does_not_exist')

    def testGet(self):
        get = self.db.get
        query = self.db.query
        for table in ('get_test_table', 'test table for get'):
            query('drop table if exists "%s"' % table)
            query('create table "%s" ('
                "n integer, t text) with oids" % table)
            for n, t in enumerate('xyz'):
                query('insert into "%s" values('"%d, '%s')"
                    % (table, n + 1, t))
            self.assertRaises(pg.ProgrammingError, get, table, 2)
            r = get(table, 2, 'n')
            oid_table = table
            if ' ' in table:
                oid_table = '"%s"' % oid_table
            oid_table = 'oid(public.%s)' % oid_table
            self.assertIn(oid_table, r)
            oid = r[oid_table]
            self.assertIsInstance(oid, int)
            result = {'t': 'y', 'n': 2, oid_table: oid}
            self.assertEqual(r, result)
            self.assertEqual(get(table + ' *', 2, 'n'), r)
            self.assertEqual(get(table, oid, 'oid')['t'], 'y')
            self.assertEqual(get(table, 1, 'n')['t'], 'x')
            self.assertEqual(get(table, 3, 'n')['t'], 'z')
            self.assertEqual(get(table, 2, 'n')['t'], 'y')
            self.assertRaises(pg.DatabaseError, get, table, 4, 'n')
            r['n'] = 3
            self.assertEqual(get(table, r, 'n')['t'], 'z')
            self.assertEqual(get(table, 1, 'n')['t'], 'x')
            query('alter table "%s" alter n set not null' % table)
            query('alter table "%s" add primary key (n)' % table)
            self.assertEqual(get(table, 3)['t'], 'z')
            self.assertEqual(get(table, 1)['t'], 'x')
            self.assertEqual(get(table, 2)['t'], 'y')
            r['n'] = 1
            self.assertEqual(get(table, r)['t'], 'x')
            r['n'] = 3
            self.assertEqual(get(table, r)['t'], 'z')
            r['n'] = 2
            self.assertEqual(get(table, r)['t'], 'y')
            query('drop table "%s"' % table)

    def testGetWithCompositeKey(self):
        get = self.db.get
        query = self.db.query
        table = 'get_test_table_1'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, t text, primary key (n))" % table)
        for n, t in enumerate('abc'):
            query("insert into %s values("
                "%d, '%s')" % (table, n + 1, t))
        self.assertEqual(get(table, 2)['t'], 'b')
        query("drop table %s" % table)
        table = 'get_test_table_2'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query("insert into %s values("
                    "%d, %d, '%s')" % (table, n + 1, m + 1, t))
        self.assertRaises(pg.ProgrammingError, get, table, 2)
        self.assertEqual(get(table, dict(n=2, m=2))['t'], 'd')
        self.assertEqual(get(table, dict(n=1, m=2),
                             ('n', 'm'))['t'], 'b')
        self.assertEqual(get(table, dict(n=3, m=2),
                             frozenset(['n', 'm']))['t'], 'f')
        query("drop table %s" % table)

    def testGetFromView(self):
        self.db.query('delete from test where i4=14')
        self.db.query('insert into test (i4, v4) values('
            "14, 'abc4')")
        r = self.db.get('test_view', 14, 'i4')
        self.assertIn('v4', r)
        self.assertEqual(r['v4'], 'abc4')

    def testGetLittleBobbyTables(self):
        get = self.db.get
        query = self.db.query
        query("drop table if exists test_students")
        query("create table test_students (firstname varchar primary key,"
            " nickname varchar, grade char(2))")
        query("insert into test_students values ("
              "'D''Arcy', 'Darcey', 'A+')")
        query("insert into test_students values ("
              "'Sheldon', 'Moonpie', 'A+')")
        query("insert into test_students values ("
              "'Robert', 'Little Bobby Tables', 'D-')")
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
        except pg.DatabaseError, error:
            self.assertEqual(str(error),
                'No such record in public.test_students where firstname = '
                "'D'' Arcy'")
        try:
            get('test_students', "Robert'); TRUNCATE TABLE test_students;--")
        except pg.DatabaseError, error:
            self.assertEqual(str(error),
                'No such record in public.test_students where firstname = '
                "'Robert''); TRUNCATE TABLE test_students;--'")
        q = "select * from test_students order by 1 limit 4"
        r = query(q).getresult()
        self.assertEqual(len(r), 3)
        self.assertEqual(r[1][2], 'D-')
        query('drop table test_students')

    def testInsert(self):
        insert = self.db.insert
        query = self.db.query
        server_version = self.db.server_version
        for table in ('insert_test_table', 'test table for insert'):
            query('drop table if exists "%s"' % table)
            query('create table "%s" ('
                "i2 smallint, i4 integer, i8 bigint,"
                " d numeric, f4 real, f8 double precision, m money,"
                " v4 varchar(4), c4 char(4), t text,"
                " b boolean, ts timestamp) with oids" % table)
            oid_table = table
            if ' ' in table:
                oid_table = '"%s"' % oid_table
            oid_table = 'oid(public.%s)' % oid_table
            tests = [dict(i2=None, i4=None, i8=None),
                (dict(i2='', i4='', i8=''), dict(i2=None, i4=None, i8=None)),
                (dict(i2=0, i4=0, i8=0), dict(i2=0, i4=0, i8=0)),
                dict(i2=42, i4=123456, i8=9876543210),
                dict(i2=2 ** 15 - 1,
                    i4=int(2 ** 31 - 1), i8=long(2 ** 63 - 1)),
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
                    data = test
                    change = {}
                else:
                    data, change = test
                expect = data.copy()
                expect.update(change)
                if data.get('m') and server_version < 90100:
                    # PostgreSQL < 9.1 cannot directly convert numbers to money
                    data['m'] = "'%s'::money" % data['m']
                self.assertEqual(insert(table, data), data)
                self.assertIn(oid_table, data)
                oid = data[oid_table]
                self.assertIsInstance(oid, int)
                data = dict(item for item in data.iteritems()
                    if item[0] in expect)
                ts = expect.get('ts')
                if ts == 'current_timestamp':
                    ts = expect['ts'] = data['ts']
                    if len(ts) > 19:
                        self.assertEqual(ts[19], '.')
                        ts = ts[:19]
                    else:
                        self.assertEqual(len(ts), 19)
                    self.assertTrue(ts[:4].isdigit())
                    self.assertEqual(ts[4], '-')
                    self.assertEqual(ts[10], ' ')
                    self.assertTrue(ts[11:13].isdigit())
                    self.assertEqual(ts[13], ':')
                self.assertEqual(data, expect)
                data = query(
                    'select oid,* from "%s"' % table).dictresult()[0]
                self.assertEqual(data['oid'], oid)
                data = dict(item for item in data.iteritems()
                    if item[0] in expect)
                self.assertEqual(data, expect)
                query('delete from "%s"' % table)
            query('drop table "%s"' % table)

    def testUpdate(self):
        update = self.db.update
        query = self.db.query
        for table in ('update_test_table', 'test table for update'):
            query('drop table if exists "%s"' % table)
            query('create table "%s" ('
                "n integer, t text) with oids" % table)
            for n, t in enumerate('xyz'):
                query('insert into "%s" values('
                    "%d, '%s')" % (table, n + 1, t))
            self.assertRaises(pg.ProgrammingError, self.db.get, table, 2)
            r = self.db.get(table, 2, 'n')
            r['t'] = 'u'
            s = update(table, r)
            self.assertEqual(s, r)
            r = query('select t from "%s" where n=2' % table
                      ).getresult()[0][0]
            self.assertEqual(r, 'u')
            query('drop table "%s"' % table)

    def testUpdateWithCompositeKey(self):
        update = self.db.update
        query = self.db.query
        table = 'update_test_table_1'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, t text, primary key (n))" % table)
        for n, t in enumerate('abc'):
            query("insert into %s values("
                "%d, '%s')" % (table, n + 1, t))
        self.assertRaises(pg.ProgrammingError, update,
                          table, dict(t='b'))
        self.assertEqual(update(table, dict(n=2, t='d'))['t'], 'd')
        r = query('select t from "%s" where n=2' % table
                  ).getresult()[0][0]
        self.assertEqual(r, 'd')
        query("drop table %s" % table)
        table = 'update_test_table_2'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query("insert into %s values("
                    "%d, %d, '%s')" % (table, n + 1, m + 1, t))
        self.assertRaises(pg.ProgrammingError, update,
                          table, dict(n=2, t='b'))
        self.assertEqual(update(table,
                                dict(n=2, m=2, t='x'))['t'], 'x')
        r = [r[0] for r in query('select t from "%s" where n=2'
            ' order by m' % table).getresult()]
        self.assertEqual(r, ['c', 'x'])
        query("drop table %s" % table)

    def testClear(self):
        clear = self.db.clear
        query = self.db.query
        for table in ('clear_test_table', 'test table for clear'):
            query('drop table if exists "%s"' % table)
            query('create table "%s" ('
                "n integer, b boolean, d date, t text)" % table)
            r = clear(table)
            result = {'n': 0, 'b': 'f', 'd': '', 't': ''}
            self.assertEqual(r, result)
            r['a'] = r['n'] = 1
            r['d'] = r['t'] = 'x'
            r['b'] = 't'
            r['oid'] = 1L
            r = clear(table, r)
            result = {'a': 1, 'n': 0, 'b': 'f', 'd': '', 't': '', 'oid': 1L}
            self.assertEqual(r, result)
            query('drop table "%s"' % table)

    def testDelete(self):
        delete = self.db.delete
        query = self.db.query
        for table in ('delete_test_table', 'test table for delete'):
            query('drop table if exists "%s"' % table)
            query('create table "%s" ('
                "n integer, t text) with oids" % table)
            for n, t in enumerate('xyz'):
                query('insert into "%s" values('
                    "%d, '%s')" % (table, n + 1, t))
            self.assertRaises(pg.ProgrammingError, self.db.get, table, 2)
            r = self.db.get(table, 1, 'n')
            s = delete(table, r)
            self.assertEqual(s, 1)
            r = self.db.get(table, 3, 'n')
            s = delete(table, r)
            self.assertEqual(s, 1)
            s = delete(table, r)
            self.assertEqual(s, 0)
            r = query('select * from "%s"' % table).dictresult()
            self.assertEqual(len(r), 1)
            r = r[0]
            result = {'n': 2, 't': 'y'}
            self.assertEqual(r, result)
            r = self.db.get(table, 2, 'n')
            s = delete(table, r)
            self.assertEqual(s, 1)
            s = delete(table, r)
            self.assertEqual(s, 0)
            self.assertRaises(pg.DatabaseError, self.db.get, table, 2, 'n')
            query('drop table "%s"' % table)

    def testDeleteWithCompositeKey(self):
        query = self.db.query
        table = 'delete_test_table_1'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, t text, primary key (n))" % table)
        for n, t in enumerate('abc'):
            query("insert into %s values("
                "%d, '%s')" % (table, n + 1, t))
        self.assertRaises(pg.ProgrammingError, self.db.delete,
            table, dict(t='b'))
        self.assertEqual(self.db.delete(table, dict(n=2)), 1)
        r = query('select t from "%s" where n=2' % table
                  ).getresult()
        self.assertEqual(r, [])
        self.assertEqual(self.db.delete(table, dict(n=2)), 0)
        r = query('select t from "%s" where n=3' % table
                  ).getresult()[0][0]
        self.assertEqual(r, 'c')
        query("drop table %s" % table)
        table = 'delete_test_table_2'
        query("drop table if exists %s" % table)
        query("create table %s ("
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query("insert into %s values("
                    "%d, %d, '%s')" % (table, n + 1, m + 1, t))
        self.assertRaises(pg.ProgrammingError, self.db.delete,
            table, dict(n=2, t='b'))
        self.assertEqual(self.db.delete(table, dict(n=2, m=2)), 1)
        r = [r[0] for r in query('select t from "%s" where n=2'
            ' order by m' % table).getresult()]
        self.assertEqual(r, ['c'])
        self.assertEqual(self.db.delete(table, dict(n=2, m=2)), 0)
        r = [r[0] for r in query('select t from "%s" where n=3'
            ' order by m' % table).getresult()]
        self.assertEqual(r, ['e', 'f'])
        self.assertEqual(self.db.delete(table, dict(n=3, m=1)), 1)
        r = [r[0] for r in query('select t from "%s" where n=3'
            ' order by m' % table).getresult()]
        self.assertEqual(r, ['f'])
        query("drop table %s" % table)

    def testTruncate(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, None)
        self.assertRaises(TypeError, truncate, 42)
        self.assertRaises(TypeError, truncate, dict(test_table=None))
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table (n smallint)")
        for i in range(3):
            query("insert into test_table values (1)")
        q = "select count(*) from test_table"
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate('test_table')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        for i in range(3):
            query("insert into test_table values (1)")
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate('public.test_table')
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        query("drop table if exists test_table_2")
        query('create table test_table_2 (n smallint)')
        for t in (list, tuple, set):
            for i in range(3):
                query("insert into test_table values (1)")
                query("insert into test_table_2 values (2)")
            q = ("select (select count(*) from test_table),"
                " (select count(*) from test_table_2)")
            r = query(q).getresult()[0]
            self.assertEqual(r, (3, 3))
            truncate(t(['test_table', 'test_table_2']))
            r = query(q).getresult()[0]
            self.assertEqual(r, (0, 0))
        query("drop table test_table_2")
        query("drop table test_table")

    def testTempCrud(self):
        query = self.db.query
        table = 'test_temp_table'
        query("drop table if exists %s" % table)
        query("create temporary table %s"
              " (n int primary key, t varchar)" % table)
        self.db.insert(table, dict(n=1, t='one'))
        self.db.insert(table, dict(n=2, t='too'))
        self.db.insert(table, dict(n=3, t='three'))
        r = self.db.get(table, 2)
        self.assertEqual(r['t'], 'too')
        self.db.update(table, dict(n=2, t='two'))
        r = self.db.get(table, 2)
        self.assertEqual(r['t'], 'two')
        self.db.delete(table, r)
        r = query('select n, t from %s order by 1' % table).getresult()
        self.assertEqual(r, [(1, 'one'), (3, 'three')])
        query("drop table %s" % table)

    def testTruncateRestart(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', restart='invalid')
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table (n serial, t text)")
        for n in range(3):
            query("insert into test_table (t) values ('test')")
        q = "select count(n), min(n), max(n) from test_table"
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 1, 3))
        truncate('test_table')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, None, None))
        for n in range(3):
            query("insert into test_table (t) values ('test')")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 4, 6))
        truncate('test_table', restart=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, None, None))
        for n in range(3):
            query("insert into test_table (t) values ('test')")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 1, 3))
        query("drop table test_table")

    def testTruncateCascade(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', cascade='invalid')
        query = self.db.query
        query("drop table if exists test_child")
        query("drop table if exists test_parent")
        query("create table test_parent (n smallint primary key)")
        query("create table test_child ("
            " n smallint primary key references test_parent (n))")
        for n in range(3):
            query("insert into test_parent (n) values (%d)" % n)
            query("insert into test_child (n) values (%d)" % n)
        q = ("select (select count(*) from test_parent),"
            " (select count(*) from test_child)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        self.assertRaises(pg.ProgrammingError, truncate, 'test_parent')
        truncate(['test_parent', 'test_child'])
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
            query("insert into test_parent (n) values (%d)" % n)
            query("insert into test_child (n) values (%d)" % n)
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        truncate('test_parent', cascade=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
            query("insert into test_parent (n) values (%d)" % n)
            query("insert into test_child (n) values (%d)" % n)
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 3))
        truncate('test_child')
        r = query(q).getresult()[0]
        self.assertEqual(r, (3, 0))
        self.assertRaises(pg.ProgrammingError, truncate, 'test_parent')
        truncate('test_parent', cascade=True)
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        query("drop table test_child")
        query("drop table test_parent")

    def testTruncateOnly(self):
        truncate = self.db.truncate
        self.assertRaises(TypeError, truncate, 'test_table', only='invalid')
        query = self.db.query
        query("drop table if exists test_child")
        query("drop table if exists test_parent")
        query("create table test_parent (n smallint)")
        query("create table test_child ("
            " m smallint) inherits (test_parent)")
        for n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
        q = ("select (select count(*) from test_parent),"
            " (select count(*) from test_child)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3))
        truncate('test_parent')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
        r = query(q).getresult()[0]
        self.assertEqual(r, (6, 3))
        truncate('test_parent*')
        r = query(q).getresult()[0]
        self.assertEqual(r, (0, 0))
        for n in range(3):
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
        query("drop table if exists test_parent_2")
        query("create table test_parent_2 (n smallint)")
        query("drop table if exists test_child_2")
        query("create table test_child_2 ("
            " m smallint) inherits (test_parent_2)")
        for n in range(3):
            query("insert into test_parent (n) values (1)")
            query("insert into test_child (n, m) values (2, 3)")
            query("insert into test_parent_2 (n) values (1)")
            query("insert into test_child_2 (n, m) values (2, 3)")
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
        self.assertRaises(ValueError, truncate,
            ['test_parent*', 'test_child'], only=[True, False])
        truncate(['test_parent*', 'test_child'], only=[False, True])
        query("drop table test_child_2")
        query("drop table test_parent_2")
        query("drop table test_child")
        query("drop table test_parent")

    def testTruncateQuoted(self):
        truncate = self.db.truncate
        query = self.db.query
        table = "test table for truncate()"
        query('drop table if exists "%s"' % table)
        query('create table "%s" (n smallint)' % table)
        for i in range(3):
            query('insert into "%s" values (1)' % table)
        q = 'select count(*) from "%s"' % table
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate(table)
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        for i in range(3):
            query('insert into "%s" values (1)' % table)
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 3)
        truncate('public."%s"' % table)
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 0)
        query('drop table "%s"' % table)

    def testTransaction(self):
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table (n integer)")
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
        self.assertRaises(pg.ProgrammingError, self.db.rollback, 'before8')
        self.db.commit()
        self.db.start()
        query("insert into test_table values (9)")
        self.db.end()
        r = [r[0] for r in query(
            "select * from test_table order by 1").getresult()]
        self.assertEqual(r, [1, 2, 5, 7, 9])
        query("drop table test_table")

    @unittest.skipIf(no_with, 'context managers not supported')
    def testContextManager(self):
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table (n integer check(n>0))")
        # wrap "with" statements to avoid SyntaxError in Python < 2.5
        exec """from __future__ import with_statement\nif True:
        with self.db:
            query("insert into test_table values (1)")
            query("insert into test_table values (2)")
        try:
            with self.db:
                query("insert into test_table values (3)")
                query("insert into test_table values (4)")
                raise ValueError('test transaction should rollback')
        except ValueError, error:
            self.assertEqual(str(error), 'test transaction should rollback')
        with self.db:
            query("insert into test_table values (5)")
        try:
            with self.db:
                query("insert into test_table values (6)")
                query("insert into test_table values (-1)")
        except pg.ProgrammingError, error:
            self.assertTrue('check' in str(error))
        with self.db:
            query("insert into test_table values (7)")\n"""
        r = [r[0] for r in query(
            "select * from test_table order by 1").getresult()]
        self.assertEqual(r, [1, 2, 5, 7])
        query("drop table test_table")

    def testBytea(self):
        query = self.db.query
        query('drop table if exists bytea_test')
        query('create table bytea_test ('
            'data bytea)')
        s = "It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        r = self.db.escape_bytea(s)
        query('insert into bytea_test values('
            "'%s')" % r)
        r = query('select * from bytea_test').getresult()
        self.assertTrue(len(r) == 1)
        r = r[0]
        self.assertTrue(len(r) == 1)
        r = r[0]
        r = self.db.unescape_bytea(r)
        self.assertEqual(r, s)
        query('drop table bytea_test')

    def testNotificationHandler(self):
        # the notification handler itself is tested separately
        f = self.db.notification_handler
        callback = lambda arg_dict: None
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

    def testDebugWithCallable(self):
        if debug:
            self.assertEqual(self.db.debug, debug)
        else:
            self.assertIsNone(self.db.debug)
        s = []
        self.db.debug = s.append
        try:
            self.db.query("select 1")
            self.db.query("select 2")
            self.assertEqual(s, ["select 1", "select 2"])
        finally:
            self.db.debug = debug


class TestSchemas(unittest.TestCase):
    """Test correct handling of schemas (namespaces)."""

    cls_set_up = False

    @classmethod
    def setUpClass(cls):
        db = DB()
        query = db.query
        for num_schema in range(5):
            if num_schema:
                schema = "s%d" % num_schema
                query("drop schema if exists %s cascade" % (schema,))
                try:
                    query("create schema %s" % (schema,))
                except pg.ProgrammingError:
                    raise RuntimeError("The test user cannot create schemas.\n"
                        "Grant create on database %s to the user"
                        " for running these tests." % dbname)
            else:
                schema = "public"
                query("drop table if exists %s.t" % (schema,))
                query("drop table if exists %s.t%d" % (schema, num_schema))
            query("create table %s.t with oids as select 1 as n, %d as d"
                  % (schema, num_schema))
            query("create table %s.t%d with oids as select 1 as n, %d as d"
                  % (schema, num_schema, num_schema))
        db.close()
        cls.cls_set_up = True

    @classmethod
    def tearDownClass(cls):
        db = DB()
        query = db.query
        for num_schema in range(5):
            if num_schema:
                schema = "s%d" % num_schema
                query("drop schema %s cascade" % (schema,))
            else:
                schema = "public"
                query("drop table %s.t" % (schema,))
                query("drop table %s.t%d" % (schema, num_schema))
        db.close()

    def setUp(self):
        self.assertTrue(self.cls_set_up)
        self.db = DB()

    def tearDown(self):
        self.db.close()

    def testGetTables(self):
        tables = self.db.get_tables()
        for num_schema in range(5):
            if num_schema:
                schema = "s" + str(num_schema)
            else:
                schema = "public"
            for t in (schema + ".t",
                    schema + ".t" + str(num_schema)):
                self.assertIn(t, tables)

    def testGetAttnames(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        result = {'oid': 'int', 'd': 'int', 'n': 'int'}
        r = get_attnames("t")
        self.assertEqual(r, result)
        r = get_attnames("s4.t4")
        self.assertEqual(r, result)
        query("drop table if exists s3.t3m")
        query("create table s3.t3m with oids as select 1 as m")
        result_m = {'oid': 'int', 'm': 'int'}
        r = get_attnames("s3.t3m")
        self.assertEqual(r, result_m)
        query("set search_path to s1,s3")
        r = get_attnames("t3")
        self.assertEqual(r, result)
        r = get_attnames("t3m")
        self.assertEqual(r, result_m)
        query("drop table s3.t3m")

    def testGet(self):
        get = self.db.get
        query = self.db.query
        PrgError = pg.ProgrammingError
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

    def testMangling(self):
        get = self.db.get
        query = self.db.query
        r = get("t", 1, 'n')
        self.assertIn('oid(public.t)', r)
        query("set search_path to s2")
        r = get("t2", 1, 'n')
        self.assertIn('oid(s2.t2)', r)
        query("set search_path to s3")
        r = get("t", 1, 'n')
        self.assertIn('oid(s3.t)', r)


if __name__ == '__main__':
    unittest.main()
