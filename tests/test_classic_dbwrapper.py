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

import pg  # the module under test

from decimal import Decimal

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
# The current user must have create schema privilege on the database.
dbname = 'unittest'
dbhost = None
dbport = 5432

debug = False  # let DB wrapper print debugging output

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
    unicode
except NameError:  # Python >= 3.0
    unicode = str

try:
    from collections import OrderedDict
except ImportError:  # Python 2.6 or 3.0
    OrderedDict = dict

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
            'begin',
            'cancel',
            'clear',
            'close',
            'commit',
            'db',
            'dbname',
            'debug',
            'delete',
            'end',
            'endcopy',
            'error',
            'escape_bytea',
            'escape_identifier',
            'escape_literal',
            'escape_string',
            'fileno',
            'get',
            'get_attnames',
            'get_databases',
            'get_notice_receiver',
            'get_relations',
            'get_tables',
            'getline',
            'getlo',
            'getnotify',
            'has_table_privilege',
            'host',
            'insert',
            'inserttable',
            'locreate',
            'loimport',
            'notification_handler',
            'options',
            'parameter',
            'pkey',
            'port',
            'protocol_version',
            'putline',
            'query',
            'release',
            'reopen',
            'reset',
            'rollback',
            'savepoint',
            'server_version',
            'set_notice_receiver',
            'source',
            'start',
            'status',
            'transaction',
            'unescape_bytea',
            'update',
            'upsert',
            'use_regtypes',
            'user',
        ]
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

    def testAttributeUser(self):
        no_user = 'Deprecated facility'
        user = self.db.user
        self.assertTrue(user)
        self.assertIsInstance(user, str)
        self.assertNotEqual(user, no_user)
        self.assertEqual(user, self.db.db.user)

    def testMethodEscapeLiteral(self):
        self.assertEqual(self.db.escape_literal(''), "''")

    def testMethodEscapeIdentifier(self):
        self.assertEqual(self.db.escape_identifier(''), '""')

    def testMethodEscapeString(self):
        self.assertEqual(self.db.escape_string(''), '')

    def testMethodEscapeBytea(self):
        self.assertEqual(self.db.escape_bytea('').replace(
            '\\x', '').replace('\\', ''), '')

    def testMethodUnescapeBytea(self):
        self.assertEqual(self.db.unescape_bytea(''), b'')

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
        except pg.ProgrammingError as error:
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
        self.assertRaises(pg.InternalError, self.db.close)
        self.assertRaises(pg.InternalError, self.db.query, 'select 1')

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

    @classmethod
    def setUpClass(cls):
        db = DB()
        db.query("drop table if exists test cascade")
        db.query("create table test ("
            "i2 smallint, i4 integer, i8 bigint,"
            "d numeric, f4 real, f8 double precision, m money, "
            "v4 varchar(4), c4 char(4), t text)")
        db.query("create or replace view test_view as"
            " select i4, v4 from test")
        db.close()

    @classmethod
    def tearDownClass(cls):
        db = DB()
        db.query("drop table test cascade")
        db.close()

    def setUp(self):
        self.db = DB()
        query = self.db.query
        query('set client_encoding=utf8')
        query('set standard_conforming_strings=on')
        query("set lc_monetary='C'")
        query("set datestyle='ISO,YMD'")
        query('set bytea_output=hex')

    def tearDown(self):
        self.db.close()

    def testClassName(self):
        self.assertEqual(self.db.__class__.__name__, 'DB')

    def testModuleName(self):
        self.assertEqual(self.db.__module__, 'pg')
        self.assertEqual(self.db.__class__.__module__, 'pg')

    def testEscapeLiteral(self):
        f = self.db.escape_literal
        r = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b"'plain'")
        r = f(u"plain")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u"'plain'")
        r = f(u"that's käse".encode('utf-8'))
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"'that''s käse'".encode('utf-8'))
        r = f(u"that's käse")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u"'that''s käse'")
        self.assertEqual(f(r"It's fine to have a \ inside."),
            r" E'It''s fine to have a \\ inside.'")
        self.assertEqual(f('No "quotes" must be escaped.'),
            "'No \"quotes\" must be escaped.'")

    def testEscapeIdentifier(self):
        f = self.db.escape_identifier
        r = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'"plain"')
        r = f(u"plain")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'"plain"')
        r = f(u"that's käse".encode('utf-8'))
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u'"that\'s käse"'.encode('utf-8'))
        r = f(u"that's käse")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'"that\'s käse"')
        self.assertEqual(f(r"It's fine to have a \ inside."),
            '"It\'s fine to have a \\ inside."')
        self.assertEqual(f('All "quotes" must be escaped.'),
            '"All ""quotes"" must be escaped."')

    def testEscapeString(self):
        f = self.db.escape_string
        r = f(b"plain")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b"plain")
        r = f(u"plain")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u"plain")
        r = f(u"that's käse".encode('utf-8'))
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"that''s käse".encode('utf-8'))
        r = f(u"that's käse")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u"that''s käse")
        self.assertEqual(f(r"It's fine to have a \ inside."),
            r"It''s fine to have a \ inside.")

    def testEscapeBytea(self):
        f = self.db.escape_bytea
        # note that escape_byte always returns hex output since Pg 9.0,
        # regardless of the bytea_output setting
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'\\x706c61696e')
        r = f(u'plain')
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'\\x706c61696e')
        r = f(u"das is' käse".encode('utf-8'))
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'\\x64617320697327206bc3a47365')
        r = f(u"das is' käse")
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'\\x64617320697327206bc3a47365')
        self.assertEqual(f(b'O\x00ps\xff!'), b'\\x4f007073ff21')

    def testUnescapeBytea(self):
        f = self.db.unescape_bytea
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(u'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(b"das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"das is' käse".encode('utf8'))
        r = f(u"das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"das is' käse".encode('utf8'))
        self.assertEqual(f(r'O\\000ps\\377!'), b'O\\000ps\\377!')
        self.assertEqual(f(r'\\x706c61696e'), b'\\x706c61696e')
        self.assertEqual(f(r'\\x746861742773206be47365'),
            b'\\x746861742773206be47365')
        self.assertEqual(f(r'\\x4f007073ff21'), b'\\x4f007073ff21')

    def testGetAttnames(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table("
            " n int, alpha smallint, beta bool,"
            " gamma char(5), tau text, v varchar(3))")
        r = get_attnames("test_table")
        self.assertIsInstance(r, dict)
        self.assertEquals(r, dict(
            n='int', alpha='int', beta='bool',
            gamma='text', tau='text', v='text'))
        query("drop table test_table")

    def testGetAttnamesWithQuotes(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        table = 'test table for get_attnames()'
        query('drop table if exists "%s"' % table)
        query('create table "%s"('
            '"Prime!" smallint,'
            '"much space" integer, "Questions?" text)' % table)
        r = get_attnames(table)
        self.assertIsInstance(r, dict)
        self.assertEquals(r, {
            'Prime!': 'int', 'much space': 'int', 'Questions?': 'text'})
        query('drop table "%s"' % table)

    def testGetAttnamesWithRegtypes(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table("
            " n int, alpha smallint, beta bool,"
            " gamma char(5), tau text, v varchar(3))")
        self.db.use_regtypes(True)
        try:
            r = get_attnames("test_table")
            self.assertIsInstance(r, dict)
        finally:
            self.db.use_regtypes(False)
        self.assertEquals(r, dict(
            n='integer', alpha='smallint', beta='boolean',
            gamma='character', tau='text', v='character varying'))
        query("drop table test_table")

    def testGetAttnamesIsCached(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table(col int)")
        r = get_attnames("test_table")
        self.assertIsInstance(r, dict)
        self.assertEquals(r, dict(col='int'))
        query("drop table test_table")
        query("create table test_table(col text)")
        r = get_attnames("test_table")
        self.assertEquals(r, dict(col='int'))
        r = get_attnames("test_table", flush=True)
        self.assertEquals(r, dict(col='text'))
        query("drop table test_table")
        r = get_attnames("test_table")
        self.assertEquals(r, dict(col='text'))
        self.assertRaises(pg.ProgrammingError,
            get_attnames, "test_table", flush=True)

    def testGetAttnamesIsOrdered(self):
        get_attnames = self.db.get_attnames
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table("
            " n int, alpha smallint, v varchar(3),"
            " gamma char(5), tau text, beta bool)")
        r = get_attnames("test_table")
        self.assertIsInstance(r, OrderedDict)
        self.assertEquals(r, OrderedDict([
            ('n', 'int'), ('alpha', 'int'), ('v', 'text'),
            ('gamma', 'text'), ('tau', 'text'), ('beta', 'bool')]))
        query("drop table test_table")
        if OrderedDict is dict:
            self.skipTest('OrderedDict is not supported')
        r = ' '.join(list(r.keys()))
        self.assertEquals(r, 'n alpha v gamma tau beta')

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
        except pg.ProgrammingError as error:
            self.assertEqual(error.sqlstate, '22012')

    def testPkey(self):
        query = self.db.query
        pkey = self.db.pkey
        for t in ('pkeytest', 'primary key test'):
            for n in range(7):
                query('drop table if exists "%s%d"' % (t, n))
            query('create table "%s0" ('
                "a smallint)" % t)
            query('create table "%s1" ('
                "b smallint primary key)" % t)
            query('create table "%s2" ('
                "c smallint, d smallint primary key)" % t)
            query('create table "%s3" ('
                "e smallint, f smallint, g smallint, "
                "h smallint, i smallint, "
                "primary key (f, h))" % t)
            query('create table "%s4" ('
                "more_than_one_letter varchar primary key)" % t)
            query('create table "%s5" ('
                '"with space" date primary key)' % t)
            query('create table "%s6" ('
                'a_very_long_column_name varchar, '
                '"with space" date, '
                '"42" int, '
                "primary key (a_very_long_column_name, "
                '"with space", "42"))' % t)
            self.assertRaises(KeyError, pkey, '%s0' % t)
            self.assertEqual(pkey('%s1' % t), 'b')
            self.assertEqual(pkey('%s2' % t), 'd')
            r = pkey('%s3' % t)
            self.assertIsInstance(r, frozenset)
            self.assertEqual(r, frozenset('fh'))
            self.assertEqual(pkey('%s4' % t), 'more_than_one_letter')
            self.assertEqual(pkey('%s5' % t), 'with space')
            r = pkey('%s6' % t)
            self.assertIsInstance(r, frozenset)
            self.assertEqual(r, frozenset([
                'a_very_long_column_name', 'with space', '42']))
            # a newly added primary key will be detected
            query('alter table "%s0" add primary key (a)' % t)
            self.assertEqual(pkey('%s0' % t), 'a')
            # a changed primary key will not be detected,
            # indicating that the internal cache is operating
            query('alter table "%s1" rename column b to x' % t)
            self.assertEqual(pkey('%s1' % t), 'b')
            # we get the changed primary key when the cache is flushed
            self.assertEqual(pkey('%s1' % t, flush=True), 'x')
            for n in range(7):
                query('drop table "%s%d"' % (t, n))

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

    def testAttnames(self):
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'does_not_exist')
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'has.too.many.dots')
        for table in ('attnames_test_table', 'test table for attnames'):
            self.db.query('drop table if exists "%s"' % table)
            self.db.query('create table "%s" ('
                'a smallint, b integer, c bigint, '
                'e numeric, f float, f2 double precision, m money, '
                'x smallint, y smallint, z smallint, '
                'Normal_NaMe smallint, "Special Name" smallint, '
                't text, u char(2), v varchar(2), '
                'primary key (y, u)) with oids' % table)
            attributes = self.db.get_attnames(table)
            result = {'a': 'int', 'c': 'int', 'b': 'int',
                'e': 'num', 'f': 'float', 'f2': 'float', 'm': 'money',
                'normal_name': 'int', 'Special Name': 'int',
                'u': 'text', 't': 'text', 'v': 'text',
                'y': 'int', 'x': 'int', 'z': 'int', 'oid': 'int'}
            self.assertEqual(attributes, result)
            self.db.query('drop table "%s"' % table)

    def testHasTablePrivilege(self):
        can = self.db.has_table_privilege
        self.assertEqual(can('test'), True)
        self.assertEqual(can('test', 'select'), True)
        self.assertEqual(can('test', 'SeLeCt'), True)
        self.assertEqual(can('test', 'SELECT'), True)
        self.assertEqual(can('test', 'insert'), True)
        self.assertEqual(can('test', 'update'), True)
        self.assertEqual(can('test', 'delete'), True)
        self.assertEqual(can('pg_views', 'select'), True)
        self.assertEqual(can('pg_views', 'delete'), False)
        self.assertRaises(pg.ProgrammingError, can, 'test', 'foobar')
        self.assertRaises(pg.ProgrammingError, can, 'table_does_not_exist')

    def testGet(self):
        get = self.db.get
        query = self.db.query
        table = 'get_test_table'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, t text) with oids" % table)
        for n, t in enumerate('xyz'):
            query('insert into "%s" values('"%d, '%s')"
                % (table, n + 1, t))
        self.assertRaises(pg.ProgrammingError, get, table, 2)
        r = get(table, 2, 'n')
        oid_table = 'oid(%s)' % table
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
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, t text, primary key (n))" % table)
        for n, t in enumerate('abc'):
            query('insert into "%s" values('
                "%d, '%s')" % (table, n + 1, t))
        self.assertEqual(get(table, 2)['t'], 'b')
        query('drop table "%s"' % table)
        table = 'get_test_table_2'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query('insert into "%s" values('
                    "%d, %d, '%s')" % (table, n + 1, m + 1, t))
        self.assertRaises(pg.ProgrammingError, get, table, 2)
        self.assertEqual(get(table, dict(n=2, m=2))['t'], 'd')
        r = get(table, dict(n=1, m=2), ('n', 'm'))
        self.assertEqual(r['t'], 'b')
        r = get(table, dict(n=3, m=2), frozenset(['n', 'm']))
        self.assertEqual(r['t'], 'f')
        query('drop table "%s"' % table)

    def testGetWithQuotedNames(self):
        get = self.db.get
        query = self.db.query
        table = 'test table for get()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        query('insert into "%s"'
              " values(17, 1001, 'No!')" % table)
        r = get(table, 17)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 17)
        self.assertEqual(r['much space'], 1001)
        self.assertEqual(r['Questions?'], 'No!')
        query('drop table "%s"' % table)

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
        except pg.DatabaseError as error:
            self.assertEqual(str(error),
                'No such record in test_students\nwhere "firstname" = $1\n'
                'with $1="D\' Arcy"')
        try:
            get('test_students', "Robert'); TRUNCATE TABLE test_students;--")
        except pg.DatabaseError as error:
            self.assertEqual(str(error),
                'No such record in test_students\nwhere "firstname" = $1\n'
                'with $1="Robert\'); TRUNCATE TABLE test_students;--"')
        q = "select * from test_students order by 1 limit 4"
        r = query(q).getresult()
        self.assertEqual(len(r), 3)
        self.assertEqual(r[1][2], 'D-')
        query('drop table test_students')

    def testInsert(self):
        insert = self.db.insert
        query = self.db.query
        bool_on = pg.get_bool()
        decimal = pg.get_decimal()
        table = 'insert_test_table'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "i2 smallint, i4 integer, i8 bigint,"
            " d numeric, f4 real, f8 double precision, m money,"
            " v4 varchar(4), c4 char(4), t text,"
            " b boolean, ts timestamp) with oids" % table)
        oid_table = 'oid(%s)' % table
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
            (dict(m=('-1234.56')), dict(m=Decimal('-1234.56'))),
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
            self.assertIn(oid_table, data)
            oid = data[oid_table]
            self.assertIsInstance(oid, int)
            data = dict(item for item in data.items()
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
            data = dict(item for item in data.items()
                if item[0] in expect)
            self.assertEqual(data, expect)
            query('delete from "%s"' % table)
        query('drop table "%s"' % table)

    def testInsertWithQuotedNames(self):
        insert = self.db.insert
        query = self.db.query
        table = 'test table for insert()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        r = {'Prime!': 11, 'much space': 2002, 'Questions?': 'What?'}
        r = insert(table, r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 11)
        self.assertEqual(r['much space'], 2002)
        self.assertEqual(r['Questions?'], 'What?')
        r = query('select * from "%s" limit 2' % table).dictresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r['Prime!'], 11)
        self.assertEqual(r['much space'], 2002)
        self.assertEqual(r['Questions?'], 'What?')
        query('drop table "%s"' % table)

    def testUpdate(self):
        update = self.db.update
        query = self.db.query
        table = 'update_test_table'
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
        q = 'select t from "%s" where n=2' % table
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'u')
        query('drop table "%s"' % table)

    def testUpdateWithCompositeKey(self):
        update = self.db.update
        query = self.db.query
        table = 'update_test_table_1'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, t text, primary key (n))" % table)
        for n, t in enumerate('abc'):
            query('insert into "%s" values('
                "%d, '%s')" % (table, n + 1, t))
        self.assertRaises(pg.ProgrammingError, update,
                          table, dict(t='b'))
        s = dict(n=2, t='d')
        r = update(table, s)
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'd')
        q = 'select t from "%s" where n=2' % table
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'd')
        s.update(dict(n=4, t='e'))
        r = update(table, s)
        self.assertEqual(r['n'], 4)
        self.assertEqual(r['t'], 'e')
        q = 'select t from "%s" where n=2' % table
        r = query(q).getresult()[0][0]
        self.assertEqual(r, 'd')
        q = 'select t from "%s" where n=4' % table
        r = query(q).getresult()
        self.assertEqual(len(r), 0)
        query('drop table "%s"' % table)
        table = 'update_test_table_2'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query('insert into "%s" values('
                    "%d, %d, '%s')" % (table, n + 1, m + 1, t))
        self.assertRaises(pg.ProgrammingError, update,
                          table, dict(n=2, t='b'))
        self.assertEqual(update(table,
                                dict(n=2, m=2, t='x'))['t'], 'x')
        q = 'select t from "%s" where n=2 order by m' % table
        r = [r[0] for r in query(q).getresult()]
        self.assertEqual(r, ['c', 'x'])
        query('drop table "%s"' % table)

    def testUpdateWithQuotedNames(self):
        update = self.db.update
        query = self.db.query
        table = 'test table for update()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        query('insert into "%s"'
              " values(13, 3003, 'Why!')" % table)
        r = {'Prime!': 13, 'much space': 7007, 'Questions?': 'When?'}
        r = update(table, r)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 13)
        self.assertEqual(r['much space'], 7007)
        self.assertEqual(r['Questions?'], 'When?')
        r = query('select * from "%s" limit 2' % table).dictresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r['Prime!'], 13)
        self.assertEqual(r['much space'], 7007)
        self.assertEqual(r['Questions?'], 'When?')
        query('drop table "%s"' % table)

    def testUpsert(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'upsert_test_table'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer primary key, t text) with oids" % table)
        s = dict(n=1, t='x')
        try:
            r = upsert(table, s)
        except pg.ProgrammingError as error:
            if self.db.server_version < 90500:
                self.skipTest('database does not support upsert')
            self.fail(str(error))
        self.assertIs(r, s)
        self.assertEqual(r['n'], 1)
        self.assertEqual(r['t'], 'x')
        s.update(n=2, t='y')
        r = upsert(table, s, **dict.fromkeys(s))
        self.assertIs(r, s)
        self.assertEqual(r['n'], 2)
        self.assertEqual(r['t'], 'y')
        q = 'select n, t from "%s" order by n limit 3' % table
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
        query('drop table "%s"' % table)

    def testUpsertWithCompositeKey(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'upsert_test_table_2'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, m integer, t text, primary key (n, m))" % table)
        s = dict(n=1, m=2, t='x')
        try:
            r = upsert(table, s)
        except pg.ProgrammingError as error:
            if self.db.server_version < 90500:
                self.skipTest('database does not support upsert')
            self.fail(str(error))
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
        q = 'select n, m, t from "%s" order by n, m limit 3' % table
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
        query('drop table "%s"' % table)

    def testUpsertWithQuotedNames(self):
        upsert = self.db.upsert
        query = self.db.query
        table = 'test table for upsert()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        s = {'Prime!': 31, 'much space': 9009, 'Questions?': 'Yes.'}
        try:
            r = upsert(table, s)
        except pg.ProgrammingError as error:
            if self.db.server_version < 90500:
                self.skipTest('database does not support upsert')
            self.fail(str(error))
        self.assertIs(r, s)
        self.assertEqual(r['Prime!'], 31)
        self.assertEqual(r['much space'], 9009)
        self.assertEqual(r['Questions?'], 'Yes.')
        q = 'select * from "%s" limit 2' % table
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

    def testClear(self):
        clear = self.db.clear
        query = self.db.query
        f = False if pg.get_bool() else 'f'
        table = 'clear_test_table'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, b boolean, d date, t text)" % table)
        r = clear(table)
        result = {'n': 0, 'b': f, 'd': '', 't': ''}
        self.assertEqual(r, result)
        r['a'] = r['n'] = 1
        r['d'] = r['t'] = 'x'
        r['b'] = 't'
        r['oid'] = long(1)
        r = clear(table, r)
        result = {'a': 1, 'n': 0, 'b': f, 'd': '', 't': '',
            'oid': long(1)}
        self.assertEqual(r, result)
        query('drop table "%s"' % table)

    def testClearWithQuotedNames(self):
        clear = self.db.clear
        query = self.db.query
        table = 'test table for clear()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        r = clear(table)
        self.assertIsInstance(r, dict)
        self.assertEqual(r['Prime!'], 0)
        self.assertEqual(r['much space'], 0)
        self.assertEqual(r['Questions?'], '')
        query('drop table "%s"' % table)

    def testDelete(self):
        delete = self.db.delete
        query = self.db.query
        table = 'delete_test_table'
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
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
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
        query('drop table "%s"' % table)
        table = 'delete_test_table_2'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            "n integer, m integer, t text, primary key (n, m))" % table)
        for n in range(3):
            for m in range(2):
                t = chr(ord('a') + 2 * n + m)
                query('insert into "%s" values('
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
        query('drop table "%s"' % table)

    def testDeleteWithQuotedNames(self):
        delete = self.db.delete
        query = self.db.query
        table = 'test table for delete()'
        query('drop table if exists "%s"' % table)
        query('create table "%s" ('
            '"Prime!" smallint primary key,'
            '"much space" integer, "Questions?" text)' % table)
        query('insert into "%s"'
              " values(19, 5005, 'Yes!')" % table)
        r = {'Prime!': 17}
        r = delete(table, r)
        self.assertEqual(r, 0)
        r = query('select count(*) from "%s"' % table).getresult()
        self.assertEqual(r[0][0], 1)
        r = {'Prime!': 19}
        r = delete(table, r)
        self.assertEqual(r, 1)
        r = query('select count(*) from "%s"' % table).getresult()
        self.assertEqual(r[0][0], 0)
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

    def testContextManager(self):
        query = self.db.query
        query("drop table if exists test_table")
        query("create table test_table (n integer check(n>0))")
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
        except pg.ProgrammingError as error:
            self.assertTrue('check' in str(error))
        with self.db:
            query("insert into test_table values (7)")
        r = [r[0] for r in query(
            "select * from test_table order by 1").getresult()]
        self.assertEqual(r, [1, 2, 5, 7])
        query("drop table test_table")

    def testBytea(self):
        query = self.db.query
        query('drop table if exists bytea_test')
        query('create table bytea_test (n smallint primary key, data bytea)')
        s = b"It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        r = self.db.escape_bytea(s)
        query('insert into bytea_test values(3,$1)', (r,))
        r = query('select * from bytea_test where n=3').getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 3)
        r = r[1]
        self.assertIsInstance(r, str)
        r = self.db.unescape_bytea(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        query('drop table bytea_test')

    def testInsertUpdateGetBytea(self):
        query = self.db.query
        query('drop table if exists bytea_test')
        query('create table bytea_test (n smallint primary key, data bytea)')
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
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        r = query('select * from bytea_test where n=5').getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 5)
        r = r[1]
        self.assertIsInstance(r, str)
        r = self.db.unescape_bytea(r)
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        r = self.db.get('bytea_test', dict(n=5))
        self.assertIsInstance(r, dict)
        self.assertIn('n', r)
        self.assertEqual(r['n'], 5)
        self.assertIn('data', r)
        r = r['data']
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, s)
        query('drop table bytea_test')

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


class TestDBClassNonStdOpts(TestDBClass):
    """Test the methods of the DB class with non-standard global options."""

    @classmethod
    def setUpClass(cls):
        cls.saved_options = {}
        cls.set_option('decimal', float)
        not_bool = not pg.get_bool()
        cls.set_option('bool', not_bool)
        unnamed_result = lambda q: q.getresult()
        cls.set_option('namedresult', unnamed_result)
        super(TestDBClassNonStdOpts, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestDBClassNonStdOpts, cls).tearDownClass()
        cls.reset_option('namedresult')
        cls.reset_option('bool')
        cls.reset_option('decimal')

    @classmethod
    def set_option(cls, option, value):
        cls.saved_options[option] = getattr(pg, 'get_' + option)()
        return getattr(pg, 'set_' + option)(value)

    @classmethod
    def reset_option(cls, option):
        return getattr(pg, 'set_' + option)(cls.saved_options[option])


class TestSchemas(unittest.TestCase):
    """Test correct handling of schemas (namespaces)."""

    @classmethod
    def setUpClass(cls):
        db = DB()
        query = db.query
        query("set client_min_messages=warning")
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

    @classmethod
    def tearDownClass(cls):
        db = DB()
        query = db.query
        query("set client_min_messages=warning")
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
        self.db = DB()
        self.db.query("set client_min_messages=warning")

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

    def testMunging(self):
        get = self.db.get
        query = self.db.query
        r = get("t", 1, 'n')
        self.assertIn('oid(t)', r)
        query("set search_path to s2")
        r = get("t2", 1, 'n')
        self.assertIn('oid(t2)', r)
        query("set search_path to s3")
        r = get("t", 1, 'n')
        self.assertIn('oid(t)', r)


if __name__ == '__main__':
    unittest.main()
