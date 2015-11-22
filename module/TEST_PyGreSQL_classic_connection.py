#! /usr/bin/python
# -*- coding: utf-8 -*-

"""Test the classic PyGreSQL interface.

Sub-tests for the low-level connection object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.

"""

from __future__ import print_function

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest
import sys
import tempfile
import threading
import time

import pg  # the module under test

from decimal import Decimal

try:
    long
except NameError:  # Python >= 3.0
    long = int

unicode_strings = str is not bytes

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
dbname = 'unittest'
dbhost = None
dbport = 5432

try:
    from LOCAL_PyGreSQL import *
except ImportError:
    pass


def connect():
    """Create a basic pg connection to the test database."""
    connection = pg.connect(dbname, dbhost, dbport)
    connection.query("set client_min_messages=warning")
    return connection


class TestCanConnect(unittest.TestCase):
    """Test whether a basic connection to PostgreSQL is possible."""

    def testCanConnect(self):
        try:
            connection = connect()
        except pg.Error as error:
            self.fail('Cannot connect to database %s:\n%s' % (dbname, error))
        try:
            connection.close()
        except pg.Error:
            self.fail('Cannot close the database connection')


class TestConnectObject(unittest.TestCase):
    """"Test existence of basic pg connection methods."""

    def setUp(self):
        self.connection = connect()

    def tearDown(self):
        try:
            self.connection.close()
        except pg.InternalError:
            pass

    def testClassName(self):
        self.assertEqual(self.connection.__class__.__name__, 'pgconnobject')

    def testModuleName(self):
        self.assertEqual(self.connection.__module__, 'pg')

    def testAllConnectAttributes(self):
        attributes = '''db error host options port
            protocol_version server_version status tty user'''.split()
        connection = self.connection
        connection_attributes = [a for a in dir(connection)
            if not a.startswith('__')
            and not callable(getattr(connection, a))]
        self.assertEqual(attributes, connection_attributes)

    def testAllConnectMethods(self):
        methods = '''cancel close endcopy
            escape_bytea escape_identifier escape_literal escape_string
            fileno get_notice_receiver getline getlo getnotify
            inserttable locreate loimport parameter putline query reset
            set_notice_receiver source transaction'''.split()
        connection = self.connection
        connection_methods = [a for a in dir(connection)
            if not a.startswith('__')
            and callable(getattr(connection, a))]
        self.assertEqual(methods, connection_methods)

    def testAttributeDb(self):
        self.assertEqual(self.connection.db, dbname)

    def testAttributeError(self):
        error = self.connection.error
        self.assertTrue(not error or 'krb5_' in error)

    def testAttributeHost(self):
        def_host = 'localhost'
        self.assertIsInstance(self.connection.host, str)
        self.assertEqual(self.connection.host, dbhost or def_host)

    def testAttributeOptions(self):
        no_options = ''
        self.assertEqual(self.connection.options, no_options)

    def testAttributePort(self):
        def_port = 5432
        self.assertIsInstance(self.connection.port, int)
        self.assertEqual(self.connection.port, dbport or def_port)

    def testAttributeProtocolVersion(self):
        protocol_version = self.connection.protocol_version
        self.assertIsInstance(protocol_version, int)
        self.assertTrue(2 <= protocol_version < 4)

    def testAttributeServerVersion(self):
        server_version = self.connection.server_version
        self.assertIsInstance(server_version, int)
        self.assertTrue(70400 <= server_version < 100000)

    def testAttributeStatus(self):
        status_ok = 1
        self.assertIsInstance(self.connection.status, int)
        self.assertEqual(self.connection.status, status_ok)

    def testAttributeTty(self):
        def_tty = ''
        self.assertIsInstance(self.connection.tty, str)
        self.assertEqual(self.connection.tty, def_tty)

    def testAttributeUser(self):
        no_user = 'Deprecated facility'
        user = self.connection.user
        self.assertTrue(user)
        self.assertIsInstance(user, str)
        self.assertNotEqual(user, no_user)

    def testMethodQuery(self):
        query = self.connection.query
        query("select 1+1")
        query("select 1+$1", (1,))
        query("select 1+$1+$2", (2, 3))
        query("select 1+$1+$2", [2, 3])

    def testMethodQueryEmpty(self):
        self.assertRaises(ValueError, self.connection.query, '')

    def testMethodEndcopy(self):
        try:
            self.connection.endcopy()
        except IOError:
            pass

    def testMethodClose(self):
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

    def testMethodReset(self):
        query = self.connection.query
        # check that client encoding gets reset
        encoding = query('show client_encoding').getresult()[0][0].upper()
        changed_encoding = 'LATIN1' if encoding == 'UTF8' else 'UTF8'
        self.assertNotEqual(encoding, changed_encoding)
        self.connection.query("set client_encoding=%s" % changed_encoding)
        new_encoding = query('show client_encoding').getresult()[0][0].upper()
        self.assertEqual(new_encoding, changed_encoding)
        self.connection.reset()
        new_encoding = query('show client_encoding').getresult()[0][0].upper()
        self.assertNotEqual(new_encoding, changed_encoding)
        self.assertEqual(new_encoding, encoding)

    def testMethodCancel(self):
        r = self.connection.cancel()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 1)

    def testCancelLongRunningThread(self):
        errors = []

        def sleep():
            try:
                self.connection.query('select pg_sleep(5)').getresult()
            except pg.ProgrammingError as error:
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

    def testMethodFileNo(self):
        r = self.connection.fileno()
        self.assertIsInstance(r, int)
        self.assertGreaterEqual(r, 0)


class TestSimpleQueries(unittest.TestCase):
    """"Test simple queries via a basic pg connection."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def testSelect0(self):
        q = "select 0"
        self.c.query(q)

    def testSelect0Semicolon(self):
        q = "select 0;"
        self.c.query(q)

    def testSelectDotSemicolon(self):
        q = "select .;"
        self.assertRaises(pg.ProgrammingError, self.c.query, q)

    def testGetresult(self):
        q = "select 0"
        result = [(0,)]
        r = self.c.query(q).getresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, tuple)
        self.assertIsInstance(v[0], int)
        self.assertEqual(r, result)

    def testGetresultLong(self):
        q = "select 9876543210"
        result = long(9876543210)
        self.assertIsInstance(result, long)
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, long)
        self.assertEqual(v, result)

    def testGetresultDecimal(self):
        q = "select 98765432109876543210"
        result = Decimal(98765432109876543210)
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, Decimal)
        self.assertEqual(v, result)

    def testGetresultString(self):
        result = 'Hello, world!'
        q = "select '%s'" % result
        v = self.c.query(q).getresult()[0][0]
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def testDictresult(self):
        q = "select 0 as alias0"
        result = [{'alias0': 0}]
        r = self.c.query(q).dictresult()
        self.assertIsInstance(r, list)
        v = r[0]
        self.assertIsInstance(v, dict)
        self.assertIsInstance(v['alias0'], int)
        self.assertEqual(r, result)

    def testDictresultLong(self):
        q = "select 9876543210 as longjohnsilver"
        result = long(9876543210)
        self.assertIsInstance(result, long)
        v = self.c.query(q).dictresult()[0]['longjohnsilver']
        self.assertIsInstance(v, long)
        self.assertEqual(v, result)

    def testDictresultDecimal(self):
        q = "select 98765432109876543210 as longjohnsilver"
        result = Decimal(98765432109876543210)
        v = self.c.query(q).dictresult()[0]['longjohnsilver']
        self.assertIsInstance(v, Decimal)
        self.assertEqual(v, result)

    def testDictresultString(self):
        result = 'Hello, world!'
        q = "select '%s' as greeting" % result
        v = self.c.query(q).dictresult()[0]['greeting']
        self.assertIsInstance(v, str)
        self.assertEqual(v, result)

    def testNamedresult(self):
        q = "select 0 as alias0"
        result = [(0,)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('alias0',))
        self.assertEqual(v.alias0, 0)

    def testGet3Cols(self):
        q = "select 1,2,3"
        result = [(1, 2, 3)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def testGet3DictCols(self):
        q = "select 1 as a,2 as b,3 as c"
        result = [dict(a=1, b=2, c=3)]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def testGet3NamedCols(self):
        q = "select 1 as a,2 as b,3 as c"
        result = [(1, 2, 3)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        v = r[0]
        self.assertEqual(v._fields, ('a', 'b', 'c'))
        self.assertEqual(v.b, 2)

    def testGet3Rows(self):
        q = "select 3 union select 1 union select 2 order by 1"
        result = [(1,), (2,), (3,)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def testGet3DictRows(self):
        q = ("select 3 as alias3"
            " union select 1 union select 2 order by 1")
        result = [{'alias3': 1}, {'alias3': 2}, {'alias3': 3}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def testGet3NamedRows(self):
        q = ("select 3 as alias3"
            " union select 1 union select 2 order by 1")
        result = [(1,), (2,), (3,)]
        r = self.c.query(q).namedresult()
        self.assertEqual(r, result)
        for v in r:
            self.assertEqual(v._fields, ('alias3',))

    def testDictresultNames(self):
        q = "select 'MixedCase' as MixedCaseAlias"
        result = [{'mixedcasealias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)
        q = "select 'MixedCase' as \"MixedCaseAlias\""
        result = [{'MixedCaseAlias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def testNamedresultNames(self):
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

    def testBigGetresult(self):
        num_cols = 100
        num_rows = 100
        q = "select " + ','.join(map(str, range(num_cols)))
        q = ' union all '.join((q,) * num_rows)
        r = self.c.query(q).getresult()
        result = [tuple(range(num_cols))] * num_rows
        self.assertEqual(r, result)

    def testListfields(self):
        q = ('select 0 as a, 0 as b, 0 as c,'
            ' 0 as c, 0 as b, 0 as a,'
            ' 0 as lowercase, 0 as UPPERCASE,'
            ' 0 as MixedCase, 0 as "MixedCase",'
            ' 0 as a_long_name_with_underscores,'
            ' 0 as "A long name with Blanks"')
        r = self.c.query(q).listfields()
        result = ('a', 'b', 'c', 'c', 'b', 'a',
            'lowercase', 'uppercase', 'mixedcase', 'MixedCase',
            'a_long_name_with_underscores',
            'A long name with Blanks')
        self.assertEqual(r, result)

    def testFieldname(self):
        q = "select 0 as z, 0 as a, 0 as x, 0 as y"
        r = self.c.query(q).fieldname(2)
        self.assertEqual(r, 'x')
        r = self.c.query(q).fieldname(3)
        self.assertEqual(r, 'y')

    def testFieldnum(self):
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

    def testNtuples(self):
        q = "select 1 where false"
        r = self.c.query(q).ntuples()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 0)
        q = ("select 1 as a, 2 as b, 3 as c, 4 as d"
            " union select 5 as a, 6 as b, 7 as c, 8 as d")
        r = self.c.query(q).ntuples()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 2)
        q = ("select 1 union select 2 union select 3"
            " union select 4 union select 5 union select 6")
        r = self.c.query(q).ntuples()
        self.assertIsInstance(r, int)
        self.assertEqual(r, 6)

    def testQuery(self):
        query = self.c.query
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
        q = "delete from test_table"
        r = query(q)
        self.assertIsInstance(r, str)
        self.assertEqual(r, '5')
        query("drop table test_table")

    def testPrint(self):
        q = ("select 1 as a, 'hello' as h, 'w' as world"
            " union select 2, 'xyz', 'uvw'")
        r = self.c.query(q)
        f = tempfile.TemporaryFile('r+')
        stdout, sys.stdout = sys.stdout, f
        try:
            print(r)
        except Exception:
            pass
        finally:
            sys.stdout = stdout
        f.seek(0)
        r = f.read()
        f.close()
        self.assertEqual(r,
            'a|  h  |world\n'
            '-+-----+-----\n'
            '1|hello|w    \n'
            '2|xyz  |uvw  \n'
            '(2 rows)\n')


class TestParamQueries(unittest.TestCase):
    """"Test queries with parameters via a basic pg connection."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def testQueryWithNoneParam(self):
        self.assertEqual(self.c.query("select $1::integer", (None,)
            ).getresult(), [(None,)])
        self.assertEqual(self.c.query("select $1::text", [None]
            ).getresult(), [(None,)])

    def testQueryWithBoolParams(self):
        query = self.c.query
        self.assertEqual(query("select false").getresult(), [('f',)])
        self.assertEqual(query("select true").getresult(), [('t',)])
        self.assertEqual(query("select $1::bool", (None,)).getresult(),
            [(None,)])
        self.assertEqual(query("select $1::bool", ('f',)).getresult(), [('f',)])
        self.assertEqual(query("select $1::bool", ('t',)).getresult(), [('t',)])
        self.assertEqual(query("select $1::bool", ('false',)).getresult(),
            [('f',)])
        self.assertEqual(query("select $1::bool", ('true',)).getresult(),
            [('t',)])
        self.assertEqual(query("select $1::bool", ('n',)).getresult(), [('f',)])
        self.assertEqual(query("select $1::bool", ('y',)).getresult(), [('t',)])
        self.assertEqual(query("select $1::bool", (0,)).getresult(), [('f',)])
        self.assertEqual(query("select $1::bool", (1,)).getresult(), [('t',)])
        self.assertEqual(query("select $1::bool", (False,)).getresult(),
            [('f',)])
        self.assertEqual(query("select $1::bool", (True,)).getresult(),
            [('t',)])

    def testQueryWithIntParams(self):
        query = self.c.query
        self.assertEqual(query("select 1+1").getresult(), [(2,)])
        self.assertEqual(query("select 1+$1", (1,)).getresult(), [(2,)])
        self.assertEqual(query("select 1+$1", [1]).getresult(), [(2,)])
        self.assertEqual(query("select $1::integer", (2,)).getresult(), [(2,)])
        self.assertEqual(query("select $1::text", (2,)).getresult(), [('2',)])
        self.assertEqual(query("select 1+$1::numeric", [1]).getresult(),
            [(Decimal('2'),)])
        self.assertEqual(query("select 1, $1::integer", (2,)
            ).getresult(), [(1, 2)])
        self.assertEqual(query("select 1 union select $1", (2,)
            ).getresult(), [(1,), (2,)])
        self.assertEqual(query("select $1::integer+$2", (1, 2)
            ).getresult(), [(3,)])
        self.assertEqual(query("select $1::integer+$2", [1, 2]
            ).getresult(), [(3,)])
        self.assertEqual(query("select 0+$1+$2+$3+$4+$5+$6", list(range(6))
            ).getresult(), [(15,)])

    def testQueryWithStrParams(self):
        query = self.c.query
        self.assertEqual(query("select $1||', world!'", ('Hello',)
            ).getresult(), [('Hello, world!',)])
        self.assertEqual(query("select $1||', world!'", ['Hello']
            ).getresult(), [('Hello, world!',)])
        self.assertEqual(query("select $1||', '||$2||'!'", ('Hello', 'world'),
            ).getresult(), [('Hello, world!',)])
        self.assertEqual(query("select $1::text", ('Hello, world!',)
            ).getresult(), [('Hello, world!',)])
        self.assertEqual(query("select $1::text,$2::text", ('Hello', 'world')
            ).getresult(), [('Hello', 'world')])
        self.assertEqual(query("select $1::text,$2::text", ['Hello', 'world']
            ).getresult(), [('Hello', 'world')])
        self.assertEqual(query("select $1::text union select $2::text",
            ('Hello', 'world')).getresult(), [('Hello',), ('world',)])
        self.assertEqual(query("select $1||', '||$2||'!'", ('Hello',
            'w\xc3\xb6rld')).getresult(), [('Hello, w\xc3\xb6rld!',)])

    def testQueryWithUnicodeParams(self):
        query = self.c.query
        query('set client_encoding = utf8')
        self.assertEqual(query("select $1||', '||$2||'!'",
            ('Hello', u'wörld')).getresult(), [('Hello, wörld!',)])
        self.assertEqual(query("select $1||', '||$2||'!'",
            ('Hello', u'мир')).getresult(),
            [('Hello, мир!',)])
        query('set client_encoding = latin1')
        r = query("select $1||', '||$2||'!'", ('Hello', u'wörld')).getresult()
        if unicode_strings:
            self.assertEqual(r, [('Hello, wörld!',)])
        else:
            self.assertEqual(r, [(u'Hello, wörld!'.encode('latin1'),)])
        self.assertRaises(UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', u'мир'))
        query('set client_encoding = iso_8859_1')
        r = query("select $1||', '||$2||'!'", ('Hello', u'wörld')).getresult()
        if unicode_strings:
            self.assertEqual(r, [('Hello, wörld!',)])
        else:
            self.assertEqual(r, [(u'Hello, wörld!'.encode('latin1'),)])
        self.assertRaises(UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', u'мир'))
        query('set client_encoding = iso_8859_5')
        self.assertRaises(UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', u'wörld'))
        r = query("select $1||', '||$2||'!'", ('Hello', u'мир')).getresult()
        if unicode_strings:
            self.assertEqual(r, [('Hello, мир!',)])
        else:
            self.assertEqual(r, [(u'Hello, мир!'.encode('cyrillic'),)])
        query('set client_encoding = sql_ascii')
        self.assertRaises(UnicodeError, query, "select $1||', '||$2||'!'",
            ('Hello', u'wörld'))

    def testQueryWithMixedParams(self):
        self.assertEqual(self.c.query("select $1+2,$2||', world!'",
            (1, 'Hello'),).getresult(), [(3, 'Hello, world!')])
        self.assertEqual(self.c.query("select $1::integer,$2::date,$3::text",
            (4711, None, 'Hello!'),).getresult(), [(4711, None, 'Hello!')])

    def testQueryWithDuplicateParams(self):
        self.assertRaises(pg.ProgrammingError,
            self.c.query, "select $1+$1", (1,))
        self.assertRaises(pg.ProgrammingError,
            self.c.query, "select $1+$1", (1, 2))

    def testQueryWithZeroParams(self):
        self.assertEqual(self.c.query("select 1+1", []
            ).getresult(), [(2,)])

    def testQueryWithGarbage(self):
        garbage = r"'\{}+()-#[]oo324"
        self.assertEqual(self.c.query("select $1::text AS garbage", (garbage,)
            ).dictresult(), [{'garbage': garbage}])

    def testUnicodeQuery(self):
        query = self.c.query
        self.assertEqual(query(u"select 1+1").getresult(), [(2,)])
        if unicode_strings:
            self.assertEqual(query("select 'Hello, wörld!'").getresult(),
                [('Hello, wörld!',)])
        else:
            self.assertRaises(TypeError, query, u"select 'Hello, wörld!'")


class TestInserttable(unittest.TestCase):
    """"Test inserttable method."""

    @classmethod
    def setUpClass(cls):
        c = connect()
        c.query("drop table if exists test cascade")
        c.query("create table test ("
            "i2 smallint, i4 integer, i8 bigint, b boolean, dt date, ti time,"
            "d numeric, f4 real, f8 double precision, m money,"
            "c char(1), v4 varchar(4), c4 char(4), t text)")
        c.close()

    @classmethod
    def tearDownClass(cls):
        c = connect()
        c.query("drop table test cascade")
        c.close()

    def setUp(self):
        self.c = connect()
        self.c.query("set lc_monetary='C'")
        self.c.query("set datestyle='ISO,YMD'")

    def tearDown(self):
        self.c.query("truncate table test")
        self.c.close()

    data = [
        (-1, -1, long(-1), True, '1492-10-12', '08:30:00',
            -1.2345, -1.75, -1.875, '-1.25', '-', 'r?', '!u', 'xyz'),
        (0, 0, long(0), False, '1607-04-14', '09:00:00',
            0.0, 0.0, 0.0, '0.0', ' ', '0123', '4567', '890'),
        (1, 1, long(1), True, '1801-03-04', '03:45:00',
            1.23456, 1.75, 1.875, '1.25', 'x', 'bc', 'cdef', 'g'),
        (2, 2, long(2), False, '1903-12-17', '11:22:00',
            2.345678, 2.25, 2.125, '2.75', 'y', 'q', 'ijk', 'mnop\nstux!')]

    def get_back(self):
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
                self.assertIsInstance(row[2], long)
            if row[3] is not None:  # boolean
                self.assertIsInstance(row[3], str)
                row[3] = {'f': False, 't': True}.get(row[3])
            if row[4] is not None:  # date
                self.assertIsInstance(row[4], str)
                self.assertTrue(row[4].replace('-', '').isdigit())
            if row[5] is not None:  # time
                self.assertIsInstance(row[5], str)
                self.assertTrue(row[5].replace(':', '').isdigit())
            if row[6] is not None:  # numeric
                self.assertIsInstance(row[6], Decimal)
                row[6] = float(row[6])
            if row[7] is not None:  # real
                self.assertIsInstance(row[7], float)
            if row[8] is not None:  # double precision
                self.assertIsInstance(row[8], float)
                row[8] = float(row[8])
            if row[9] is not None:  # money
                self.assertIsInstance(row[9], Decimal)
                row[9] = str(float(row[9]))
            if row[10] is not None:  # char(1)
                self.assertIsInstance(row[10], str)
                self.assertEqual(len(row[10]), 1)
            if row[11] is not None:  # varchar(4)
                self.assertIsInstance(row[11], str)
                self.assertLessEqual(len(row[11]), 4)
            if row[12] is not None:  # char(4)
                self.assertIsInstance(row[12], str)
                self.assertEqual(len(row[12]), 4)
                row[12] = row[12].rstrip()
            if row[13] is not None:  # text
                self.assertIsInstance(row[13], str)
            row = tuple(row)
            data.append(row)
        return data

    def testInserttable1Row(self):
        data = self.data[2:3]
        self.c.inserttable("test", data)
        self.assertEqual(self.get_back(), data)

    def testInserttable4Rows(self):
        data = self.data
        self.c.inserttable("test", data)
        self.assertEqual(self.get_back(), data)

    def testInserttableMultipleRows(self):
        num_rows = 100
        data = self.data[2:3] * num_rows
        self.c.inserttable("test", data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def testInserttableMultipleCalls(self):
        num_rows = 10
        data = self.data[2:3]
        for _i in range(num_rows):
            self.c.inserttable("test", data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def testInserttableNullValues(self):
        data = [(None,) * 14] * 100
        self.c.inserttable("test", data)
        self.assertEqual(self.get_back(), data)

    def testInserttableMaxValues(self):
        data = [(2 ** 15 - 1, int(2 ** 31 - 1), long(2 ** 31 - 1),
            True, '2999-12-31', '11:59:59', 1e99,
            1.0 + 1.0 / 32, 1.0 + 1.0 / 32, None,
            "1", "1234", "1234", "1234" * 100)]
        self.c.inserttable("test", data)
        self.assertEqual(self.get_back(), data)


class TestDirectSocketAccess(unittest.TestCase):
    """"Test copy command with direct socket access."""

    @classmethod
    def setUpClass(cls):
        c = connect()
        c.query("drop table if exists test cascade")
        c.query("create table test (i int, v varchar(16))")
        c.close()

    @classmethod
    def tearDownClass(cls):
        c = connect()
        c.query("drop table test cascade")
        c.close()

    def setUp(self):
        self.c = connect()
        self.c.query("set datestyle='ISO,YMD'")

    def tearDown(self):
        self.c.query("truncate table test")
        self.c.close()

    def testPutline(self):
        putline = self.c.putline
        query = self.c.query
        data = list(enumerate("apple pear plum cherry banana".split()))
        query("copy test from stdin")
        try:
            for i, v in data:
                putline("%d\t%s\n" % (i, v))
            putline("\\.\n")
        finally:
            self.c.endcopy()
        r = query("select * from test").getresult()
        self.assertEqual(r, data)

    def testPutlineBytesAndUnicode(self):
        putline = self.c.putline
        query = self.c.query
        query("set client_encoding=utf8")
        query("copy test from stdin")
        try:
            putline(u"47\tkäse\n".encode('utf8'))
            putline("35\twürstel\n")
            putline(b"\\.\n")
        finally:
            self.c.endcopy()
        r = query("select * from test").getresult()
        self.assertEqual(r, [(47, 'käse'), (35, 'würstel')])

    def testGetline(self):
        getline = self.c.getline
        query = self.c.query
        data = list(enumerate("apple banana pear plum strawberry".split()))
        n = len(data)
        self.c.inserttable('test', data)
        query("copy test to stdout")
        try:
            for i in range(n + 2):
                v = getline()
                if i < n:
                    self.assertEqual(v, '%d\t%s' % data[i])
                elif i == n:
                    self.assertEqual(v, '\\.')
                else:
                    self.assertIsNone(v)
        finally:
            try:
                self.c.endcopy()
            except IOError:
                pass

    def testGetlineBytesAndUnicode(self):
        getline = self.c.getline
        query = self.c.query
        data = [(54, u'käse'.encode('utf8')), (73, u'würstel')]
        self.c.inserttable('test', data)
        query("copy test to stdout")
        try:
            v = getline()
            self.assertIsInstance(v, str)
            self.assertEqual(v, '54\tkäse')
            v = getline()
            self.assertIsInstance(v, str)
            self.assertEqual(v, '73\twürstel')
            self.assertEqual(getline(), '\\.')
            self.assertIsNone(getline())
        finally:
            try:
                self.c.endcopy()
            except IOError:
                pass

    def testParameterChecks(self):
        self.assertRaises(TypeError, self.c.putline)
        self.assertRaises(TypeError, self.c.getline, 'invalid')
        self.assertRaises(TypeError, self.c.endcopy, 'invalid')


class TestNotificatons(unittest.TestCase):
    """"Test notification support."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def testGetNotify(self):
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
            try:
                query("notify test_notify, 'test_payload'")
            except pg.ProgrammingError:  # PostgreSQL < 9.0
                pass
            else:
                r = getnotify()
                self.assertTrue(isinstance(r, tuple))
                self.assertEqual(len(r), 3)
                self.assertIsInstance(r[0], str)
                self.assertIsInstance(r[1], int)
                self.assertIsInstance(r[2], str)
                self.assertEqual(r[0], 'test_notify')
                self.assertEqual(r[2], 'test_payload')
                self.assertIsNone(getnotify())
        finally:
            query('unlisten test_notify')

    def testGetNoticeReceiver(self):
        self.assertIsNone(self.c.get_notice_receiver())

    def testSetNoticeReceiver(self):
        self.assertRaises(TypeError, self.c.set_notice_receiver, None)
        self.assertRaises(TypeError, self.c.set_notice_receiver, 42)
        self.assertIsNone(self.c.set_notice_receiver(lambda notice: None))

    def testSetAndGetNoticeReceiver(self):
        r = lambda notice: None
        self.assertIsNone(self.c.set_notice_receiver(r))
        self.assertIs(self.c.get_notice_receiver(), r)

    def testNoticeReceiver(self):
        self.c.query('''create function bilbo_notice() returns void AS $$
            begin
                raise warning 'Bilbo was here!';
            end;
            $$ language plpgsql''')
        try:
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
            self.c.query('''select bilbo_notice()''')
            self.assertEqual(received, dict(
                pgcnx=self.c, message='WARNING:  Bilbo was here!\n',
                severity='WARNING', primary='Bilbo was here!',
                detail=None, hint=None))
        finally:
            self.c.query('''drop function bilbo_notice();''')


class TestConfigFunctions(unittest.TestCase):
    """Test the functions for changing default settings.

    To test the effect of most of these functions, we need a database
    connection.  That's why they are covered in this test module.

    """

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def testGetDecimalPoint(self):
        point = pg.get_decimal_point()
        self.assertIsInstance(point, str)
        self.assertEqual(point, '.')

    def testSetDecimalPoint(self):
        d = pg.Decimal
        point = pg.get_decimal_point()
        query = self.c.query
        # check that money values can be interpreted correctly
        # if and only if the decimal point is set appropriately
        # for the current lc_monetary setting
        query("set lc_monetary='en_US.UTF-8'")
        pg.set_decimal_point('.')
        r = query("select '34.25'::money").getresult()[0][0]
        self.assertIsInstance(r, d)
        self.assertEqual(r, d('34.25'))
        pg.set_decimal_point(',')
        r = query("select '34.25'::money").getresult()[0][0]
        self.assertNotEqual(r, d('34.25'))
        query("set lc_monetary='de_DE.UTF-8'")
        pg.set_decimal_point(',')
        r = query("select '34,25'::money").getresult()[0][0]
        self.assertIsInstance(r, d)
        self.assertEqual(r, d('34.25'))
        pg.set_decimal_point('.')
        r = query("select '34,25'::money").getresult()[0][0]
        self.assertNotEqual(r, d('34.25'))
        pg.set_decimal_point(point)

    def testSetDecimal(self):
        d = pg.Decimal
        query = self.c.query
        r = query("select 3425::numeric").getresult()[0][0]
        self.assertIsInstance(r, d)
        self.assertEqual(r, d('3425'))
        pg.set_decimal(long)
        r = query("select 3425::numeric").getresult()[0][0]
        self.assertNotIsInstance(r, d)
        self.assertIsInstance(r, long)
        self.assertEqual(r, long(3425))
        pg.set_decimal(d)

    def testSetNamedresult(self):
        query = self.c.query

        r = query("select 1 as x, 2 as y").namedresult()[0]
        self.assertIsInstance(r, tuple)
        self.assertEqual(r, (1, 2))
        self.assertIsNot(type(r), tuple)
        self.assertEqual(r._fields, ('x', 'y'))
        self.assertEqual(r._asdict(), {'x': 1, 'y': 2})
        self.assertEqual(r.__class__.__name__, 'Row')

        _namedresult = pg._namedresult
        self.assertTrue(callable(_namedresult))
        pg.set_namedresult(_namedresult)

        r = query("select 1 as x, 2 as y").namedresult()[0]
        self.assertIsInstance(r, tuple)
        self.assertEqual(r, (1, 2))
        self.assertIsNot(type(r), tuple)
        self.assertEqual(r._fields, ('x', 'y'))
        self.assertEqual(r._asdict(), {'x': 1, 'y': 2})
        self.assertEqual(r.__class__.__name__, 'Row')

        def _listresult(q):
            return [list(row) for row in q.getresult()]

        pg.set_namedresult(_listresult)

        try:
            r = query("select 1 as x, 2 as y").namedresult()[0]
            self.assertIsInstance(r, list)
            self.assertEqual(r, [1, 2])
            self.assertIsNot(type(r), tuple)
            self.assertFalse(hasattr(r, '_fields'))
            self.assertNotEqual(r.__class__.__name__, 'Row')
        finally:
            pg.set_namedresult(_namedresult)


if __name__ == '__main__':
    unittest.main()
