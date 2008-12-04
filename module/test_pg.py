#!/usr/bin/env python
#
# test_pg.py
#
# Written by Christoph Zwerschke
#
# $Id: test_pg.py,v 1.24 2008-12-04 21:11:54 cito Exp $
#

"""Test the classic PyGreSQL interface in the pg module.

The testing is done against a real local PostgreSQL database.

There are a few drawbacks:
* A local PostgreSQL database must be up and running, and
the user who is running the tests must be a trusted superuser.
* The performance of the API is not tested.
* Connecting to a remote host is not tested.
* Passing user, password and options is not tested.
* Status and error messages from the connection are not tested.
* It would be more reasonable to create a test for the underlying
shared library functions in the _pg module and assume they are ok.
The pg and pgdb modules should be tested against _pg mock functions.

"""

import pg
import unittest

debug = 0

# Try to load german locale for Umlaut tests
german = 1
try:
    import locale
    locale.setlocale(locale.LC_ALL, ('de', 'latin1'))
except Exception:
    try:
        locale.setlocale(locale.LC_ALL, 'german')
    except Exception:
        german = 0

try:
    from decimal import Decimal
except ImportError:
    Decimal = float


def smart_ddl(conn, cmd):
    """Execute DDL, but don't complain about minor things."""
    try:
        if cmd.startswith('create table '):
            i = cmd.find(' as select ')
            if i < 0:
                i = len(cmd)
            conn.query(cmd[:i] + ' with oids' + cmd[i:])
        else:
            conn.query(cmd)
    except pg.ProgrammingError:
        if cmd.startswith('drop table ') \
            or cmd.startswith('set ') \
            or cmd.startswith('alter database '):
            pass
        elif cmd.startswith('create table '):
            conn.query(cmd)
        else:
            raise


class TestAuxiliaryFunctions(unittest.TestCase):
    """Test the auxiliary functions external to the connection class."""

    def testIsQuoted(self):
        f = pg._is_quoted
        self.assert_(f('A'))
        self.assert_(f('0'))
        self.assert_(f('#'))
        self.assert_(f('*'))
        self.assert_(f('.'))
        self.assert_(f(' '))
        self.assert_(f('a b'))
        self.assert_(f('a+b'))
        self.assert_(f('a*b'))
        self.assert_(f('a.b'))
        self.assert_(f('0ab'))
        self.assert_(f('aBc'))
        self.assert_(f('ABC'))
        self.assert_(f('"a"'))
        self.assert_(not f('a'))
        self.assert_(not f('a0'))
        self.assert_(not f('_'))
        self.assert_(not f('_a'))
        self.assert_(not f('_0'))
        self.assert_(not f('_a_0_'))
        self.assert_(not f('ab'))
        self.assert_(not f('ab0'))
        self.assert_(not f('abc'))
        self.assert_(not f('abc'))
        if german:
            self.assert_(not f('\xe4'))
            self.assert_(f('\xc4'))
            self.assert_(not f('k\xe4se'))
            self.assert_(f('K\xe4se'))
            self.assert_(not f('emmentaler_k\xe4se'))
            self.assert_(f('emmentaler k\xe4se'))
            self.assert_(f('EmmentalerK\xe4se'))
            self.assert_(f('Emmentaler K\xe4se'))

    def testIsUnquoted(self):
        f = pg._is_unquoted
        self.assert_(f('A'))
        self.assert_(not f('0'))
        self.assert_(not f('#'))
        self.assert_(not f('*'))
        self.assert_(not f('.'))
        self.assert_(not f(' '))
        self.assert_(not f('a b'))
        self.assert_(not f('a+b'))
        self.assert_(not f('a*b'))
        self.assert_(not f('a.b'))
        self.assert_(not f('0ab'))
        self.assert_(f('aBc'))
        self.assert_(f('ABC'))
        self.assert_(not f('"a"'))
        self.assert_(f('a0'))
        self.assert_(f('_'))
        self.assert_(f('_a'))
        self.assert_(f('_0'))
        self.assert_(f('_a_0_'))
        self.assert_(f('ab'))
        self.assert_(f('ab0'))
        self.assert_(f('abc'))
        self.assert_(f('\xe4'))
        self.assert_(f('\xc4'))
        self.assert_(f('k\xe4se'))
        self.assert_(f('K\xe4se'))
        self.assert_(f('emmentaler_k\xe4se'))
        self.assert_(not f('emmentaler k\xe4se'))
        self.assert_(f('EmmentalerK\xe4se'))
        self.assert_(not f('Emmentaler K\xe4se'))

    def testSplitFirstPart(self):
        f = pg._split_first_part
        self.assertEqual(f('a.b'), ['a', 'b'])
        self.assertEqual(f('a.b.c'), ['a', 'b.c'])
        self.assertEqual(f('"a.b".c'), ['a.b', 'c'])
        self.assertEqual(f('a."b.c"'), ['a', '"b.c"'])
        self.assertEqual(f('A.b.c'), ['a', 'b.c'])
        self.assertEqual(f('Ab.c'), ['ab', 'c'])
        self.assertEqual(f('aB.c'), ['ab', 'c'])
        self.assertEqual(f('AB.c'), ['ab', 'c'])
        self.assertEqual(f('A b.c'), ['A b', 'c'])
        self.assertEqual(f('a B.c'), ['a B', 'c'])
        self.assertEqual(f('"A".b.c'), ['A', 'b.c'])
        self.assertEqual(f('"A""B".c'), ['A"B', 'c'])
        self.assertEqual(f('a.b.c.d.e.f.g'), ['a', 'b.c.d.e.f.g'])
        self.assertEqual(f('"a.b.c.d.e.f".g'), ['a.b.c.d.e.f', 'g'])
        self.assertEqual(f('a.B.c.D.e.F.g'), ['a', 'B.c.D.e.F.g'])
        self.assertEqual(f('A.b.C.d.E.f.G'), ['a', 'b.C.d.E.f.G'])

    def testSplitParts(self):
        f = pg._split_parts
        self.assertEqual(f('a.b'), ['a', 'b'])
        self.assertEqual(f('a.b.c'), ['a', 'b', 'c'])
        self.assertEqual(f('"a.b".c'), ['a.b', 'c'])
        self.assertEqual(f('a."b.c"'), ['a', 'b.c'])
        self.assertEqual(f('A.b.c'), ['a', 'b', 'c'])
        self.assertEqual(f('Ab.c'), ['ab', 'c'])
        self.assertEqual(f('aB.c'), ['ab', 'c'])
        self.assertEqual(f('AB.c'), ['ab', 'c'])
        self.assertEqual(f('A b.c'), ['A b', 'c'])
        self.assertEqual(f('a B.c'), ['a B', 'c'])
        self.assertEqual(f('"A".b.c'), ['A', 'b', 'c'])
        self.assertEqual(f('"A""B".c'), ['A"B', 'c'])
        self.assertEqual(f('a.b.c.d.e.f.g'),
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'])
        self.assertEqual(f('"a.b.c.d.e.f".g'),
            ['a.b.c.d.e.f', 'g'])
        self.assertEqual(f('a.B.c.D.e.F.g'),
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'])
        self.assertEqual(f('A.b.C.d.E.f.G'),
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'])

    def testJoinParts(self):
        f = pg._join_parts
        self.assertEqual(f(('a',)), 'a')
        self.assertEqual(f(('a', 'b')), 'a.b')
        self.assertEqual(f(('a', 'b', 'c')), 'a.b.c')
        self.assertEqual(f(('a', 'b', 'c', 'd', 'e', 'f', 'g')),
            'a.b.c.d.e.f.g')
        self.assertEqual(f(('A', 'b')), '"A".b')
        self.assertEqual(f(('a', 'B')), 'a."B"')
        self.assertEqual(f(('a b', 'c')), '"a b".c')
        self.assertEqual(f(('a', 'b c')), 'a."b c"')
        self.assertEqual(f(('a_b', 'c')), 'a_b.c')
        self.assertEqual(f(('a', 'b_c')), 'a.b_c')
        self.assertEqual(f(('0', 'a')), '"0".a')
        self.assertEqual(f(('0_', 'a')), '"0_".a')
        self.assertEqual(f(('_0', 'a')), '_0.a')
        self.assertEqual(f(('_a', 'b')), '_a.b')
        self.assertEqual(f(('a', 'B', '0', 'c0', 'C0',
            'd e', 'f_g', 'h.i', 'jklm', 'nopq')),
            'a."B"."0".c0."C0"."d e".f_g."h.i".jklm.nopq')


class TestHasConnect(unittest.TestCase):
    """Test existence of basic pg module functions."""

    def testhasPgError(self):
        self.assert_(issubclass(pg.Error, StandardError))

    def testhasPgWarning(self):
        self.assert_(issubclass(pg.Warning, StandardError))

    def testhasPgInterfaceError(self):
        self.assert_(issubclass(pg.InterfaceError, pg.Error))

    def testhasPgDatabaseError(self):
        self.assert_(issubclass(pg.DatabaseError, pg.Error))

    def testhasPgInternalError(self):
        self.assert_(issubclass(pg.InternalError, pg.DatabaseError))

    def testhasPgOperationalError(self):
        self.assert_(issubclass(pg.OperationalError, pg.DatabaseError))

    def testhasPgProgrammingError(self):
        self.assert_(issubclass(pg.ProgrammingError, pg.DatabaseError))

    def testhasPgIntegrityError(self):
        self.assert_(issubclass(pg.IntegrityError, pg.DatabaseError))

    def testhasPgDataError(self):
        self.assert_(issubclass(pg.DataError, pg.DatabaseError))

    def testhasPgNotSupportedError(self):
        self.assert_(issubclass(pg.NotSupportedError, pg.DatabaseError))

    def testhasConnect(self):
        self.assert_(callable(pg.connect))

    def testhasEscapeString(self):
        self.assert_(callable(pg.escape_string))

    def testhasEscapeBytea(self):
        self.assert_(callable(pg.escape_bytea))

    def testhasUnescapeBytea(self):
        self.assert_(callable(pg.unescape_bytea))

    def testDefHost(self):
        d0 = pg.get_defhost()
        d1 = 'pgtesthost'
        pg.set_defhost(d1)
        self.assertEqual(pg.get_defhost(), d1)
        pg.set_defhost(d0)
        self.assertEqual(pg.get_defhost(), d0)

    def testDefPort(self):
        d0 = pg.get_defport()
        d1 = 1234
        pg.set_defport(d1)
        self.assertEqual(pg.get_defport(), d1)
        if d0 is None:
            d0 = -1
        pg.set_defport(d0)
        if d0 == -1:
            d0 = None
        self.assertEqual(pg.get_defport(), d0)

    def testDefOpt(self):
        d0 = pg.get_defopt()
        d1 = '-h pgtesthost -p 1234'
        pg.set_defopt(d1)
        self.assertEqual(pg.get_defopt(), d1)
        pg.set_defopt(d0)
        self.assertEqual(pg.get_defopt(), d0)

    def testDefTty(self):
        d0 = pg.get_deftty()
        d1 = 'pgtesttty'
        pg.set_deftty(d1)
        self.assertEqual(pg.get_deftty(), d1)
        pg.set_deftty(d0)
        self.assertEqual(pg.get_deftty(), d0)

    def testDefBase(self):
        d0 = pg.get_defbase()
        d1 = 'pgtestdb'
        pg.set_defbase(d1)
        self.assertEqual(pg.get_defbase(), d1)
        pg.set_defbase(d0)
        self.assertEqual(pg.get_defbase(), d0)


class TestEscapeFunctions(unittest.TestCase):
    """"Test pg escape and unescape functions."""

    def testEscapeString(self):
        self.assertEqual(pg.escape_string('plain'), 'plain')
        self.assertEqual(pg.escape_string(
            "that's k\xe4se"), "that''s k\xe4se")
        self.assertEqual(pg.escape_string(
            r"It's fine to have a \ inside."),
            r"It''s fine to have a \\ inside.")

    def testEscapeBytea(self):
        self.assertEqual(pg.escape_bytea('plain'), 'plain')
        self.assertEqual(pg.escape_bytea(
            "that's k\xe4se"), "that''s k\\\\344se")
        self.assertEqual(pg.escape_bytea(
            'O\x00ps\xff!'), r'O\\000ps\\377!')

    def testUnescapeBytea(self):
        self.assertEqual(pg.unescape_bytea('plain'), 'plain')
        self.assertEqual(pg.unescape_bytea(
            "that's k\\344se"), "that's k\xe4se")
        self.assertEqual(pg.unescape_bytea(
            r'O\000ps\377!'), 'O\x00ps\xff!')


class TestCanConnect(unittest.TestCase):
    """Test whether a basic connection to PostgreSQL is possible."""

    def testCanConnectTemplate1(self):
        dbname = 'template1'
        try:
            connection = pg.connect(dbname)
        except Exception:
            self.fail('Cannot connect to database ' + dbname)
        try:
            connection.close()
        except Exception:
            self.fail('Cannot close the database connection')


class TestConnectObject(unittest.TestCase):
    """"Test existence of basic pg connection methods."""

    def setUp(self):
        dbname = 'template1'
        self.dbname = dbname
        self.connection = pg.connect(dbname)

    def tearDown(self):
        self.connection.close()

    def testAllConnectAttributes(self):
        attributes = ['db', 'error', 'host', 'options', 'port',
            'protocol_version', 'server_version', 'status', 'tty', 'user']
        connection_attributes = [a for a in dir(self.connection)
            if not callable(eval("self.connection." + a))]
        self.assertEqual(attributes, connection_attributes)

    def testAllConnectMethods(self):
        methods = ['cancel', 'close', 'endcopy', 'escape_bytea',
            'escape_string', 'fileno', 'getline', 'getlo', 'getnotify',
            'inserttable', 'locreate', 'loimport', 'parameter', 'putline',
            'query', 'reset', 'source', 'transaction']
        connection_methods = [a for a in dir(self.connection)
            if callable(eval("self.connection." + a))]
        self.assertEqual(methods, connection_methods)

    def testAttributeDb(self):
        self.assertEqual(self.connection.db, self.dbname)

    def testAttributeError(self):
        error = self.connection.error
        self.assert_(not error or 'krb5_' in error)

    def testAttributeHost(self):
        def_host = 'localhost'
        self.assertEqual(self.connection.host, def_host)

    def testAttributeOptions(self):
        no_options = ''
        self.assertEqual(self.connection.options, no_options)

    def testAttributePort(self):
        def_port = 5432
        self.assertEqual(self.connection.port, def_port)

    def testAttributeProtocolVersion(self):
        protocol_version = self.connection.protocol_version
        self.assert_(isinstance(protocol_version, int))
        self.assert_(2 <= protocol_version < 4)

    def testAttributeServerVersion(self):
        server_version = self.connection.server_version
        self.assert_(isinstance(server_version, int))
        self.assert_(70400 <= server_version < 90000)

    def testAttributeStatus(self):
        status_ok = 1
        self.assertEqual(self.connection.status, status_ok)

    def testAttributeTty(self):
        def_tty = ''
        self.assertEqual(self.connection.tty, def_tty)

    def testAttributeUser(self):
        no_user = 'Deprecated facility'
        user = self.connection.user
        self.assert_(self.connection.user)
        self.assertNotEqual(self.connection.user, no_user)

    def testMethodQuery(self):
        self.connection.query("select 1+1")

    def testMethodEndcopy(self):
        try:
            self.connection.endcopy()
        except IOError:
            pass

    def testMethodClose(self):
        self.connection.close()
        try:
            self.connection.reset()
            self.fail('Reset should give an error for a closed connection')
        except Exception:
            pass
        self.assertRaises(pg.InternalError, self.connection.close)
        try:
            self.connection.query('select 1')
            self.fail('Query should give an error for a closed connection')
        except Exception:
            pass
        self.connection = pg.connect(self.dbname)


class TestSimpleQueries(unittest.TestCase):
    """"Test simple queries via a basic pg connection."""

    def setUp(self):
        dbname = 'template1'
        self.c = pg.connect(dbname)

    def tearDown(self):
        self.c.close()

    def testSelect0(self):
        q = "select 0"
        self.c.query(q)

    def testSelect0Semicolon(self):
        q = "select 0;"
        self.c.query(q)

    def testSelectSemicolon(self):
        q = "select ;"
        self.assertRaises(pg.ProgrammingError, self.c.query, q)

    def testGetresult(self):
        q = "select 0"
        result = [(0,)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def testDictresult(self):
        q = "select 0 as alias0"
        result = [{'alias0': 0}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

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

    def testGet3Rows(self):
        q = "select 3 union select 1 union select 2 order by 1"
        result = [(1,), (2,), (3,)]
        r = self.c.query(q).getresult()
        self.assertEqual(r, result)

    def testGet3DictRows(self):
        q = "select 3 as alias3" \
            " union select 1 union select 2 order by 1"
        result = [{'alias3': 1}, {'alias3': 2}, {'alias3': 3}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def testDictresultNames(self):
        q = "select 'MixedCase' as MixedCaseAlias"
        result = [{'mixedcasealias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)
        q = "select 'MixedCase' as \"MixedCaseAlias\""
        result = [{'MixedCaseAlias': 'MixedCase'}]
        r = self.c.query(q).dictresult()
        self.assertEqual(r, result)

    def testBigGetresult(self):
        num_cols = 100
        num_rows = 100
        q = "select " + ','.join(map(str, xrange(num_cols)))
        q = ' union all '.join((q,) * num_rows)
        r = self.c.query(q).getresult()
        result = [tuple(range(num_cols))] * num_rows
        self.assertEqual(r, result)

    def testListfields(self):
        q = 'select 0 as a, 0 as b, 0 as c,' \
            ' 0 as c, 0 as b, 0 as a,' \
            ' 0 as lowercase, 0 as UPPERCASE,' \
            ' 0 as MixedCase, 0 as "MixedCase",' \
            ' 0 as a_long_name_with_underscores,' \
            ' 0 as "A long name with Blanks"'
        r = self.c.query(q).listfields()
        result = ('a', 'b', 'c', 'c', 'b', 'a',
            'lowercase', 'uppercase', 'mixedcase', 'MixedCase',
            'a_long_name_with_underscores',
            'A long name with Blanks')
        self.assertEqual(r, result)

    def testFieldname(self):
        q = "select 0 as z, 0 as a, 0 as x, 0 as y"
        r = self.c.query(q).fieldname(2)
        result = "x"
        self.assertEqual(r, result)

    def testFieldnum(self):
        q = "select 0 as z, 0 as a, 0 as x, 0 as y"
        r = self.c.query(q).fieldnum("x")
        result = 2
        self.assertEqual(r, result)

    def testNtuples(self):
        q = "select 1 as a, 2 as b, 3 as c, 4 as d" \
            " union select 5 as a, 6 as b, 7 as c, 8 as d"
        r = self.c.query(q).ntuples()
        result = 2
        self.assertEqual(r, result)

    def testQuery(self):
        smart_ddl(self.c, "drop table test_table")
        q = "create table test_table (n integer) with oids"
        r = self.c.query(q)
        self.assert_(r is None)
        q = "insert into test_table values (1)"
        r = self.c.query(q)
        self.assert_(isinstance(r, int)), r
        q = "insert into test_table select 2"
        r = self.c.query(q)
        self.assert_(isinstance(r, int))
        oid = r
        q = "select oid from test_table where n=2"
        r = self.c.query(q).getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r, oid)
        q = "insert into test_table select 3 union select 4 union select 5"
        r = self.c.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '3')
        q = "update test_table set n=4 where n<5"
        r = self.c.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '4')
        q = "delete from test_table"
        r = self.c.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '5')

    def testPrint(self):
        q = "select 1 as a, 'hello' as h, 'w' as world" \
            " union select 2, 'xyz', 'uvw'"
        r = self.c.query(q)
        t = '~test_pg_testPrint_temp.tmp'
        s = open(t, 'w')
        import sys, os
        stdout, sys.stdout = sys.stdout, s
        try:
            print r
        except Exception:
            pass
        sys.stdout = stdout
        s.close()
        r = filter(bool, open(t, 'r').read().splitlines())
        os.remove(t)
        self.assertEqual(r,
            ['a|h    |world',
            '-+-----+-----',
            '1|hello|w    ',
            '2|xyz  |uvw  ',
            '(2 rows)'])

    def testGetNotify(self):
        self.assert_(self.c.getnotify() is None)
        self.c.query('listen test_notify')
        try:
            self.assert_(self.c.getnotify() is None)
            self.c.query('notify test_notify')
            r = self.c.getnotify()
            self.assert_(isinstance(r, tuple))
            self.assertEqual(len(r), 2)
            self.assert_(isinstance(r[0], str))
            self.assert_(isinstance(r[1], int))
            self.assertEqual(r[0], 'test_notify')
        finally:
            self.c.query('unlisten test_notify')


class TestInserttable(unittest.TestCase):
    """"Test inserttable method."""

    # Test database needed: must be run as a DBTestSuite.

    def setUp(self):
        dbname = DBTestSuite.dbname
        self.c = pg.connect(dbname)
        self.c.query('truncate table test')

    def tearDown(self):
        self.c.close()

    def testInserttable1Row(self):
        d = Decimal is float and 1.0 or None
        data = [(1, 1, 1L, d, 1.0, 1.0, d, "1", "1111", "1")]
        self.c.inserttable("test", data)
        r = self.c.query("select * from test").getresult()
        self.assertEqual(r, data)

    def testInserttable4Rows(self):
        data = [(-1, -1, -1L, None, -1.0, -1.0, None, "-1", "-1-1", "-1"),
            (0, 0, 0L, None, 0.0, 0.0, None, "0", "0000", "0"),
            (1, 1, 1L, None, 1.0, 1.0, None, "1", "1111", "1"),
            (2, 2, 2L, None, 2.0, 2.0, None, "2", "2222", "2")]
        self.c.inserttable("test", data)
        r = self.c.query("select * from test order by 1").getresult()
        self.assertEqual(r, data)

    def testInserttableMultipleRows(self):
        num_rows = 100
        data = [(1, 1, 1L, None, 1.0, 1.0, None, "1", "1111", "1")] * num_rows
        self.c.inserttable("test", data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def testInserttableMultipleCalls(self):
        num_rows = 10
        data = [(1, 1, 1L, None, 1.0, 1.0, None, "1", "1111", "1")]
        for i in range(num_rows):
            self.c.inserttable("test", data)
        r = self.c.query("select count(*) from test").getresult()[0][0]
        self.assertEqual(r, num_rows)

    def testInserttableNullValues(self):
        num_rows = 100
        data = [(None,) * 10]
        self.c.inserttable("test", data)
        r = self.c.query("select * from test").getresult()
        self.assertEqual(r, data)

    def testInserttableMaxValues(self):
        data = [(2**15 - 1, int(2**31 - 1), long(2**31 - 1),
            None, 1.0 + 1.0/32, 1.0 + 1.0/32, None,
            "1234", "1234", "1234" * 10)]
        self.c.inserttable("test", data)
        r = self.c.query("select * from test").getresult()
        self.assertEqual(r, data)


class TestDBClassBasic(unittest.TestCase):
    """"Test existence of the DB class wrapped pg connection methods."""

    def setUp(self):
        dbname = 'template1'
        self.dbname = dbname
        self.db = pg.DB(dbname)

    def tearDown(self):
        self.db.close()

    def testAllDBAttributes(self):
        attributes = ['cancel', 'clear', 'close', 'db', 'dbname', 'debug',
            'delete', 'endcopy', 'error', 'escape_bytea', 'escape_string',
            'fileno', 'get', 'get_attnames', 'get_databases', 'get_relations',
            'get_tables', 'getline', 'getlo', 'getnotify', 'host', 'insert',
            'inserttable', 'locreate', 'loimport', 'options', 'parameter',
            'pkey', 'port', 'protocol_version', 'putline', 'query', 'reopen',
            'reset', 'server_version', 'source', 'status', 'transaction',
            'tty', 'unescape_bytea', 'update', 'user']
        db_attributes = [a for a in dir(self.db)
            if not a.startswith('_')]
        self.assertEqual(attributes, db_attributes)

    def testAttributeDb(self):
        self.assertEqual(self.db.db.db, self.dbname)

    def testAttributeDbname(self):
        self.assertEqual(self.db.dbname, self.dbname)

    def testAttributeError(self):
        error = self.db.error
        self.assert_(not error or 'krb5_' in error)
        self.assertEqual(self.db.error, self.db.db.error)

    def testAttributeHost(self):
        def_host = 'localhost'
        host = self.db.host
        self.assertEqual(host, def_host)
        self.assertEqual(host, self.db.db.host)

    def testAttributeOptions(self):
        no_options = ''
        options = self.db.options
        self.assertEqual(options, no_options)
        self.assertEqual(options, self.db.db.options)

    def testAttributePort(self):
        def_port = 5432
        port = self.db.port
        self.assertEqual(port, def_port)
        self.assertEqual(port, self.db.db.port)

    def testAttributeProtocolVersion(self):
        protocol_version = self.db.protocol_version
        self.assert_(isinstance(protocol_version, int))
        self.assert_(2 <= protocol_version < 4)
        self.assertEqual(protocol_version, self.db.db.protocol_version)

    def testAttributeServerVersion(self):
        server_version = self.db.server_version
        self.assert_(isinstance(server_version, int))
        self.assert_(70400 <= server_version < 90000)
        self.assertEqual(server_version, self.db.db.server_version)

    def testAttributeStatus(self):
        status_ok = 1
        status = self.db.status
        self.assertEqual(status, status_ok)
        self.assertEqual(status, self.db.db.status)

    def testAttributeTty(self):
        def_tty = ''
        tty = self.db.tty
        self.assertEqual(tty, def_tty)
        self.assertEqual(tty, self.db.db.tty)

    def testAttributeUser(self):
        no_user = 'Deprecated facility'
        user = self.db.user
        self.assert_(user)
        self.assertNotEqual(user, no_user)
        self.assertEqual(user, self.db.db.user)

    def testMethodEscapeString(self):
        self.assertEqual(self.db.escape_string("plain"), "plain")
        self.assertEqual(self.db.escape_string(
            "that's k\xe4se"), "that''s k\xe4se")
        self.assertEqual(self.db.escape_string(
            r"It's fine to have a \ inside."),
            r"It''s fine to have a \\ inside.")

    def testMethodEscapeBytea(self):
        self.assertEqual(self.db.escape_bytea("plain"), "plain")
        self.assertEqual(self.db.escape_bytea(
            "that's k\xe4se"), "that''s k\\\\344se")
        self.assertEqual(self.db.escape_bytea(
            'O\x00ps\xff!'), r'O\\000ps\\377!')

    def testMethodUnescapeBytea(self):
        self.assertEqual(self.db.unescape_bytea("plain"), "plain")
        self.assertEqual(self.db.unescape_bytea(
            "that's k\\344se"), "that's k\xe4se")
        self.assertEqual(pg.unescape_bytea(
            r'O\000ps\377!'), 'O\x00ps\xff!')

    def testMethodQuery(self):
        self.db.query("select 1+1")

    def testMethodEndcopy(self):
        try:
            self.db.endcopy()
        except IOError:
            pass

    def testMethodClose(self):
        self.db.close()
        try:
            self.db.reset()
            self.fail('Reset should give an error for a closed connection')
        except Exception:
            pass
        self.assertRaises(pg.InternalError, self.db.close)
        self.assertRaises(pg.InternalError, self.db.query, 'select 1')
        self.db = pg.DB(self.dbname)

    def testExistingConnection(self):
        db = pg.DB(self.db.db)
        self.assertEqual(self.db.db, db.db)
        self.assert_(db.db)
        db.close()
        self.assert_(db.db)
        db.reopen()
        self.assert_(db.db)
        db.close()
        self.assert_(db.db)
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
    """"Test the methods of the DB class wrapped pg connection."""

    # Test database needed: must be run as a DBTestSuite.

    def setUp(self):
        dbname = DBTestSuite.dbname
        self.dbname = dbname
        self.db = pg.DB(dbname)
        if debug:
            self.db.debug = 'DEBUG: %s'

    def tearDown(self):
        self.db.close()

    def testEscapeString(self):
        self.assertEqual(self.db.escape_string("plain"), "plain")
        self.assertEqual(self.db.escape_string(
            "that's k\xe4se"), "that''s k\xe4se")
        self.assertEqual(self.db.escape_string(
            r"It's fine to have a \ inside."),
            r"It''s fine to have a \\ inside.")

    def testEscapeBytea(self):
        self.assertEqual(self.db.escape_bytea("plain"), "plain")
        self.assertEqual(self.db.escape_bytea(
            "that's k\xe4se"), "that''s k\\\\344se")
        self.assertEqual(self.db.escape_bytea(
            'O\x00ps\xff!'), r'O\\000ps\\377!')

    def testUnescapeBytea(self):
        self.assertEqual(self.db.unescape_bytea("plain"), "plain")
        self.assertEqual(self.db.unescape_bytea(
            "that's k\\344se"), "that's k\xe4se")
        self.assertEqual(pg.unescape_bytea(
            r'O\000ps\377!'), 'O\x00ps\xff!')

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
        self.assertEqual(f(123456789, 'int'), '123456789')
        self.assertEqual(f(123456987, 'num'), '123456987')
        self.assertEqual(f(1.23654789, 'num'), '1.23654789')
        self.assertEqual(f(12365478.9, 'num'), '12365478.9')
        self.assertEqual(f('123456789', 'num'), '123456789')
        self.assertEqual(f('1.23456789', 'num'), '1.23456789')
        self.assertEqual(f('12345678.9', 'num'), '12345678.9')
        self.assertEqual(f(123, 'money'), "'123.00'")
        self.assertEqual(f('123', 'money'), "'123.00'")
        self.assertEqual(f(123.45, 'money'), "'123.45'")
        self.assertEqual(f('123.45', 'money'), "'123.45'")
        self.assertEqual(f(123.454, 'money'), "'123.45'")
        self.assertEqual(f('123.454', 'money'), "'123.45'")
        self.assertEqual(f(123.456, 'money'), "'123.46'")
        self.assertEqual(f('123.456', 'money'), "'123.46'")
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
        self.assertEqual(f('ab\\c', 'text'), "'ab\\\\c'")
        self.assertEqual(f("a\\b'c", 'text'), "'a\\\\b''c'")

    def testQuery(self):
        smart_ddl(self.db, "drop table test_table")
        q = "create table test_table (n integer) with oids"
        r = self.db.query(q)
        self.assert_(r is None)
        q = "insert into test_table values (1)"
        r = self.db.query(q)
        self.assert_(isinstance(r, int)), r
        q = "insert into test_table select 2"
        r = self.db.query(q)
        self.assert_(isinstance(r, int))
        oid = r
        q = "select oid from test_table where n=2"
        r = self.db.query(q).getresult()
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertEqual(r, oid)
        q = "insert into test_table select 3 union select 4 union select 5"
        r = self.db.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '3')
        q = "update test_table set n=4 where n<5"
        r = self.db.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '4')
        q = "delete from test_table"
        r = self.db.query(q)
        self.assert_(isinstance(r, str))
        self.assertEqual(r, '5')

    def testPkey(self):
        smart_ddl(self.db, "drop table pkeytest0")
        smart_ddl(self.db, "create table pkeytest0 ("
            "a smallint)")
        smart_ddl(self.db, 'drop table pkeytest1')
        smart_ddl(self.db, "create table pkeytest1 ("
            "b smallint primary key)")
        smart_ddl(self.db, 'drop table pkeytest2')
        smart_ddl(self.db, "create table pkeytest2 ("
            "c smallint, d smallint primary key)")
        smart_ddl(self.db, "drop table pkeytest3")
        smart_ddl(self.db, "create table pkeytest3 ("
            "e smallint, f smallint, g smallint, "
            "h smallint, i smallint, "
            "primary key (f,h))")
        self.assertRaises(KeyError, self.db.pkey, 'pkeytest0')
        self.assertEqual(self.db.pkey('pkeytest1'), 'b')
        self.assertEqual(self.db.pkey('pkeytest2'), 'd')
        self.assertEqual(self.db.pkey('pkeytest3'), 'f')
        self.assertEqual(self.db.pkey('pkeytest0', 'none'), 'none')
        self.assertEqual(self.db.pkey('pkeytest0'), 'none')
        self.db.pkey(None, {'t': 'a', 'n.t': 'b'})
        self.assertEqual(self.db.pkey('t'), 'a')
        self.assertEqual(self.db.pkey('n.t'), 'b')
        self.assertRaises(KeyError, self.db.pkey, 'pkeytest0')

    def testGetDatabases(self):
        databases = self.db.get_databases()
        self.assert_('template0' in databases)
        self.assert_('template1' in databases)
        self.assert_(self.dbname in databases)

    def testGetTables(self):
        result1 = self.db.get_tables()
        tables = ('"A very Special Name"',
            '"A_MiXeD_quoted_NaMe"', 'a1', 'a2',
            'A_MiXeD_NaMe', '"another special name"',
            'averyveryveryveryveryveryverylongtablename',
            'b0', 'b3', 'x', 'xx', 'xXx', 'y', 'z')
        for t in tables:
            smart_ddl(self.db, 'drop table ' + t)
            smart_ddl(self.db, "create table %s"
                " as select 0" % t)
        result3 = self.db.get_tables()
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
            self.db.query('drop table ' + t)
        result2 = self.db.get_tables()
        self.assertEqual(result2, result1)

    def testGetRelations(self):
        result = self.db.get_relations()
        self.assert_('public.test' in result)
        self.assert_('public.test_view' in result)
        result = self.db.get_relations('rv')
        self.assert_('public.test' in result)
        self.assert_('public.test_view' in result)
        result = self.db.get_relations('r')
        self.assert_('public.test' in result)
        self.assert_('public.test_view' not in result)
        result = self.db.get_relations('v')
        self.assert_('public.test' not in result)
        self.assert_('public.test_view' in result)
        result = self.db.get_relations('cisSt')
        self.assert_('public.test' not in result)
        self.assert_('public.test_view' not in result)

    def testAttnames(self):
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'does_not_exist')
        self.assertRaises(pg.ProgrammingError,
            self.db.get_attnames, 'has.too.many.dots')
        for table in ('attnames_test_table', 'test table for attnames'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                'a smallint, b integer, c bigint, '
                'e numeric, f float, f2 double precision, m money, '
                'x smallint, y smallint, z smallint, '
                'Normal_NaMe smallint, "Special Name" smallint, '
                't text, u char(2), v varchar(2), '
                'primary key (y, u))' % table)
            attributes = self.db.get_attnames(table)
            result = {'a': 'int', 'c': 'int', 'b': 'int',
                'e': 'num', 'f': 'float', 'f2': 'float', 'm': 'money',
                'normal_name': 'int', 'Special Name': 'int',
                'u': 'text', 't': 'text', 'v': 'text',
                'y': 'int', 'x': 'int', 'z': 'int', 'oid': 'int' }
            self.assertEqual(attributes, result)

    def testGet(self):
        for table in ('get_test_table', 'test table for get'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                "n integer, t text)" % table)
            for n, t in enumerate('xyz'):
                self.db.query('insert into "%s" values('
                    "%d, '%s')" % (table, n+1, t))
            self.assertRaises(KeyError, self.db.get, table, 2)
            r = self.db.get(table, 2, 'n')
            oid_table = table
            if ' ' in table:
                oid_table = '"%s"' % oid_table
            oid_table = 'oid(public.%s)' % oid_table
            self.assert_(oid_table in r)
            self.assert_(isinstance(r[oid_table], int))
            result = {'t': 'y', 'n': 2, oid_table: r[oid_table]}
            self.assertEqual(r, result)
            self.assertEqual(self.db.get(table, r[oid_table], 'oid')['t'], 'y')
            self.assertEqual(self.db.get(table, 1, 'n')['t'], 'x')
            self.assertEqual(self.db.get(table, 3, 'n')['t'], 'z')
            self.assertEqual(self.db.get(table, 2, 'n')['t'], 'y')
            self.assertRaises(pg.DatabaseError, self.db.get, table, 4, 'n')
            r['n'] = 3
            self.assertEqual(self.db.get(table, r, 'n')['t'], 'z')
            self.assertEqual(self.db.get(table, 1, 'n')['t'], 'x')
            self.db.query('alter table "%s" alter n set not null' % table)
            self.db.query('alter table "%s" add primary key (n)' % table)
            self.assertEqual(self.db.get(table, 3)['t'], 'z')
            self.assertEqual(self.db.get(table, 1)['t'], 'x')
            self.assertEqual(self.db.get(table, 2)['t'], 'y')
            r['n'] = 1
            self.assertEqual(self.db.get(table, r)['t'], 'x')
            r['n'] = 3
            self.assertEqual(self.db.get(table, r)['t'], 'z')
            r['n'] = 2
            self.assertEqual(self.db.get(table, r)['t'], 'y')

    def testGetFromView(self):
        self.db.query('delete from test where i4=14')
        self.db.query('insert into test (i4, v4) values('
            "14, 'abc4')")
        r = self.db.get('test_view', 14, 'i4')
        self.assert_('v4' in r)
        self.assertEqual(r['v4'], 'abc4')

    def testInsert(self):
        for table in ('insert_test_table', 'test table for insert'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                "i2 smallint, i4 integer, i8 bigint,"
                "d numeric, f4 real, f8 double precision, m money, "
                "v4 varchar(4), c4 char(4), t text,"
                "b boolean, ts timestamp)" % table)
            data = dict(i2 = 2**15 - 1,
                i4 = int(2**31 - 1), i8 = long(2**31 - 1),
                d = Decimal('123456789.9876543212345678987654321'),
                f4 = 1.0 + 1.0/32, f8 = 1.0 + 1.0/32,
                m = "1234.56", v4 = "1234", c4 = "1234", t = "1234" * 10,
                b = 1, ts = 'current_date')
            r = self.db.insert(table, data)
            self.assertEqual(r, data)
            oid_table = table
            if ' ' in table:
                oid_table = '"%s"' % oid_table
            oid_table = 'oid(public.%s)' % oid_table
            self.assert_(oid_table in r)
            self.assert_(isinstance(r[oid_table], int))
            s = self.db.query('select oid,* from "%s"' % table).dictresult()[0]
            s[oid_table] = s['oid']
            del s['oid']
            self.assertEqual(r, s)

    def testUpdate(self):
        for table in ('update_test_table', 'test table for update'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                "n integer, t text)" % table)
            for n, t in enumerate('xyz'):
                self.db.query('insert into "%s" values('
                    "%d, '%s')" % (table, n+1, t))
            self.assertRaises(KeyError, self.db.get, table, 2)
            r = self.db.get(table, 2, 'n')
            r['t'] = 'u'
            s = self.db.update(table, r)
            self.assertEqual(s, r)
            r = self.db.query('select t from "%s" where n=2' % table
                ).getresult()[0][0]
            self.assertEqual(r, 'u')

    def testClear(self):
        for table in ('clear_test_table', 'test table for clear'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                "n integer, b boolean, d date, t text)" % table)
            r = self.db.clear(table)
            result = {'n': 0, 'b': 'f', 'd': '', 't': ''}
            self.assertEqual(r, result)
            r['a'] = r['n'] = 1
            r['d'] = r['t'] = 'x'
            r['b']
            r['oid'] = 1L
            r = self.db.clear(table, r)
            result = {'a': 1, 'n': 0, 'b': 'f', 'd': '', 't': '', 'oid': 1L}
            self.assertEqual(r, result)

    def testDelete(self):
        for table in ('delete_test_table', 'test table for delete'):
            smart_ddl(self.db, 'drop table "%s"' % table)
            smart_ddl(self.db, 'create table "%s" ('
                "n integer, t text)" % table)
            for n, t in enumerate('xyz'):
                self.db.query('insert into "%s" values('
                    "%d, '%s')" % (table, n+1, t))
            self.assertRaises(KeyError, self.db.get, table, 2)
            r = self.db.get(table, 1, 'n')
            s = self.db.delete(table, r)
            r = self.db.get(table, 3, 'n')
            s = self.db.delete(table, r)
            r = self.db.query('select * from "%s"' % table).dictresult()
            self.assertEqual(len(r), 1)
            r = r[0]
            result = {'n': 2, 't': 'y'}
            self.assertEqual(r, result)
            r = self.db.get(table, 2, 'n')
            s = self.db.delete(table, r)
            self.assertRaises(pg.DatabaseError, self.db.get, table, 2, 'n')

    def testBytea(self):
        smart_ddl(self.db, 'drop table bytea_test')
        smart_ddl(self.db, 'create table bytea_test ('
            'data bytea)')
        s = "It's all \\ kinds \x00 of\r nasty \xff stuff!\n"
        r = self.db.escape_bytea(s)
        self.db.query('insert into bytea_test values('
            "'%s')" % r)
        r = self.db.query('select * from bytea_test').getresult()
        self.assert_(len(r) == 1)
        r = r[0]
        self.assert_(len(r) == 1)
        r = r[0]
        r = self.db.unescape_bytea(r)
        self.assertEqual(r, s)


class TestSchemas(unittest.TestCase):
    """"Test correct handling of schemas (namespaces)."""

    # Test database needed: must be run as a DBTestSuite.

    def setUp(self):
        dbname = DBTestSuite.dbname
        self.dbname = dbname
        self.db = pg.DB(dbname)

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
                self.assert_(t in tables, t + ' not in get_tables()')

    def testGetAttnames(self):
        result = {'oid': 'int', 'd': 'int', 'n': 'int'}
        r = self.db.get_attnames("t")
        self.assertEqual(r, result)
        r = self.db.get_attnames("s4.t4")
        self.assertEqual(r, result)
        smart_ddl(self.db, "create table s3.t3m"
            " as select 1 as m")
        result_m = {'oid': 'int', 'm': 'int'}
        r = self.db.get_attnames("s3.t3m")
        self.assertEqual(r, result_m)
        self.db.query("set search_path to s1,s3")
        r = self.db.get_attnames("t3")
        self.assertEqual(r, result)
        r = self.db.get_attnames("t3m")
        self.assertEqual(r, result_m)

    def testGet(self):
        self.assertEqual(self.db.get("t", 1, 'n')['d'], 0)
        self.assertEqual(self.db.get("t0", 1, 'n')['d'], 0)
        self.assertEqual(self.db.get("public.t", 1, 'n')['d'], 0)
        self.assertEqual(self.db.get("public.t0", 1, 'n')['d'], 0)
        self.assertRaises(pg.ProgrammingError, self.db.get, "public.t1", 1, 'n')
        self.assertEqual(self.db.get("s1.t1", 1, 'n')['d'], 1)
        self.assertEqual(self.db.get("s3.t", 1, 'n')['d'], 3)
        self.db.query("set search_path to s2,s4")
        self.assertRaises(pg.ProgrammingError, self.db.get, "t1", 1, 'n')
        self.assertEqual(self.db.get("t4", 1, 'n')['d'], 4)
        self.assertRaises(pg.ProgrammingError, self.db.get, "t3", 1, 'n')
        self.assertEqual(self.db.get("t", 1, 'n')['d'], 2)
        self.assertEqual(self.db.get("s3.t3", 1, 'n')['d'], 3)
        self.db.query("set search_path to s1,s3")
        self.assertRaises(pg.ProgrammingError, self.db.get, "t2", 1, 'n')
        self.assertEqual(self.db.get("t3", 1, 'n')['d'], 3)
        self.assertRaises(pg.ProgrammingError, self.db.get, "t4", 1, 'n')
        self.assertEqual(self.db.get("t", 1, 'n')['d'], 1)
        self.assertEqual(self.db.get("s4.t4", 1, 'n')['d'], 4)

    def testMangling(self):
        r = self.db.get("t", 1, 'n')
        self.assert_('oid(public.t)' in r)
        self.db.query("set search_path to s2")
        r = self.db.get("t2", 1, 'n')
        self.assert_('oid(s2.t2)' in r)
        self.db.query("set search_path to s3")
        r = self.db.get("t", 1, 'n')
        self.assert_('oid(s3.t)' in r)


class DBTestSuite(unittest.TestSuite):
    """Test suite that provides a test database."""

    dbname = "testpg_tempdb"

    # It would be too slow to create and drop the test database for
    # every single test, so it is done once for the whole suite only.

    def setUp(self):
        dbname = self.dbname
        c = pg.connect("template1")
        try:
            c.query("drop database " + dbname)
        except pg.Error:
            pass
        c.query("create database " + dbname
            + " template=template0")
        for s in ('client_min_messages = warning',
            'default_with_oids = on',
            'standard_conforming_strings = off',
            'escape_string_warning = off'):
            smart_ddl(c, 'alter database %s set %s' % (dbname, s))
        c.close()
        c = pg.connect(dbname)
        smart_ddl(c, "create table test ("
            "i2 smallint, i4 integer, i8 bigint,"
            "d numeric, f4 real, f8 double precision, m money, "
            "v4 varchar(4), c4 char(4), t text)")
        c.query("create view test_view as"
            " select i4, v4 from test")
        for num_schema in range(5):
            if num_schema:
                schema = "s%d" % num_schema
                c.query("create schema " + schema)
            else:
                schema = "public"
            smart_ddl(c, "create table %s.t"
                " as select 1 as n, %d as d"
                % (schema, num_schema))
            smart_ddl(c, "create table %s.t%d"
                " as select 1 as n, %d as d"
                % (schema, num_schema, num_schema))
        c.close()

    def tearDown(self):
        dbname = self.dbname
        c = pg.connect(dbname)
        c.query("checkpoint")
        c.close()
        c = pg.connect("template1")
        c.query("drop database " + dbname)
        c.close()

    def __call__(self, result):
        self.setUp()
        unittest.TestSuite.__call__(self, result)
        self.tearDown()


if __name__ == '__main__':

    # All tests that do not need a database:
    TestSuite1 = unittest.TestSuite((
        unittest.makeSuite(TestAuxiliaryFunctions),
        unittest.makeSuite(TestHasConnect),
        unittest.makeSuite(TestEscapeFunctions),
        unittest.makeSuite(TestCanConnect),
        unittest.makeSuite(TestConnectObject),
        unittest.makeSuite(TestSimpleQueries),
        unittest.makeSuite(TestDBClassBasic),
        ))

    # All tests that need a test database:
    TestSuite2 = DBTestSuite((
        unittest.makeSuite(TestInserttable),
        unittest.makeSuite(TestDBClass),
        unittest.makeSuite(TestSchemas),
        ))

    # All tests together in one test suite:
    TestSuite = unittest.TestSuite((
        TestSuite1,
        TestSuite2
    ))

    unittest.TextTestRunner(verbosity=2).run(TestSuite)
