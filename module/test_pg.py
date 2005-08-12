# test_pg.py
# Written by Christoph Zwerschke
# $Id: test_pg.py,v 1.1 2005-08-12 22:02:58 cito Exp $

"""Test the classic PyGreSQL interface in the pg module.

The testing is done against a real local PostgreSQL database.

There are a few drawbacks:
* A local PostgreSQL database must be up and running, and
the user who is running the test needs create database privilege
* The performance of the API is not tested
* Connecting to a remote host is not tested
* Passing user, password and options is not tested
* Status and error messages from the connection are not tested
* It would be more reasonable to create a test for the underlying
shared library functions in the _pg module and assume they are ok.
The pg and pgdb modules should be tested against _pg mock functions.

"""

import pg
import unittest

import locale
locale.setlocale(locale.LC_ALL, '')


class TestAuxiliaryFunctions(unittest.TestCase):
	"""Test the auxiliary functions external to the connection class."""

	def testQuote(self):
		f = pg._quote
		self.assertEqual(f(None, None), 'NULL')
		self.assertEqual(f(None, 'int'), 'NULL')
		self.assertEqual(f(None, 'decimal'), 'NULL')
		self.assertEqual(f(None, 'money'), 'NULL')
		self.assertEqual(f(None, 'bool'), 'NULL')
		self.assertEqual(f(None, 'date'), 'NULL')
		self.assertEqual(f('', 'int'), 'NULL')
		self.assertEqual(f('', 'seq'), 'NULL')
		self.assertEqual(f('', 'decimal'), 'NULL')
		self.assertEqual(f('', 'money'), 'NULL')
		self.assertEqual(f('', 'bool'), 'NULL')
		self.assertEqual(f('', 'date'), 'NULL')
		self.assertEqual(f('', 'text'), "''")
		self.assertEqual(f(123456789, 'int'), '123456789')
		self.assertEqual(f(123654789, 'seq'), '123654789')
		self.assertEqual(f(123456987, 'decimal'), '123456987')
		self.assertEqual(f(1.23654789, 'decimal'), '1.23654789')
		self.assertEqual(f(12365478.9, 'decimal'), '12365478.9')
		self.assertEqual(f('123456789', 'decimal'), '123456789')
		self.assertEqual(f('1.23456789', 'decimal'), '1.23456789')
		self.assertEqual(f('12345678.9', 'decimal'), '12345678.9')
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
		self.assertEqual(f("ab'c", 'text'), "'ab\\'c'")
		self.assertEqual(f('ab\\c', 'text'), "'ab\\\\c'")
		self.assertEqual(f("a\\b'c", 'text'), "'a\\\\b\\'c'")

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
		self.assertEqual(f('a.b.c.d.e.f.g'), ['a', 'b', 'c', 'd', 'e', 'f', 'g'])
		self.assertEqual(f('"a.b.c.d.e.f".g'), ['a.b.c.d.e.f', 'g'])
		self.assertEqual(f('a.B.c.D.e.F.g'), ['a', 'b', 'c', 'd', 'e', 'f', 'g'])
		self.assertEqual(f('A.b.C.d.E.f.G'), ['a', 'b', 'c', 'd', 'e', 'f', 'g'])

	def testJoinParts(self):
		f = pg._join_parts
		self.assertEqual(f(('a',)), 'a')
		self.assertEqual(f(('a', 'b')), 'a.b')
		self.assertEqual(f(('a', 'b', 'c')), 'a.b.c')
		self.assertEqual(f(('a', 'b', 'c', 'd', 'e', 'f', 'g')), 'a.b.c.d.e.f.g')
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
		self.assertEqual(f(('a', 'B', '0', 'c0', 'C0', 'd e', 'f_g', 'h.i', 'jklm', 'nopq')),
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


class TestCanConnect(unittest.TestCase):
	"""Test whether a basic connection to PostGreSQL is possible."""

	def testCanConnectTemplate1(self):
		dbname = 'template1'
		try:
			connection = pg.connect(dbname)
		except:
			self.fail('Cannot connect to database ' + dbname)
		try:
			connection.close()
		except:
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
		attributes = ['db', 'error', 'host', 'options',
			'port', 'status', 'tty', 'user']
		connection_attributes = [a for a in dir(self.connection)
			if not callable(eval("self.connection." + a))]
		self.assertEqual(attributes, connection_attributes)

	def testAllConnectMethods(self):
		methods = ['close', 'endcopy', 'fileno',
			'getline', 'getlo', 'getnotify',
			'inserttable', 'locreate', 'loimport',
			'putline', 'query', 'reset', 'source']
		connection_methods = [a for a in dir(self.connection)
			if callable(eval("self.connection." + a))]
		self.assertEqual(methods, connection_methods)

	def testAttributeDb(self):
		self.assertEqual(self.connection.db, self.dbname)

	def testAttributeError(self):
		no_error = ''
		self.assertEqual(self.connection.error, no_error)

	def testAttributeHost(self):
		def_host = 'localhost'
		self.assertEqual(self.connection.host, def_host)

	def testAttributeOptions(self):
		no_options = ''
		self.assertEqual(self.connection.options, no_options)

	def testAttributePort(self):
		def_port = 5432
		self.assertEqual(self.connection.port, def_port)

	def testAttributeStatus(self):
		status_ok = 1
		self.assertEqual(self.connection.status, status_ok)

	def testAttributeTty(self):
		def_tty = ''
		self.assertEqual(self.connection.tty, def_tty)

	def testAttributeUser(self):
		def_user = 'Deprecated facility'
		self.assertEqual(self.connection.user, def_user)

	def testMethodQuery(self):
		self.connection.query("select 1+1")

	def testMethodEndcopy(self):
		self.connection.endcopy()

	def testMethodClose(self):
		self.connection.close()
		try:
			self.connection.reset()
			fail('Reset should give an error for a closed connection')
		except:
			pass
		self.assertRaises(pg.InternalError, self.connection.close)
		try:
			self.connection.query('select 1')
			self.fail('Query should give an error for a closed connection')
		except:
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
		result = [(1,2,3)]
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
		data = [(1, 1, 1L, 1.0, 1.0, 1.0, "1", "1111", "1")]
		self.c.inserttable("test", data)
		r = self.c.query("select * from test").getresult()
		self.assertEqual(r, data)

	def testInserttable4Rows(self):
		data = [(-1, -1, -1L, -1.0, -1.0, -1.0, "-1", "-1-1", "-1"),
			(0, 0, 0L, 0.0, 0.0, 0.0, "0", "0000", "0"),
			(1, 1, 1L, 1.0, 1.0, 1.0, "1", "1111", "1"),
			(2, 2, 2L, 2.0, 2.0, 2.0, "2", "2222", "2")]
		self.c.inserttable("test", data)
		r = self.c.query("select * from test order by 1").getresult()
		self.assertEqual(r, data)

	def testInserttableMultipleRows(self):
		num_rows = 100
		data = [(1, 1, 1L, 1.0, 1.0, 1.0, "1", "1111", "1")] * num_rows
		self.c.inserttable("test", data)
		r = self.c.query("select count(*) from test").getresult()[0][0]
		self.assertEqual(r, num_rows)

	def testInserttableMultipleCalls(self):
		num_rows = 10
		data = [(1, 1, 1L, 1.0, 1.0, 1.0, "1", "1111", "1")]
		for i in range(num_rows):
			self.c.inserttable("test", data)
		r = self.c.query("select count(*) from test").getresult()[0][0]
		self.assertEqual(r, num_rows)

	def testInserttableNullValues(self):
		num_rows = 100
		data = [(None,) * 9]
		self.c.inserttable("test", data)
		r = self.c.query("select * from test").getresult()
		self.assertEqual(r, data)

	def testInserttableMaxValues(self):
		data = [(2**15 - 1, int(2**31 - 1), long(2**31 - 1),
			1.0 + 1.0/32, 1.0 + 1.0/32, 1.0 + 1.0/32,
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
		attributes = ['clear', 'close', 'db', 'dbname',
			'debug', 'delete', 'endcopy', 'error', 'fileno',
			'get', 'get_attnames', 'get_databases', 'get_tables',
			'getline', 'getlo', 'getnotify', 'host',
			'insert', 'inserttable', 'locreate', 'loimport',
			'options', 'pkey', 'port', 'putline', 'query',
			'reopen', 'reset', 'source', 'status',
			'tty', 'update', 'user']
		db_attributes = [a for a in dir(self.db)
			if not a.startswith('_')]
		self.assertEqual(attributes, db_attributes)

	def testAttributeDb(self):
		self.assertEqual(self.db.db.db, self.dbname)

	def testAttributeDbname(self):
		self.assertEqual(self.db.dbname, self.dbname)

	def testAttributeError(self):
		no_error = ''
		self.assertEqual(self.db.error, no_error)
		self.assertEqual(self.db.db.error, no_error)

	def testAttributeHost(self):
		def_host = 'localhost'
		self.assertEqual(self.db.host, def_host)
		self.assertEqual(self.db.db.host, def_host)

	def testAttributeOptions(self):
		no_options = ''
		self.assertEqual(self.db.options, no_options)
		self.assertEqual(self.db.db.options, no_options)

	def testAttributePort(self):
		def_port = 5432
		self.assertEqual(self.db.port, def_port)
		self.assertEqual(self.db.db.port, def_port)

	def testAttributeStatus(self):
		status_ok = 1
		self.assertEqual(self.db.status, status_ok)
		self.assertEqual(self.db.db.status, status_ok)

	def testAttributeTty(self):
		def_tty = ''
		self.assertEqual(self.db.tty, def_tty)
		self.assertEqual(self.db.db.tty, def_tty)

	def testAttributeUser(self):
		def_user = 'Deprecated facility'
		self.assertEqual(self.db.user, def_user)
		self.assertEqual(self.db.db.user, def_user)

	def testMethodQuery(self):
		self.db.query("select 1+1")

	def testMethodEndcopy(self):
		self.db.endcopy()

	def testMethodClose(self):
		self.db.close()
		try:
			self.db.reset()
			fail('Reset should give an error for a closed connection')
		except:
			pass
		self.assertRaises(pg.InternalError, self.db.close)
		self.assertRaises(pg.InternalError, self.db.query, 'select 1')
		self.db = pg.DB(self.dbname)


class TestDBClass(unittest.TestCase):
	""""Test the methods of the DB class wrapped pg connection."""

	# Test database needed: must be run as a DBTestSuite.

	def setUp(self):
		dbname = DBTestSuite.dbname
		self.dbname = dbname
		self.db = pg.DB(dbname)

	def tearDown(self):
		self.db.close()

	def testPkey(self):
		try:
			self.db.query('drop table pkeytest0')
		except pg.ProgrammingError:
			pass
		self.db.query("create table pkeytest0 ("
			"a smallint)")
		try:
			self.db.query('drop table pkeytest1')
		except pg.ProgrammingError:
			pass
		self.db.query("create table pkeytest1 ("
			"b smallint primary key)")
		try:
			self.db.query('drop table pkeytest2')
		except pg.ProgrammingError:
			pass
		self.db.query("create table pkeytest2 ("
			"c smallint, d smallint primary key)")
		try:
			self.db.query('drop table pkeytest3')
		except pg.ProgrammingError:
			pass
		self.db.query("create table pkeytest3 ("
			"e smallint, f smallint, g smallint, "
			"h smallint, i smallint, "
			"primary key (f,h))")
		self.assertRaises(KeyError, self.db.pkey, "pkeytest0")
		self.assertEqual(self.db.pkey("pkeytest1"), "b")
		self.assertEqual(self.db.pkey("pkeytest2"), "d")
		self.assertEqual(self.db.pkey("pkeytest3"), "f")

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
			try:
				self.db.query('drop table ' + t)
			except pg.ProgrammingError:
				pass
			self.db.query("create table %s as select 0" % t)
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

	def testAttnames(self):
		self.assertRaises(pg.ProgrammingError,
			self.db.get_attnames, 'does_not_exist')
		self.assertRaises(pg.ProgrammingError,
			self.db.get_attnames, 'has.too.many.dots')
		for table in ('attnames_test_table', 'test table for attnames'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" ('
				'a smallint, b integer, c bigint, '
				'e decimal, f float, f2 double precision, '
				'x smallint, y smallint, z smallint, '
				'Normal_NaMe smallint, "Special Name" smallint, '
				't text, u char(2), v varchar(2), '
				'primary key (y, u))' % table)
			attributes = self.db.get_attnames(table)
			result = {'a': 'int', 'c': 'int', 'b': 'int',
				'e': 'text', 'f': 'decimal', 'f2': 'decimal',
					'normal_name': 'int', 'Special Name': 'int',
					'u': 'text', 't': 'text', 'v': 'text',
					'y': 'int', 'x': 'int', 'z': 'int', 'oid': 'int' }
			self.assertEqual(attributes, result)

	def testGet(self):
		for table in ('get_test_table', 'test table for get'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" as '
				"select 1 as n, 'x' as t "
				"union select 2, 'y' union select 3, 'z'" % table)
			self.assertRaises(KeyError, self.db.get, table, 2)
			r = self.db.get(table, 2, 'n')
			oid_table = table
			if ' ' in table:
				oid_table = '"%s"' % oid_table
			oid_table = 'oid(public.%s)' % oid_table
			self.assert_(oid_table in r)
			self.assertEqual(type(r[oid_table]), type(1L))
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

	def testInsert(self):
		for table in ('insert_test_table', 'test table for insert'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" (' \
				"i2 smallint, i4 integer, i8 bigint," \
				"d decimal, f4 real, f8 double precision," \
				"v4 varchar(4), c4 char(4), t text)" % table)
			data = dict(i2 = 2**15 - 1,
				i4 = int(2**31 - 1), i8 = long(2**31 - 1),
				d = 1.0 + 1.0/32, f4 = 1.0 + 1.0/32, f8 = 1.0 + 1.0/32,
				v4 = "1234", c4 = "1234", t  = "1234" * 10)
			r = self.db.insert(table, data)
			self.assertEqual(r, data)
			oid_table = table
			if ' ' in table:
				oid_table = '"%s"' % oid_table
			oid_table = 'oid(public.%s)' % oid_table
			self.assert_(oid_table in r)
			self.assertEqual(type(r[oid_table]), type(1L))
			s = self.db.query('select oid,* from "%s"' % table).dictresult()[0]
			s[oid_table] = s['oid']
			del s['oid']
			self.assertEqual(r, s)

	def testUpdate(self):
		for table in ('update_test_table', 'test table for update'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" as '
				"select 1 as n, 'x' as t "
				"union select 2, 'y' union select 3, 'z'" % table)
			r = self.db.get(table, 2, 'n')
			r['t'] = 'u'
			s = self.db.update(table, r)
			self.assertEqual(s, r)
			r = self.db.query('select t from "%s" where n=2' % table
				).getresult()[0][0]
			self.assertEqual(r, 'u')

	def testClear(self):
		for table in ('clear_test_table', 'test table for clear'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" ('
				"n integer, b boolean, d date, t text)" % table)
			r = self.db.clear(table)
			result = {'n': 0, 'b': 'f', 'd': 'now()', 't': ''}
			self.assertEqual(r, result)
			r['a'] = r['n'] = 1
			r['d'] = r['t'] = 'x'
			r['oid'] = 1L
			r = self.db.clear(table, r)
			result = {'a': 1, 'n': 0, 'b': 'f', 'd': 'now()', 't': '', 'oid': 1L}
			self.assertEqual(r, result)

	def testDelete(self):
		for table in ('delete_test_table', 'test table for delete'):
			try:
				self.db.query('drop table "%s"' % table)
			except pg.ProgrammingError:
				pass
			self.db.query('create table "%s" as '
				"select 1 as n, 'x' as t "
				"union select 2, 'y' union select 3, 'z'" % table)
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
		self.db.query("create table s3.t3m as select 1 as m")
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
		c.close()
		c = pg.connect(dbname)
		c.query("create table test (" \
			"i2 smallint, i4 integer, i8 bigint," \
			"d decimal, f4 real, f8 double precision," \
			"v4 varchar(4), c4 char(4), t text)")
		for num_schema in range(5):
			if num_schema:
				schema = "s%d" % num_schema
				c.query("create schema " + schema)
			else:
				schema = "public"
			c.query("create table %s.t as "
				"select 1 as n, %d as d"
				% (schema, num_schema))
			c.query("create table %s.t%d as "
				"select 1 as n, %d as d"
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
