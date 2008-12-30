#!/usr/bin/env python

import sys, unittest
from pg import *

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
dbname = 'unittest'
dbhost = None
dbport = 5432

try:
    from LOCAL_PyGreSQL import *
except ImportError:
    pass

db = DB(dbname, dbhost, dbport)
db.query("SET DATESTYLE TO 'ISO'")
db.query("SET TIME ZONE 'EST5EDT'")
db.query("SET DEFAULT_WITH_OIDS=TRUE")

class utility_test(unittest.TestCase):

    def setUp(self):
        # create test tables if they don't exist
        for t in ('_test1', '_test2'):
            try:
                db.query("CREATE SCHEMA " + t)
            except Exception:
                pass
            try:
                db.query("CREATE TABLE %s._test_schema "
                    "(%s int PRIMARY KEY)" % (t, t))
            except Exception:
                pass
        try:
            db.query("CREATE TABLE _test_schema "
                "(_test int PRIMARY KEY, _i interval, dvar int DEFAULT 999)")
        except Exception:
            pass
        try:
            db.query("CREATE VIEW _test_vschema AS "
                "SELECT _test, 'abc'::text AS _test2  FROM _test_schema")
        except Exception:
            pass

    def test_invalidname(self):
        """Make sure that invalid table names are caught"""

        self.failUnlessRaises(ProgrammingError, db.get_attnames, 'x.y.z')

    def test_schema(self):
        """Does it differentiate the same table name in different schemas"""

        # see if they differentiate the table names properly
        self.assertEqual(
            db.get_attnames('_test_schema'),
            {'_test': 'int', 'oid': 'int', '_i': 'date', 'dvar': 'int'}
        )

        self.assertEqual(
            db.get_attnames('public._test_schema'),
            {'_test': 'int', 'oid': 'int', '_i': 'date', 'dvar': 'int'}
        )

        self.assertEqual(
            db.get_attnames('_test1._test_schema'),
            {'_test1': 'int', 'oid': 'int'}
        )

        self.assertEqual(
            db.get_attnames('_test2._test_schema'),
            {'_test2': 'int', 'oid': 'int'}
        )

    def test_pkey(self):
        self.assertEqual(db.pkey('_test_schema'), '_test')
        self.assertEqual(db.pkey('public._test_schema'), '_test')
        self.assertEqual(db.pkey('_test1._test_schema'), '_test1')

        self.assertEqual(db.pkey('_test_schema',
                {'test1': 'a', 'test2.test3': 'b'}),
                {'public.test1': 'a', 'test2.test3': 'b'})
        self.assertEqual(db.pkey('test1'), 'a')
        self.assertEqual(db.pkey('public.test1'), 'a')

    def test_get(self):
        try:
            db.query("INSERT INTO _test_schema VALUES (1234)")
        except Exception:
            pass # OK if it already exists

        db.get('_test_schema', 1234)
        db.get('_test_schema', 1234, keyname = '_test')
        self.failUnlessRaises(ProgrammingError, db.get, '_test_vschema', 1234)
        db.get('_test_vschema', 1234, keyname = '_test')

    def test_insert(self):
        db.query("DELETE FROM _test_schema")

        d = dict(_test = 1234)
        db.insert('_test_schema', d)
        self.assertEqual(d['dvar'], 999)

        db.insert('_test_schema', _test = 1235)
        self.assertEqual(d['dvar'], 999)

    def test_mixed_case(self):
        try:
            db.query('CREATE TABLE _test_mc ("_Test" int PRIMARY KEY)')
        except Exception:
            pass

        db.query("DELETE FROM _test_mc")
        d = dict(_Test = 1234)
        db.insert('_test_mc', d)

    def test_update(self):
        try:
            db.query("INSERT INTO _test_schema VALUES (1234)")
        except Exception:
            pass # OK if it already exists

        r = db.get('_test_schema', 1234)
        r['dvar'] = 123
        db.update('_test_schema', r)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 123)

        r = db.get('_test_schema', 1234)
        db.update('_test_schema', _test = 1234, dvar = 456)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 456)

        r = db.get('_test_schema', 1234)
        db.update('_test_schema', r, dvar = 456)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 456)

    def test_quote(self):
        _quote = db._quote
        self.assertEqual(_quote(0, 'int'), "0")
        self.assertEqual(_quote(0, 'num'), "0")
        self.assertEqual(_quote('0', 'int'), "0")
        self.assertEqual(_quote('0', 'num'), "0")
        self.assertEqual(_quote(1, 'int'), "1")
        self.assertEqual(_quote(1, 'text'), "'1'")
        self.assertEqual(_quote(1, 'num'), "1")
        self.assertEqual(_quote('1', 'int'), "1")
        self.assertEqual(_quote('1', 'text'), "'1'")
        self.assertEqual(_quote('1', 'num'), "1")
        self.assertEqual(_quote(None, 'int'), "NULL")
        self.assertEqual(_quote(1, 'money'), "'1.00'")
        self.assertEqual(_quote('1', 'money'), "'1.00'")
        self.assertEqual(_quote(1.234, 'money'), "'1.23'")
        self.assertEqual(_quote('1.234', 'money'), "'1.23'")
        self.assertEqual(_quote(0, 'bool'), "'f'")
        self.assertEqual(_quote('', 'bool'), "NULL")
        self.assertEqual(_quote('f', 'bool'), "'f'")
        self.assertEqual(_quote('off', 'bool'), "'f'")
        self.assertEqual(_quote('no', 'bool'), "'f'")
        self.assertEqual(_quote(1, 'bool'), "'t'")
        self.assertEqual(_quote('1', 'bool'), "'t'")
        self.assertEqual(_quote('t', 'bool'), "'t'")
        self.assertEqual(_quote('on', 'bool'), "'t'")
        self.assertEqual(_quote('yes', 'bool'), "'t'")
        self.assertEqual(_quote('true', 'bool'), "'t'")
        self.assertEqual(_quote('y', 'bool'), "'t'")
        self.assertEqual(_quote('', 'date'), "NULL")
        self.assertEqual(_quote('date', 'date'), "'date'")
        self.assertEqual(_quote('', 'text'), "''")
        self.assertEqual(_quote("\\", 'text'), "'\\\\'")
        self.assertEqual(_quote("'", 'text'), "''''")

if __name__ == '__main__':
    unittest.main()
