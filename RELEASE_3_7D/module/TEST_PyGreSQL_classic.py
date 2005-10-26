#!/usr/bin/env python

import sys, unittest
from pg import *

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
dbname = 'unittest'
dbhost = None
dbport = 5432

try: from LOCAL_PyGreSQL import *
except: pass

db = DB(dbname, dbhost, dbport)
db.query("SET DATESTYLE TO 'ISO'")
db.query("SET TIME ZONE 'EST5EDT'")

class utility_test(unittest.TestCase):
    def setUp(self):
        # create test tables if they don't exist
        for t in ('_test1', '_test2'):
            try: db.query("CREATE SCHEMA " + t)
            except: pass
            try: db.query("CREATE TABLE %s._test_schema (%s int PRIMARY KEY)" % (t, t))
            except: pass

        try: db.query("CREATE TABLE _test_schema (_test int PRIMARY KEY)")
        except: pass

    def test_invalidname(self):
        """Make sure that invalid table names are caught"""

        self.failUnlessRaises(ProgrammingError, db.get_attnames, 'x.y.z')

    def test_schema(self):
        """Does it differentiate the same table name in different schemas"""

        # see if they differentiate the table names properly
        self.assertEqual(
            db.get_attnames('_test_schema'),
            {'_test': 'int', 'oid': 'int'}
        )

        self.assertEqual(
            db.get_attnames('public._test_schema'),
            {'_test': 'int', 'oid': 'int'}
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

if __name__ == '__main__':
    unittest.main()
