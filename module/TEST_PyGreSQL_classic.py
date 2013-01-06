#!/usr/bin/env python

from __future__ import with_statement

import sys, thread, time
import unittest
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

def opendb():
    db = DB(dbname, dbhost, dbport)
    db.query("SET DATESTYLE TO 'ISO'")
    db.query("SET TIME ZONE 'EST5EDT'")
    db.query("SET DEFAULT_WITH_OIDS=TRUE")
    db.query("SET STANDARD_CONFORMING_STRINGS=FALSE")
    return db

def cb1(arg_dict):
    global cb1_return
    if arg_dict is None:
        cb1_return = 'timed out'
    else:
        cb1_return = arg_dict

class UtilityTest(unittest.TestCase):

    def setUp(self):
        """Setup test tables or empty them if they already exist."""
        db = opendb()

        for t in ('_test1', '_test2'):
            try:
                db.query("CREATE SCHEMA " + t)
            except Error:
                pass
            try:
                db.query("CREATE TABLE %s._test_schema "
                    "(%s int PRIMARY KEY)" % (t, t))
            except Error:
                db.query("DELETE FROM %s._test_schema" % t)
        try:
            db.query("CREATE TABLE _test_schema "
                "(_test int PRIMARY KEY, _i interval, dvar int DEFAULT 999)")
        except Error:
            db.query("DELETE FROM _test_schema")
        try:
            db.query("CREATE VIEW _test_vschema AS "
                "SELECT _test, 'abc'::text AS _test2  FROM _test_schema")
        except Error:
            pass

    def test_invalidname(self):
        """Make sure that invalid table names are caught"""
        db = opendb()
        self.failUnlessRaises(ProgrammingError, db.get_attnames, 'x.y.z')

    def test_schema(self):
        """Does it differentiate the same table name in different schemas"""
        db = opendb()
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
        db = opendb()
        self.assertEqual(db.pkey('_test_schema'), '_test')
        self.assertEqual(db.pkey('public._test_schema'), '_test')
        self.assertEqual(db.pkey('_test1._test_schema'), '_test1')

        self.assertEqual(db.pkey('_test_schema',
                {'test1': 'a', 'test2.test3': 'b'}),
                {'public.test1': 'a', 'test2.test3': 'b'})
        self.assertEqual(db.pkey('test1'), 'a')
        self.assertEqual(db.pkey('public.test1'), 'a')

    def test_get(self):
        db = opendb()
        db.query("INSERT INTO _test_schema VALUES (1234)")
        db.get('_test_schema', 1234)
        db.get('_test_schema', 1234, keyname='_test')
        self.failUnlessRaises(ProgrammingError, db.get, '_test_vschema', 1234)
        db.get('_test_vschema', 1234, keyname='_test')

    def test_params(self):
        db = opendb()
        db.query("INSERT INTO _test_schema VALUES ($1, $2, $3)", 12, None, 34)
        d = db.get('_test_schema', 12)
        self.assertEqual(d['dvar'], 34)

    def test_insert(self):
        db = opendb()
        d = dict(_test=1234)
        db.insert('_test_schema', d)
        self.assertEqual(d['dvar'], 999)
        db.insert('_test_schema', _test=1235)
        self.assertEqual(d['dvar'], 999)

    def test_context_manager(self):
        db = opendb()
        t = '_test_schema'
        d = dict(_test=1235)
        with db:
            db.insert(t, d)
            d['_test'] += 1
            db.insert(t, d)
        try:
            with db:
                d['_test'] += 1
                db.insert(t, d)
                db.insert(t, d)
        except ProgrammingError:
            pass
        with db:
            d['_test'] += 1
            db.insert(t, d)
            d['_test'] += 1
            db.insert(t, d)
        self.assertTrue(db.get(t, 1235))
        self.assertTrue(db.get(t, 1236))
        self.assertRaises(DatabaseError, db.get, t, 1237)
        self.assertTrue(db.get(t, 1238))
        self.assertTrue(db.get(t, 1239))

    def test_sqlstate(self):
        db = opendb()
        db.query("INSERT INTO _test_schema VALUES (1234)")
        try:
            db.query("INSERT INTO _test_schema VALUES (1234)")
        except DatabaseError, error:
            # currently PyGreSQL does not support IntegrityError
            self.assert_(isinstance(error, ProgrammingError))
            # the SQLSTATE error code for unique violation is 23505
            self.assertEqual(error.sqlstate, '23505')

    def test_mixed_case(self):
        db = opendb()
        try:
            db.query('CREATE TABLE _test_mc ("_Test" int PRIMARY KEY)')
        except Error:
            db.query("DELETE FROM _test_mc")
        d = dict(_Test=1234)
        db.insert('_test_mc', d)

    def test_update(self):
        db = opendb()
        db.query("INSERT INTO _test_schema VALUES (1234)")

        r = db.get('_test_schema', 1234)
        r['dvar'] = 123
        db.update('_test_schema', r)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 123)

        r = db.get('_test_schema', 1234)
        db.update('_test_schema', _test=1234, dvar=456)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 456)

        r = db.get('_test_schema', 1234)
        db.update('_test_schema', r, dvar=456)
        r = db.get('_test_schema', 1234)
        self.assertEqual(r['dvar'], 456)

    def test_quote(self):
        db = opendb()
        q = db._quote
        self.assertEqual(q(0, 'int'), "0")
        self.assertEqual(q(0, 'num'), "0")
        self.assertEqual(q('0', 'int'), "0")
        self.assertEqual(q('0', 'num'), "0")
        self.assertEqual(q(1, 'int'), "1")
        self.assertEqual(q(1, 'text'), "'1'")
        self.assertEqual(q(1, 'num'), "1")
        self.assertEqual(q('1', 'int'), "1")
        self.assertEqual(q('1', 'text'), "'1'")
        self.assertEqual(q('1', 'num'), "1")
        self.assertEqual(q(None, 'int'), "NULL")
        self.assertEqual(q(1, 'money'), "1")
        self.assertEqual(q('1', 'money'), "1")
        self.assertEqual(q(1.234, 'money'), "1.234")
        self.assertEqual(q('1.234', 'money'), "1.234")
        self.assertEqual(q(0, 'money'), "0")
        self.assertEqual(q(0.00, 'money'), "0.0")
        self.assertEqual(q(Decimal('0.00'), 'money'), "0.00")
        self.assertEqual(q(None, 'money'), "NULL")
        self.assertEqual(q('', 'money'), "NULL")
        self.assertEqual(q(0, 'bool'), "'f'")
        self.assertEqual(q('', 'bool'), "NULL")
        self.assertEqual(q('f', 'bool'), "'f'")
        self.assertEqual(q('off', 'bool'), "'f'")
        self.assertEqual(q('no', 'bool'), "'f'")
        self.assertEqual(q(1, 'bool'), "'t'")
        self.assertEqual(q(9999, 'bool'), "'t'")
        self.assertEqual(q(-9999, 'bool'), "'t'")
        self.assertEqual(q('1', 'bool'), "'t'")
        self.assertEqual(q('t', 'bool'), "'t'")
        self.assertEqual(q('on', 'bool'), "'t'")
        self.assertEqual(q('yes', 'bool'), "'t'")
        self.assertEqual(q('true', 'bool'), "'t'")
        self.assertEqual(q('y', 'bool'), "'t'")
        self.assertEqual(q('', 'date'), "NULL")
        self.assertEqual(q('some_date', 'date'), "'some_date'")
        self.assertEqual(q('current_timestamp', 'date'), "current_timestamp")
        self.assertEqual(q('', 'text'), "''")
        self.assertEqual(q("'", 'text'), "''''")
        self.assertEqual(q("\\", 'text'), "'\\\\'")

    # note that notify can be created as part of the DB class or
    # independently.

    def test_notify_DB(self):
        global cb1_return

        db = opendb()
        db2 = opendb()
        # Listen for 'event_1'
        pgn = db2.pgnotify('event_1', cb1)
        thread.start_new_thread(pgn, ())
        time.sleep(1)
        # Generate notification from the other connection.
        db.query('notify event_1')
        time.sleep(1)
        # Check that callback has been invoked.
        self.assertEquals(cb1_return['event'], 'event_1')

    def test_notify_timeout_DB(self):
        db = opendb()
        db2 = opendb()
        global cb1_return
        # Listen for 'event_1'
        pgn = db2.pgnotify('event_1', cb1, {}, 1)
        thread.start_new_thread(pgn, ())
        # Sleep long enough to time out.
        time.sleep(2)
        # Verify that we've indeed timed out.
        self.assertEquals(cb1_return, 'timed out')

    def test_notify(self):
        db = opendb()
        db2 = opendb()
        global cb1_return
        # Listen for 'event_1'
        pgn = pgnotify(db2, 'event_1', cb1)
        thread.start_new_thread(pgn, ())
        time.sleep(1)
        # Generate notification from the other connection.
        db.query('notify event_1')
        time.sleep(1)
        # Check that callback has been invoked.
        self.assertEquals(cb1_return['event'], 'event_1')

    def test_notify_timeout(self):
        db = opendb()
        db2 = opendb()
        global cb1_return
        # Listen for 'event_1'
        pgn = pgnotify(db2, 'event_1', cb1, {}, 1)
        thread.start_new_thread(pgn, ())
        # Sleep long enough to time out.
        time.sleep(2)
        # Verify that we've indeed timed out.
        self.assertEquals(cb1_return, 'timed out')

if __name__ == '__main__':
    suite = unittest.TestSuite()

    if len(sys.argv) > 1: test_list = sys.argv[1:]
    else: test_list = unittest.getTestCaseNames(UtilityTest, 'test_')

    if len(sys.argv) == 2 and sys.argv[1] == '-l':
        print '\n'.join(unittest.getTestCaseNames(UtilityTest, 'test_'))
        sys.exit(1)

    for test_name in test_list:
        try:
            suite.addTest(UtilityTest(test_name))
        except:
            print "\n ERROR: %s.\n" % sys.exc_value
            sys.exit(1)

    rc = unittest.TextTestRunner(verbosity=1).run(suite)
    sys.exit(len(rc.errors+rc.failures) != 0)

