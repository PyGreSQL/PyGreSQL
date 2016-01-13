#! /usr/bin/python

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest

import sys
from time import sleep
from threading import Thread

from pg import *

# check whether the "with" statement is supported
no_with = sys.version_info[:2] < (2, 5)

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
    db.query("SET DEFAULT_WITH_OIDS=FALSE")
    db.query("SET STANDARD_CONFORMING_STRINGS=FALSE")
    return db


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
            {'_test': 'int', '_i': 'date', 'dvar': 'int'}
        )
        self.assertEqual(
            db.get_attnames('public._test_schema'),
            {'_test': 'int', '_i': 'date', 'dvar': 'int'}
        )
        self.assertEqual(
            db.get_attnames('_test1._test_schema'),
            {'_test1': 'int'}
        )
        self.assertEqual(
            db.get_attnames('_test2._test_schema'),
            {'_test2': 'int'}
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

    @unittest.skipIf(no_with, 'context managers not supported')
    def test_context_manager(self):
        db = opendb()
        t = '_test_schema'
        d = dict(_test=1235)
        # wrap "with" statements to avoid SyntaxError in Python < 2.5
        exec """from __future__ import with_statement\nif True:
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
            db.insert(t, d)\n"""
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
        self.assertIn('dvar', r)
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
        self.assertEqual(q(False, 'date'), "NULL")
        self.assertEqual(q(0, 'date'), "NULL")
        self.assertEqual(q('some_date', 'date'), "'some_date'")
        self.assertEqual(q('current_timestamp', 'date'), "current_timestamp")
        self.assertEqual(q('', 'text'), "''")
        self.assertEqual(q("'", 'text'), "''''")
        self.assertEqual(q("\\", 'text'), "'\\\\'")

    def notify_callback(self, arg_dict):
        if arg_dict:
            arg_dict['called'] = True
        else:
            self.notify_timeout = True

    def test_notify(self, options=None):
        if not options:
            options = {}
        run_as_method = options.get('run_as_method')
        call_notify = options.get('call_notify')
        two_payloads = options.get('two_payloads')
        db = opendb()
        if db.server_version < 90000:  # PostgreSQL < 9.0
            self.skipTest('Notify with payload not supported')
        # Get function under test, can be standalone or DB method.
        if run_as_method:
            fut = db.notification_handler
        else:
            # functools.partial is not available in Python < 2.5
            fut = lambda *args: NotificationHandler(db, *args)
        arg_dict = dict(event=None, called=False)
        self.notify_timeout = False
        # Listen for 'event_1'.
        target = fut('event_1', self.notify_callback, arg_dict, 5)
        thread = Thread(None, target)
        thread.start()
        try:
            # Wait until the thread has started.
            for n in range(500):
                if target.listening:
                    break
                sleep(0.01)
            self.assertTrue(target.listening)
            self.assertTrue(thread.isAlive())
            # Open another connection for sending notifications.
            db2 = opendb()
            # Generate notification from the other connection.
            if two_payloads:
                db2.begin()
            if call_notify:
                if two_payloads:
                    target.notify(db2, payload='payload 0')
                target.notify(db2, payload='payload 1')
            else:
                if two_payloads:
                    db2.query("notify event_1, 'payload 0'")
                db2.query("notify event_1, 'payload 1'")
            if two_payloads:
                db2.commit()
            # Wait until the notification has been caught.
            for n in range(500):
                if arg_dict['called'] or self.notify_timeout:
                    break
                sleep(0.01)
            # Check that callback has been invoked.
            self.assertTrue(arg_dict['called'])
            self.assertEqual(arg_dict['event'], 'event_1')
            self.assertEqual(arg_dict['extra'], 'payload 1')
            self.assertTrue(isinstance(arg_dict['pid'], int))
            self.assertFalse(self.notify_timeout)
            arg_dict['called'] = False
            self.assertTrue(thread.isAlive())
            # Generate stop notification.
            if call_notify:
                target.notify(db2, stop=True, payload='payload 2')
            else:
                db2.query("notify stop_event_1, 'payload 2'")
            db2.close()
            # Wait until the notification has been caught.
            for n in range(500):
                if arg_dict['called'] or self.notify_timeout:
                    break
                sleep(0.01)
            # Check that callback has been invoked.
            self.assertTrue(arg_dict['called'])
            self.assertEqual(arg_dict['event'], 'stop_event_1')
            self.assertEqual(arg_dict['extra'], 'payload 2')
            self.assertTrue(isinstance(arg_dict['pid'], int))
            self.assertFalse(self.notify_timeout)
            thread.join(5)
            self.assertFalse(thread.isAlive())
            self.assertFalse(target.listening)
        finally:
            target.close()
            if thread.isAlive():
                thread.join(5)

    def test_notify_other_options(self):
        for run_as_method in False, True:
            for call_notify in False, True:
                for two_payloads in False, True:
                    options = dict(
                        run_as_method=run_as_method,
                        call_notify=call_notify,
                        two_payloads=two_payloads)
                    if True in options.values():
                        self.test_notify(options)

    def test_notify_timeout(self):
        for run_as_method in False, True:
            db = opendb()
            if db.server_version < 90000:  # PostgreSQL < 9.0
                self.skipTest('Notify with payload not supported')
            # Get function under test, can be standalone or DB method.
            if run_as_method:
                fut = db.notification_handler
            else:
                fut = lambda *args: NotificationHandler(db, *args)
            arg_dict = dict(event=None, called=False)
            self.notify_timeout = False
            # Listen for 'event_1' with timeout of 10ms.
            target = fut('event_1', self.notify_callback, arg_dict, 0.01)
            thread = Thread(None, target)
            thread.start()
            # Sleep 20ms, long enough to time out.
            sleep(0.02)
            # Verify that we've indeed timed out.
            self.assertFalse(arg_dict.get('called'))
            self.assertTrue(self.notify_timeout)
            self.assertFalse(thread.isAlive())
            self.assertFalse(target.listening)
            target.close()


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '-l':
        print '\n'.join(unittest.getTestCaseNames(UtilityTest, 'test_'))
        sys.exit(0)

    test_list = [name for name in sys.argv[1:] if not name.startswith('-')]
    if not test_list:
        test_list = unittest.getTestCaseNames(UtilityTest, 'test_')

    suite = unittest.TestSuite()
    for test_name in test_list:
        try:
            suite.addTest(UtilityTest(test_name))
        except Exception:
            print "\n ERROR: %s.\n" % sys.exc_value
            sys.exit(1)

    verbosity = '-v' in sys.argv[1:] and 2 or 1
    failfast = '-l' in sys.argv[1:]
    runner = unittest.TextTestRunner(verbosity=verbosity, failfast=failfast)
    rc = runner.run(suite)
    sys.exit((rc.errors or rc.failures) and 1 or 0)
