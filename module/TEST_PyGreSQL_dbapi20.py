#!/usr/bin/env python
# $Id: TEST_PyGreSQL_dbapi20.py,v 1.6 2006-12-26 21:06:49 darcy Exp $

import dbapi20
import unittest
import pgdb
import popen2

# We need a database to test against.  If LOCAL_PyGreSQL.py exists we will
# get our information from that.  Otherwise we use the defaults.
dbname = 'dbapi20_test'
dbhost = None
dbport = 5432
try: from LOCAL_PyGreSQL import *
except: pass

class test_PyGreSQL(dbapi20.DatabaseAPI20Test):
    driver = pgdb
    connect_args = ()
    connect_kw_args = {'dsn': ':' + dbname}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self)

        con = self._connect()
        con.close()
        try:
            con = self._connect()
            con.close()
        except:
            cmd = "psql -c 'create database dbapi20_test'"
            cout,cin = popen2.popen2(cmd)
            cin.close()
            cout.read()

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_nextset(self): pass
    def test_setoutputsize(self): pass

if __name__ == '__main__':
    unittest.main()
