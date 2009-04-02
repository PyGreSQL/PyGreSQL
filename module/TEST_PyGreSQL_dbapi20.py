#!/usr/bin/env python
# $Id: TEST_PyGreSQL_dbapi20.py,v 1.16 2009-04-02 22:18:59 cito Exp $

import dbapi20
import unittest
import pgdb
import popen2

# We need a database to test against.
# If LOCAL_PyGreSQL.py exists we will get our information from that.
# Otherwise we use the defaults.
dbname = 'dbapi20_test'
dbhost = None
dbport = 5432
try:
    from LOCAL_PyGreSQL import *
except ImportError:
    pass


class test_PyGreSQL(dbapi20.DatabaseAPI20Test):

    driver = pgdb
    connect_args = ()
    connect_kw_args = {'dsn': ':' + dbname}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp in case this does something in the future
        dbapi20.DatabaseAPI20Test.setUp(self)
        try:
            con = self._connect()
            con.close()
        except Exception:
            cmd = "psql -c 'create database dbapi20_test'"
            cout, cin = popen2.popen2(cmd)
            cin.close()
            cout.read()

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_row_factory(self):
        class myCursor(pgdb.pgdbCursor):
            def row_factory(self, row):
                d = {}
                for idx, col in enumerate(self.description):
                    d[col[0]] = row[idx]
                return d

        con = self._connect()
        curs = myCursor(con)
        ret = curs.execute("select 1 as a, 2 as b")
        self.assert_(ret is curs, 'execute() should return cursor')
        self.assertEqual(curs.fetchone(), {'a': 1, 'b': 2})

    def test_cursor_iteration(self):
        con = self._connect()
        curs = con.cursor()
        curs.execute("select 1 union select 2 union select 3")
        self.assertEqual([r[0] for r in curs], range(1, 4))

    def test_fetch_2_rows(self):
        Decimal = pgdb.decimal_type()
        values = ['test', 'test', True, 5, 6L, 5.7,
            Decimal('234.234234'), Decimal('75.45'),
            '2008-10-20 15:25:35', 7897234L]
        table = self.table_prefix + 'booze'
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("create table %s ("
                "stringtest varchar,"
                "binarytest bytea,"
                "booltest bool,"
                "integertest int4,"
                "longtest int8,"
                "floattest float8,"
                "numerictest numeric,"
                "moneytest money,"
                "datetimetest timestamp,"
                "rowidtest oid)" % table)
            for i in range(2):
                cur.execute("insert into %s values ("
                    "%%s,%%s,%%s,%%s,%%s,%%s,%%s,"
                    "'%%s'::money,%%s,%%s)" % table, values)
            cur.execute("select * from %s" % table)
            rows = cur.fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], values)
            self.assertEqual(rows[0], rows[1])
        finally:
            con.close()

    def test_nextset(self):
        pass # not implemented

    def test_setoutputsize(self):
        pass # not implemented

    def test_connection_errors(self):
        con = self._connect()
        self.assertEqual(con.Error, pgdb.Error)
        self.assertEqual(con.Warning, pgdb.Warning)
        self.assertEqual(con.InterfaceError, pgdb.InterfaceError)
        self.assertEqual(con.DatabaseError, pgdb.DatabaseError)
        self.assertEqual(con.InternalError, pgdb.InternalError)
        self.assertEqual(con.OperationalError, pgdb.OperationalError)
        self.assertEqual(con.ProgrammingError, pgdb.ProgrammingError)
        self.assertEqual(con.IntegrityError, pgdb.IntegrityError)
        self.assertEqual(con.DataError, pgdb.DataError)
        self.assertEqual(con.NotSupportedError, pgdb.NotSupportedError)

    def test_cursor_connection(self):
        con = self._connect()
        curs = con.cursor()
        self.assertEqual(curs.connection, con)

    def test_pgdb_type(self):
        self.assertEqual(pgdb.STRING, pgdb.STRING)
        self.assertNotEqual(pgdb.STRING, pgdb.INTEGER)
        self.assertNotEqual(pgdb.STRING, pgdb.BOOL)
        self.assertNotEqual(pgdb.BOOL, pgdb.INTEGER)
        self.assertEqual(pgdb.INTEGER, pgdb.INTEGER)
        self.assertNotEqual(pgdb.INTEGER, pgdb.NUMBER)
        self.assertEqual('char', pgdb.STRING)
        self.assertEqual('varchar', pgdb.STRING)
        self.assertEqual('text', pgdb.STRING)
        self.assertNotEqual('numeric', pgdb.STRING)
        self.assertEqual('numeric', pgdb.NUMERIC)
        self.assertEqual('numeric', pgdb.NUMBER)
        self.assertEqual('int4', pgdb.NUMBER)
        self.assertNotEqual('int4', pgdb.NUMERIC)
        self.assertEqual('int2', pgdb.SMALLINT)
        self.assertNotEqual('int4', pgdb.SMALLINT)
        self.assertEqual('int2', pgdb.INTEGER)
        self.assertEqual('int4', pgdb.INTEGER)
        self.assertEqual('int8', pgdb.INTEGER)
        self.assertNotEqual('int4', pgdb.LONG)
        self.assertEqual('int8', pgdb.LONG)
        self.assert_('char' in pgdb.STRING)
        self.assert_(pgdb.NUMERIC <= pgdb.NUMBER)
        self.assert_(pgdb.NUMBER >= pgdb.INTEGER)
        self.assert_(pgdb.TIME <= pgdb.DATETIME)
        self.assert_(pgdb.DATETIME >= pgdb.DATE)


if __name__ == '__main__':
    unittest.main()
