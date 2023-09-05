#!/usr/bin/python

"""Python DB API 2.0 driver compliance unit test suite.

This software is Public Domain and may be used without restrictions.

Some modernization of the code has been done by the PyGreSQL team.
"""

from __future__ import annotations

import time
import unittest
from contextlib import suppress
from typing import Any, ClassVar

__version__ = '1.15.0'

class DatabaseAPI20Test(unittest.TestCase):
    """Test a database self.driver for DB API 2.0 compatibility.

    This implementation tests Gadfly, but the TestCase
    is structured so that other self.drivers can subclass this
    test case to ensure compliance with the DB-API. It is
    expected that this TestCase may be expanded i  qn the future
    if ambiguities or edge conditions are discovered.

    The 'Optional Extensions' are not yet being tested.

    self.drivers should subclass this test, overriding setUp, tearDown,
    self.driver, connect_args and connect_kw_args. Class specification
    should be as follows:

    import dbapi20
    class mytest(dbapi20.DatabaseAPI20Test):
        [...]

    Don't 'import DatabaseAPI20Test from dbapi20', or you will
    confuse the unit tester - just 'import dbapi20'.
    """

    # The self.driver module. This should be the module where the 'connect'
    # method is to be found
    driver: Any = None
    connect_args: tuple = ()  # List of arguments to pass to connect
    connect_kw_args: ClassVar[dict[str, Any]] = {}  # Keyword arguments
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = f'create table {table_prefix}booze (name varchar(20))'
    ddl2 = (f'create table {table_prefix}barflys (name varchar(20),'
            ' drink varchar(30))')
    xddl1 = f'drop table {table_prefix}booze'
    xddl2 = f'drop table {table_prefix}barflys'
    insert = 'insert'

    lowerfunc = 'lower'  # Name of stored procedure to convert str to lowercase

    # Some drivers may need to override these helpers, for example adding
    # a 'commit' after the execute.
    def execute_ddl1(self, cursor):
        cursor.execute(self.ddl1)

    def execute_ddl2(self, cursor):
        cursor.execute(self.ddl2)

    def setUp(self):
        """Set up test fixture.

        self.drivers should override this method to perform required setup
        if any is necessary, such as creating the database.
        """
        pass

    def tearDown(self):
        """Tear down test fixture.

        self.drivers should override this method to perform required cleanup
        if any is necessary, such as deleting the test database.
        The default drops the tables that may be created.
        """
        try:
            con = self._connect()
            try:
                cur = con.cursor()
                for ddl in (self.xddl1, self.xddl2):
                    try:
                        cur.execute(ddl)
                        con.commit()
                    except self.driver.Error:
                        # Assume table didn't exist. Other tests will check if
                        # execute is busted.
                        pass
            finally:
                con.close()
        except Exception:
            pass

    def _connect(self):
        try:
            con = self.driver.connect(
                *self.connect_args, **self.connect_kw_args)
        except AttributeError:
            self.fail("No connect method found in self.driver module")
        if not isinstance(con, self.driver.Connection):
            self.fail("The connect method does not return a Connection")
        return con

    def test_connect(self):
        con = self._connect()
        con.close()

    def test_apilevel(self):
        try:
            # Must exist
            apilevel = self.driver.apilevel
            # Must equal 2.0
            self.assertEqual(apilevel, '2.0')
        except AttributeError:
            self.fail("Driver doesn't define apilevel")

    def test_threadsafety(self):
        try:
            # Must exist
            threadsafety = self.driver.threadsafety
            # Must be a valid value
            self.assertIn(threadsafety, (0, 1, 2, 3))
        except AttributeError:
            self.fail("Driver doesn't define threadsafety")

    def test_paramstyle(self):
        try:
            # Must exist
            paramstyle = self.driver.paramstyle
            # Must be a valid value
            self.assertIn(paramstyle, (
                'qmark', 'numeric', 'named', 'format', 'pyformat'))
        except AttributeError:
            self.fail("Driver doesn't define paramstyle")

    def test_exceptions(self):
        # Make sure required exceptions exist, and are in the
        # defined hierarchy.
        sub = issubclass
        self.assertTrue(sub(self.driver.Warning, Exception))
        self.assertTrue(sub(self.driver.Error, Exception))

        self.assertTrue(sub(self.driver.InterfaceError, self.driver.Error))
        self.assertTrue(sub(self.driver.DatabaseError, self.driver.Error))
        self.assertTrue(sub(self.driver.OperationalError, self.driver.Error))
        self.assertTrue(sub(self.driver.IntegrityError, self.driver.Error))
        self.assertTrue(sub(self.driver.InternalError, self.driver.Error))
        self.assertTrue(sub(self.driver.ProgrammingError, self.driver.Error))
        self.assertTrue(sub(self.driver.NotSupportedError, self.driver.Error))

    def test_exceptions_as_connection_attributes(self):
        # OPTIONAL EXTENSION
        # Test for the optional DB API 2.0 extension, where the exceptions
        # are exposed as attributes on the Connection object
        # I figure this optional extension will be implemented by any
        # driver author who is using this test suite, so it is enabled
        # by default.
        con = self._connect()
        drv = self.driver
        self.assertIs(con.Warning, drv.Warning)
        self.assertIs(con.Error, drv.Error)
        self.assertIs(con.InterfaceError, drv.InterfaceError)
        self.assertIs(con.DatabaseError, drv.DatabaseError)
        self.assertIs(con.OperationalError, drv.OperationalError)
        self.assertIs(con.IntegrityError, drv.IntegrityError)
        self.assertIs(con.InternalError, drv.InternalError)
        self.assertIs(con.ProgrammingError, drv.ProgrammingError)
        self.assertIs(con.NotSupportedError, drv.NotSupportedError)

    def test_commit(self):
        con = self._connect()
        try:
            # Commit must work, even if it doesn't do anything
            con.commit()
        finally:
            con.close()

    def test_rollback(self):
        con = self._connect()
        # If rollback is defined, it should either work or throw
        # the documented exception
        if hasattr(con, 'rollback'):
            with suppress(self.driver.NotSupportedError):
                # noinspection PyCallingNonCallable
                con.rollback()

    def test_cursor(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertIsNotNone(cur)
        finally:
            con.close()

    def test_cursor_isolation(self):
        con = self._connect()
        try:
            # Make sure cursors created from the same connection have
            # the documented transaction isolation level
            cur1 = con.cursor()
            cur2 = con.cursor()
            self.execute_ddl1(cur1)
            cur1.execute(f"{self.insert} into {self.table_prefix}booze"
                         " values ('Victoria Bitter')")
            cur2.execute(f"select name from {self.table_prefix}booze")
            booze = cur2.fetchall()
            self.assertEqual(len(booze), 1)
            self.assertEqual(len(booze[0]), 1)
            self.assertEqual(booze[0][0], 'Victoria Bitter')
        finally:
            con.close()

    def test_description(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.execute_ddl1(cur)
            self.assertIsNone(
                cur.description,
                'cursor.description should be none after executing a'
                ' statement that can return no rows (such as DDL)')
            cur.execute(f'select name from {self.table_prefix}booze')
            self.assertEqual(
                len(cur.description), 1,
                'cursor.description describes too many columns')
            self.assertEqual(
                len(cur.description[0]), 7,
                'cursor.description[x] tuples must have 7 elements')
            self.assertEqual(
                cur.description[0][0].lower(), 'name',
                'cursor.description[x][0] must return column name')
            self.assertEqual(
                cur.description[0][1], self.driver.STRING,
                'cursor.description[x][1] must return column type.'
                f' Got: {cur.description[0][1]!r}')

            # Make sure self.description gets reset
            self.execute_ddl2(cur)
            self.assertIsNone(
                cur.description,
                'cursor.description not being set to None when executing'
                ' no-result statements (eg. DDL)')
        finally:
            con.close()

    def test_rowcount(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.execute_ddl1(cur)
            self.assertIn(
                cur.rowcount, (-1, 0),  # Bug #543885
                'cursor.rowcount should be -1 or 0 after executing no-result'
                ' statements')
            cur.execute(f"{self.insert} into {self.table_prefix}booze"
                        " values ('Victoria Bitter')")
            self.assertIn(
                cur.rowcount, (-1, 1),
                'cursor.rowcount should == number or rows inserted, or'
                ' set to -1 after executing an insert statement')
            cur.execute(f"select name from {self.table_prefix}booze")
            self.assertIn(
                cur.rowcount, (-1, 1),
                'cursor.rowcount should == number of rows returned, or'
                ' set to -1 after executing a select statement')
            self.execute_ddl2(cur)
            self.assertIn(
                cur.rowcount, (-1, 0),  # Bug #543885
                'cursor.rowcount should be -1 or 0 after executing no-result'
                ' statements')
        finally:
            con.close()

    lower_func = 'lower'

    def test_callproc(self):
        con = self._connect()
        try:
            cur = con.cursor()
            if self.lower_func and hasattr(cur, 'callproc'):
                # noinspection PyCallingNonCallable
                r = cur.callproc(self.lower_func, ('FOO',))
                self.assertEqual(len(r), 1)
                self.assertEqual(r[0], 'FOO')
                r = cur.fetchall()
                self.assertEqual(len(r), 1, 'callproc produced no result set')
                self.assertEqual(
                    len(r[0]), 1, 'callproc produced invalid result set')
                self.assertEqual(
                    r[0][0], 'foo', 'callproc produced invalid results')
        finally:
            con.close()

    def test_close(self):
        con = self._connect()
        try:
            cur = con.cursor()
        finally:
            con.close()

        # cursor.execute should raise an Error if called after connection
        # closed
        self.assertRaises(self.driver.Error, self.execute_ddl1, cur)

        # connection.commit should raise an Error if called after connection'
        # closed.'
        self.assertRaises(self.driver.Error, con.commit)

    def test_non_idempotent_close(self):
        con = self._connect()
        con.close()
        # connection.close should raise an Error if called more than once
        # (the usefulness of this test and this feature is questionable)
        self.assertRaises(self.driver.Error, con.close)

    def test_execute(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self._paraminsert(cur)
        finally:
            con.close()

    def _paraminsert(self, cur):
        self.execute_ddl2(cur)
        table_prefix = self.table_prefix
        insert = f"{self.insert} into {table_prefix}barflys values"
        cur.execute(
            f"{insert} ('Victoria Bitter',"
            " 'thi%s :may ca%(u)se? troub:1e')")
        self.assertIn(cur.rowcount, (-1, 1))

        if self.driver.paramstyle == 'qmark':
            cur.execute(
                f"{insert} (?, 'thi%s :may ca%(u)se? troub:1e')",
                ("Cooper's",))
        elif self.driver.paramstyle == 'numeric':
            cur.execute(
                f"{insert} (:1, 'thi%s :may ca%(u)se? troub:1e')",
                ("Cooper's",))
        elif self.driver.paramstyle == 'named':
            cur.execute(
                f"{insert} (:beer, 'thi%s :may ca%(u)se? troub:1e')",
                {'beer': "Cooper's"})
        elif self.driver.paramstyle == 'format':
            cur.execute(
                f"{insert} (%s, 'thi%%s :may ca%%(u)se? troub:1e')",
                ("Cooper's",))
        elif self.driver.paramstyle == 'pyformat':
            cur.execute(
                f"{insert} (%(beer)s, 'thi%%s :may ca%%(u)se? troub:1e')",
                {'beer': "Cooper's"})
        else:
            self.fail('Invalid paramstyle')
        self.assertIn(cur.rowcount, (-1, 1))

        cur.execute(f'select name, drink from {table_prefix}barflys')
        res = cur.fetchall()
        self.assertEqual(len(res), 2, 'cursor.fetchall returned too few rows')
        beers = [res[0][0], res[1][0]]
        beers.sort()
        self.assertEqual(
            beers[0], "Cooper's",
            'cursor.fetchall retrieved incorrect data, or data inserted'
            ' incorrectly')
        self.assertEqual(
            beers[1], "Victoria Bitter",
            'cursor.fetchall retrieved incorrect data, or data inserted'
            ' incorrectly')
        trouble = "thi%s :may ca%(u)se? troub:1e"
        self.assertEqual(
            res[0][1], trouble,
            'cursor.fetchall retrieved incorrect data, or data inserted'
            f' incorrectly. Got: {res[0][1]!r}, Expected: {trouble!r}')
        self.assertEqual(
            res[1][1], trouble,
            'cursor.fetchall retrieved incorrect data, or data inserted'
            f' incorrectly. Got: {res[1][1]!r}, Expected: {trouble!r}')

    def test_executemany(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.execute_ddl1(cur)
            table_prefix = self.table_prefix
            insert = f'{self.insert} into {table_prefix}booze values'
            largs = [("Cooper's",), ("Boag's",)]
            margs = [{'beer': "Cooper's"}, {'beer': "Boag's"}]
            if self.driver.paramstyle == 'qmark':
                cur.executemany(f'{insert} (?)', largs)
            elif self.driver.paramstyle == 'numeric':
                cur.executemany(f'{insert} (:1)', largs)
            elif self.driver.paramstyle == 'named':
                cur.executemany(f'{insert} (:beer)', margs)
            elif self.driver.paramstyle == 'format':
                cur.executemany(f'{insert} (%s)', largs)
            elif self.driver.paramstyle == 'pyformat':
                cur.executemany(f'{insert} (%(beer)s)', margs)
            else:
                self.fail('Unknown paramstyle')
            self.assertIn(
                cur.rowcount, (-1, 2),
                'insert using cursor.executemany set cursor.rowcount to'
                f' incorrect value {cur.rowcount!r}')
            cur.execute(f'select name from {table_prefix}booze')
            res = cur.fetchall()
            self.assertEqual(
                len(res), 2,
                'cursor.fetchall retrieved incorrect number of rows')
            beers = [res[0][0], res[1][0]]
            beers.sort()
            self.assertEqual(beers[0], "Boag's", 'incorrect data retrieved')
            self.assertEqual(beers[1], "Cooper's", 'incorrect data retrieved')
        finally:
            con.close()

    def test_fetchone(self):
        con = self._connect()
        try:
            cur = con.cursor()

            # cursor.fetchone should raise an Error if called before
            # executing a select-type query
            self.assertRaises(self.driver.Error, cur.fetchone)

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannot return rows
            self.execute_ddl1(cur)
            self.assertRaises(self.driver.Error, cur.fetchone)

            cur.execute(f'select name from {self.table_prefix}booze')
            self.assertIsNone(
                cur.fetchone(),
                'cursor.fetchone should return None if a query retrieves'
                ' no rows')
            self.assertIn(cur.rowcount, (-1, 0))

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannot return rows
            cur.execute(
                f"{self.insert} into {self.table_prefix}booze"
                " values ('Victoria Bitter')")
            self.assertRaises(self.driver.Error, cur.fetchone)

            cur.execute(f'select name from {self.table_prefix}booze')
            r = cur.fetchone()
            self.assertEqual(
                len(r), 1,
                'cursor.fetchone should have retrieved a single row')
            self.assertEqual(
                r[0], 'Victoria Bitter',
                'cursor.fetchone retrieved incorrect data')
            self.assertIsNone(
                cur.fetchone(),
                'cursor.fetchone should return None if no more rows available')
            self.assertIn(cur.rowcount, (-1, 1))
        finally:
            con.close()

    def test_next(self):
        """Test extension for getting the next row."""
        con = self._connect()
        try:
            cur = con.cursor()
            if not hasattr(cur, 'next'):
                return

            # cursor.next should raise an Error if called before
            # executing a select-type query
            self.assertRaises(self.driver.Error, cur.next)

            # cursor.next should raise an Error if called after
            # executing a query that cannot return rows
            self.execute_ddl1(cur)
            self.assertRaises(self.driver.Error, cur.next)

            # cursor.next should return None if a query retrieves no rows
            cur.execute(f'select name from {self.table_prefix}booze')
            self.assertRaises(StopIteration, cur.next)
            self.assertIn(cur.rowcount, (-1, 0))

            # cursor.next should raise an Error if called after
            # executing a query that cannot return rows
            cur.execute(f"{self.insert} into {self.table_prefix}booze"
                        " values ('Victoria Bitter')")
            self.assertRaises(self.driver.Error, cur.next)

            cur.execute(f'select name from {self.table_prefix}booze')
            r = cur.next()
            self.assertEqual(
                len(r), 1,
                'cursor.fetchone should have retrieved a single row')
            self.assertEqual(
                r[0], 'Victoria Bitter',
                'cursor.next retrieved incorrect data')
            # cursor.next should raise StopIteration if no more rows available
            self.assertRaises(StopIteration, cur.next)
            self.assertIn(cur.rowcount, (-1, 1))
        finally:
            con.close()

    samples = (
        'Carlton Cold',
        'Carlton Draft',
        'Mountain Goat',
        'Redback',
        'Victoria Bitter',
        'XXXX'
    )

    def _populate(self):
        """Return a list of SQL commands to setup the DB for fetching tests."""
        populate = [
            f"{self.insert} into {self.table_prefix}booze values ('{s}')"
            for s in self.samples]
        return populate

    def test_fetchmany(self):
        con = self._connect()
        try:
            cur = con.cursor()

            # cursor.fetchmany should raise an Error if called without
            # issuing a query
            self.assertRaises(self.driver.Error, cur.fetchmany, 4)

            self.execute_ddl1(cur)
            for sql in self._populate():
                cur.execute(sql)

            cur.execute(f'select name from {self.table_prefix}booze')
            r = cur.fetchmany()
            self.assertEqual(
                len(r), 1,
                'cursor.fetchmany retrieved incorrect number of rows,'
                ' default of arraysize is one.')
            cur.arraysize = 10
            r = cur.fetchmany(3)  # Should get 3 rows
            self.assertEqual(
                len(r), 3,
                'cursor.fetchmany retrieved incorrect number of rows')
            r = cur.fetchmany(4)  # Should get 2 more
            self.assertEqual(
                len(r), 2,
                'cursor.fetchmany retrieved incorrect number of rows')
            r = cur.fetchmany(4)  # Should be an empty sequence
            self.assertEqual(
                len(r), 0,
                'cursor.fetchmany should return an empty sequence after'
                ' results are exhausted')
            self.assertIn(cur.rowcount, (-1, 6))

            # Same as above, using cursor.arraysize
            cur.arraysize = 4
            cur.execute(f'select name from {self.table_prefix}booze')
            r = cur.fetchmany()  # Should get 4 rows
            self.assertEqual(
                len(r), 4,
                'cursor.arraysize not being honoured by fetchmany')
            r = cur.fetchmany()  # Should get 2 more
            self.assertEqual(len(r), 2)
            r = cur.fetchmany()  # Should be an empty sequence
            self.assertEqual(len(r), 0)
            self.assertIn(cur.rowcount, (-1, 6))

            cur.arraysize = 6
            cur.execute(f'select name from {self.table_prefix}booze')
            rows = cur.fetchmany()  # Should get all rows
            self.assertIn(cur.rowcount, (-1, 6))
            self.assertEqual(len(rows), 6)
            self.assertEqual(len(rows), 6)
            rows = [r[0] for r in rows]
            rows.sort()

            # Make sure we get the right data back out
            for i in range(0, 6):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'incorrect data retrieved by cursor.fetchmany')

            rows = cur.fetchmany()  # Should return an empty list
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchmany should return an empty sequence if'
                ' called after the whole result set has been fetched')
            self.assertIn(cur.rowcount, (-1, 6))

            self.execute_ddl2(cur)
            cur.execute(f'select name from {self.table_prefix}barflys')
            r = cur.fetchmany()  # Should get empty sequence
            self.assertEqual(
                len(r), 0,
                'cursor.fetchmany should return an empty sequence if'
                ' query retrieved no rows')
            self.assertIn(cur.rowcount, (-1, 0))

        finally:
            con.close()

    def test_fetchall(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # cursor.fetchall should raise an Error if called
            # without executing a query that may return rows (such
            # as a select)
            self.assertRaises(self.driver.Error, cur.fetchall)

            self.execute_ddl1(cur)
            for sql in self._populate():
                cur.execute(sql)

            # cursor.fetchall should raise an Error if called
            # after executing a a statement that cannot return rows
            self.assertRaises(self.driver.Error, cur.fetchall)

            cur.execute(f'select name from {self.table_prefix}booze')
            rows = cur.fetchall()
            self.assertIn(cur.rowcount, (-1, len(self.samples)))
            self.assertEqual(
                len(rows), len(self.samples),
                'cursor.fetchall did not retrieve all rows')
            rows = sorted(r[0] for r in rows)
            for i in range(0, len(self.samples)):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'cursor.fetchall retrieved incorrect rows')
            rows = cur.fetchall()
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchall should return an empty list if called'
                ' after the whole result set has been fetched')
            self.assertIn(cur.rowcount, (-1, len(self.samples)))

            self.execute_ddl2(cur)
            cur.execute(f'select name from {self.table_prefix}barflys')
            rows = cur.fetchall()
            self.assertIn(cur.rowcount, (-1, 0))
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchall should return an empty list if'
                ' a select query returns no rows')

        finally:
            con.close()

    def test_mixedfetch(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.execute_ddl1(cur)
            for sql in self._populate():
                cur.execute(sql)

            cur.execute(f'select name from {self.table_prefix}booze')
            rows1 = cur.fetchone()
            rows23 = cur.fetchmany(2)
            rows4 = cur.fetchone()
            rows56 = cur.fetchall()
            self.assertIn(cur.rowcount, (-1, 6))
            self.assertEqual(
                len(rows23), 2,
                'fetchmany returned incorrect number of rows')
            self.assertEqual(
                len(rows56), 2,
                'fetchall returned incorrect number of rows')

            rows = [rows1[0]]
            rows.extend([rows23[0][0], rows23[1][0]])
            rows.append(rows4[0])
            rows.extend([rows56[0][0], rows56[1][0]])
            rows.sort()
            for i in range(0, len(self.samples)):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'incorrect data retrieved or inserted')
        finally:
            con.close()

    def help_nextset_setup(self, cur):
        """Set up nextset test.

        Should create a procedure called deleteme that returns two result sets,
        first the number of rows in booze, then "name from booze".
        """
        raise NotImplementedError('Helper not implemented')
        # sql = """
        #    create procedure deleteme as
        #    begin
        #        select count(*) from booze
        #        select name from booze
        #    end
        # """
        # cur.execute(sql)

    def help_nextset_teardown(self, cur):
        """Clean up after nextset test.

        If cleaning up is needed after test_nextset.
        """
        raise NotImplementedError('Helper not implemented')
        # cur.execute("drop procedure deleteme")

    def test_nextset(self):
        """Test the nextset functionality."""
        raise NotImplementedError('Drivers need to override this test')
        # example test implementation only:
        # con = self._connect()
        # try:
        #     cur = con.cursor()
        #     if not hasattr(cur, 'nextset'):
        #         return
        #     try:
        #         self.executeDDL1(cur)
        #         for sql in self._populate():
        #             cur.execute(sql)
        #         self.help_nextset_setup(cur)
        #         cur.callproc('deleteme')
        #         number_of_rows = cur.fetchone()
        #         self.assertEqual(number_of_rows[0], len(self.samples))
        #         self.assertTrue(cur.nextset())
        #         names = cur.fetchall()
        #         self.assertEqual(len(names), len(self.samples))
        #         self.assertIsNone(
        #             cur.nextset(), 'No more return sets, should return None')
        #     finally:
        #         self.help_nextset_teardown(cur)
        # finally:
        #     con.close()

    def test_arraysize(self):
        # Not much here - rest of the tests for this are in test_fetchmany
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertTrue(hasattr(cur, 'arraysize'),
                            'cursor.arraysize must be defined')
        finally:
            con.close()

    def test_setinputsizes(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.setinputsizes((25,))
            self._paraminsert(cur)  # Make sure cursor still works
        finally:
            con.close()

    def test_setoutputsize_basic(self):
        # Basic test is to make sure setoutputsize doesn't blow up
        con = self._connect()
        try:
            cur = con.cursor()
            cur.setoutputsize(1000)
            cur.setoutputsize(2000, 0)
            self._paraminsert(cur)  # Make sure the cursor still works
        finally:
            con.close()

    def test_setoutputsize(self):
        # Real test for setoutputsize is driver dependant
        raise NotImplementedError('Driver needed to override this test')

    def test_none(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.execute_ddl2(cur)
            # inserting NULL to the second column, because some drivers might
            # need the first one to be primary key, which means it needs
            # to have a non-NULL value
            cur.execute(f"{self.insert} into {self.table_prefix}barflys"
                        " values ('a', NULL)")
            cur.execute(f'select drink from {self.table_prefix}barflys')
            r = cur.fetchall()
            self.assertEqual(len(r), 1)
            self.assertEqual(len(r[0]), 1)
            self.assertIsNone(r[0][0], 'NULL value not returned as None')
        finally:
            con.close()

    def test_date(self):
        d1 = self.driver.Date(2002, 12, 25)
        d2 = self.driver.DateFromTicks(
            time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
        # Can we assume this? API doesn't specify, but it seems implied
        self.assertEqual(str(d1), str(d2))

    def test_time(self):
        t1 = self.driver.Time(13, 45, 30)
        t2 = self.driver.TimeFromTicks(
            time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))
        # Can we assume this? API doesn't specify, but it seems implied
        self.assertEqual(str(t1), str(t2))

    def test_timestamp(self):
        t1 = self.driver.Timestamp(2002, 12, 25, 13, 45, 30)
        t2 = self.driver.TimestampFromTicks(
            time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0))
        )
        # Can we assume this? API doesn't specify, but it seems implied
        self.assertEqual(str(t1), str(t2))

    def test_binary_string(self):
        self.driver.Binary(b'Something')
        self.driver.Binary(b'')

    def test_string_type(self):
        self.assertTrue(hasattr(self.driver, 'STRING'),
                        'module.STRING must be defined')

    def test_binary_type(self):
        self.assertTrue(hasattr(self.driver, 'BINARY'),
                        'module.BINARY must be defined.')

    def test_number_type(self):
        self.assertTrue(hasattr(self.driver, 'NUMBER'),
                        'module.NUMBER must be defined.')

    def test_datetime_type(self):
        self.assertTrue(hasattr(self.driver, 'DATETIME'),
                        'module.DATETIME must be defined.')

    def test_rowid_type(self):
        self.assertTrue(hasattr(self.driver, 'ROWID'),
                        'module.ROWID must be defined.')
