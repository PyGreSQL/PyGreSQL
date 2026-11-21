#!/usr/bin/python

"""Test reference counting in the C extension module.

Functions of the C extension that return an existing object must return
a new reference to that object. A missing or surplus reference is often
not noticed immediately, but leads to crashes or memory leaks later.

These tests call each such function many times and check that the
reference count of the returned object does not change. The objects
are kept alive with additional references, so that a missing reference
makes the test fail instead of crashing the interpreter. A warm-up call
before each check excludes one-time effects such as caching.

Note that None, True and False are immortal since Python 3.12, so the
checks for functions returning these objects are only effective with
older Python versions.
"""

from __future__ import annotations

import sys
import unittest
from collections.abc import Callable
from decimal import Decimal
from functools import partial
from typing import Any

import pg  # the module under test

from .config import dbhost, dbname, dbpasswd, dbport, dbuser

N = 100  # number of calls for each check

# extra references that compensate for missing references after a failure
compensation: list[list] = []

# reference counts are not reliable in the free-threaded build
free_threaded = not getattr(sys, '_is_gil_enabled', lambda: True)()


def connect() -> pg.Connection:
    """Create a basic pg connection to the test database."""
    return pg.connect(dbname, dbhost, dbport, user=dbuser, passwd=dbpasswd)


def hook(*args: Any) -> None:
    """Do nothing (used as a callback function)."""


def refcount_drift(func: Callable[[], Any], obj: Any) -> int:
    """Get the change of the refcount of obj after calling func N times."""
    keep = [obj] * (2 * N)  # so that a missing reference cannot crash
    func()  # warm-up, to exclude one-time effects such as caching
    before = sys.getrefcount(obj)
    for _i in range(N):
        func()
    drift = sys.getrefcount(obj) - before
    if drift < 0:
        # keep the extra references so that the object is not freed
        # while it is still in use, which would crash later tests
        compensation.append(keep)
    return drift


@unittest.skipIf(free_threaded, 'reference counts are not reliable')
class RefCountTestCase(unittest.TestCase):
    """Base class providing a check for reference count changes."""

    def assert_refcount_unchanged(
            self, func: Callable[[], Any], obj: Any) -> None:
        """Check that calling func does not change the refcount of obj."""
        drift = refcount_drift(func, obj)
        self.assertEqual(
            drift, 0, f'Reference count of {obj!r} changed by {drift}'
            f' after {N} calls')


class TestConnectionRefCounts(RefCountTestCase):
    """Test reference counts with connection methods and attributes."""

    def setUp(self):
        self.c = connect()

    def tearDown(self):
        self.c.close()

    def test_get_cast_hook_none(self):
        self.assertIsNone(self.c.get_cast_hook())
        self.assert_refcount_unchanged(self.c.get_cast_hook, None)

    def test_get_cast_hook(self):
        self.c.set_cast_hook(hook)
        self.assert_refcount_unchanged(self.c.get_cast_hook, hook)

    def test_set_cast_hook(self):
        self.c.set_cast_hook(hook)  # the connection keeps one reference
        self.assert_refcount_unchanged(
            lambda: self.c.set_cast_hook(hook), hook)
        self.assert_refcount_unchanged(
            lambda: self.c.set_cast_hook(None), None)

    def test_get_notice_receiver_none(self):
        self.assertIsNone(self.c.get_notice_receiver())
        self.assert_refcount_unchanged(self.c.get_notice_receiver, None)

    def test_get_notice_receiver(self):
        self.c.set_notice_receiver(hook)
        self.assert_refcount_unchanged(self.c.get_notice_receiver, hook)

    def test_set_notice_receiver(self):
        self.c.set_notice_receiver(hook)  # the connection keeps a reference
        self.assert_refcount_unchanged(
            lambda: self.c.set_notice_receiver(hook), hook)
        self.assert_refcount_unchanged(
            lambda: self.c.set_notice_receiver(None), None)

    def test_ssl_in_use(self):
        ssl_in_use = self.c.ssl_in_use
        self.assertIsInstance(ssl_in_use, bool)
        self.assert_refcount_unchanged(lambda: self.c.ssl_in_use, ssl_in_use)

    def test_query_iter(self):
        q = self.c.query('select 1')
        self.assert_refcount_unchanged(lambda: iter(q), q)

    def test_query_null_values(self):
        q = self.c.query('select null')
        self.assert_refcount_unchanged(q.getresult, None)
        self.assert_refcount_unchanged(q.dictresult, None)

    def test_query_bool_values(self):
        q = self.c.query('select true, false')
        self.assert_refcount_unchanged(q.getresult, True)
        self.assert_refcount_unchanged(q.getresult, False)

    def test_notice_pgcnx(self):
        results: list[Any] = []

        def receiver(notice):
            # the connection is only available inside the receiver
            results.append(notice.pgcnx is self.c)
            results.append(refcount_drift(lambda: notice.pgcnx, self.c))

        self.c.set_notice_receiver(receiver)
        self.c.query("do $$ begin raise warning 'Bilbo was here!'; end $$")
        self.assertEqual(results, [True, 0])

    def test_notice_not_leaked(self):
        notices: list[Any] = []
        self.c.set_notice_receiver(notices.append)
        self.c.query("do $$ begin raise warning 'Bilbo was here!'; end $$")
        self.assertEqual(len(notices), 1)
        # the list must hold the only reference to the notice
        other = [object()]
        self.assertEqual(
            sys.getrefcount(notices[0]), sys.getrefcount(other[0]))
        # the notice receiver must not leak references either
        self.c.set_notice_receiver(hook)
        self.assert_refcount_unchanged(
            lambda: self.c.query(
                "do $$ begin raise warning 'Bilbo was here!'; end $$"),
            hook)

    def test_source_pgcnx(self):
        source = self.c.source()
        self.assertIs(source.pgcnx, self.c)
        self.assert_refcount_unchanged(lambda: source.pgcnx, self.c)

    def test_source_fetch_null_values(self):
        source = self.c.source()

        def fetch_null():
            source.execute('select null')
            self.assertEqual(source.fetch(1), [(None,)])

        self.assert_refcount_unchanged(fetch_null, None)

    def test_source_putdata(self):
        self.c.query('create temporary table test_copy (n int)')
        source = self.c.source()
        data, text = b'1\n', '2\n'
        error = ValueError('Copy aborted')

        def copy_from():
            source.execute('copy test_copy from stdin')
            self.assertIsNone(source.putdata(data))
            self.assertIsNone(source.putdata(text))
            with self.assertRaises(OSError):
                source.putdata(error)

        for obj in data, text, error, None:
            self.assert_refcount_unchanged(copy_from, obj)

    def test_large_object_pgcnx(self):
        self.c.query('begin')
        try:
            large = self.c.locreate(pg.INV_READ | pg.INV_WRITE)
            self.assertIs(large.pgcnx, self.c)
            self.assert_refcount_unchanged(lambda: large.pgcnx, self.c)
            large.unlink()
        finally:
            self.c.query('rollback')


class TestModuleRefCounts(RefCountTestCase):
    """Test reference counts with module level functions."""

    def test_get_jsondecode(self):
        jsondecode = pg.get_jsondecode()
        self.addCleanup(pg.set_jsondecode, jsondecode)
        self.assert_refcount_unchanged(pg.get_jsondecode, jsondecode)
        pg.set_jsondecode(hook)
        self.assert_refcount_unchanged(pg.get_jsondecode, hook)
        pg.set_jsondecode(None)
        self.assert_refcount_unchanged(pg.get_jsondecode, None)

    def test_set_jsondecode(self):
        jsondecode = pg.get_jsondecode()
        self.addCleanup(pg.set_jsondecode, jsondecode)
        pg.set_jsondecode(hook)  # the module keeps one reference
        self.assert_refcount_unchanged(lambda: pg.set_jsondecode(hook), hook)

    def test_get_decimal(self):
        decimal = pg.get_decimal()
        self.assertIs(decimal, Decimal)
        self.assert_refcount_unchanged(pg.get_decimal, decimal)

    def test_set_decimal(self):
        decimal = pg.get_decimal()
        self.addCleanup(pg.set_decimal, decimal)
        self.assert_refcount_unchanged(
            lambda: pg.set_decimal(decimal), decimal)

    def test_get_bool_options(self):
        for get in (pg.get_bool, pg.get_array, pg.get_bytea_escaped):
            with self.subTest(get=get.__name__):
                self.assert_refcount_unchanged(get, get())

    def test_set_bool_options(self):
        for get, set_ in ((pg.get_bool, pg.set_bool),
                          (pg.get_array, pg.set_array),
                          (pg.get_bytea_escaped, pg.set_bytea_escaped)):
            with self.subTest(set=set_.__name__):
                value = get()
                self.assert_refcount_unchanged(partial(set_, value), None)

    def test_get_defaults(self):
        for name, value in (('host', 'some_host'), ('base', 'some_base'),
                            ('opt', 'some_opt'), ('user', 'some_user'),
                            ('port', 12345)):
            with self.subTest(name=name):
                get = getattr(pg, f'get_def{name}')
                set_ = getattr(pg, f'set_def{name}')
                old = get()
                # the default port is reset with -1, not with None
                reset = -1 if name == 'port' and old is None else old
                self.addCleanup(set_, reset)
                self.assert_refcount_unchanged(get, old)
                set_(value)
                value = get()
                self.assert_refcount_unchanged(get, value)


if __name__ == '__main__':
    unittest.main()
