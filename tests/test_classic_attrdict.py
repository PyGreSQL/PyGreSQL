#!/usr/bin/python

"""Test the classic PyGreSQL interface.

Sub-tests for the DB wrapper object.

Contributed by Christoph Zwerschke.

These tests need a database to test against.
"""

import unittest

import pg.attrs  # the module under test


class TestAttrDict(unittest.TestCase):
    """Test the simple ordered dictionary for attribute names."""

    cls = pg.attrs.AttrDict

    def test_init(self):
        a = self.cls()
        self.assertIsInstance(a, dict)
        self.assertEqual(a, {})
        items = [('id', 'int'), ('name', 'text')]
        a = self.cls(items)
        self.assertIsInstance(a, dict)
        self.assertEqual(a, dict(items))
        iteritems = iter(items)
        a = self.cls(iteritems)
        self.assertIsInstance(a, dict)
        self.assertEqual(a, dict(items))

    def test_iter(self):
        a = self.cls()
        self.assertEqual(list(a), [])
        keys = ['id', 'name', 'age']
        items = [(key, None) for key in keys]
        a = self.cls(items)
        self.assertEqual(list(a), keys)

    def test_keys(self):
        a = self.cls()
        self.assertEqual(list(a.keys()), [])
        keys = ['id', 'name', 'age']
        items = [(key, None) for key in keys]
        a = self.cls(items)
        self.assertEqual(list(a.keys()), keys)

    def test_values(self):
        a = self.cls()
        self.assertEqual(list(a.values()), [])
        items = [('id', 'int'), ('name', 'text')]
        values = [item[1] for item in items]
        a = self.cls(items)
        self.assertEqual(list(a.values()), values)

    def test_items(self):
        a = self.cls()
        self.assertEqual(list(a.items()), [])
        items = [('id', 'int'), ('name', 'text')]
        a = self.cls(items)
        self.assertEqual(list(a.items()), items)

    def test_get(self):
        a = self.cls([('id', 1)])
        try:
            self.assertEqual(a['id'], 1)
        except KeyError:
            self.fail('AttrDict should be readable')

    def test_set(self):
        a = self.cls()
        try:
            a['id'] = 1
        except TypeError:
            pass
        else:
            self.fail('AttrDict should be read-only')

    def test_del(self):
        a = self.cls([('id', 1)])
        try:
            del a['id']
        except TypeError:
            pass
        else:
            self.fail('AttrDict should be read-only')

    def test_write_methods(self):
        a = self.cls([('id', 1)])
        self.assertEqual(a['id'], 1)
        for method in 'clear', 'update', 'pop', 'setdefault', 'popitem':
            method = getattr(a, method)
            self.assertRaises(TypeError, method, a)


if __name__ == '__main__':
    unittest.main()
