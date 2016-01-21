#! /usr/bin/python
# -*- coding: utf-8 -*-

"""Test the classic PyGreSQL interface.

Sub-tests for the module functions and constants.

Contributed by Christoph Zwerschke.

These tests do not need a database to test against.
"""

try:
    import unittest2 as unittest  # for Python < 2.7
except ImportError:
    import unittest

import json
import re

import pg  # the module under test

try:
    long
except NameError:  # Python >= 3.0
    long = int

try:
    unicode
except NameError:  # Python >= 3.0
    unicode = str


class TestHasConnect(unittest.TestCase):
    """Test existence of basic pg module functions."""

    def testhasPgError(self):
        self.assertTrue(issubclass(pg.Error, Exception))

    def testhasPgWarning(self):
        self.assertTrue(issubclass(pg.Warning, Exception))

    def testhasPgInterfaceError(self):
        self.assertTrue(issubclass(pg.InterfaceError, pg.Error))

    def testhasPgDatabaseError(self):
        self.assertTrue(issubclass(pg.DatabaseError, pg.Error))

    def testhasPgInternalError(self):
        self.assertTrue(issubclass(pg.InternalError, pg.DatabaseError))

    def testhasPgOperationalError(self):
        self.assertTrue(issubclass(pg.OperationalError, pg.DatabaseError))

    def testhasPgProgrammingError(self):
        self.assertTrue(issubclass(pg.ProgrammingError, pg.DatabaseError))

    def testhasPgIntegrityError(self):
        self.assertTrue(issubclass(pg.IntegrityError, pg.DatabaseError))

    def testhasPgDataError(self):
        self.assertTrue(issubclass(pg.DataError, pg.DatabaseError))

    def testhasPgNotSupportedError(self):
        self.assertTrue(issubclass(pg.NotSupportedError, pg.DatabaseError))

    def testhasConnect(self):
        self.assertTrue(callable(pg.connect))

    def testhasEscapeString(self):
        self.assertTrue(callable(pg.escape_string))

    def testhasEscapeBytea(self):
        self.assertTrue(callable(pg.escape_bytea))

    def testhasUnescapeBytea(self):
        self.assertTrue(callable(pg.unescape_bytea))

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

    def testDefBase(self):
        d0 = pg.get_defbase()
        d1 = 'pgtestdb'
        pg.set_defbase(d1)
        self.assertEqual(pg.get_defbase(), d1)
        pg.set_defbase(d0)
        self.assertEqual(pg.get_defbase(), d0)


class TestEscapeFunctions(unittest.TestCase):
    """Test pg escape and unescape functions.

    The libpq interface memorizes some parameters of the last opened
    connection that influence the result of these functions.
    Therefore we cannot do rigid tests of these functions here.
    We leave this for the test module that runs with a database.

    """

    def testEscapeString(self):
        f = pg.escape_string
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(u'plain')
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'plain')
        r = f("that's cheese")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "that''s cheese")

    def testEscapeBytea(self):
        f = pg.escape_bytea
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(u'plain')
        self.assertIsInstance(r, unicode)
        self.assertEqual(r, u'plain')
        r = f("that's cheese")
        self.assertIsInstance(r, str)
        self.assertEqual(r, "that''s cheese")

    def testUnescapeBytea(self):
        f = pg.unescape_bytea
        r = f(b'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(u'plain')
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, b'plain')
        r = f(b"das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"das is' käse".encode('utf-8'))
        r = f(u"das is' k\\303\\244se")
        self.assertIsInstance(r, bytes)
        self.assertEqual(r, u"das is' käse".encode('utf-8'))
        r = f(b'O\\000ps\\377!')
        self.assertEqual(r, b'O\x00ps\xff!')
        r = f(u'O\\000ps\\377!')
        self.assertEqual(r, b'O\x00ps\xff!')


class TestConfigFunctions(unittest.TestCase):
    """Test the functions for changing default settings.

    The effect of most of these cannot be tested here, because that
    needs a database connection.  So we merely test their existence here.

    """

    def testGetDecimalPoint(self):
        r = pg.get_decimal_point()
        self.assertIsInstance(r, str)
        self.assertEqual(r, '.')

    def testSetDecimalPoint(self):
        point = pg.get_decimal_point()
        try:
            pg.set_decimal_point('*')
            r = pg.get_decimal_point()
            self.assertIsInstance(r, str)
            self.assertEqual(r, '*')
        finally:
            pg.set_decimal_point(point)
        r = pg.get_decimal_point()
        self.assertIsInstance(r, str)
        self.assertEqual(r, point)

    def testGetDecimal(self):
        r = pg.get_decimal()
        self.assertIs(r, pg.Decimal)

    def testSetDecimal(self):
        decimal_class = pg.Decimal
        try:
            pg.set_decimal(int)
            r = pg.get_decimal()
            self.assertIs(r, int)
        finally:
            pg.set_decimal(decimal_class)
        r = pg.get_decimal()
        self.assertIs(r, decimal_class)

    def testGetBool(self):
        r = pg.get_bool()
        self.assertIsInstance(r, bool)
        self.assertIs(r, False)

    def testSetBool(self):
        use_bool = pg.get_bool()
        try:
            pg.set_bool(True)
            r = pg.get_bool()
            pg.set_bool(use_bool)
            self.assertIsInstance(r, bool)
            self.assertIs(r, True)
            pg.set_bool(False)
            r = pg.get_bool()
            self.assertIsInstance(r, bool)
            self.assertIs(r, False)
        finally:
            pg.set_bool(use_bool)
        r = pg.get_bool()
        self.assertIsInstance(r, bool)
        self.assertIs(r, use_bool)

    def testGetNamedresult(self):
        r = pg.get_namedresult()
        self.assertTrue(callable(r))
        self.assertIs(r, pg._namedresult)

    def testSetNamedresult(self):
        namedresult = pg.get_namedresult()
        try:
            pg.set_namedresult(None)
            r = pg.get_namedresult()
            self.assertIsNone(r)
            f = lambda q: q.getresult()
            pg.set_namedresult(f)
            r = pg.get_namedresult()
            self.assertIs(r, f)
            self.assertRaises(TypeError, pg.set_namedresult, 'invalid')
        finally:
            pg.set_namedresult(namedresult)
        r = pg.get_namedresult()
        self.assertIs(r, namedresult)

    def testGetJsondecode(self):
        r = pg.get_jsondecode()
        self.assertTrue(callable(r))
        self.assertIs(r, json.loads)

    def testSetJsondecode(self):
        jsondecode = pg.get_jsondecode()
        try:
            pg.set_jsondecode(None)
            r = pg.get_jsondecode()
            self.assertIsNone(r)
            pg.set_jsondecode(str)
            r = pg.get_jsondecode()
            self.assertIs(r, str)
            self.assertRaises(TypeError, pg.set_jsondecode, 'invalid')
        finally:
            pg.set_jsondecode(jsondecode)
        r = pg.get_jsondecode()
        self.assertIs(r, jsondecode)


class TestModuleConstants(unittest.TestCase):
    """Test the existence of the documented module constants."""

    def testVersion(self):
        v = pg.version
        self.assertIsInstance(v, str)
        # make sure the version conforms to PEP440
        re_version = r"""^
            (\d[\.\d]*(?<= \d))
            ((?:[abc]|rc)\d+)?
            (?:(\.post\d+))?
            (?:(\.dev\d+))?
            (?:(\+(?![.])[a-zA-Z0-9\.]*[a-zA-Z0-9]))?
            $"""
        match = re.match(re_version, v, re.X)
        self.assertIsNotNone(match)


if __name__ == '__main__':
    unittest.main()
