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


class TestParseArray(unittest.TestCase):
    """Test the array parser."""

    array_expressions = [
        ('', str, ValueError),
        ('{}', None, []),
        ('{}', str, []),
        ('   {   }   ', None, []),
        ('{', str, ValueError),
        ('{{}', str, ValueError),
        ('{}{', str, ValueError),
        ('[]', str, ValueError),
        ('()', str, ValueError),
        ('{[]}', str, ['[]']),
        ('{hello}', int, ValueError),
        ('{42}', int, [42]),
        ('{ 42 }', int, [42]),
        ('{42', int, ValueError),
        ('{ 42 ', int, ValueError),
        ('{hello}', str, ['hello']),
        ('{ hello }', str, ['hello']),
        ('{hi}   ', str, ['hi']),
        ('{hi}   ?', str, ValueError),
        ('{null}', str, [None]),
        (' { NULL } ', str, [None]),
        ('   {   NULL   }   ', str, [None]),
        (' { not null } ', str, ['not null']),
        (' { not NULL } ', str, ['not NULL']),
        (' {"null"} ', str, ['null']),
        (' {"NULL"} ', str, ['NULL']),
        ('{Hi!}', str, ['Hi!']),
        ('{"Hi!"}', str, ['Hi!']),
        ('{" Hi! "}', str, [' Hi! ']),
        ('{a"}', str, ValueError),
        ('{"b}', str, ValueError),
        ('{a"b}', str, ValueError),
        (r'{a\"b}', str, ['a"b']),
        (r'{a\,b}', str, ['a,b']),
        (r'{a\bc}', str, ['abc']),
        (r'{"a\bc"}', str, ['abc']),
        (r'{\a\b\c}', str, ['abc']),
        (r'{"\a\b\c"}', str, ['abc']),
        ('{"{}"}', str, ['{}']),
        (r'{\{\}}', str, ['{}']),
        ('{"{a,b,c}"}', str, ['{a,b,c}']),
        ("{'abc'}", str, ["'abc'"]),
        ('{"abc"}', str, ['abc']),
        (r'{\"abc\"}', str, ['"abc"']),
        (r"{\'abc\'}", str, ["'abc'"]),
        (r"{abc,d,efg}", str, ['abc', 'd', 'efg']),
        ('{Hello World!}', str, ['Hello World!']),
        ('{Hello, World!}', str, ['Hello', 'World!']),
        ('{Hello,\ World!}', str, ['Hello', ' World!']),
        ('{Hello\, World!}', str, ['Hello, World!']),
        ('{"Hello World!"}', str, ['Hello World!']),
        ('{this, should, be, null}', str, ['this', 'should', 'be', None]),
        ('{This, should, be, NULL}', str, ['This', 'should', 'be', None]),
        ('{3, 2, 1, null}', int, [3, 2, 1, None]),
        ('{3, 2, 1, NULL}', int, [3, 2, 1, None]),
        ('{3,17,51}', int, [3, 17, 51]),
        (' { 3 , 17 , 51 } ', int, [3, 17, 51]),
        ('{3,17,51}', str, ['3', '17', '51']),
        (' { 3 , 17 , 51 } ', str, ['3', '17', '51']),
        ('{1,"2",abc,"def"}', str, ['1', '2', 'abc', 'def']),
        ('{{}}', int, [[]]),
        ('{{},{}}', int, [[], []]),
        ('{ {} , {} , {} }', int, [[], [], []]),
        ('{ {} , {} , {} , }', int, ValueError),
        ('{{{1,2,3},{4,5,6}}}', int, [[[1, 2, 3], [4, 5, 6]]]),
        ('{{1,2,3},{4,5,6},{7,8,9}}', int, [[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        ('{20000, 25000, 25000, 25000}', int, [20000, 25000, 25000, 25000]),
        ('{{{17,18,19},{14,15,16},{11,12,13}},'
         '{{27,28,29},{24,25,26},{21,22,23}},'
         '{{37,38,39},{34,35,36},{31,32,33}}}', int,
            [[[17, 18, 19], [14, 15, 16], [11, 12, 13]],
             [[27, 28, 29], [24, 25, 26], [21, 22, 23]],
             [[37, 38, 39], [34, 35, 36], [31, 32, 33]]]),
        ('{{"breakfast", "consulting"}, {"meeting", "lunch"}}', str,
            [['breakfast', 'consulting'], ['meeting', 'lunch']]),
        ('[1:3]={1,2,3}', int, [1, 2, 3]),
        ('[-1:1]={1,2,3}', int, [1, 2, 3]),
        ('[-1:+1]={1,2,3}', int, [1, 2, 3]),
        ('[-3:-1]={1,2,3}', int, [1, 2, 3]),
        ('[+1:+3]={1,2,3}', int, [1, 2, 3]),
        ('[]={1,2,3}', int, ValueError),
        ('[1:]={1,2,3}', int, ValueError),
        ('[:3]={1,2,3}', int, ValueError),
        ('[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}',
            int, [[[1, 2, 3], [4, 5, 6]]]),
        ('  [1:1]  [-2:-1]  [3:5]  =  { { { 1 , 2 , 3 }, {4 , 5 , 6 } } }',
            int, [[[1, 2, 3], [4, 5, 6]]]),
        ('[1:1][3:5]={{1,2,3},{4,5,6}}', int, [[1, 2, 3], [4, 5, 6]]),
        ('[3:5]={{1,2,3},{4,5,6}}', int, ValueError),
        ('[1:1][-2:-1][3:5]={{1,2,3},{4,5,6}}', int, ValueError)]

    def testParserParams(self):
        f = pg.cast_array
        self.assertRaises(TypeError, f)
        self.assertRaises(TypeError, f, None)
        self.assertRaises(TypeError, f, '{}', 1)
        self.assertRaises(TypeError, f, '{}', ',',)
        self.assertRaises(TypeError, f, '{}', None, None)
        self.assertRaises(TypeError, f, '{}', None, 1)
        self.assertRaises(TypeError, f, '{}', None, '')
        self.assertRaises(TypeError, f, '{}', None, ',;')
        self.assertEqual(f('{}'), [])
        self.assertEqual(f('{}', None), [])
        self.assertEqual(f('{}', None, b';'), [])
        self.assertEqual(f('{}', str), [])
        self.assertEqual(f('{}', str, b';'), [])

    def testParserSimple(self):
        r = pg.cast_array('{a,b,c}')
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 3)
        self.assertEqual(r, ['a', 'b', 'c'])

    def testParserNested(self):
        f = pg.cast_array
        r = f('{{a,b,c}}')
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 1)
        r = r[0]
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 3)
        self.assertEqual(r, ['a', 'b', 'c'])
        self.assertRaises(ValueError, f, '{a,{b,c}}')
        r = f('{{a,b},{c,d}}')
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 2)
        r = r[1]
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 2)
        self.assertEqual(r, ['c', 'd'])
        r = f('{{a},{b},{c}}')
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 3)
        r = r[1]
        self.assertIsInstance(r, list)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0], 'b')
        r = f('{{{{{{{abc}}}}}}}')
        for i in range(7):
            self.assertIsInstance(r, list)
            self.assertEqual(len(r), 1)
            r = r[0]
        self.assertEqual(r, 'abc')

    def testParserTooDeeplyNested(self):
        f = pg.cast_array
        for n in 3, 5, 9, 12, 16, 32, 64, 256:
            r = '%sa,b,c%s' % ('{' * n, '}' * n)
            if n > 16:  # hard coded maximum depth
                self.assertRaises(ValueError, f, r)
            else:
                r = f(r)
                for i in range(n - 1):
                    self.assertIsInstance(r, list)
                    self.assertEqual(len(r), 1)
                    r = r[0]
                self.assertEqual(len(r), 3)
                self.assertEqual(r, ['a', 'b', 'c'])

    def testParserCast(self):
        f = pg.cast_array
        self.assertEqual(f('{1}'), ['1'])
        self.assertEqual(f('{1}', None), ['1'])
        self.assertEqual(f('{1}', int), [1])
        self.assertEqual(f('{1}', str), ['1'])
        self.assertEqual(f('{a}'), ['a'])
        self.assertEqual(f('{a}', None), ['a'])
        self.assertRaises(ValueError, f, '{a}', int)
        self.assertEqual(f('{a}', str), ['a'])
        cast = lambda s: '%s is ok' % s
        self.assertEqual(f('{a}', cast), ['a is ok'])

    def testParserDelim(self):
        f = pg.cast_array
        self.assertEqual(f('{1,2}'), ['1', '2'])
        self.assertEqual(f('{1,2}', delim=b','), ['1', '2'])
        self.assertEqual(f('{1;2}'), ['1;2'])
        self.assertEqual(f('{1;2}', delim=b';'), ['1', '2'])
        self.assertEqual(f('{1,2}', delim=b';'), ['1,2'])

    def testParserWithData(self):
        f = pg.cast_array
        for expression, cast, expected in self.array_expressions:
            if expected is ValueError:
                self.assertRaises(ValueError, f, expression, cast)
            else:
                self.assertEqual(f(expression, cast), expected)

    def testParserWithoutCast(self):
        f = pg.cast_array

        for expression, cast, expected in self.array_expressions:
            if cast is not str:
                continue
            if expected is ValueError:
                self.assertRaises(ValueError, f, expression)
            else:
                self.assertEqual(f(expression), expected)

    def testParserWithDifferentDelimiter(self):
        f = pg.cast_array

        def replace_comma(value):
            if isinstance(value, str):
                return value.replace(',', ';')
            elif isinstance(value, list):
                return [replace_comma(v) for v in value]
            else:
                return value

        for expression, cast, expected in self.array_expressions:
            expression = replace_comma(expression)
            if expected is ValueError:
                self.assertRaises(ValueError, f, expression, cast)
            else:
                expected = replace_comma(expected)
                self.assertEqual(f(expression, cast, b';'), expected)


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
