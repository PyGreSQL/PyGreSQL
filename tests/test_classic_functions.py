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

    test_strings = [
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
        (r'{"a"b"}', str, ValueError),
        (r'{"a""b"}', str, ValueError),
        (r'{"a\"b"}', str, ['a"b']),
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
        self.assertRaises(TypeError, f, '{}', b',',)
        self.assertRaises(TypeError, f, '{}', None, None)
        self.assertRaises(TypeError, f, '{}', None, 1)
        self.assertRaises(TypeError, f, '{}', None, b'')
        self.assertRaises(ValueError, f, '{}', None, b'\\')
        self.assertRaises(ValueError, f, '{}', None, b'{')
        self.assertRaises(ValueError, f, '{}', None, b'}')
        self.assertRaises(TypeError, f, '{}', None, b',;')
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
        for string, cast, expected in self.test_strings:
            if expected is ValueError:
                self.assertRaises(ValueError, f, string, cast)
            else:
                self.assertEqual(f(string, cast), expected)

    def testParserWithoutCast(self):
        f = pg.cast_array

        for string, cast, expected in self.test_strings:
            if cast is not str:
                continue
            if expected is ValueError:
                self.assertRaises(ValueError, f, string)
            else:
                self.assertEqual(f(string), expected)

    def testParserWithDifferentDelimiter(self):
        f = pg.cast_array

        def replace_comma(value):
            if isinstance(value, str):
                return value.replace(',', ';')
            elif isinstance(value, list):
                return [replace_comma(v) for v in value]
            else:
                return value

        for string, cast, expected in self.test_strings:
            string = replace_comma(string)
            if expected is ValueError:
                self.assertRaises(ValueError, f, string, cast)
            else:
                expected = replace_comma(expected)
                self.assertEqual(f(string, cast, b';'), expected)


class TestParseRecord(unittest.TestCase):
    """Test the record parser."""

    test_strings = [
        ('', None, ValueError),
        ('', str, ValueError),
        ('(', None, ValueError),
        ('(', str, ValueError),
        ('()', None, (None,)),
        ('()', str, (None,)),
        ('()', int, (None,)),
        ('(,)', str, (None, None)),
        ('( , )', str, (' ', ' ')),
        ('(")', None, ValueError),
        ('("")', None, ('',)),
        ('("")', str, ('',)),
        ('("")', int, ValueError),
        ('("" )', None, (' ',)),
        ('("" )', str, (' ',)),
        ('("" )', int, ValueError),
        ('    ()    ', None, (None,)),
        ('   (   )   ', None, ('   ',)),
        ('(', str, ValueError),
        ('(()', str, ('(',)),
        ('(())', str, ValueError),
        ('()(', str, ValueError),
        ('()()', str, ValueError),
        ('[]', str, ValueError),
        ('{}', str, ValueError),
        ('([])', str, ('[]',)),
        ('(hello)', int, ValueError),
        ('(42)', int, (42,)),
        ('( 42 )', int, (42,)),
        ('(  42)', int, (42,)),
        ('(42)', str, ('42',)),
        ('( 42 )', str, (' 42 ',)),
        ('(  42)', str, ('  42',)),
        ('(42', int, ValueError),
        ('( 42 ', int, ValueError),
        ('(hello)', str, ('hello',)),
        ('( hello )', str, (' hello ',)),
        ('(hello))', str, ValueError),
        ('   (hello)   ', str, ('hello',)),
        ('   (hello)   )', str, ValueError),
        ('(hello)?', str, ValueError),
        ('(null)', str, ('null',)),
        ('(null)', int, ValueError),
        (' ( NULL ) ', str, (' NULL ',)),
        ('   (   NULL   )   ', str, ('   NULL   ',)),
        (' ( null null ) ', str, (' null null ',)),
        (' ("null") ', str, ('null',)),
        (' ("NULL") ', str, ('NULL',)),
        ('(Hi!)', str, ('Hi!',)),
        ('("Hi!")', str, ('Hi!',)),
        ("('Hi!')", str, ("'Hi!'",)),
        ('(" Hi! ")', str, (' Hi! ',)),
        ('("Hi!" )', str, ('Hi! ',)),
        ('( "Hi!")', str, (' Hi!',)),
        ('( "Hi!" )', str, (' Hi! ',)),
        ('( ""Hi!"" )', str, (' Hi! ',)),
        ('( """Hi!""" )', str, (' "Hi!" ',)),
        ('(a")', str, ValueError),
        ('("b)', str, ValueError),
        ('("a" "b)', str, ValueError),
        ('("a" "b")', str, ('a b',)),
        ('( "a" "b" "c" )', str, (' a b c ',)),
        ('(  "a"  "b"  "c"  )', str, ('  a  b  c  ',)),
        ('(  "a,b"  "c,d"  )', str, ('  a,b  c,d  ',)),
        ('( "(a,b,c)" d, e, "f,g")', str, (' (a,b,c) d', ' e', ' f,g')),
        ('(a",b,c",d,"e,f")', str, ('a,b,c', 'd', 'e,f')),
        ('( """a,b""", ""c,d"", "e,f", "g", ""h"", """i""")', str,
            (' "a,b"', ' c', 'd', ' e,f', ' g', ' h', ' "i"')),
        ('(a",b)",c"),(d,e)",f,g)', str, ('a,b)', 'c),(d,e)', 'f', 'g')),
        ('(a"b)', str, ValueError),
        (r'(a\"b)', str, ('a"b',)),
        ('(a""b)', str, ('ab',)),
        ('("a""b")', str, ('a"b',)),
        (r'(a\,b)', str, ('a,b',)),
        (r'(a\bc)', str, ('abc',)),
        (r'("a\bc")', str, ('abc',)),
        (r'(\a\b\c)', str, ('abc',)),
        (r'("\a\b\c")', str, ('abc',)),
        ('("()")', str, ('()',)),
        (r'(\,)', str, (',',)),
        (r'(\(\))', str, ('()',)),
        (r'(\)\()', str, (')(',)),
        ('("(a,b,c)")', str, ('(a,b,c)',)),
        ("('abc')", str, ("'abc'",)),
        ('("abc")', str, ('abc',)),
        (r'(\"abc\")', str, ('"abc"',)),
        (r"(\'abc\')", str, ("'abc'",)),
        ('(Hello World!)', str, ('Hello World!',)),
        ('(Hello, World!)', str, ('Hello', ' World!',)),
        ('(Hello,\ World!)', str, ('Hello', ' World!',)),
        ('(Hello\, World!)', str, ('Hello, World!',)),
        ('("Hello World!")', str, ('Hello World!',)),
        ("(this,shouldn't,be,null)", str, ('this', "shouldn't", 'be', 'null')),
        ('(null,should,be,)', str, ('null', 'should', 'be', None)),
        ('(abcABC0123!?+-*/=&%$\\\\\'\\"{[]}"""":;\\,,)', str,
            ('abcABC0123!?+-*/=&%$\\\'"{[]}":;,', None)),
        ('(3, 2, 1,)', int, (3, 2, 1, None)),
        ('(3, 2, 1, )', int, ValueError),
        ('(, 1, 2, 3)', int, (None, 1, 2, 3)),
        ('( , 1, 2, 3)', int, ValueError),
        ('(,1,,2,,3,)', int, (None, 1, None, 2, None, 3, None)),
        ('(3,17,51)', int, (3, 17, 51)),
        (' ( 3 , 17 , 51 ) ', int, (3, 17, 51)),
        ('(3,17,51)', str, ('3', '17', '51')),
        (' ( 3 , 17 , 51 ) ', str, (' 3 ', ' 17 ', ' 51 ')),
        ('(1,"2",abc,"def")', str, ('1', '2', 'abc', 'def')),
        ('(())', str, ValueError),
        ('()))', str, ValueError),
        ('()()', str, ValueError),
        ('((()', str, ('((',)),
        ('(())', int, ValueError),
        ('((),())', str, ValueError),
        ('("()","()")', str, ('()', '()')),
        ('( " () , () , () " )', str, ('  () , () , ()  ',)),
        ('(20000, 25000, 25000, 25000)', int, (20000, 25000, 25000, 25000)),
        ('("breakfast","consulting","meeting","lunch")', str,
            ('breakfast', 'consulting', 'meeting', 'lunch')),
        ('("breakfast","consulting","meeting","lunch")',
            (str, str, str), ValueError),
        ('("breakfast","consulting","meeting","lunch")', (str, str, str, str),
            ('breakfast', 'consulting', 'meeting', 'lunch')),
        ('("breakfast","consulting","meeting","lunch")',
            (str, str, str, str, str), ValueError),
        ('("fuzzy dice",42,1.9375)', None, ('fuzzy dice', '42', '1.9375')),
        ('("fuzzy dice",42,1.9375)', str, ('fuzzy dice', '42', '1.9375')),
        ('("fuzzy dice",42,1.9375)', int, ValueError),
        ('("fuzzy dice",42,1.9375)', (str, int, float),
            ('fuzzy dice', 42, 1.9375)),
        ('("fuzzy dice",42,1.9375)', (str, int), ValueError),
        ('("fuzzy dice",42,1.9375)', (str, int, float, str), ValueError),
        ('("fuzzy dice",42,)', (str, int, float), ('fuzzy dice', 42, None)),
        ('("fuzzy dice",42,)', (str, int), ValueError),
        ('("",42,)', (str, int, float), ('', 42, None)),
        ('("fuzzy dice","",1.9375)', (str, int, float), ValueError),
        ('(fuzzy dice,"42","1.9375")', (str, int, float),
            ('fuzzy dice', 42, 1.9375))]

    def testParserParams(self):
        f = pg.cast_record
        self.assertRaises(TypeError, f)
        self.assertRaises(TypeError, f, None)
        self.assertRaises(TypeError, f, '()', 1)
        self.assertRaises(TypeError, f, '()', b',',)
        self.assertRaises(TypeError, f, '()', None, None)
        self.assertRaises(TypeError, f, '()', None, 1)
        self.assertRaises(TypeError, f, '()', None, b'')
        self.assertRaises(ValueError, f, '()', None, b'\\')
        self.assertRaises(ValueError, f, '()', None, b'(')
        self.assertRaises(ValueError, f, '()', None, b')')
        self.assertRaises(TypeError, f, '{}', None, b',;')
        self.assertEqual(f('()'), (None,))
        self.assertEqual(f('()', None), (None,))
        self.assertEqual(f('()', None, b';'), (None,))
        self.assertEqual(f('()', str), (None,))
        self.assertEqual(f('()', str, b';'), (None,))

    def testParserSimple(self):
        r = pg.cast_record('(a,b,c)')
        self.assertIsInstance(r, tuple)
        self.assertEqual(len(r), 3)
        self.assertEqual(r, ('a', 'b', 'c'))

    def testParserNested(self):
        f = pg.cast_record
        self.assertRaises(ValueError, f, '((a,b,c))')
        self.assertRaises(ValueError, f, '((a,b),(c,d))')
        self.assertRaises(ValueError, f, '((a),(b),(c))')
        self.assertRaises(ValueError, f, '(((((((abc)))))))')

    def testParserManyElements(self):
        f = pg.cast_record
        for n in 3, 5, 9, 12, 16, 32, 64, 256:
            r = '(%s)' % ','.join(map(str, range(n)))
            r = f(r, int)
            self.assertEqual(r, tuple(range(n)))

    def testParserCastUniform(self):
        f = pg.cast_record
        self.assertEqual(f('(1)'), ('1',))
        self.assertEqual(f('(1)', None), ('1',))
        self.assertEqual(f('(1)', int), (1,))
        self.assertEqual(f('(1)', str), ('1',))
        self.assertEqual(f('(a)'), ('a',))
        self.assertEqual(f('(a)', None), ('a',))
        self.assertRaises(ValueError, f, '(a)', int)
        self.assertEqual(f('(a)', str), ('a',))
        cast = lambda s: '%s is ok' % s
        self.assertEqual(f('(a)', cast), ('a is ok',))

    def testParserCastNonUniform(self):
        f = pg.cast_record
        self.assertEqual(f('(1)', []), ('1',))
        self.assertEqual(f('(1)', [None]), ('1',))
        self.assertEqual(f('(1)', [str]), ('1',))
        self.assertEqual(f('(1)', [int]), (1,))
        self.assertRaises(ValueError, f, '(1)', [None, None])
        self.assertRaises(ValueError, f, '(1)', [str, str])
        self.assertRaises(ValueError, f, '(1)', [int, int])
        self.assertEqual(f('(a)', [None]), ('a',))
        self.assertEqual(f('(a)', [str]), ('a',))
        self.assertRaises(ValueError, f, '(a)', [int])
        self.assertEqual(f('(1,a)', [int, str]), (1, 'a'))
        self.assertRaises(ValueError, f, '(1,a)', [str, int])
        self.assertEqual(f('(a,1)', [str, int]), ('a', 1))
        self.assertRaises(ValueError, f, '(a,1)', [int, str])
        self.assertEqual(f('(1,a,2,b,3,c)',
            [int, str, int, str, int, str]), (1, 'a', 2, 'b', 3, 'c'))
        self.assertEqual(f('(1,a,2,b,3,c)',
            (int, str, int, str, int, str)), (1, 'a', 2, 'b', 3, 'c'))
        cast1 = lambda s: '%s is ok' % s
        self.assertEqual(f('(a)', [cast1]), ('a is ok',))
        cast2 = lambda s: 'and %s is ok, too' % s
        self.assertEqual(f('(a,b)', [cast1, cast2]),
            ('a is ok', 'and b is ok, too'))
        self.assertRaises(ValueError, f, '(a)', [cast1, cast2])
        self.assertRaises(ValueError, f, '(a,b,c)', [cast1, cast2])
        self.assertEqual(f('(1,2,3,4,5,6)',
            [int, float, str, None, cast1, cast2]),
            (1, 2.0, '3', '4', '5 is ok', 'and 6 is ok, too'))

    def testParserDelim(self):
        f = pg.cast_record
        self.assertEqual(f('(1,2)'), ('1', '2'))
        self.assertEqual(f('(1,2)', delim=b','), ('1', '2'))
        self.assertEqual(f('(1;2)'), ('1;2',))
        self.assertEqual(f('(1;2)', delim=b';'), ('1', '2'))
        self.assertEqual(f('(1,2)', delim=b';'), ('1,2',))

    def testParserWithData(self):
        f = pg.cast_record
        for string, cast, expected in self.test_strings:
            if expected is ValueError:
                self.assertRaises(ValueError, f, string, cast)
            else:
                self.assertEqual(f(string, cast), expected)

    def testParserWithoutCast(self):
        f = pg.cast_record

        for string, cast, expected in self.test_strings:
            if cast is not str:
                continue
            if expected is ValueError:
                self.assertRaises(ValueError, f, string)
            else:
                self.assertEqual(f(string), expected)

    def testParserWithDifferentDelimiter(self):
        f = pg.cast_record

        def replace_comma(value):
            if isinstance(value, str):
                return value.replace(';', '@').replace(
                    ',', ';').replace('@', ',')
            elif isinstance(value, tuple):
                return tuple(replace_comma(v) for v in value)
            else:
                return value

        for string, cast, expected in self.test_strings:
            string = replace_comma(string)
            if expected is ValueError:
                self.assertRaises(ValueError, f, string, cast)
            else:
                expected = replace_comma(expected)
                self.assertEqual(f(string, cast, b';'), expected)


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
