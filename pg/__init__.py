#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# This file contains the classic pg module.
#
# Copyright (c) 2023 by the PyGreSQL Development Team
#
# The notification handler is based on pgnotify which is
# Copyright (c) 2001 Ng Pheng Siong. All rights reserved.
#
# Please see the LICENSE.TXT file for specific restrictions.

"""PyGreSQL classic interface.

This pg module implements some basic database management stuff.
It includes the _pg module and builds on it, providing the higher
level wrapper class named DB with additional functionality.
This is known as the "classic" ("old style") PyGreSQL interface.
For a DB-API 2 compliant interface use the newer pgdb module.
"""

from __future__ import annotations

import select
import weakref
from collections import namedtuple
from contextlib import suppress
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache, partial
from inspect import signature
from json import dumps as jsonencode
from json import loads as jsondecode
from math import isinf, isnan
from operator import itemgetter
from re import compile as regex
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    ClassVar,
    Generator,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Sequence,
    TypeVar,
)
from uuid import UUID

try:
    from ._pg import version
except ImportError as e:  # noqa: F841
    import os
    libpq = 'libpq.'
    if os.name == 'nt':
        libpq += 'dll'
        import sys
        paths = [path for path in os.environ["PATH"].split(os.pathsep)
                 if os.path.exists(os.path.join(path, libpq))]
        if sys.version_info >= (3, 8):
            # see https://docs.python.org/3/whatsnew/3.8.html#ctypes
            add_dll_dir = os.add_dll_directory  # type: ignore
            for path in paths:
                with add_dll_dir(os.path.abspath(path)):
                    try:
                        from ._pg import version
                    except ImportError:
                        pass
                    else:
                        del version
                        e = None  # type: ignore
                        break
        if paths:
            libpq = 'compatible ' + libpq
    else:
        libpq += 'so'
    if e:
        raise ImportError(
            "Cannot import shared library for PyGreSQL,\n"
            f"probably because no {libpq} is installed.\n{e}") from e
else:
    del version

# import objects from extension module
from ._pg import (
    INV_READ,
    INV_WRITE,
    POLLING_FAILED,
    POLLING_OK,
    POLLING_READING,
    POLLING_WRITING,
    RESULT_DDL,
    RESULT_DML,
    RESULT_DQL,
    RESULT_EMPTY,
    SEEK_CUR,
    SEEK_END,
    SEEK_SET,
    TRANS_ACTIVE,
    TRANS_IDLE,
    TRANS_INERROR,
    TRANS_INTRANS,
    TRANS_UNKNOWN,
    Connection,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    InvalidResultError,
    MultipleResultsError,
    NoResultError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Query,
    Warning,
    cast_array,
    cast_hstore,
    cast_record,
    connect,
    escape_bytea,
    escape_string,
    get_array,
    get_bool,
    get_bytea_escaped,
    get_datestyle,
    get_decimal,
    get_decimal_point,
    get_defbase,
    get_defhost,
    get_defopt,
    get_defport,
    get_defuser,
    get_jsondecode,
    get_pqlib_version,
    set_array,
    set_bool,
    set_bytea_escaped,
    set_datestyle,
    set_decimal,
    set_decimal_point,
    set_defbase,
    set_defhost,
    set_defopt,
    set_defpasswd,
    set_defport,
    set_defuser,
    set_jsondecode,
    set_query_helpers,
    unescape_bytea,
    version,
)

__version__ = version

__all__ = [
    'DB', 'Adapter',
    'NotificationHandler', 'Typecasts',
    'Bytea', 'Hstore', 'Json', 'Literal',
    'Error', 'Warning',
    'DataError', 'DatabaseError',
    'IntegrityError', 'InterfaceError', 'InternalError',
    'InvalidResultError', 'MultipleResultsError',
    'NoResultError', 'NotSupportedError',
    'OperationalError', 'ProgrammingError',
    'Connection', 'Query',
    'INV_READ', 'INV_WRITE',
    'POLLING_OK', 'POLLING_FAILED', 'POLLING_READING', 'POLLING_WRITING',
    'RESULT_DDL', 'RESULT_DML', 'RESULT_DQL', 'RESULT_EMPTY',
    'SEEK_CUR', 'SEEK_END', 'SEEK_SET',
    'TRANS_ACTIVE', 'TRANS_IDLE', 'TRANS_INERROR',
    'TRANS_INTRANS', 'TRANS_UNKNOWN',
    'cast_array', 'cast_hstore', 'cast_record',
    'connect', 'escape_bytea', 'escape_string', 'unescape_bytea',
    'get_array', 'get_bool', 'get_bytea_escaped',
    'get_datestyle', 'get_decimal', 'get_decimal_point',
    'get_defbase', 'get_defhost', 'get_defopt', 'get_defport', 'get_defuser',
    'get_jsondecode', 'get_pqlib_version', 'get_typecast',
    'set_array', 'set_bool', 'set_bytea_escaped',
    'set_datestyle', 'set_decimal', 'set_decimal_point',
    'set_defbase', 'set_defhost', 'set_defopt',
    'set_defpasswd', 'set_defport', 'set_defuser',
    'set_jsondecode', 'set_query_helpers', 'set_typecast',
    'version', '__version__',
]

# Auxiliary classes and functions that are independent of a DB connection:

SomeNamedTuple = Any  # alias for accessing arbitrary named tuples

def get_args(func: Callable) -> list:
    return list(signature(func).parameters)


# time zones used in Postgres timestamptz output
_timezones: dict[str, str] = {
    'CET': '+0100', 'EET': '+0200', 'EST': '-0500',
    'GMT': '+0000', 'HST': '-1000', 'MET': '+0100', 'MST': '-0700',
    'UCT': '+0000', 'UTC': '+0000', 'WET': '+0000'
}


def _timezone_as_offset(tz: str) -> str:
    if tz.startswith(('+', '-')):
        if len(tz) < 5:
            return tz + '00'
        return tz.replace(':', '')
    return _timezones.get(tz, '+0000')


def _oid_key(table: str) -> str:
    """Build oid key from a table name."""
    return f'oid({table})'


class Bytea(bytes):
    """Wrapper class for marking Bytea values."""


class Hstore(dict):
    """Wrapper class for marking hstore values."""

    _re_quote = regex('^[Nn][Uu][Ll][Ll]$|[ ,=>]')

    @classmethod
    def _quote(cls, s: Any) -> str:
        if s is None:
            return 'NULL'
        if not isinstance(s, str):
            s = str(s)
        if not s:
            return '""'
        s = s.replace('"', '\\"')
        if cls._re_quote.search(s):
            s = f'"{s}"'
        return s

    def __str__(self) -> str:
        """Create a printable representation of the hstore value."""
        q = self._quote
        return ','.join(f'{q(k)}=>{q(v)}' for k, v in self.items())


class Json:
    """Wrapper class for marking Json values."""

    def __init__(self, obj: Any,
                 encode: Callable[[Any], str] | None = None) -> None:
        """Initialize the JSON object."""
        self.obj = obj
        self.encode = encode or jsonencode

    def __str__(self) -> str:
        """Create a printable representation of the JSON object."""
        obj = self.obj
        if isinstance(obj, str):
            return obj
        return self.encode(obj)


class _SimpleTypes(dict):
    """Dictionary mapping pg_type names to simple type names.

    The corresponding Python types and simple names are also mapped.
    """

    _type_aliases: Mapping[str, list[str | type]] = MappingProxyType({
        'bool': [bool],
        'bytea': [Bytea],
        'date': ['interval', 'time', 'timetz', 'timestamp', 'timestamptz',
                 'abstime', 'reltime',  # these are very old
                 'datetime', 'timedelta',  # these do not really exist
                 date, time, datetime, timedelta],
        'float': ['float4', 'float8', float],
        'int': ['cid', 'int2', 'int4', 'int8', 'oid', 'xid', int],
        'hstore': [Hstore], 'json': ['jsonb', Json], 'uuid': [UUID],
        'num': ['numeric', Decimal], 'money': [],
        'text': ['bpchar', 'char', 'name', 'varchar', bytes, str]
    })

    # noinspection PyMissingConstructor
    def __init__(self) -> None:
        """Initialize type mapping."""
        for typ, keys in self._type_aliases.items():
            keys = [typ, *keys]
            for key in keys:
                self[key] = typ
                if isinstance(key, str):
                    self[f'_{key}'] = f'{typ}[]'
                elif not isinstance(key, tuple):
                    self[List[key]] = f'{typ}[]'  # type: ignore

    @staticmethod
    def __missing__(key: str) -> str:
        """Unmapped types are interpreted as text."""
        return 'text'

    def get_type_dict(self) -> dict[type, str]:
        """Get a plain dictionary of only the types."""
        return {key: typ for key, typ in self.items()
                if not isinstance(key, (str, tuple))}


_simpletypes = _SimpleTypes()
_simple_type_dict = _simpletypes.get_type_dict()


def _quote_if_unqualified(param: str, name: int | str) -> str:
    """Quote parameter representing a qualified name.

    Puts a quote_ident() call around the given parameter unless
    the name contains a dot, in which case the name is ambiguous
    (could be a qualified name or just a name with a dot in it)
    and must be quoted manually by the caller.
    """
    if isinstance(name, str) and '.' not in name:
        return f'quote_ident({param})'
    return param


class _ParameterList(list):
    """Helper class for building typed parameter lists."""

    adapt: Callable

    def add(self, value: Any, typ:Any = None) -> str:
        """Typecast value with known database type and build parameter list.

        If this is a literal value, it will be returned as is.  Otherwise, a
        placeholder will be returned and the parameter list will be augmented.
        """
        # noinspection PyUnresolvedReferences
        value = self.adapt(value, typ)
        if isinstance(value, Literal):
            return value
        self.append(value)
        return f'${len(self)}'


class Literal(str):
    """Wrapper class for marking literal SQL values."""


class AttrDict(dict):
    """Simple read-only ordered dictionary for storing attribute names."""

    def __init__(self, *args: Any, **kw: Any) -> None:
        self._read_only = False
        super().__init__(*args, **kw)
        self._read_only = True
        error = self._read_only_error
        self.clear = self.update = error  # type: ignore
        self.pop = self.setdefault = self.popitem = error  # type: ignore

    def __setitem__(self, key: str, value: Any) -> None:
        if self._read_only:
            self._read_only_error()
        super().__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        if self._read_only:
            self._read_only_error()
        super().__delitem__(key)

    @staticmethod
    def _read_only_error(*_args: Any, **_kw: Any) -> Any:
        raise TypeError('This object is read-only')


class Adapter:
    """Class providing methods for adapting parameters to the database."""

    _bool_true_values = frozenset('t true 1 y yes on'.split())

    _date_literals = frozenset(
        'current_date current_time'
        ' current_timestamp localtime localtimestamp'.split())

    _re_array_quote = regex(r'[{},"\\\s]|^[Nn][Uu][Ll][Ll]$')
    _re_record_quote = regex(r'[(,"\\]')
    _re_array_escape = _re_record_escape = regex(r'(["\\])')

    def __init__(self, db: DB):
        """Initialize the adapter object with the given connection."""
        self.db = weakref.proxy(db)

    @classmethod
    def _adapt_bool(cls, v: Any) -> str | None:
        """Adapt a boolean parameter."""
        if isinstance(v, str):
            if not v:
                return None
            v = v.lower() in cls._bool_true_values
        return 't' if v else 'f'

    @classmethod
    def _adapt_date(cls, v: Any) -> Any:
        """Adapt a date parameter."""
        if not v:
            return None
        if isinstance(v, str) and v.lower() in cls._date_literals:
            return Literal(v)
        return v

    @staticmethod
    def _adapt_num(v: Any) -> Any:
        """Adapt a numeric parameter."""
        if not v and v != 0:
            return None
        return v

    _adapt_int = _adapt_float = _adapt_money = _adapt_num

    def _adapt_bytea(self, v: Any) -> str:
        """Adapt a bytea parameter."""
        return self.db.escape_bytea(v)

    def _adapt_json(self, v: Any) -> str | None:
        """Adapt a json parameter."""
        if not v:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, Json):
            return str(v)
        return self.db.encode_json(v)

    def _adapt_hstore(self, v: Any) -> str | None:
        """Adapt a hstore parameter."""
        if not v:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, Hstore):
            return str(v)
        if isinstance(v, dict):
            return str(Hstore(v))
        raise TypeError(f'Hstore parameter {v} has wrong type')

    def _adapt_uuid(self, v: Any) -> str | None:
        """Adapt a UUID parameter."""
        if not v:
            return None
        if isinstance(v, str):
            return v
        return str(v)

    @classmethod
    def _adapt_text_array(cls, v: Any) -> str:
        """Adapt a text type array parameter."""
        if isinstance(v, list):
            adapt = cls._adapt_text_array
            return '{' + ','.join(adapt(v) for v in v) + '}'
        if v is None:
            return 'null'
        if not v:
            return '""'
        v = str(v)
        if cls._re_array_quote.search(v):
            v = cls._re_array_escape.sub(r'\\\1', v)
            v = f'"{v}"'
        return v

    _adapt_date_array = _adapt_text_array

    @classmethod
    def _adapt_bool_array(cls, v: Any) -> str:
        """Adapt a boolean array parameter."""
        if isinstance(v, list):
            adapt = cls._adapt_bool_array
            return '{' + ','.join(adapt(v) for v in v) + '}'
        if v is None:
            return 'null'
        if isinstance(v, str):
            if not v:
                return 'null'
            v = v.lower() in cls._bool_true_values
        return 't' if v else 'f'

    @classmethod
    def _adapt_num_array(cls, v: Any) -> str:
        """Adapt a numeric array parameter."""
        if isinstance(v, list):
            adapt = cls._adapt_num_array
            v = '{' + ','.join(adapt(v) for v in v) + '}'
        if not v and v != 0:
            return 'null'
        return str(v)

    _adapt_int_array = _adapt_float_array = _adapt_money_array = \
        _adapt_num_array

    def _adapt_bytea_array(self, v: Any) -> bytes:
        """Adapt a bytea array parameter."""
        if isinstance(v, list):
            return b'{' + b','.join(
                self._adapt_bytea_array(v) for v in v) + b'}'
        if v is None:
            return b'null'
        return self.db.escape_bytea(v).replace(b'\\', b'\\\\')

    def _adapt_json_array(self, v: Any) -> str:
        """Adapt a json array parameter."""
        if isinstance(v, list):
            adapt = self._adapt_json_array
            return '{' + ','.join(adapt(v) for v in v) + '}'
        if not v:
            return 'null'
        if not isinstance(v, str):
            v = self.db.encode_json(v)
        if self._re_array_quote.search(v):
            v = self._re_array_escape.sub(r'\\\1', v)
            v = f'"{v}"'
        return v

    def _adapt_record(self, v: Any, typ: Any) -> str:
        """Adapt a record parameter with given type."""
        typ = self.get_attnames(typ).values()
        if len(typ) != len(v):
            raise TypeError(f'Record parameter {v} has wrong size')
        adapt = self.adapt
        value = []
        for v, t in zip(v, typ):  # noqa: B020
            v = adapt(v, t)
            if v is None:
                v = ''
            elif not v:
                v = '""'
            else:
                if isinstance(v, bytes):
                    if str is not bytes:
                        v = v.decode('ascii')
                else:
                    v = str(v)
                if self._re_record_quote.search(v):
                    v = self._re_record_escape.sub(r'\\\1', v)
                    v = f'"{v}"'
            value.append(v)
        v = ','.join(value)
        return f'({v})'

    def adapt(self, value: Any, typ: Any =  None) -> str:
        """Adapt a value with known database type."""
        if value is not None and not isinstance(value, Literal):
            if typ:
                simple = self.get_simple_name(typ)
            else:
                typ = simple = self.guess_simple_type(value) or 'text'
            pg_str = getattr(value, '__pg_str__', None)
            if pg_str:
                value = pg_str(typ)
            if simple == 'text':
                pass
            elif simple == 'record':
                if isinstance(value, tuple):
                    value = self._adapt_record(value, typ)
            elif simple.endswith('[]'):
                if isinstance(value, list):
                    adapt = getattr(self, f'_adapt_{simple[:-2]}_array')
                    value = adapt(value)
            else:
                adapt = getattr(self, f'_adapt_{simple}')
                value = adapt(value)
        return value

    @staticmethod
    def simple_type(name: str) -> DbType:
        """Create a simple database type with given attribute names."""
        typ = DbType(name)
        typ.simple = name
        return typ

    @staticmethod
    def get_simple_name(typ: Any) -> str:
        """Get the simple name of a database type."""
        if isinstance(typ, DbType):
            # noinspection PyUnresolvedReferences
            return typ.simple
        return _simpletypes[typ]

    @staticmethod
    def get_attnames(typ: Any) -> dict[str, dict[str, str]]:
        """Get the attribute names of a composite database type."""
        if isinstance(typ, DbType):
            return typ.attnames
        return {}

    @classmethod
    def guess_simple_type(cls, value: Any) -> str | None:
        """Try to guess which database type the given value has."""
        # optimize for most frequent types
        try:
            return _simple_type_dict[type(value)]
        except KeyError:
            pass
        if isinstance(value, (bytes, str)):
            return 'text'
        if isinstance(value, bool):
            return 'bool'
        if isinstance(value, int):
            return 'int'
        if isinstance(value, float):
            return 'float'
        if isinstance(value, Decimal):
            return 'num'
        if isinstance(value, (date, time, datetime, timedelta)):
            return 'date'
        if isinstance(value, Bytea):
            return 'bytea'
        if isinstance(value, Json):
            return 'json'
        if isinstance(value, Hstore):
            return 'hstore'
        if isinstance(value, UUID):
            return 'uuid'
        if isinstance(value, list):
            return (cls.guess_simple_base_type(value) or 'text') + '[]'
        if isinstance(value, tuple):
            simple_type = cls.simple_type
            guess = cls.guess_simple_type

            # noinspection PyUnusedLocal
            def get_attnames(self: DbType) -> AttrDict:
                return AttrDict((str(n + 1), simple_type(guess(v) or 'text'))
                                for n, v in enumerate(value))

            typ = simple_type('record')
            typ._get_attnames = get_attnames
            return typ
        return None

    @classmethod
    def guess_simple_base_type(cls, value: Any) -> str | None:
        """Try to guess the base type of a given array."""
        for v in value:
            if isinstance(v, list):
                typ = cls.guess_simple_base_type(v)
            else:
                typ = cls.guess_simple_type(v)
            if typ:
                return typ
        return None

    def adapt_inline(self, value: Any, nested: bool=False) -> Any:
        """Adapt a value that is put into the SQL and needs to be quoted."""
        if value is None:
            return 'NULL'
        if isinstance(value, Literal):
            return value
        if isinstance(value, Bytea):
            value = self.db.escape_bytea(value).decode('ascii')
        elif isinstance(value, (datetime, date, time, timedelta)):
            value = str(value)
        if isinstance(value, (bytes, str)):
            value = self.db.escape_string(value)
            return f"'{value}'"
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, float):
            if isinf(value):
                return "'-Infinity'" if value < 0 else "'Infinity'"
            if isnan(value):
                return "'NaN'"
            return value
        if isinstance(value, (int, Decimal)):
            return value
        if isinstance(value, list):
            q = self.adapt_inline
            s = '[{}]' if nested else 'ARRAY[{}]'
            return s.format(','.join(str(q(v, nested=True)) for v in value))
        if isinstance(value, tuple):
            q = self.adapt_inline
            return '({})'.format(','.join(str(q(v)) for v in value))
        if isinstance(value, Json):
            value = self.db.escape_string(str(value))
            return f"'{value}'::json"
        if isinstance(value, Hstore):
            value = self.db.escape_string(str(value))
            return f"'{value}'::hstore"
        pg_repr = getattr(value, '__pg_repr__', None)
        if not pg_repr:
            raise InterfaceError(
                f'Do not know how to adapt type {type(value)}')
        value = pg_repr()
        if isinstance(value, (tuple, list)):
            value = self.adapt_inline(value)
        return value

    def parameter_list(self) -> _ParameterList:
        """Return a parameter list for parameters with known database types.

        The list has an add(value, typ) method that will build up the
        list and return either the literal value or a placeholder.
        """
        params = _ParameterList()
        params.adapt = self.adapt
        return params

    def format_query(self, command: str,
                     values: list | tuple | dict | None = None,
                     types: list | tuple | dict | None = None,
                     inline: bool=False
                     ) -> tuple[str, _ParameterList]:
        """Format a database query using the given values and types.

        The optional types describe the values and must be passed as a list,
        tuple or string (that will be split on whitespace) when values are
        passed as a list or tuple, or as a dict if values are passed as a dict.

        If inline is set to True, then parameters will be passed inline
        together with the query string.
        """
        params = self.parameter_list()
        if not values:
            return command, params
        if inline and types:
            raise ValueError('Typed parameters must be sent separately')
        if isinstance(values, (list, tuple)):
            if inline:
                adapt = self.adapt_inline
                seq_literals = [adapt(value) for value in values]
            else:
                add = params.add
                if types:
                    if isinstance(types, str):
                        types = types.split()
                    if (not isinstance(types, (list, tuple))
                            or len(types) != len(values)):
                        raise TypeError('The values and types do not match')
                    seq_literals = [add(value, typ)
                                    for value, typ in zip(values, types)]
                else:
                    seq_literals = [add(value) for value in values]
            command %= tuple(seq_literals)
        elif isinstance(values, dict):
            # we want to allow extra keys in the dictionary,
            # so we first must find the values actually used in the command
            used_values = {}
            map_literals = dict.fromkeys(values, '')
            for key in values:
                del map_literals[key]
                try:
                    command % map_literals
                except KeyError:
                    used_values[key] = values[key]  # pyright: ignore
                map_literals[key] = ''
            if inline:
                adapt = self.adapt_inline
                map_literals = {key: adapt(value)
                            for key, value in used_values.items()}
            else:
                add = params.add
                if types:
                    if not isinstance(types, dict):
                        raise TypeError('The values and types do not match')
                    map_literals = {key: add(used_values[key], types.get(key))
                                for key in sorted(used_values)}
                else:
                    map_literals = {key: add(used_values[key])
                                for key in sorted(used_values)}
            command %= map_literals
        else:
            raise TypeError('The values must be passed as tuple, list or dict')
        return command, params


def cast_bool(value: str) -> Any:
    """Cast a boolean value."""
    if not get_bool():
        return value
    return value[0] == 't'


def cast_json(value: str) -> Any:
    """Cast a JSON value."""
    cast = get_jsondecode()
    if not cast:
        return value
    return cast(value)


def cast_num(value: str) -> Any:
    """Cast a numeric value."""
    return (get_decimal() or float)(value)


def cast_money(value: str) -> Any:
    """Cast a money value."""
    point = get_decimal_point()
    if not point:
        return value
    if point != '.':
        value = value.replace(point, '.')
    value = value.replace('(', '-')
    value = ''.join(c for c in value if c.isdigit() or c in '.-')
    return (get_decimal() or float)(value)


def cast_int2vector(value: str) -> list[int]:
    """Cast an int2vector value."""
    return [int(v) for v in value.split()]


def cast_date(value: str, connection: DB) -> Any:
    """Cast a date value."""
    # The output format depends on the server setting DateStyle.  The default
    # setting ISO and the setting for German are actually unambiguous.  The
    # order of days and months in the other two settings is however ambiguous,
    # so at least here we need to consult the setting to properly parse values.
    if value == '-infinity':
        return date.min
    if value == 'infinity':
        return date.max
    values = value.split()
    if values[-1] == 'BC':
        return date.min
    value = values[0]
    if len(value) > 10:
        return date.max
    format = connection.date_format()
    return datetime.strptime(value, format).date()


def cast_time(value: str) -> Any:
    """Cast a time value."""
    format = '%H:%M:%S.%f' if len(value) > 8 else '%H:%M:%S'
    return datetime.strptime(value, format).time()


_re_timezone = regex('(.*)([+-].*)')


def cast_timetz(value: str) -> Any:
    """Cast a timetz value."""
    m = _re_timezone.match(value)
    if m:
        value, tz = m.groups()
    else:
        tz = '+0000'
    format = '%H:%M:%S.%f' if len(value) > 8 else '%H:%M:%S'
    value += _timezone_as_offset(tz)
    format += '%z'
    return datetime.strptime(value, format).timetz()


def cast_timestamp(value: str, connection: DB) -> Any:
    """Cast a timestamp value."""
    if value == '-infinity':
        return datetime.min
    if value == 'infinity':
        return datetime.max
    values = value.split()
    if values[-1] == 'BC':
        return datetime.min
    format = connection.date_format()
    if format.endswith('-%Y') and len(values) > 2:
        values = values[1:5]
        if len(values[3]) > 4:
            return datetime.max
        formats = ['%d %b' if format.startswith('%d') else '%b %d',
                   '%H:%M:%S.%f' if len(values[2]) > 8 else '%H:%M:%S', '%Y']
    else:
        if len(values[0]) > 10:
            return datetime.max
        formats = [format, '%H:%M:%S.%f' if len(values[1]) > 8 else '%H:%M:%S']
    return datetime.strptime(' '.join(values), ' '.join(formats))


def cast_timestamptz(value: str, connection: DB) -> Any:
    """Cast a timestamptz value."""
    if value == '-infinity':
        return datetime.min
    if value == 'infinity':
        return datetime.max
    values = value.split()
    if values[-1] == 'BC':
        return datetime.min
    format = connection.date_format()
    if format.endswith('-%Y') and len(values) > 2:
        values = values[1:]
        if len(values[3]) > 4:
            return datetime.max
        formats = ['%d %b' if format.startswith('%d') else '%b %d',
                   '%H:%M:%S.%f' if len(values[2]) > 8 else '%H:%M:%S', '%Y']
        values, tz = values[:-1], values[-1]
    else:
        if format.startswith('%Y-'):
            m = _re_timezone.match(values[1])
            if m:
                values[1], tz = m.groups()
            else:
                tz = '+0000'
        else:
            values, tz = values[:-1], values[-1]
        if len(values[0]) > 10:
            return datetime.max
        formats = [format, '%H:%M:%S.%f' if len(values[1]) > 8 else '%H:%M:%S']
    values.append(_timezone_as_offset(tz))
    formats.append('%z')
    return datetime.strptime(' '.join(values), ' '.join(formats))


_re_interval_sql_standard = regex(
    '(?:([+-])?([0-9]+)-([0-9]+) ?)?'
    '(?:([+-]?[0-9]+)(?!:) ?)?'
    '(?:([+-])?([0-9]+):([0-9]+):([0-9]+)(?:\\.([0-9]+))?)?')

_re_interval_postgres = regex(
    '(?:([+-]?[0-9]+) ?years? ?)?'
    '(?:([+-]?[0-9]+) ?mons? ?)?'
    '(?:([+-]?[0-9]+) ?days? ?)?'
    '(?:([+-])?([0-9]+):([0-9]+):([0-9]+)(?:\\.([0-9]+))?)?')

_re_interval_postgres_verbose = regex(
    '@ ?(?:([+-]?[0-9]+) ?years? ?)?'
    '(?:([+-]?[0-9]+) ?mons? ?)?'
    '(?:([+-]?[0-9]+) ?days? ?)?'
    '(?:([+-]?[0-9]+) ?hours? ?)?'
    '(?:([+-]?[0-9]+) ?mins? ?)?'
    '(?:([+-])?([0-9]+)(?:\\.([0-9]+))? ?secs?)? ?(ago)?')

_re_interval_iso_8601 = regex(
    'P(?:([+-]?[0-9]+)Y)?'
    '(?:([+-]?[0-9]+)M)?'
    '(?:([+-]?[0-9]+)D)?'
    '(?:T(?:([+-]?[0-9]+)H)?'
    '(?:([+-]?[0-9]+)M)?'
    '(?:([+-])?([0-9]+)(?:\\.([0-9]+))?S)?)?')


def cast_interval(value: str) -> timedelta:
    """Cast an interval value."""
    # The output format depends on the server setting IntervalStyle, but it's
    # not necessary to consult this setting to parse it.  It's faster to just
    # check all possible formats, and there is no ambiguity here.
    m = _re_interval_iso_8601.match(value)
    if m:
        s = [v or '0' for v in m.groups()]
        secs_ago = s.pop(5) == '-'
        d = [int(v) for v in s]
        years, mons, days, hours, mins, secs, usecs = d
        if secs_ago:
            secs = -secs
            usecs = -usecs
    else:
        m = _re_interval_postgres_verbose.match(value)
        if m:
            s, ago = [v or '0' for v in m.groups()[:8]], m.group(9)
            secs_ago = s.pop(5) == '-'
            d = [-int(v) for v in s] if ago else [int(v) for v in s]
            years, mons, days, hours, mins, secs, usecs = d
            if secs_ago:
                secs = - secs
                usecs = -usecs
        else:
            m = _re_interval_postgres.match(value)
            if m and any(m.groups()):
                s = [v or '0' for v in m.groups()]
                hours_ago = s.pop(3) == '-'
                d = [int(v) for v in s]
                years, mons, days, hours, mins, secs, usecs = d
                if hours_ago:
                    hours = -hours
                    mins = -mins
                    secs = -secs
                    usecs = -usecs
            else:
                m = _re_interval_sql_standard.match(value)
                if m and any(m.groups()):
                    s = [v or '0' for v in m.groups()]
                    years_ago = s.pop(0) == '-'
                    hours_ago = s.pop(3) == '-'
                    d = [int(v) for v in s]
                    years, mons, days, hours, mins, secs, usecs = d
                    if years_ago:
                        years = -years
                        mons = -mons
                    if hours_ago:
                        hours = -hours
                        mins = -mins
                        secs = -secs
                        usecs = -usecs
                else:
                    raise ValueError(f'Cannot parse interval: {value}')
    days += 365 * years + 30 * mons
    return timedelta(days=days, hours=hours, minutes=mins,
                     seconds=secs, microseconds=usecs)


class Typecasts(dict):
    """Dictionary mapping database types to typecast functions.

    The cast functions get passed the string representation of a value in
    the database which they need to convert to a Python object.  The
    passed string will never be None since NULL values are already
    handled before the cast function is called.

    Note that the basic types are already handled by the C extension.
    They only need to be handled here as record or array components.
    """

    # the default cast functions
    # (str functions are ignored but have been added for faster access)
    defaults: ClassVar[dict[str, Callable]] = {
        'char': str, 'bpchar': str, 'name': str,
        'text': str, 'varchar': str, 'sql_identifier': str,
        'bool': cast_bool, 'bytea': unescape_bytea,
        'int2': int, 'int4': int, 'serial': int, 'int8': int, 'oid': int,
        'hstore': cast_hstore, 'json': cast_json, 'jsonb': cast_json,
        'float4': float, 'float8': float,
        'numeric': cast_num, 'money': cast_money,
        'date': cast_date, 'interval': cast_interval,
        'time': cast_time, 'timetz': cast_timetz,
        'timestamp': cast_timestamp, 'timestamptz': cast_timestamptz,
        'int2vector': cast_int2vector, 'uuid': UUID,
        'anyarray': cast_array, 'record': cast_record}  # pyright: ignore

    connection: DB | None = None  # set in a connection specific instance

    def __missing__(self, typ: str) -> Callable | None:
        """Create a cast function if it is not cached.

        Note that this class never raises a KeyError,
        but returns None when no special cast function exists.
        """
        if not isinstance(typ, str):
            raise TypeError(f'Invalid type: {typ}')
        cast: Callable | None = self.defaults.get(typ)
        if cast:
            # store default for faster access
            cast = self._add_connection(cast)
            self[typ] = cast
        elif typ.startswith('_'):
            base_cast = self[typ[1:]]
            cast = self.create_array_cast(base_cast)
            if base_cast:
                self[typ] = cast
        else:
            attnames = self.get_attnames(typ)
            if attnames:
                casts = [self[v.pgtype] for v in attnames.values()]
                cast = self.create_record_cast(typ, attnames, casts)
                self[typ] = cast
        return cast

    @staticmethod
    def _needs_connection(func: Callable) -> bool:
        """Check if a typecast function needs a connection argument."""
        try:
            args = get_args(func)
        except (TypeError, ValueError):
            return False
        return 'connection' in args[1:]

    def _add_connection(self, cast: Callable) -> Callable:
        """Add a connection argument to the typecast function if necessary."""
        if not self.connection or not self._needs_connection(cast):
            return cast
        return partial(cast, connection=self.connection)

    def get(self, typ: str, default: Callable | None = None # type: ignore
            ) -> Callable | None:
        """Get the typecast function for the given database type."""
        return self[typ] or default

    def set(self, typ: str | Sequence[str], cast: Callable | None) -> None:
        """Set a typecast function for the specified database type(s)."""
        if isinstance(typ, str):
            typ = [typ]
        if cast is None:
            for t in typ:
                self.pop(t, None)
                self.pop(f'_{t}', None)
        else:
            if not callable(cast):
                raise TypeError("Cast parameter must be callable")
            for t in typ:
                self[t] = self._add_connection(cast)
                self.pop(f'_{t}', None)

    def reset(self, typ: str | Sequence[str] | None = None) -> None:
        """Reset the typecasts for the specified type(s) to their defaults.

        When no type is specified, all typecasts will be reset.
        """
        if typ is None:
            self.clear()
        else:
            if isinstance(typ, str):
                typ = [typ]
            for t in typ:
                self.pop(t, None)

    @classmethod
    def get_default(cls, typ: str) -> Any:
        """Get the default typecast function for the given database type."""
        return cls.defaults.get(typ)

    @classmethod
    def set_default(cls, typ: str | Sequence[str],
                    cast: Callable | None) -> None:
        """Set a default typecast function for the given database type(s)."""
        if isinstance(typ, str):
            typ = [typ]
        defaults = cls.defaults
        if cast is None:
            for t in typ:
                defaults.pop(t, None)
                defaults.pop(f'_{t}', None)
        else:
            if not callable(cast):
                raise TypeError("Cast parameter must be callable")
            for t in typ:
                defaults[t] = cast
                defaults.pop(f'_{t}', None)

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_attnames(self, typ: Any) -> AttrDict:
        """Return the fields for the given record type.

        This method will be replaced with the get_attnames() method of DbTypes.
        """
        return AttrDict()

    # noinspection PyMethodMayBeStatic
    def dateformat(self) -> str:
        """Return the current date format.

        This method will be replaced with the dateformat() method of DbTypes.
        """
        return '%Y-%m-%d'

    def create_array_cast(self, basecast: Callable) -> Callable:
        """Create an array typecast for the given base cast."""
        cast_array = self['anyarray']

        def cast(v: Any) -> list:
            return cast_array(v, basecast)
        return cast

    def create_record_cast(self, name: str, fields: AttrDict,
                           casts: list[Callable]) -> Callable:
        """Create a named record typecast for the given fields and casts."""
        cast_record = self['record']
        record = namedtuple(name, fields)  # type: ignore

        def cast(v: Any) -> record:
            # noinspection PyArgumentList
            return record(*cast_record(v, casts))
        return cast


def get_typecast(typ: str) -> Callable | None:
    """Get the global typecast function for the given database type."""
    return Typecasts.get_default(typ)


def set_typecast(typ: str | Sequence[str], cast: Callable | None) -> None:
    """Set a global typecast function for the given database type(s).

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call db.db_types.reset_typecast().
    """
    Typecasts.set_default(typ, cast)


class DbType(str):
    """Class augmenting the simple type name with additional info.

    The following additional information is provided:

        oid: the PostgreSQL type OID
        pgtype: the internal PostgreSQL data type name
        regtype: the registered PostgreSQL data type name
        simple: the more coarse-grained PyGreSQL type name
        typlen: the internal size, negative if variable
        typtype: b = base type, c = composite type etc.
        category: A = Array, b = Boolean, C = Composite etc.
        delim: delimiter for array types
        relid: corresponding table for composite types
        attnames: attributes for composite types
    """

    oid: int
    pgtype: str
    regtype: str
    simple: str 
    typlen: int
    typtype: str
    category: str
    delim: str
    relid: int

    _get_attnames: Callable[[DbType], AttrDict]

    @property
    def attnames(self) -> AttrDict:
        """Get names and types of the fields of a composite type."""
        # noinspection PyUnresolvedReferences
        return self._get_attnames(self)


class DbTypes(dict):
    """Cache for PostgreSQL data types.

    This cache maps type OIDs and names to DbType objects containing
    information on the associated database type.
    """

    _num_types = frozenset('int float num money int2 int4 int8'
                           ' float4 float8 numeric money'.split())

    def __init__(self, db: DB) -> None:
        """Initialize type cache for connection."""
        super().__init__()
        self._db = weakref.proxy(db)
        self._regtypes = False
        self._typecasts = Typecasts()
        self._typecasts.get_attnames = self.get_attnames  # type: ignore
        self._typecasts.connection = self._db
        self._query_pg_type = (
            "SELECT oid, typname, oid::pg_catalog.regtype,"
            " typlen, typtype, typcategory, typdelim, typrelid"
            " FROM pg_catalog.pg_type"
            " WHERE oid OPERATOR(pg_catalog.=) {}::pg_catalog.regtype")

    def add(self, oid: int, pgtype: str, regtype: str,
            typlen: int, typtype: str, category: str, delim: str, relid: int
            ) -> DbType:
        """Create a PostgreSQL type name with additional info."""
        if oid in self:
            return self[oid]
        simple = 'record' if relid else _simpletypes[pgtype]
        typ = DbType(regtype if self._regtypes else simple)
        typ.oid = oid
        typ.simple = simple
        typ.pgtype = pgtype
        typ.regtype = regtype
        typ.typlen = typlen
        typ.typtype = typtype
        typ.category = category
        typ.delim = delim
        typ.relid = relid
        typ._get_attnames = self.get_attnames  # type: ignore
        return typ

    def __missing__(self, key: int | str) -> DbType:
        """Get the type info from the database if it is not cached."""
        try:
            cmd = self._query_pg_type.format(_quote_if_unqualified('$1', key))
            res = self._db.query(cmd, (key,)).getresult()
        except ProgrammingError:
            res = None
        if not res:
            raise KeyError(f'Type {key} could not be found')
        res = res[0]
        typ = self.add(*res)
        self[typ.oid] = self[typ.pgtype] = typ
        return typ

    def get(self, key: int | str,  # type: ignore
            default: DbType | None = None) -> DbType | None:
        """Get the type even if it is not cached."""
        try:
            return self[key]
        except KeyError:
            return default

    def get_attnames(self, typ: Any) -> AttrDict | None:
        """Get names and types of the fields of a composite type."""
        if not isinstance(typ, DbType):
            typ = self.get(typ)
            if not typ:
                return None
        if not typ.relid:
            return None
        return self._db.get_attnames(typ.relid, with_oid=False)

    def get_typecast(self, typ: Any) -> Callable | None:
        """Get the typecast function for the given database type."""
        return self._typecasts.get(typ)

    def set_typecast(self, typ: str | Sequence[str], cast: Callable) -> None:
        """Set a typecast function for the specified database type(s)."""
        self._typecasts.set(typ, cast)

    def reset_typecast(self, typ: str | Sequence[str] | None = None) -> None:
        """Reset the typecast function for the specified database type(s)."""
        self._typecasts.reset(typ)

    def typecast(self, value: Any, typ: str) -> Any:
        """Cast the given value according to the given database type."""
        if value is None:
            # for NULL values, no typecast is necessary
            return None
        if not isinstance(typ, DbType):
            db_type = self.get(typ)
            if db_type:
                typ = db_type.pgtype
        cast = self.get_typecast(typ) if typ else None
        if not cast or cast is str:
            # no typecast is necessary
            return value
        return cast(value)


# The result rows for database operations are returned as named tuples
# by default. Since creating namedtuple classes is a somewhat expensive
# operation, we cache up to 1024 of these classes by default.

# noinspection PyUnresolvedReferences
@lru_cache(maxsize=1024)
def _row_factory(names: Sequence[str]) -> Callable[[Sequence], NamedTuple]:
    """Get a namedtuple factory for row results with the given names."""
    try:
        return namedtuple('Row', names, rename=True)._make  # type: ignore
    except ValueError:  # there is still a problem with the field names
        names = [f'column_{n}' for n in range(len(names))]
        return namedtuple('Row', names)._make  # type: ignore


def set_row_factory_size(maxsize: int | None) -> None:
    """Change the size of the namedtuple factory cache.

    If maxsize is set to None, the cache can grow without bound.
    """
    # noinspection PyGlobalUndefined
    global _row_factory
    _row_factory = lru_cache(maxsize)(_row_factory.__wrapped__)


# Helper functions used by the query object

def _dictiter(q: Query) -> Generator[dict[str, Any], None, None]:
    """Get query result as an iterator of dictionaries."""
    fields: tuple[str, ...] = q.listfields()
    for r in q:
        yield dict(zip(fields, r))


def _namediter(q: Query) -> Generator[SomeNamedTuple, None, None]:
    """Get query result as an iterator of named tuples."""
    row = _row_factory(q.listfields())
    for r in q:
        yield row(r)


def _namednext(q: Query) -> SomeNamedTuple:
    """Get next row from query result as a named tuple."""
    return _row_factory(q.listfields())(next(q))


def _scalariter(q: Query) -> Generator[Any, None, None]:
    """Get query result as an iterator of scalar values."""
    for r in q:
        yield r[0]


class _MemoryQuery:
    """Class that embodies a given query result."""

    result: Any
    fields: tuple[str, ...]

    def __init__(self, result: Any, fields: Sequence[str]) -> None:
        """Create query from given result rows and field names."""
        self.result = result
        self.fields = tuple(fields)

    def listfields(self) -> tuple[str, ...]:
        """Return the stored field names of this query."""
        return self.fields

    def getresult(self) -> Any:
        """Return the stored result of this query."""
        return self.result

    def __iter__(self) -> Iterator[Any]:
        return iter(self.result)

# Error messages

E = TypeVar('E', bound=Error)

def _error(msg: str, cls: type[E]) -> E:
    """Return specified error object with empty sqlstate attribute."""
    error = cls(msg)
    if isinstance(error, DatabaseError):
        error.sqlstate = None
    return error


def _db_error(msg: str) -> DatabaseError:
    """Return DatabaseError."""
    return _error(msg, DatabaseError)


def _int_error(msg: str) -> InternalError:
    """Return InternalError."""
    return _error(msg, InternalError)


def _prg_error(msg: str) -> ProgrammingError:
    """Return ProgrammingError."""
    return _error(msg, ProgrammingError)


# Initialize the C module

set_decimal(Decimal)
set_jsondecode(jsondecode)
set_query_helpers(_dictiter, _namediter, _namednext, _scalariter)


# The notification handler

class NotificationHandler:
    """A PostgreSQL client-side asynchronous notification handler."""

    def __init__(self, db: DB, event: str, callback: Callable,
                 arg_dict: dict | None = None,
                 timeout: int | float | None = None,
                 stop_event: str | None = None):
        """Initialize the notification handler.

        You must pass a PyGreSQL database connection, the name of an
        event (notification channel) to listen for and a callback function.

        You can also specify a dictionary arg_dict that will be passed as
        the single argument to the callback function, and a timeout value
        in seconds (a floating point number denotes fractions of seconds).
        If it is absent or None, the callers will never time out.  If the
        timeout is reached, the callback function will be called with a
        single argument that is None.  If you set the timeout to zero,
        the handler will poll notifications synchronously and return.

        You can specify the name of the event that will be used to signal
        the handler to stop listening as stop_event. By default, it will
        be the event name prefixed with 'stop_'.
        """
        self.db: DB | None = db
        self.event = event
        self.stop_event = stop_event or f'stop_{event}'
        self.listening = False
        self.callback = callback
        if arg_dict is None:
            arg_dict = {}
        self.arg_dict = arg_dict
        self.timeout = timeout

    def __del__(self) -> None:
        """Delete the notification handler."""
        self.unlisten()

    def close(self) -> None:
        """Stop listening and close the connection."""
        if self.db:
            self.unlisten()
            self.db.close()
            self.db = None

    def listen(self) -> None:
        """Start listening for the event and the stop event."""
        db = self.db
        if db and not self.listening:
            db.query(f'listen "{self.event}"')
            db.query(f'listen "{self.stop_event}"')
            self.listening = True

    def unlisten(self) -> None:
        """Stop listening for the event and the stop event."""
        db = self.db
        if db and self.listening:
            db.query(f'unlisten "{self.event}"')
            db.query(f'unlisten "{self.stop_event}"')
            self.listening = False

    def notify(self, db: DB | None = None, stop: bool = False,
               payload: str | None = None) -> Query | None:
        """Generate a notification.

        Optionally, you can pass a payload with the notification.

        If you set the stop flag, a stop notification will be sent that
        will cause the handler to stop listening.

        Note: If the notification handler is running in another thread, you
        must pass a different database connection since PyGreSQL database
        connections are not thread-safe.
        """
        if not self.listening:
            return None
        if not db:
            db = self.db
            if not db:
                return None
        event = self.stop_event if stop else self.event
        cmd = f'notify "{event}"'
        if payload:
            cmd += f", '{payload}'"
        return db.query(cmd)

    def __call__(self) -> None:
        """Invoke the notification handler.

        The handler is a loop that listens for notifications on the event
        and stop event channels.  When either of these notifications are
        received, its associated 'pid', 'event' and 'extra' (the payload
        passed with the notification) are inserted into its arg_dict
        dictionary and the callback is invoked with this dictionary as
        a single argument.  When the handler receives a stop event, it
        stops listening to both events and return.

        In the special case that the timeout of the handler has been set
        to zero, the handler will poll all events synchronously and return.
        If will keep listening until it receives a stop event.

        Note: If you run this loop in another thread, don't use the same
        database connection for database operations in the main thread.
        """
        if not self.db:
            return
        self.listen()
        poll = self.timeout == 0
        rlist = [] if poll else [self.db.fileno()]
        while self.db and self.listening:
            # noinspection PyUnboundLocalVariable
            if poll or select.select(rlist, [], [], self.timeout)[0]:
                while self.db and self.listening:
                    notice = self.db.getnotify()
                    if not notice:  # no more messages
                        break
                    event, pid, extra = notice
                    if event not in (self.event, self.stop_event):
                        self.unlisten()
                        raise _db_error(
                            f'Listening for "{self.event}"'
                            f' and "{self.stop_event}",'
                            f' but notified of "{event}"')
                    if event == self.stop_event:
                        self.unlisten()
                    self.arg_dict.update(pid=pid, event=event, extra=extra)
                    self.callback(self.arg_dict)
                if poll:
                    break
            else:   # we timed out
                self.unlisten()
                self.callback(None)


# The actual PostgreSQL database connection interface:

class DB:
    """Wrapper class for the _pg connection type."""

    db: Connection | None = None  # invalid fallback for underlying connection
    _db_args: Any  # either the connectoin args or the underlying connection

    def __init__(self, *args: Any, **kw: Any) -> None:
        """Create a new connection.

        You can pass either the connection parameters or an existing
        _pg or pgdb connection. This allows you to use the methods
        of the classic pg interface with a DB-API 2 pgdb connection.
        """
        if not args and len(kw) == 1:
            db = kw.get('db')
        elif not kw and len(args) == 1:
            db = args[0]
        else:
            db = None
        if db:
            if isinstance(db, DB):
                db = db.db
            else:
                with suppress(AttributeError):
                    # noinspection PyUnresolvedReferences
                    db = db._cnx
        if not db or not hasattr(db, 'db') or not hasattr(db, 'query'):
            db = connect(*args, **kw)
            self._db_args = args, kw
            self._closeable = True
        else:
            self._db_args = db
            self._closeable = False
        self.db = db
        self.dbname = db.db
        self._regtypes = False
        self._attnames: dict[str, AttrDict] = {}
        self._generated: dict[str, frozenset[str]] = {}
        self._pkeys: dict[str, str | tuple[str, ...]] = {}
        self._privileges: dict[tuple[str, str], bool] = {}
        self.adapter = Adapter(self)
        self.dbtypes = DbTypes(self)
        self._query_attnames = (
            "SELECT a.attname,"
            " t.oid, t.typname, t.oid::pg_catalog.regtype,"
            " t.typlen, t.typtype, t.typcategory, t.typdelim, t.typrelid"
            " FROM pg_catalog.pg_attribute a"
            " JOIN pg_catalog.pg_type t"
            " ON t.oid OPERATOR(pg_catalog.=) a.atttypid"
            " WHERE a.attrelid OPERATOR(pg_catalog.=)"
            " {}::pg_catalog.regclass"
            " AND {} AND NOT a.attisdropped ORDER BY a.attnum")
        if db.server_version < 120000:
            self._query_generated = (
                "a.attidentity OPERATOR(pg_catalog.=) 'a'"
            )
        else:
            self._query_generated = (
                "(a.attidentity OPERATOR(pg_catalog.=) 'a' OR"
                " a.attgenerated OPERATOR(pg_catalog.!=) '')"
            )
        db.set_cast_hook(self.dbtypes.typecast)
        # For debugging scripts, self.debug can be set
        # * to a string format specification (e.g. in CGI set to "%s<BR>"),
        # * to a file object to write debug statements or
        # * to a callable object which takes a string argument
        # * to any other true value to just print debug statements
        self.debug: Any = None

    def __getattr__(self, name: str) -> Any:
        """Get the specified attritbute of the connection."""
        # All undefined members are same as in underlying connection:
        if self.db:
            return getattr(self.db, name)
        else:
            raise _int_error('Connection is not valid')

    def __dir__(self) -> list[str]:
        """List all attributes of the connection."""
        # Custom dir function including the attributes of the connection:
        attrs = set(self.__class__.__dict__)
        attrs.update(self.__dict__)
        attrs.update(dir(self.db))
        return sorted(attrs)

    # Context manager methods

    def __enter__(self) -> DB:
        """Enter the runtime context. This will start a transaction."""
        self.begin()
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context. This will end the transaction."""
        if et is None and ev is None and tb is None:
            self.commit()
        else:
            self.rollback()

    def __del__(self) -> None:
        """Delete the connection."""
        try:
            db = self.db
        except AttributeError:
            db = None
        if db:
            with suppress(TypeError):  # when already closed
                db.set_cast_hook(None)
            if self._closeable:
                with suppress(InternalError):  # when already closed
                    db.close()

    # Auxiliary methods

    def _do_debug(self, *args: Any) -> None:
        """Print a debug message."""
        if self.debug:
            s = '\n'.join(str(arg) for arg in args)
            if isinstance(self.debug, str):
                print(self.debug % s)
            elif hasattr(self.debug, 'write'):
                # noinspection PyCallingNonCallable
                self.debug.write(s + '\n')
            elif callable(self.debug):
                self.debug(s)
            else:
                print(s)

    def _escape_qualified_name(self, s: str) -> str:
        """Escape a qualified name.

        Escapes the name for use as an SQL identifier, unless the
        name contains a dot, in which case the name is ambiguous
        (could be a qualified name or just a name with a dot in it)
        and must be quoted manually by the caller.
        """
        if '.' not in s:
            s = self.escape_identifier(s)
        return s

    @staticmethod
    def _make_bool(d: Any) -> bool | str:
        """Get boolean value corresponding to d."""
        return bool(d) if get_bool() else ('t' if d else 'f')

    @staticmethod
    def _list_params(params: Sequence) -> str:
        """Create a human readable parameter list."""
        return ', '.join(f'${n}={v!r}' for n, v in enumerate(params, 1))

    @property
    def _valid_db(self) -> Connection:
        """Get underlying connection and make sure it is not closed."""
        db = self.db
        if not db:
            raise _int_error('Connection already closed')
        return db

    # Public methods

    # escape_string and escape_bytea exist as methods,
    # so we define unescape_bytea as a method as well
    unescape_bytea = staticmethod(unescape_bytea)

    @staticmethod
    def decode_json(s: str) -> Any:
        """Decode a JSON string coming from the database."""
        return (get_jsondecode() or jsondecode)(s)

    @staticmethod
    def encode_json(d: Any) -> str:
        """Encode a JSON string for use within SQL."""
        return jsonencode(d)

    def close(self) -> None:
        """Close the database connection."""
        # Wraps shared library function so we can track state.
        db = self._valid_db
        with suppress(TypeError):  # when already closed
            db.set_cast_hook(None)
        if self._closeable:
            db.close()
        self.db = None

    def reset(self) -> None:
        """Reset connection with current parameters.

        All derived queries and large objects derived from this connection
        will not be usable after this call.
        """
        self._valid_db.reset()

    def reopen(self) -> None:
        """Reopen connection to the database.

        Used in case we need another connection to the same database.
        Note that we can still reopen a database that we have closed.
        """
        # There is no such shared library function.
        if self._closeable:
            args, kw = self._db_args
            db = connect(*args, **kw)
            if self.db:
                self.db.set_cast_hook(None)
                self.db.close()
            db.set_cast_hook(self.dbtypes.typecast)
            self.db = db
        else:
            self.db = self._db_args

    def begin(self, mode: str | None = None) -> Query:
        """Begin a transaction."""
        qstr = 'BEGIN'
        if mode:
            qstr += ' ' + mode
        return self.query(qstr)

    start = begin

    def commit(self) -> Query:
        """Commit the current transaction."""
        return self.query('COMMIT')

    end = commit

    def rollback(self, name: str | None = None) -> Query:
        """Roll back the current transaction."""
        qstr = 'ROLLBACK'
        if name:
            qstr += ' TO ' + name
        return self.query(qstr)

    abort = rollback

    def savepoint(self, name: str) -> Query:
        """Define a new savepoint within the current transaction."""
        return self.query('SAVEPOINT ' + name)

    def release(self, name: str) -> Query:
        """Destroy a previously defined savepoint."""
        return self.query('RELEASE ' + name)

    def get_parameter(self,
                      parameter: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str] | dict[str, Any]
                      ) -> str | list[str] | dict[str, str]:
        """Get the value of a run-time parameter.

        If the parameter is a string, the return value will also be a string
        that is the current setting of the run-time parameter with that name.

        You can get several parameters at once by passing a list, set or dict.
        When passing a list of parameter names, the return value will be a
        corresponding list of parameter settings.  When passing a set of
        parameter names, a new dict will be returned, mapping these parameter
        names to their settings.  Finally, if you pass a dict as parameter,
        its values will be set to the current parameter settings corresponding
        to its keys.

        By passing the special name 'all' as the parameter, you can get a dict
        of all existing configuration parameters.
        """
        values: Any
        if isinstance(parameter, str):
            parameter = [parameter]
            values = None
        elif isinstance(parameter, (list, tuple)):
            values = []
        elif isinstance(parameter, (set, frozenset)):
            values = {}
        elif isinstance(parameter, dict):
            values = parameter
        else:
            raise TypeError(
                'The parameter must be a string, list, set or dict')
        if not parameter:
            raise TypeError('No parameter has been specified')
        query = self._valid_db.query
        params: Any = {} if isinstance(values, dict) else []
        for param_key in parameter:
            param = param_key.strip().lower() if isinstance(
                param_key, (bytes, str)) else None
            if not param:
                raise TypeError('Invalid parameter')
            if param == 'all':
                cmd = 'SHOW ALL'
                values = query(cmd).getresult()
                values = {value[0]: value[1] for value in values}
                break
            if isinstance(params, dict):
                params[param] = param_key
            else:
                params.append(param)
        else:
            for param in params:
                cmd = f'SHOW {param}'
                value = query(cmd).singlescalar()
                if values is None:
                    values = value
                elif isinstance(values, list):
                    values.append(value)
                else:
                    values[params[param]] = value
        return values

    def set_parameter(self,
                      parameter: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str] | dict[str, Any],
                      value: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str]| None = None,
                      local: bool = False) -> None:
        """Set the value of a run-time parameter.

        If the parameter and the value are strings, the run-time parameter
        will be set to that value.  If no value or None is passed as a value,
        then the run-time parameter will be restored to its default value.

        You can set several parameters at once by passing a list of parameter
        names, together with a single value that all parameters should be
        set to or with a corresponding list of values.  You can also pass
        the parameters as a set if you only provide a single value.
        Finally, you can pass a dict with parameter names as keys.  In this
        case, you should not pass a value, since the values for the parameters
        will be taken from the dict.

        By passing the special name 'all' as the parameter, you can reset
        all existing settable run-time parameters to their default values.

        If you set local to True, then the command takes effect for only the
        current transaction.  After commit() or rollback(), the session-level
        setting takes effect again.  Setting local to True will appear to
        have no effect if it is executed outside a transaction, since the
        transaction will end immediately.
        """
        if isinstance(parameter, str):
            parameter = {parameter: value}
        elif isinstance(parameter, (list, tuple)):
            if isinstance(value, (list, tuple)):
                parameter = dict(zip(parameter, value))
            else:
                parameter = dict.fromkeys(parameter, value)
        elif isinstance(parameter, (set, frozenset)):
            if isinstance(value, (list, tuple, set, frozenset)):
                value = set(value)
                if len(value) == 1:
                    value = next(iter(value))
            if not (value is None or isinstance(value, str)):
                raise ValueError(
                    'A single value must be specified'
                    ' when parameter is a set')
            parameter = dict.fromkeys(parameter, value)
        elif isinstance(parameter, dict):
            if value is not None:
                raise ValueError(
                    'A value must not be specified'
                    ' when parameter is a dictionary')
        else:
            raise TypeError(
                'The parameter must be a string, list, set or dict')
        if not parameter:
            raise TypeError('No parameter has been specified')
        params: dict[str, str | None] = {}
        for param, param_value in parameter.items():
            param = param.strip().lower() if isinstance(param, str) else None
            if not param:
                raise TypeError('Invalid parameter')
            if param == 'all':
                if param_value is not None:
                    raise ValueError(
                        'A value must not be specified'
                        " when parameter is 'all'")
                params = {'all': None}
                break
            params[param] = param_value
        local_clause = ' LOCAL' if local else ''
        for param, param_value in params.items():
            cmd = (f'RESET{local_clause} {param}'
                   if param_value is None else
                   f'SET{local_clause} {param} TO {param_value}')
            self._do_debug(cmd)
            self._valid_db.query(cmd)

    def query(self, command: str, *args: Any) -> Query:
        """Execute a SQL command string.

        This method simply sends a SQL query to the database.  If the query is
        an insert statement that inserted exactly one row into a table that
        has OIDs, the return value is the OID of the newly inserted row.
        If the query is an update or delete statement, or an insert statement
        that did not insert exactly one row in a table with OIDs, then the
        number of rows affected is returned as a string.  If it is a statement
        that returns rows as a result (usually a select statement, but maybe
        also an "insert/update ... returning" statement), this method returns
        a Query object that can be accessed via getresult() or dictresult()
        or simply printed.  Otherwise, it returns `None`.

        The query can contain numbered parameters of the form $1 in place
        of any data constant.  Arguments given after the query string will
        be substituted for the corresponding numbered parameter.  Parameter
        values can also be given as a single list or tuple argument.
        """
        # Wraps shared library function for debugging.
        db = self._valid_db
        if args:
            self._do_debug(command, args)
            return db.query(command, args)
        self._do_debug(command)
        return db.query(command)

    def query_formatted(self, command: str,
                        parameters: tuple | list | dict | None = None,
                        types: tuple | list | dict | None = None,
                        inline: bool =False) -> Query:
        """Execute a formatted SQL command string.

        Similar to query, but using Python format placeholders of the form
        %s or %(names)s instead of PostgreSQL placeholders of the form $1.
        The parameters must be passed as a tuple, list or dict.  You can
        also pass a corresponding tuple, list or dict of database types in
        order to format the parameters properly in case there is ambiguity.

        If you set inline to True, the parameters will be sent to the database
        embedded in the SQL command, otherwise they will be sent separately.
        """
        return self.query(*self.adapter.format_query(
            command, parameters, types, inline))

    def query_prepared(self, name: str, *args: Any) -> Query:
        """Execute a prepared SQL statement.

        This works like the query() method, except that instead of passing
        the SQL command, you pass the name of a prepared statement.  If you
        pass an empty name, the unnamed statement will be executed.
        """
        if name is None:
            name = ''
        db = self._valid_db
        if args:
            self._do_debug('EXECUTE', name, args)
            return db.query_prepared(name, args)
        self._do_debug('EXECUTE', name)
        return db.query_prepared(name)

    def prepare(self, name: str, command: str) -> None:
        """Create a prepared SQL statement.

        This creates a prepared statement for the given command with the
        given name for later execution with the query_prepared() method.

        The name can be empty to create an unnamed statement, in which case
        any pre-existing unnamed statement is automatically replaced;
        otherwise it is an error if the statement name is already
        defined in the current database session. We recommend always using
        named queries, since unnamed queries have a limited lifetime and
        can be automatically replaced or destroyed by various operations.
        """
        if name is None:
            name = ''
        self._do_debug('prepare', name, command)
        self._valid_db.prepare(name, command)

    def describe_prepared(self, name: str | None = None) -> Query:
        """Describe a prepared SQL statement.

        This method returns a Query object describing the result columns of
        the prepared statement with the given name. If you omit the name,
        the unnamed statement will be described if you created one before.
        """
        if name is None:
            name = ''
        return self._valid_db.describe_prepared(name)

    def delete_prepared(self, name: str | None = None) -> Query:
        """Delete a prepared SQL statement.

        This deallocates a previously prepared SQL statement with the given
        name, or deallocates all prepared statements if you do not specify a
        name. Note that prepared statements are also deallocated automatically
        when the current session ends.
        """
        if not name:
            name = 'ALL'
        cmd = f"DEALLOCATE {name}"
        self._do_debug(cmd)
        return self._valid_db.query(cmd)

    def pkey(self, table: str, composite: bool = False, flush: bool = False
             ) -> str | tuple[str, ...]:
        """Get the primary key of a table.

        Single primary keys are returned as strings unless you
        set the composite flag.  Composite primary keys are always
        represented as tuples.  Note that this raises a KeyError
        if the table does not have a primary key.

        If flush is set then the internal cache for primary keys will
        be flushed.  This may be necessary after the database schema or
        the search path has been changed.
        """
        pkeys = self._pkeys
        if flush:
            pkeys.clear()
            self._do_debug('The pkey cache has been flushed')
        try:  # cache lookup
            pkey = pkeys[table]
        except KeyError as e:  # cache miss, check the database
            cmd = ("SELECT"  # noqa: S608
                   " a.attname, a.attnum, i.indkey"
                   " FROM pg_catalog.pg_index i"
                   " JOIN pg_catalog.pg_attribute a"
                   " ON a.attrelid OPERATOR(pg_catalog.=) i.indrelid"
                   " AND a.attnum OPERATOR(pg_catalog.=) ANY(i.indkey)"
                   " AND NOT a.attisdropped"
                   " WHERE i.indrelid OPERATOR(pg_catalog.=)"
                   " {}::pg_catalog.regclass"
                   " AND i.indisprimary ORDER BY a.attnum").format(
                  _quote_if_unqualified('$1', table))
            res = self._valid_db.query(cmd, (table,)).getresult()
            if not res:
                raise KeyError(f'Table {table} has no primary key') from e
            # we want to use the order defined in the primary key index here,
            # not the order as defined by the columns in the table
            if len(res) > 1:
                indkey = res[0][2]
                pkey = tuple(row[0] for row in sorted(
                    res, key=lambda row: indkey.index(row[1])))
            else:
                pkey = res[0][0]
            pkeys[table] = pkey  # cache it
        if composite and not isinstance(pkey, tuple):
            pkey = (pkey,)
        return pkey

    def pkeys(self, table: str) -> tuple[str, ...]:
        """Get the primary key of a table as a tuple.

        Same as pkey() with 'composite' set to True.
        """
        return self.pkey(table, True)  # type: ignore

    def get_databases(self) -> list[str]:
        """Get list of databases in the system."""
        return [r[0] for r in self._valid_db.query(
            'SELECT datname FROM pg_catalog.pg_database').getresult()]

    def get_relations(self, kinds: str | Sequence[str] | None = None,
                      system: bool = False) -> list[str]:
        """Get list of relations in connected database of specified kinds.

        If kinds is None or empty, all kinds of relations are returned.
        Otherwise, kinds can be a string or sequence of type letters
        specifying which kind of relations you want to list.

        Set the system flag if you want to get the system relations as well.
        """
        where_parts = []
        if kinds:
            where_parts.append(
                "r.relkind IN ({})".format(','.join(f"'{k}'" for k in kinds)))
        if not system:
            where_parts.append("s.nspname NOT SIMILAR"
                               " TO 'pg/_%|information/_schema' ESCAPE '/'")
        where = " WHERE " + ' AND '.join(where_parts) if where_parts else ''
        cmd = ("SELECT"  # noqa: S608
               " pg_catalog.quote_ident(s.nspname) OPERATOR(pg_catalog.||)"
               " '.' OPERATOR(pg_catalog.||) pg_catalog.quote_ident(r.relname)"
               " FROM pg_catalog.pg_class r"
               " JOIN pg_catalog.pg_namespace s"
               f" ON s.oid OPERATOR(pg_catalog.=) r.relnamespace{where}"
               " ORDER BY s.nspname, r.relname")
        return [r[0] for r in self._valid_db.query(cmd).getresult()]

    def get_tables(self, system: bool = False) -> list[str]:
        """Return list of tables in connected database.

        Set the system flag if you want to get the system tables as well.
        """
        return self.get_relations('r', system)

    def get_attnames(self, table: str, with_oid: bool=True, flush: bool=False
                     ) -> AttrDict:
        """Given the name of a table, dig out the set of attribute names.

        Returns a read-only dictionary of attribute names (the names are
        the keys, the values are the names of the attributes' types)
        with the column names in the proper order if you iterate over it.

        If flush is set, then the internal cache for attribute names will
        be flushed. This may be necessary after the database schema or
        the search path has been changed.

        By default, only a limited number of simple types will be returned.
        You can get the registered types after calling use_regtypes(True).
        """
        attnames = self._attnames
        if flush:
            attnames.clear()
            self._do_debug('The attnames cache has been flushed')
        try:  # cache lookup
            names = attnames[table]
        except KeyError:  # cache miss, check the database
            cmd = "a.attnum OPERATOR(pg_catalog.>) 0"
            if with_oid:
                cmd = f"({cmd} OR a.attname OPERATOR(pg_catalog.=) 'oid')"
            cmd = self._query_attnames.format(
                _quote_if_unqualified('$1', table), cmd)
            res = self._valid_db.query(cmd, (table,)).getresult()
            types = self.dbtypes
            names = AttrDict((name[0], types.add(*name[1:])) for name in res)
            attnames[table] = names  # cache it
        return names

    def get_generated(self, table: str, flush: bool = False) -> frozenset[str]:
        """Given the name of a table, dig out the set of generated columns.

        Returns a set of column names that are generated and unalterable.

        If flush is set, then the internal cache for generated columns will
        be flushed. This may be necessary after the database schema or
        the search path has been changed.
        """
        generated = self._generated
        if flush:
            generated.clear()
            self._do_debug('The generated cache has been flushed')
        try:  # cache lookup
            names = generated[table]
        except KeyError:  # cache miss, check the database
            cmd = "a.attnum OPERATOR(pg_catalog.>) 0"
            cmd = f"{cmd} AND {self._query_generated}"
            cmd = self._query_attnames.format(
                _quote_if_unqualified('$1', table), cmd)
            res = self._valid_db.query(cmd, (table,)).getresult()
            names = frozenset(name[0] for name in res)
            generated[table] = names  # cache it
        return names

    def use_regtypes(self, regtypes: bool | None = None) -> bool:
        """Use registered type names instead of simplified type names."""
        if regtypes is None:
            return self.dbtypes._regtypes
        regtypes = bool(regtypes)
        if regtypes != self.dbtypes._regtypes:
            self.dbtypes._regtypes = regtypes
            self._attnames.clear()
            self.dbtypes.clear()
        return regtypes

    def has_table_privilege(self, table: str, privilege: str = 'select',
                            flush: bool = False) -> bool:
        """Check whether current user has specified table privilege.

        If flush is set, then the internal cache for table privileges will
        be flushed. This may be necessary after privileges have been changed.
        """
        privileges = self._privileges
        if flush:
            privileges.clear()
            self._do_debug('The privileges cache has been flushed')
        privilege = privilege.lower()
        try:  # ask cache
            ret = privileges[table, privilege]
        except KeyError:  # cache miss, ask the database
            cmd = "SELECT pg_catalog.has_table_privilege({}, $2)".format(
                _quote_if_unqualified('$1', table))
            query = self._valid_db.query(cmd, (table, privilege))
            ret = query.singlescalar() == self._make_bool(True)
            privileges[table, privilege] = ret  # cache it
        return ret

    def get(self, table: str, row: Any,
            keyname: str | tuple[str, ...] | None = None) -> dict[str, Any]:
        """Get a row from a database table or view.

        This method is the basic mechanism to get a single row.  It assumes
        that the keyname specifies a unique row.  It must be the name of a
        single column or a tuple of column names.  If the keyname is not
        specified, then the primary key for the table is used.

        If row is a dictionary, then the value for the key is taken from it.
        Otherwise, the row must be a single value or a tuple of values
        corresponding to the passed keyname or primary key.  The fetched row
        from the table will be returned as a new dictionary or used to replace
        the existing values when row was passed as a dictionary.

        The OID is also put into the dictionary if the table has one, but
        in order to allow the caller to work with multiple tables, it is
        munged as "oid(table)" using the actual name of the table.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        attnames = self.get_attnames(table)
        qoid = _oid_key(table) if 'oid' in attnames else None
        if keyname and isinstance(keyname, str):
            keyname = (keyname,)
        if qoid and isinstance(row, dict) and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if not keyname:
            try:  # if keyname is not specified, try using the primary key
                keyname = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                # try using the oid instead
                if qoid and isinstance(row, dict) and 'oid' in row:
                    keyname = ('oid',)
                else:
                    raise _prg_error(
                        f'Table {table} has no primary key') from e
            else:  # the table has a primary key
                # check whether all key columns have values
                if isinstance(row, dict) and not set(keyname).issubset(row):
                    # try using the oid instead
                    if qoid and 'oid' in row:
                        keyname = ('oid',)
                    else:
                        raise KeyError(
                            'Missing value in row for specified keyname')
        if not isinstance(row, dict):
            if not isinstance(row, (tuple, list)):
                row = [row]
            if len(keyname) != len(row):
                raise KeyError(
                    'Differing number of items in keyname and row')
            row = dict(zip(keyname, row))
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        what = 'oid, *' if qoid else '*'
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keyname)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        t = self._escape_qualified_name(table)
        cmd = f'SELECT {what} FROM {t} WHERE {where} LIMIT 1'  # noqa: S608s
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if not res:
            # make where clause in error message better readable
            where = where.replace('OPERATOR(pg_catalog.=)', '=')
            raise _db_error(
                f'No such record in {table}\nwhere {where}\nwith '
                + self._list_params(params))
        for n, value in res[0].items():
            if qoid and n == 'oid':
                n = qoid
            row[n] = value
        return row

    def insert(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> dict[str, Any]:
        """Insert a row into a database table.

        This method inserts a row into a table.  The name of the table must
        be passed as the first parameter.  The other parameters are used for
        providing the data of the row that shall be inserted into the table.
        If a dictionary is supplied as the second parameter, it starts with
        that.  Otherwise, it uses a blank dictionary.
        Either way the dictionary is updated from the keywords.

        The dictionary is then reloaded with the values actually inserted in
        order to pick up values modified by rules, triggers, etc.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        if row is None:
            row = {}
        row.update(kw)
        if 'oid' in row:
            del row['oid']  # do not insert oid
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = _oid_key(table) if 'oid' in attnames else None
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        name_list, value_list = [], []
        for n in attnames:
            if n in row and n not in generated:
                name_list.append(col(n))
                value_list.append(adapt(row[n], attnames[n]))
        if not name_list:
            raise _prg_error('No column found that can be inserted')
        names, values = ', '.join(name_list), ', '.join(value_list)
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'INSERT INTO {t} ({names})'  # noqa: S608
               f' VALUES ({values}) RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # this should always be true
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        return row

    def update(self, table: str, row: dict[str, Any] | None = None, **kw : Any
               ) -> dict[str, Any]:
        """Update an existing row in a database table.

        Similar to insert, but updates an existing row.  The update is based
        on the primary key of the table or the OID value as munged by get()
        or passed as keyword.  The OID will take precedence if provided, so
        that it is possible to update the primary key itself.

        The dictionary is then modified to reflect any changes caused by the
        update due to triggers, rules, default values, etc.
        """
        if table.endswith('*'):
            table = table[:-1].rstrip()  # need parent table name
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = _oid_key(table) if 'oid' in attnames else None
        if row is None:
            row = {}
        elif 'oid' in row:
            del row['oid']  # only accept oid key from named args for safety
        row.update(kw)
        if qoid and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if qoid and 'oid' in row:  # try using the oid
            keynames: tuple[str, ...] = ('oid',)
            keyset = set(keynames)
        else:  # try using the primary key
            try:
                keynames = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                raise _prg_error(f'Table {table} has no primary key') from e
            keyset = set(keynames)
            # check whether all key columns have values
            if not keyset.issubset(row):
                raise KeyError('Missing value for primary key in row')
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keynames)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        values_list = []
        for n in attnames:
            if n in row and n not in keyset and n not in generated:
                values_list.append(f'{col(n)} = {adapt(row[n], attnames[n])}')
        if not values_list:
            return row
        values = ', '.join(values_list)
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'UPDATE {t} SET {values}'  # noqa: S608
               f' WHERE {where} RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # may be empty when row does not exist
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        return row

    def upsert(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> dict[str, Any]:
        """Insert a row into a database table with conflict resolution.

        This method inserts a row into a table, but instead of raising a
        ProgrammingError exception in case a row with the same primary key
        already exists, an update will be executed instead.  This will be
        performed as a single atomic operation on the database, so race
        conditions can be avoided.

        Like the insert method, the first parameter is the name of the
        table and the second parameter can be used to pass the values to
        be inserted as a dictionary.

        Unlike the insert und update statement, keyword parameters are not
        used to modify the dictionary, but to specify which columns shall
        be updated in case of a conflict, and in which way:

        A value of False or None means the column shall not be updated,
        a value of True means the column shall be updated with the value
        that has been proposed for insertion, i.e. has been passed as value
        in the dictionary.  Columns that are not specified by keywords but
        appear as keys in the dictionary are also updated like in the case
        keywords had been passed with the value True.

        So if in the case of a conflict you want to update every column
        that has been passed in the dictionary row, you would call
        upsert(table, row). If you don't want to do anything in case
        of a conflict, i.e. leave the existing row as it is, call
        upsert(table, row, **dict.fromkeys(row)).

        If you need more fine-grained control of what gets updated, you can
        also pass strings in the keyword parameters.  These strings will
        be used as SQL expressions for the update columns.  In these
        expressions you can refer to the value that already exists in
        the table by prefixing the column name with "included.", and to
        the value that has been proposed for insertion by prefixing the
        column name with the "excluded."

        The dictionary is modified in any case to reflect the values in
        the database after the operation has completed.

        Note: The method uses the PostgreSQL "upsert" feature which is
        only available since PostgreSQL 9.5.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        if row is None:
            row = {}
        if 'oid' in row:
            del row['oid']  # do not insert oid
        if 'oid' in kw:
            del kw['oid']  # do not update oid
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = _oid_key(table) if 'oid' in attnames else None
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        name_list, value_list = [], []
        for n in attnames:
            if n in row and n not in generated:
                name_list.append(col(n))
                value_list.append(adapt(row[n], attnames[n]))
        names, values = ', '.join(name_list), ', '.join(value_list)
        try:
            keynames = self.pkeys(table)
        except KeyError as e:
            raise _prg_error(f'Table {table} has no primary key') from e
        target = ', '.join(col(k) for k in keynames)
        update = []
        keyset = set(keynames)
        keyset.add('oid')
        for n in attnames:
            if n not in keyset and n not in generated:
                value = kw.get(n, n in row)
                if value:
                    if not isinstance(value, str):
                        value = f'excluded.{col(n)}'
                    update.append(f'{col(n)} = {value}')
        if not values:
            return row
        do = 'update set ' + ', '.join(update) if update else 'nothing'
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'INSERT INTO {t} AS included ({names})'  # noqa: S608
               f' VALUES ({values})'
               f' ON CONFLICT ({target}) DO {do} RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # may be empty with "do nothing"
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        else:
            self.get(table, row)
        return row

    def clear(self, table: str, row: dict[str, Any] | None = None
              ) -> dict[str, Any]:
        """Clear all the attributes to values determined by the types.

        Numeric types are set to 0, Booleans are set to false, and everything
        else is set to the empty string.  If the row argument is present,
        it is used as the row dictionary and any entries matching attribute
        names are cleared with everything else left unchanged.
        """
        # At some point we will need a way to get defaults from a table.
        if row is None:
            row = {}  # empty if argument is not present
        attnames = self.get_attnames(table)
        for n, t in attnames.items():
            if n == 'oid':
                continue
            t = t.simple
            if t in DbTypes._num_types:
                row[n] = 0
            elif t == 'bool':
                row[n] = self._make_bool(False)
            else:
                row[n] = ''
        return row

    def delete(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> int:
        """Delete an existing row in a database table.

        This method deletes the row from a table.  It deletes based on the
        primary key of the table or the OID value as munged by get() or
        passed as keyword.  The OID will take precedence if provided.

        The return value is the number of deleted rows (i.e. 0 if the row
        did not exist and 1 if the row was deleted).

        Note that if the row cannot be deleted because e.g. it is still
        referenced by another table, this method raises a ProgrammingError.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        attnames = self.get_attnames(table)
        qoid = _oid_key(table) if 'oid' in attnames else None
        if row is None:
            row = {}
        elif 'oid' in row:
            del row['oid']  # only accept oid key from named args for safety
        row.update(kw)
        if qoid and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if qoid and 'oid' in row:  # try using the oid
            keynames: tuple[str, ...] = ('oid',)
        else:  # try using the primary key
            try:
                keynames = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                raise _prg_error(f'Table {table} has no primary key') from e
            # check whether all key columns have values
            if not set(keynames).issubset(row):
                raise KeyError('Missing value for primary key in row')
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keynames)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        t = self._escape_qualified_name(table)
        cmd = f'DELETE FROM {t} WHERE {where}'  # noqa: S608
        self._do_debug(cmd, params)
        res = self._valid_db.query(cmd, params)
        return int(res)  # type: ignore

    def truncate(self, table: str | list[str] | tuple[str, ...] |
                 set[str] | frozenset[str], restart: bool = False,
                 cascade: bool = False, only: bool = False) -> Query:
        """Empty a table or set of tables.

        This method quickly removes all rows from the given table or set
        of tables.  It has the same effect as an unqualified DELETE on each
        table, but since it does not actually scan the tables it is faster.
        Furthermore, it reclaims disk space immediately, rather than requiring
        a subsequent VACUUM operation. This is most useful on large tables.

        If restart is set to True, sequences owned by columns of the truncated
        table(s) are automatically restarted.  If cascade is set to True, it
        also truncates all tables that have foreign-key references to any of
        the named tables.  If the parameter 'only' is not set to True, all the
        descendant tables (if any) will also be truncated. Optionally, a '*'
        can be specified after the table name to explicitly indicate that
        descendant tables are included.
        """
        if isinstance(table, str):
            table_only = {table: only}
            table = [table]
        elif isinstance(table, (list, tuple)):
            if isinstance(only, (list, tuple)):
                table_only = dict(zip(table, only))
            else:
                table_only = dict.fromkeys(table, only)
        elif isinstance(table, (set, frozenset)):
            table_only = dict.fromkeys(table, only)
        else:
            raise TypeError('The table must be a string, list or set')
        if not (restart is None or isinstance(restart, (bool, int))):
            raise TypeError('Invalid type for the restart option')
        if not (cascade is None or isinstance(cascade, (bool, int))):
            raise TypeError('Invalid type for the cascade option')
        tables = []
        for t in table:
            u = table_only.get(t)
            if not (u is None or isinstance(u, (bool, int))):
                raise TypeError('Invalid type for the only option')
            if t.endswith('*'):
                if u:
                    raise ValueError(
                        'Contradictory table name and only options')
                t = t[:-1].rstrip()
            t = self._escape_qualified_name(t)
            if u:
                t = f'ONLY {t}'
            tables.append(t)
        cmd_parts = ['TRUNCATE', ', '.join(tables)]
        if restart:
            cmd_parts.append('RESTART IDENTITY')
        if cascade:
            cmd_parts.append('CASCADE')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        return self._valid_db.query(cmd)

    def get_as_list(
            self, table: str,
            what: str | list[str] | tuple[str, ...] | None = None,
            where: str | list[str] | tuple[str, ...] | None = None,
            order: str | list[str] | tuple[str, ...] | bool | None = None,
            limit: int | None = None, offset: int | None = None,
            scalar: bool = False) -> list:
        """Get a table as a list.

        This gets a convenient representation of the table as a list
        of named tuples in Python.  You only need to pass the name of
        the table (or any other SQL expression returning rows).  Note that
        by default this will return the full content of the table which
        can be huge and overflow your memory.  However, you can control
        the amount of data returned using the other optional parameters.

        The parameter 'what' can restrict the query to only return a
        subset of the table columns.  It can be a string, list or a tuple.

        The parameter 'where' can restrict the query to only return a
        subset of the table rows.  It can be a string, list or a tuple
        of SQL expressions that all need to be fulfilled.

        The parameter 'order' specifies the ordering of the rows.  It can
        also be a string, list or a tuple.  If no ordering is specified,
        the result will be ordered by the primary key(s) or all columns if
        no primary key exists.  You can set 'order' to False if you don't
        care about the ordering.  The parameters 'limit' and 'offset' can be
        integers specifying the maximum number of rows returned and a number
        of rows skipped over.

        If you set the 'scalar' option to True, then instead of the
        named tuples you will get the first items of these tuples.
        This is useful if the result has only one column anyway.
        """
        if not table:
            raise TypeError('The table name is missing')
        if what:
            if isinstance(what, (list, tuple)):
                what = ', '.join(map(str, what))
            if order is None:
                order = what
        else:
            what = '*'
        cmd_parts = ['SELECT', what, 'FROM', table]
        if where:
            if isinstance(where, (list, tuple)):
                where = ' AND '.join(map(str, where))
            cmd_parts.extend(['WHERE', where])
        if order is None or order is True:
            try:
                order = self.pkeys(table)
            except (KeyError, ProgrammingError):
                with suppress(KeyError, ProgrammingError):
                    order = list(self.get_attnames(table))
        if order and not isinstance(order, bool):
            if isinstance(order, (list, tuple)):
                order = ', '.join(map(str, order))
            cmd_parts.extend(['ORDER BY', order])
        if limit:
            cmd_parts.append(f'LIMIT {limit}')
        if offset:
            cmd_parts.append(f'OFFSET {offset}')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        query = self._valid_db.query(cmd)
        res = query.namedresult()
        if res and scalar:
            res = [row[0] for row in res]
        return res

    def get_as_dict(
            self, table: str,
            keyname: str | list[str] | tuple[str, ...] | None = None, 
            what: str | list[str] | tuple[str, ...] | None = None,
            where: str | list[str] | tuple[str, ...] | None = None,
            order: str | list[str] | tuple[str, ...] | bool | None = None,
            limit: int | None = None, offset: int | None = None,
            scalar: bool = False) -> dict:
        """Get a table as a dictionary.

        This method is similar to get_as_list(), but returns the table
        as a Python dict instead of a Python list, which can be even
        more convenient. The primary key column(s) of the table will
        be used as the keys of the dictionary, while the other column(s)
        will be the corresponding values.  The keys will be named tuples
        if the table has a composite primary key.  The rows will be also
        named tuples unless the 'scalar' option has been set to True.
        With the optional parameter 'keyname' you can specify an alternative
        set of columns to be used as the keys of the dictionary.  It must
        be set as a string, list or a tuple.

        The dictionary will be ordered using the order specified with the
        'order' parameter or the key column(s) if not specified.  You can
        set 'order' to False if you don't care about the ordering.
        """
        if not table:
            raise TypeError('The table name is missing')
        if not keyname:
            try:
                keyname = self.pkeys(table)
            except (KeyError, ProgrammingError) as e:
                raise _prg_error(f'Table {table} has no primary key') from e
        if isinstance(keyname, str):
            keynames: list[str] | tuple[str, ...] = (keyname,)
        elif isinstance(keyname, (list, tuple)):
            keynames = keyname
        else:
            raise KeyError('The keyname must be a string, list or tuple')
        if what:
            if isinstance(what, (list, tuple)):
                what = ', '.join(map(str, what))
            if order is None:
                order = what
        else:
            what = '*'
        cmd_parts = ['SELECT', what, 'FROM', table]
        if where:
            if isinstance(where, (list, tuple)):
                where = ' AND '.join(map(str, where))
            cmd_parts.extend(['WHERE', where])
        if order is None or order is True:
            order = keyname
        if order and not isinstance(order, bool):
            if isinstance(order, (list, tuple)):
                order = ', '.join(map(str, order))
            cmd_parts.extend(['ORDER BY', order])
        if limit:
            cmd_parts.append(f'LIMIT {limit}')
        if offset:
            cmd_parts.append(f'OFFSET {offset}')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        query = self._valid_db.query(cmd)
        res = query.getresult()
        if not res:
            return {}
        keyset = set(keynames)
        fields = query.listfields()
        if not keyset.issubset(fields):
            raise KeyError('Missing keyname in row')
        key_index: list[int] = []
        row_index: list[int] = []
        for i, f in enumerate(fields):
            (key_index if f in keyset else row_index).append(i)
        key_tuple = len(key_index) > 1
        get_key = itemgetter(*key_index)
        keys = map(get_key, res)
        if scalar:
            row_index = row_index[:1]
            row_is_tuple = False
        else:
            row_is_tuple = len(row_index) > 1
        if scalar or row_is_tuple:
            get_row: Callable[[tuple], tuple] = itemgetter(  # pyright: ignore
                *row_index)
        else:
            frst_index = row_index[0]

            def get_row(row : tuple) -> tuple:
                return row[frst_index],  # tuple with one item

            row_is_tuple = True
        rows = map(get_row, res)
        if key_tuple or row_is_tuple:
            if key_tuple:
                keys = _namediter(_MemoryQuery(keys, keynames))  # type: ignore
            if row_is_tuple:
                fields = tuple(f for f in fields if f not in keyset)
                rows = _namediter(_MemoryQuery(rows, fields))  # type: ignore
        # noinspection PyArgumentList
        return dict(zip(keys, rows))

    def notification_handler(self, event: str, callback: Callable,
                             arg_dict: dict | None = None,
                             timeout: int | float | None = None,
                             stop_event: str | None = None
                             ) -> NotificationHandler:
        """Get notification handler that will run the given callback."""
        return NotificationHandler(self, event, callback,
                                   arg_dict, timeout, stop_event)


# if run as script, print some information

if __name__ == '__main__':
    print('PyGreSQL version', version)
    print()
    print(__doc__)
