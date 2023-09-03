#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# This file contains the DB-API 2 compatible pgdb module.
#
# Copyright (c) 2023 by the PyGreSQL Development Team
#
# Please see the LICENSE.TXT file for specific restrictions.

"""pgdb - DB-API 2.0 compliant module for PyGreSQL.

(c) 1999, Pascal Andre <andre@via.ecp.fr>.
See package documentation for further information on copyright.

Inline documentation is sparse.
See DB-API 2.0 specification for usage information:
http://www.python.org/peps/pep-0249.html

Basic usage:

    pgdb.connect(connect_string) # open a connection
    # connect_string = 'host:database:user:password:opt'
    # All parts are optional. You may also pass host through
    # password as keyword arguments. To pass a port,
    # pass it in the host keyword parameter:
    connection = pgdb.connect(host='localhost:5432')

    cursor = connection.cursor() # open a cursor

    cursor.execute(query[, params])
    # Execute a query, binding params (a dictionary) if they are
    # passed. The binding syntax is the same as the % operator
    # for dictionaries, and no quoting is done.

    cursor.executemany(query, list of params)
    # Execute a query many times, binding each param dictionary
    # from the list.

    cursor.fetchone() # fetch one row, [value, value, ...]

    cursor.fetchall() # fetch all rows, [[value, value, ...], ...]

    cursor.fetchmany([size])
    # returns size or cursor.arraysize number of rows,
    # [[value, value, ...], ...] from result set.
    # Default cursor.arraysize is 1.

    cursor.description # returns information about the columns
    #	[(column_name, type_name, display_size,
    #		internal_size, precision, scale, null_ok), ...]
    # Note that display_size, precision, scale and null_ok
    # are not implemented.

    cursor.rowcount # number of rows available in the result set
    # Available after a call to execute.

    connection.commit() # commit transaction

    connection.rollback() # or rollback transaction

    cursor.close() # close the cursor

    connection.close() # close the connection
"""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterable
from contextlib import suppress
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal as StdDecimal
from functools import lru_cache, partial
from inspect import signature
from json import dumps as jsonencode
from json import loads as jsondecode
from math import isinf, isnan
from re import compile as regex
from time import localtime
from typing import (
    Any,
    Callable,
    ClassVar,
    Generator,
    Mapping,
    NamedTuple,
    Sequence,
    TypeVar,
)
from uuid import UUID as Uuid  # noqa: N811

try:
    from _pg import version
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
                        from _pg import version  # type: ignore
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
from _pg import (
    RESULT_DQL,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    cast_array,
    cast_hstore,
    cast_record,
    unescape_bytea,
    version,
)
from _pg import (
    Connection as Cnx,  # base connection
)
from _pg import (
    connect as get_cnx,  # get base connection
)

__version__ = version

__all__ = [
    'Connection', 'Cursor',
    'Date', 'Time', 'Timestamp',
    'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary', 'Interval', 'Uuid',
    'Hstore', 'Json', 'Literal', 'DbType',
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'BOOL',
    'SMALLINT', 'INTEGER', 'LONG', 'FLOAT', 'NUMERIC', 'MONEY',
    'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
    'UUID', 'HSTORE', 'JSON', 'ARRAY', 'RECORD',
    'Error', 'Warning',
    'InterfaceError', 'DatabaseError', 'DataError', 'OperationalError',
    'IntegrityError', 'InternalError', 'ProgrammingError', 'NotSupportedError',
    'apilevel', 'connect', 'paramstyle', 'threadsafety',
    'get_typecast', 'set_typecast', 'reset_typecast',
    'version', '__version__',
]

Decimal: type = StdDecimal


# *** Module Constants ***

# compliant with DB API 2.0
apilevel = '2.0'

# module may be shared, but not connections
threadsafety = 1

# this module use extended python format codes
paramstyle = 'pyformat'

# shortcut methods have been excluded from DB API 2 and
# are not recommended by the DB SIG, but they can be handy
shortcutmethods = 1


# *** Internal Type Handling ***

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


def decimal_type(decimal_type: type | None = None) -> type:
    """Get or set global type to be used for decimal values.

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call con.type_cache.reset_typecast().
    """
    global Decimal
    if decimal_type is not None:
        Decimal = decimal_type
        set_typecast('numeric', decimal_type)
    return Decimal


def cast_bool(value: str) -> bool | None:
    """Cast boolean value in database format to bool."""
    return value[0] in ('t', 'T') if value else None


def cast_money(value: str) -> StdDecimal | None:
    """Cast money value in database format to Decimal."""
    if not value:
        return None
    value = value.replace('(', '-')
    return Decimal(''.join(c for c in value if c.isdigit() or c in '.-'))


def cast_int2vector(value: str) -> list[int]:
    """Cast an int2vector value."""
    return [int(v) for v in value.split()]


def cast_date(value: str, cnx: Cnx) -> date:
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
    format = cnx.date_format()
    return datetime.strptime(value, format).date()


def cast_time(value: str) -> time:
    """Cast a time value."""
    fmt = '%H:%M:%S.%f' if len(value) > 8 else '%H:%M:%S'
    return datetime.strptime(value, fmt).time()


_re_timezone = regex('(.*)([+-].*)')


def cast_timetz(value: str) -> time:
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


def cast_timestamp(value: str, cnx: Cnx) -> datetime:
    """Cast a timestamp value."""
    if value == '-infinity':
        return datetime.min
    if value == 'infinity':
        return datetime.max
    values = value.split()
    if values[-1] == 'BC':
        return datetime.min
    format = cnx.date_format()
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


def cast_timestamptz(value: str, cnx: Cnx) -> datetime:
    """Cast a timestamptz value."""
    if value == '-infinity':
        return datetime.min
    if value == 'infinity':
        return datetime.max
    values = value.split()
    if values[-1] == 'BC':
        return datetime.min
    format = cnx.date_format()
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
    """

    # the default cast functions
    # (str functions are ignored but have been added for faster access)
    defaults: ClassVar[dict[str, Callable]] = {
        'char': str, 'bpchar': str, 'name': str,
        'text': str, 'varchar': str, 'sql_identifier': str,
        'bool': cast_bool, 'bytea': unescape_bytea,
        'int2': int, 'int4': int, 'serial': int, 'int8': int, 'oid': int,
        'hstore': cast_hstore, 'json': jsondecode, 'jsonb': jsondecode,
        'float4': float, 'float8': float,
        'numeric': Decimal, 'money': cast_money,
        'date': cast_date, 'interval': cast_interval,
        'time': cast_time, 'timetz': cast_timetz,
        'timestamp': cast_timestamp, 'timestamptz': cast_timestamptz,
        'int2vector': cast_int2vector, 'uuid': Uuid,
        'anyarray': cast_array, 'record': cast_record}

    cnx: Cnx | None = None  # for local connection specific instances

    def __missing__(self, typ: str) -> Callable | None:
        """Create a cast function if it is not cached.

        Note that this class never raises a KeyError,
        but returns None when no special cast function exists.
        """
        if not isinstance(typ, str):
            raise TypeError(f'Invalid type: {typ}')
        cast = self.defaults.get(typ)
        if cast:
            # store default for faster access
            cast = self._add_connection(cast)
            self[typ] = cast
        elif typ.startswith('_'):
            # create array cast
            base_cast = self[typ[1:]]
            cast = self.create_array_cast(base_cast)
            if base_cast:
                # store only if base type exists
                self[typ] = cast
        return cast

    @staticmethod
    def _needs_connection(func: Callable) -> bool:
        """Check if a typecast function needs a connection argument."""
        try:
            args = get_args(func)
        except (TypeError, ValueError):
            return False
        return 'cnx' in args[1:]

    def _add_connection(self, cast: Callable) -> Callable:
        """Add a connection argument to the typecast function if necessary."""
        if not self.cnx or not self._needs_connection(cast):
            return cast
        return partial(cast, cnx=self.cnx)

    def get(self, typ: str, default: Callable | None = None  # type: ignore
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
        defaults = self.defaults
        if typ is None:
            self.clear()
            self.update(defaults)
        else:
            if isinstance(typ, str):
                typ = [typ]
            for t in typ:
                cast = defaults.get(t)
                if cast:
                    self[t] = self._add_connection(cast)
                    t = f'_{t}'
                    cast = defaults.get(t)
                    if cast:
                        self[t] = self._add_connection(cast)
                    else:
                        self.pop(t, None)
                else:
                    self.pop(t, None)
                    self.pop(f'_{t}', None)

    def create_array_cast(self, basecast: Callable) -> Callable:
        """Create an array typecast for the given base cast."""
        cast_array = self['anyarray']

        def cast(v: Any) -> list:
            return cast_array(v, basecast)
        return cast

    def create_record_cast(self, name: str, fields: Sequence[str],
                           casts: Sequence[str]) -> Callable:
        """Create a named record typecast for the given fields and casts."""
        cast_record = self['record']
        record = namedtuple(name, fields)  # type: ignore

        def cast(v: Any) -> record:
            # noinspection PyArgumentList
            return record(*cast_record(v, casts))
        return cast


_typecasts = Typecasts()  # this is the global typecast dictionary


def get_typecast(typ: str) -> Callable | None:
    """Get the global typecast function for the given database type."""
    return _typecasts.get(typ)


def set_typecast(typ: str | Sequence[str], cast: Callable | None) -> None:
    """Set a global typecast function for the given database type(s).

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call con.type_cache.reset_typecast().
    """
    _typecasts.set(typ, cast)


def reset_typecast(typ: str | Sequence[str] | None = None) -> None:
    """Reset the global typecasts for the given type(s) to their default.

    When no type is specified, all typecasts will be reset.

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call con.type_cache.reset_typecast().
    """
    _typecasts.reset(typ)


class LocalTypecasts(Typecasts):
    """Map typecasts, including local composite types, to cast functions."""

    defaults = _typecasts

    cnx: Cnx | None = None  # set in connection specific instances

    def __missing__(self, typ: str) -> Callable | None:
        """Create a cast function if it is not cached."""
        cast: Callable | None
        if typ.startswith('_'):
            base_cast = self[typ[1:]]
            cast = self.create_array_cast(base_cast)
            if base_cast:
                self[typ] = cast
        else:
            cast = self.defaults.get(typ)
            if cast:
                cast = self._add_connection(cast)
                self[typ] = cast
            else:
                fields = self.get_fields(typ)
                if fields:
                    casts = [self[field.type] for field in fields]
                    field_names = [field.name for field in fields]
                    cast = self.create_record_cast(typ, field_names, casts)
                    self[typ] = cast
        return cast

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_fields(self, typ: str) -> list[FieldInfo]:
        """Return the fields for the given record type.

        This method will be replaced with a method that looks up the fields
        using the type cache of the connection.
        """
        return []


class TypeCode(str):
    """Class representing the type_code used by the DB-API 2.0.

    TypeCode objects are strings equal to the PostgreSQL type name,
    but carry some additional information.
    """

    oid: int
    len: int
    type: str
    category: str
    delim: str
    relid: int

    # noinspection PyShadowingBuiltins
    @classmethod
    def create(cls, oid: int, name: str, len: int, type: str, category: str,
               delim: str, relid: int) -> TypeCode:
        """Create a type code for a PostgreSQL data type."""
        self = cls(name)
        self.oid = oid
        self.len = len
        self.type = type
        self.category = category
        self.delim = delim
        self.relid = relid
        return self


FieldInfo = namedtuple('FieldInfo', ('name', 'type'))


class TypeCache(dict):
    """Cache for database types.

    This cache maps type OIDs and names to TypeCode strings containing
    important information on the associated database type.
    """

    def __init__(self, cnx: Cnx) -> None:
        """Initialize type cache for connection."""
        super().__init__()
        self._escape_string = cnx.escape_string
        self._src = cnx.source()
        self._typecasts = LocalTypecasts()
        self._typecasts.get_fields = self.get_fields  # type: ignore
        self._typecasts.cnx = cnx
        self._query_pg_type = (
            "SELECT oid, typname,"
            " typlen, typtype, typcategory, typdelim, typrelid"
            " FROM pg_catalog.pg_type WHERE oid OPERATOR(pg_catalog.=) {}")

    def __missing__(self, key: int | str) -> TypeCode:
        """Get the type info from the database if it is not cached."""
        oid: int | str
        if isinstance(key, int):
            oid = key
        else:
            if '.' not in key and '"' not in key:
                key = f'"{key}"'
            oid = f"'{self._escape_string(key)}'::pg_catalog.regtype"
        try:
            self._src.execute(self._query_pg_type.format(oid))
        except ProgrammingError:
            res = None
        else:
            res = self._src.fetch(1)
        if not res:
            raise KeyError(f'Type {key} could not be found')
        res = res[0]
        type_code = TypeCode.create(
            int(res[0]), res[1], int(res[2]),
            res[3], res[4], res[5], int(res[6]))
        # noinspection PyUnresolvedReferences
        self[type_code.oid] = self[str(type_code)] = type_code
        return type_code

    def get(self, key: int | str,  # type: ignore
            default: TypeCode | None = None) -> TypeCode | None:
        """Get the type even if it is not cached."""
        try:
            return self[key]
        except KeyError:
            return default

    def get_fields(self, typ: int | str | TypeCode) -> list[FieldInfo] | None:
        """Get the names and types of the fields of composite types."""
        if isinstance(typ, TypeCode):
            relid = typ.relid
        else:
            type_code = self.get(typ)
            if not type_code:
                return None
            relid = type_code.relid
        if not relid:
            return None  # this type is not composite
        self._src.execute(
            "SELECT attname, atttypid"  # noqa: S608
            " FROM pg_catalog.pg_attribute"
            f" WHERE attrelid OPERATOR(pg_catalog.=) {relid}"
            " AND attnum OPERATOR(pg_catalog.>) 0"
            " AND NOT attisdropped ORDER BY attnum")
        return [FieldInfo(name, self.get(int(oid)))
                for name, oid in self._src.fetch(-1)]

    def get_typecast(self, typ: str) -> Callable | None:
        """Get the typecast function for the given database type."""
        return self._typecasts[typ]

    def set_typecast(self, typ: str | Sequence[str],
                     cast: Callable | None) -> None:
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
        cast = self._typecasts[typ]
        if cast is None or cast is str:
            # no typecast is necessary
            return value
        return cast(value)

    def get_row_caster(self, types: Sequence[str]) -> Callable:
        """Get a typecast function for a complete row of values."""
        typecasts = self._typecasts
        casts = [typecasts[typ] for typ in types]
        casts = [cast if cast is not str else None for cast in casts]

        def row_caster(row: Sequence) -> Sequence:
            return [value if cast is None or value is None else cast(value)
                    for cast, value in zip(casts, row)]

        return row_caster


class _QuoteDict(dict):
    """Dictionary with auto quoting of its items.

    The quote attribute must be set to the desired quote function.
    """

    quote: Callable[[str], str]

    def __getitem__(self, key: str) -> str:
        # noinspection PyUnresolvedReferences
        return self.quote(super().__getitem__(key))


# *** Error Messages ***

E = TypeVar('E', bound=DatabaseError)


def _db_error(msg: str, cls:type[E] = DatabaseError) -> type[E]:
    """Return DatabaseError with empty sqlstate attribute."""
    error = cls(msg)
    error.sqlstate = None
    return error


def _op_error(msg: str) -> OperationalError:
    """Return OperationalError."""
    return _db_error(msg, OperationalError)


# *** Row Tuples ***

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


# *** Cursor Object ***

class Cursor:
    """Cursor object."""

    def __init__(self, connection: Connection) -> None:
        """Create a cursor object for the database connection."""
        self.connection = self._connection = connection
        cnx = connection._cnx
        if not cnx:
            raise _op_error("Connection has been closed")
        self._cnx = cnx
        self.type_cache = connection.type_cache
        self._src = self._cnx.source()
        # the official attribute for describing the result columns
        self._description: list[CursorDescription] | bool | None = None
        if self.row_factory is Cursor.row_factory:
            # the row factory needs to be determined dynamically
            self.row_factory = None  # type: ignore
        else:
            self.build_row_factory = None  # type: ignore
        self.rowcount = -1
        self.arraysize = 1
        self.lastrowid = None

    def __iter__(self) -> Cursor:
        """Make cursor compatible to the iteration protocol."""
        return self

    def __enter__(self) -> Cursor:
        """Enter the runtime context for the cursor object."""
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context for the cursor object."""
        self.close()

    def _quote(self, value: Any) -> Any:
        """Quote value depending on its type."""
        if value is None:
            return 'NULL'
        if isinstance(value, (Hstore, Json)):
            value = str(value)
        if isinstance(value, (bytes, str)):
            cnx = self._cnx
            if isinstance(value, Binary):
                value = cnx.escape_bytea(value).decode('ascii')
            else:
                value = cnx.escape_string(value)
            return f"'{value}'"
        if isinstance(value, float):
            if isinf(value):
                return "'-Infinity'" if value < 0 else "'Infinity'"
            if isnan(value):
                return "'NaN'"
            return value
        if isinstance(value, (int, Decimal, Literal)):
            return value
        if isinstance(value, datetime):
            if value.tzinfo:
                return f"'{value}'::timestamptz"
            return f"'{value}'::timestamp"
        if isinstance(value, date):
            return f"'{value}'::date"
        if isinstance(value, time):
            if value.tzinfo:
                return f"'{value}'::timetz"
            return f"'{value}'::time"
        if isinstance(value, timedelta):
            return f"'{value}'::interval"
        if isinstance(value, Uuid):
            return f"'{value}'::uuid"
        if isinstance(value, list):
            # Quote value as an ARRAY constructor. This is better than using
            # an array literal because it carries the information that this is
            # an array and not a string.  One issue with this syntax is that
            # you need to add an explicit typecast when passing empty arrays.
            # The ARRAY keyword is actually only necessary at the top level.
            if not value:  # exception for empty array
                return "'{}'"
            q = self._quote
            v = ','.join(str(q(v)) for v in value)
            return f'ARRAY[{v}]'
        if isinstance(value, tuple):
            # Quote as a ROW constructor.  This is better than using a record
            # literal because it carries the information that this is a record
            # and not a string.  We don't use the keyword ROW in order to make
            # this usable with the IN syntax as well.  It is only necessary
            # when the records has a single column which is not really useful.
            q = self._quote
            v = ','.join(str(q(v)) for v in value)
            return f'({v})'
        try:  # noinspection PyUnresolvedReferences
            value = value.__pg_repr__()
        except AttributeError as e:
            raise InterfaceError(
                f'Do not know how to adapt type {type(value)}') from e
        if isinstance(value, (tuple, list)):
            value = self._quote(value)
        return value

    def _quoteparams(self, string: str,
                     parameters: Mapping | Sequence | None) -> str:
        """Quote parameters.

        This function works for both mappings and sequences.

        The function should be used even when there are no parameters,
        so that we have a consistent behavior regarding percent signs.
        """
        if not parameters:
            try:
                return string % ()  # unescape literal quotes if possible
            except (TypeError, ValueError):
                return string  # silently accept unescaped quotes
        if isinstance(parameters, dict):
            parameters = _QuoteDict(parameters)
            parameters.quote = self._quote
        else:
            parameters = tuple(map(self._quote, parameters))
        return string % parameters

    def _make_description(self, info: tuple[int, str, int, int, int]
                          ) -> CursorDescription:
        """Make the description tuple for the given field info."""
        name, typ, size, mod = info[1:]
        type_code = self.type_cache[typ]
        if mod > 0:
            mod -= 4
        precision: int | None
        scale: int | None
        if type_code == 'numeric':
            precision, scale = mod >> 16, mod & 0xffff
            size = precision
        else:
            if not size:
                size = type_code.size
            if size == -1:
                size = mod
            precision = scale = None
        return CursorDescription(
            name, type_code, None, size, precision, scale, None)

    @property
    def description(self) -> list[CursorDescription] | None:
        """Read-only attribute describing the result columns."""
        description = self._description
        if description is None:
            return None
        if not isinstance(description, list):
            make = self._make_description
            description = [make(info) for info in self._src.listinfo()]
            self._description = description
        return description

    @property
    def colnames(self) -> Sequence[str] | None:
        """Unofficial convenience method for getting the column names."""
        description = self.description
        return None if description is None else [d[0] for d in description]

    @property
    def coltypes(self) -> Sequence[TypeCode] | None:
        """Unofficial convenience method for getting the column types."""
        description = self.description
        return None if description is None else [d[1] for d in description]

    def close(self) -> None:
        """Close the cursor object."""
        self._src.close()

    def execute(self, operation: str, parameters: Sequence | None = None
                ) -> Cursor:
        """Prepare and execute a database operation (query or command)."""
        # The parameters may also be specified as list of tuples to e.g.
        # insert multiple rows in a single operation, but this kind of
        # usage is deprecated.  We make several plausibility checks because
        # tuples can also be passed with the meaning of ROW constructors.
        if (parameters and isinstance(parameters, list)
                and len(parameters) > 1
                and all(isinstance(p, tuple) for p in parameters)
                and all(len(p) == len(parameters[0]) for p in parameters[1:])):
            return self.executemany(operation, parameters)
        # not a list of tuples
        return self.executemany(operation, [parameters])

    def executemany(self, operation: str,
                    seq_of_parameters: Sequence[Sequence | None]) -> Cursor:
        """Prepare operation and execute it against a parameter sequence."""
        if not seq_of_parameters:
            # don't do anything without parameters
            return self
        self._description = None
        self.rowcount = -1
        # first try to execute all queries
        rowcount = 0
        sql = "BEGIN"
        try:
            if not self._connection._tnx and not self._connection.autocommit:
                try:
                    self._src.execute(sql)
                except DatabaseError:
                    raise  # database provides error message
                except Exception as e:
                    raise _op_error("Can't start transaction") from e
                else:
                    self._connection._tnx = True
            for parameters in seq_of_parameters:
                sql = operation
                sql = self._quoteparams(sql, parameters)
                rows = self._src.execute(sql)
                if rows:  # true if not DML
                    rowcount += rows
                else:
                    self.rowcount = -1
        except DatabaseError:
            raise  # database provides error message
        except Error as err:
            # noinspection PyTypeChecker
            raise _db_error(
                f"Error in '{sql}': '{err}'", InterfaceError) from err
        except Exception as err:
            raise _op_error(f"Internal error in '{sql}': {err}") from err
        # then initialize result raw count and description
        if self._src.resulttype == RESULT_DQL:
            self._description = True  # fetch on demand
            self.rowcount = self._src.ntuples
            self.lastrowid = None
            build_row_factory = self.build_row_factory
            if build_row_factory:  # type: ignore
                self.row_factory = build_row_factory()  # type: ignore
        else:
            self.rowcount = rowcount
            self.lastrowid = self._src.oidstatus()
        # return the cursor object, so you can write statements such as
        # "cursor.execute(...).fetchall()" or "for row in cursor.execute(...)"
        return self

    def fetchone(self) -> Sequence | None:
        """Fetch the next row of a query result set."""
        res = self.fetchmany(1, False)
        try:
            return res[0]
        except IndexError:
            return None

    def fetchall(self) -> Sequence[Sequence]:
        """Fetch all (remaining) rows of a query result."""
        return self.fetchmany(-1, False)

    def fetchmany(self, size: int | None = None, keep: bool = False
                  ) -> Sequence[Sequence]:
        """Fetch the next set of rows of a query result.

        The number of rows to fetch per call is specified by the
        size parameter. If it is not given, the cursor's arraysize
        determines the number of rows to be fetched. If you set
        the keep parameter to true, this is kept as new arraysize.
        """
        if size is None:
            size = self.arraysize
        if keep:
            self.arraysize = size
        try:
            result = self._src.fetch(size)
        except DatabaseError:
            raise
        except Error as err:
            raise _db_error(str(err)) from err
        row_factory = self.row_factory
        coltypes = self.coltypes
        if coltypes is None:
            # cannot determine column types, return raw result
            return [row_factory(row) for row in result]
        if len(result) > 5:
            # optimize the case where we really fetch many values
            # by looking up all type casting functions upfront
            cast_row = self.type_cache.get_row_caster(coltypes)
            return [row_factory(cast_row(row)) for row in result]
        cast_value = self.type_cache.typecast
        return [row_factory([cast_value(value, typ)
                for typ, value in zip(coltypes, row)]) for row in result]

    def callproc(self, procname: str, parameters: Sequence | None = None
                 ) -> Sequence | None:
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each input
        argument that the procedure expects. The result of the call is the
        same as this input sequence; replacement of output and input/output
        parameters in the return value is currently not supported.

        The procedure may also provide a result set as output. These can be
        requested through the standard fetch methods of the cursor.
        """
        n = len(parameters) if parameters else 0
        s = ','.join(n * ['%s'])
        query = f'select * from "{procname}"({s})'  # noqa: S608
        self.execute(query, parameters)
        return parameters

    # noinspection PyShadowingBuiltins
    def copy_from(self, stream: Any, table: str,
                  format: str | None = None, sep: str | None = None,
                  null: str | None = None, size: int | None  = None,
                  columns: Sequence[str] | None = None) -> Cursor:
        """Copy data from an input stream to the specified table.

        The input stream can be a file-like object with a read() method or
        it can also be an iterable returning a row or multiple rows of input
        on each iteration.

        The format must be 'text', 'csv' or 'binary'. The sep option sets the
        column separator (delimiter) used in the non binary formats.
        The null option sets the textual representation of NULL in the input.

        The size option sets the size of the buffer used when reading data
        from file-like objects.

        The copy operation can be restricted to a subset of columns. If no
        columns are specified, all of them will be copied.
        """
        binary_format = format == 'binary'
        try:
            read = stream.read
        except AttributeError as e:
            if size:
                raise ValueError(
                    "Size must only be set for file-like objects") from e
            input_type: type | tuple[type, ...]
            type_name: str
            if binary_format:
                input_type = bytes
                type_name = 'byte strings'
            else:
                input_type = (bytes, str)
                type_name = 'strings'

            if isinstance(stream, (bytes, str)):
                if not isinstance(stream, input_type):
                    raise ValueError(f"The input must be {type_name}") from e
                if not binary_format:
                    if isinstance(stream, str):
                        if not stream.endswith('\n'):
                            stream += '\n'
                    else:
                        if not stream.endswith(b'\n'):
                            stream += b'\n'

                def chunks() -> Generator:
                    yield stream

            elif isinstance(stream, Iterable):

                def chunks() -> Generator:
                    for chunk in stream:
                        if not isinstance(chunk, input_type):
                            raise ValueError(
                                f"Input stream must consist of {type_name}")
                        if isinstance(chunk, str):
                            if not chunk.endswith('\n'):
                                chunk += '\n'
                        else:
                            if not chunk.endswith(b'\n'):
                                chunk += b'\n'
                        yield chunk

            else:
                raise TypeError("Need an input stream to copy from") from e
        else:
            if size is None:
                size = 8192
            elif not isinstance(size, int):
                raise TypeError("The size option must be an integer")
            if size > 0:

                def chunks() -> Generator:
                    while True:
                        buffer = read(size)
                        yield buffer
                        if not buffer or len(buffer) < size:
                            break

            else:

                def chunks() -> Generator:
                    yield read()

        if not table or not isinstance(table, str):
            raise TypeError("Need a table to copy to")
        if table.lower().startswith('select '):
            raise ValueError("Must specify a table, not a query")
        cnx = self._cnx
        table = '.'.join(map(cnx.escape_identifier, table.split('.', 1)))
        operation_parts = [f'copy {table}']
        options = []
        parameters = []
        if format is not None:
            if not isinstance(format, str):
                raise TypeError("The format option must be be a string")
            if format not in ('text', 'csv', 'binary'):
                raise ValueError("Invalid format")
            options.append(f'format {format}')
        if sep is not None:
            if not isinstance(sep, str):
                raise TypeError("The sep option must be a string")
            if format == 'binary':
                raise ValueError(
                    "The sep option is not allowed with binary format")
            if len(sep) != 1:
                raise ValueError(
                    "The sep option must be a single one-byte character")
            options.append('delimiter %s')
            parameters.append(sep)
        if null is not None:
            if not isinstance(null, str):
                raise TypeError("The null option must be a string")
            options.append('null %s')
            parameters.append(null)
        if columns:
            if not isinstance(columns, str):
                columns = ','.join(map(cnx.escape_identifier, columns))
            operation_parts.append(f'({columns})')
        operation_parts.append("from stdin")
        if options:
            operation_parts.append(f"({','.join(options)})")
        operation = ' '.join(operation_parts)

        putdata = self._src.putdata
        self.execute(operation, parameters)

        try:
            for chunk in chunks():
                putdata(chunk)
        except BaseException as error:
            self.rowcount = -1
            # the following call will re-raise the error
            putdata(error)
        else:
            self.rowcount = putdata(None)

        # return the cursor object, so you can chain operations
        return self

    # noinspection PyShadowingBuiltins
    def copy_to(self, stream: Any, table: str,
                format: str | None = None, sep: str | None = None,
                null: str | None = None, decode: bool | None = None,
                columns: Sequence[str] | None = None) -> Cursor | Generator:
        """Copy data from the specified table to an output stream.

        The output stream can be a file-like object with a write() method or
        it can also be None, in which case the method will return a generator
        yielding a row on each iteration.

        Output will be returned as byte strings unless you set decode to true.

        Note that you can also use a select query instead of the table name.

        The format must be 'text', 'csv' or 'binary'. The sep option sets the
        column separator (delimiter) used in the non binary formats.
        The null option sets the textual representation of NULL in the output.

        The copy operation can be restricted to a subset of columns. If no
        columns are specified, all of them will be copied.
        """
        binary_format = format == 'binary'
        if stream is None:
            write = None
        else:
            try:
                write = stream.write
            except AttributeError as e:
                raise TypeError("Need an output stream to copy to") from e
        if not table or not isinstance(table, str):
            raise TypeError("Need a table to copy to")
        cnx = self._cnx
        if table.lower().startswith('select '):
            if columns:
                raise ValueError("Columns must be specified in the query")
            table = f'({table})'
        else:
            table = '.'.join(map(cnx.escape_identifier, table.split('.', 1)))
        operation_parts = [f'copy {table}']
        options = []
        parameters = []
        if format is not None:
            if not isinstance(format, str):
                raise TypeError("The format option must be a string")
            if format not in ('text', 'csv', 'binary'):
                raise ValueError("Invalid format")
            options.append(f'format {format}')
        if sep is not None:
            if not isinstance(sep, str):
                raise TypeError("The sep option must be a string")
            if binary_format:
                raise ValueError(
                    "The sep option is not allowed with binary format")
            if len(sep) != 1:
                raise ValueError(
                    "The sep option must be a single one-byte character")
            options.append('delimiter %s')
            parameters.append(sep)
        if null is not None:
            if not isinstance(null, str):
                raise TypeError("The null option must be a string")
            options.append('null %s')
            parameters.append(null)
        if decode is None:
            decode = format != 'binary'
        else:
            if not isinstance(decode, (int, bool)):
                raise TypeError("The decode option must be a boolean")
            if decode and binary_format:
                raise ValueError(
                    "The decode option is not allowed with binary format")
        if columns:
            if not isinstance(columns, str):
                columns = ','.join(map(cnx.escape_identifier, columns))
            operation_parts.append(f'({columns})')

        operation_parts.append("to stdout")
        if options:
            operation_parts.append(f"({','.join(options)})")
        operation = ' '.join(operation_parts)

        getdata = self._src.getdata
        self.execute(operation, parameters)

        def copy() -> Generator:
            self.rowcount = 0
            while True:
                row = getdata(decode)
                if isinstance(row, int):
                    if self.rowcount != row:
                        self.rowcount = row
                    break
                self.rowcount += 1
                yield row

        if write is None:
            # no input stream, return the generator
            return copy()

        # write the rows to the file-like input stream
        for row in copy():
            # noinspection PyUnboundLocalVariable
            write(row)

        # return the cursor object, so you can chain operations
        return self

    def __next__(self) -> Sequence:
        """Return the next row (support for the iteration protocol)."""
        res = self.fetchone()
        if res is None:
            raise StopIteration
        return res

    # Note that the iterator protocol now uses __next()__ instead of next(),
    # but we keep it for backward compatibility of pgdb.
    next = __next__

    @staticmethod
    def nextset() -> bool | None:
        """Not supported."""
        raise NotSupportedError("The nextset() method is not supported")

    @staticmethod
    def setinputsizes(sizes: Sequence[int]) -> None:
        """Not supported."""
        pass  # unsupported, but silently passed

    @staticmethod
    def setoutputsize(size: int, column: int = 0) -> None:
        """Not supported."""
        pass  # unsupported, but silently passed

    @staticmethod
    def row_factory(row: Sequence) -> Sequence:
        """Process rows before they are returned.

        You can overwrite this statically with a custom row factory, or
        you can build a row factory dynamically with build_row_factory().

        For example, you can create a Cursor class that returns rows as
        Python dictionaries like this:

            class DictCursor(pgdb.Cursor):

                def row_factory(self, row):
                    return {desc[0]: value
                        for desc, value in zip(self.description, row)}

            cur = DictCursor(con)  # get one DictCursor instance or
            con.cursor_type = DictCursor  # always use DictCursor instances
        """
        raise NotImplementedError

    def build_row_factory(self) -> Callable[[Sequence], Sequence] | None:
        """Build a row factory based on the current description.

        This implementation builds a row factory for creating named tuples.
        You can overwrite this method if you want to dynamically create
        different row factories whenever the column description changes.
        """
        names = self.colnames
        return _row_factory(tuple(names)) if names else None


CursorDescription = namedtuple('CursorDescription', (
    'name', 'type_code', 'display_size', 'internal_size',
    'precision', 'scale', 'null_ok'))


# *** Connection Objects ***

class Connection:
    """Connection object."""

    # expose the exceptions as attributes on the connection object
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def __init__(self, cnx: Cnx) -> None:
        """Create a database connection object."""
        self._cnx = cnx  # connection
        self._tnx = False  # transaction state
        self.type_cache = TypeCache(cnx)
        self.cursor_type = Cursor
        self.autocommit = False
        try:
            self._cnx.source()
        except Exception as e:
            raise _op_error("Invalid connection") from e

    def __enter__(self) -> Connection:
        """Enter the runtime context for the connection object.

        The runtime context can be used for running transactions.

        This also starts a transaction in autocommit mode.
        """
        if self.autocommit:
            cnx = self._cnx
            if not cnx:
                raise _op_error("Connection has been closed")
            try:
                cnx.source().execute("BEGIN")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise _op_error("Can't start transaction") from e
            else:
                self._tnx = True
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context for the connection object.

        This does not close the connection, but it ends a transaction.
        """
        if et is None and ev is None and tb is None:
            self.commit()
        else:
            self.rollback()

    def close(self) -> None:
        """Close the connection object."""
        if not self._cnx:
            raise _op_error("Connection has been closed")
        if self._tnx:
            with suppress(DatabaseError):
                self.rollback()
        self._cnx.close()
        self._cnx = None           

    @property
    def closed(self) -> bool:
        """Check whether the connection has been closed or is broken."""
        try:
            return not self._cnx or self._cnx.status != 1
        except TypeError:
            return True

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        if not self._cnx:
            raise _op_error("Connection has been closed")
        if self._tnx:
            self._tnx = False
            try:
                self._cnx.source().execute("COMMIT")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise _op_error("Can't commit transaction") from e

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        if not self._cnx:
            raise _op_error("Connection has been closed")
        if self._tnx:
            self._tnx = False
            try:
                self._cnx.source().execute("ROLLBACK")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise _op_error("Can't rollback transaction") from e

    def cursor(self) -> Cursor:
        """Return a new cursor object using the connection."""
        if not self._cnx:
            raise _op_error("Connection has been closed")
        try:
            return self.cursor_type(self)
        except Exception as e:
            raise _op_error("Invalid connection") from e

    if shortcutmethods:  # otherwise do not implement and document this

        def execute(self, operation: str,
                    parameters: Sequence | None = None) -> Cursor:
            """Shortcut method to run an operation on an implicit cursor."""
            cursor = self.cursor()
            cursor.execute(operation, parameters)
            return cursor

        def executemany(self, operation: str,
                        seq_of_parameters: Sequence[Sequence | None]
                        ) -> Cursor:
            """Shortcut method to run an operation against a sequence."""
            cursor = self.cursor()
            cursor.executemany(operation, seq_of_parameters)
            return cursor


# *** Module Interface ***

def connect(dsn: str | None = None,
            user: str | None = None, password: str | None = None,
            host: str | None = None, database: str | None = None,
            **kwargs: Any) -> Connection:
    """Connect to a database."""
    # first get params from DSN
    dbport = -1
    dbhost: str | None = ""
    dbname: str | None = ""
    dbuser: str | None = ""
    dbpasswd: str | None = ""
    dbopt: str | None = ""
    if dsn:
        try:
            params = dsn.split(":", 4)
            dbhost = params[0]
            dbname = params[1]
            dbuser = params[2]
            dbpasswd = params[3]
            dbopt = params[4]
        except (AttributeError, IndexError, TypeError):
            pass

    # override if necessary
    if user is not None:
        dbuser = user
    if password is not None:
        dbpasswd = password
    if database is not None:
        dbname = database
    if host:
        try:
            params = host.split(":", 1)
            dbhost = params[0]
            dbport = int(params[1])
        except (AttributeError, IndexError, TypeError, ValueError):
            pass

    # empty host is localhost
    if dbhost == "":
        dbhost = None
    if dbuser == "":
        dbuser = None

    # pass keyword arguments as connection info string
    if kwargs:
        kwarg_list = list(kwargs.items())
        kw_parts = []
        if dbname and '=' in dbname:
            kw_parts.append(dbname)
        else:
            kwarg_list.insert(0, ('dbname', dbname))
        for kw, value in kwarg_list:
            value = str(value)
            if not value or ' ' in value:
                value = value.replace('\\', '\\\\').replace("'", "\\'")
                value = f"'{value}'"
            kw_parts.append(f'{kw}={value}')
        dbname = ' '.join(kw_parts)
    # open the connection
    cnx = get_cnx(dbname, dbhost, dbport, dbopt, dbuser, dbpasswd)
    return Connection(cnx)


# *** Types Handling ***

class DbType(frozenset):
    """Type class for a couple of PostgreSQL data types.

    PostgreSQL is object-oriented: types are dynamic.
    We must thus use type names as internal type codes.
    """

    def __new__(cls, values: str | Iterable[str]) -> DbType:
        """Create new type object."""
        if isinstance(values, str):
            values = values.split()
        return super().__new__(cls, values)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        """Check whether types are considered equal."""
        if isinstance(other, str):
            if other.startswith('_'):
                other = other[1:]
            return other in self
        return super().__eq__(other)

    def __ne__(self, other: Any) -> bool:
        """Check whether types are not considered equal."""
        if isinstance(other, str):
            if other.startswith('_'):
                other = other[1:]
            return other not in self
        return super().__ne__(other)


class ArrayType:
    """Type class for PostgreSQL array types."""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return other.startswith('_')
        return isinstance(other, ArrayType)

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, str):
            return not other.startswith('_')
        return not isinstance(other, ArrayType)


class RecordType:
    """Type class for PostgreSQL record types."""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TypeCode):
            # noinspection PyUnresolvedReferences
            return other.type == 'c'
        if isinstance(other, str):
            return other == 'record'
        return isinstance(other, RecordType)

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, TypeCode):
            # noinspection PyUnresolvedReferences
            return other.type != 'c'
        if isinstance(other, str):
            return other != 'record'
        return not isinstance(other, RecordType)


# Mandatory type objects defined by DB-API 2 specs:

STRING = DbType('char bpchar name text varchar')
BINARY = DbType('bytea')
NUMBER = DbType('int2 int4 serial int8 float4 float8 numeric money')
DATETIME = DbType('date time timetz timestamp timestamptz interval'
                ' abstime reltime')  # these are very old
ROWID = DbType('oid')


# Additional type objects (more specific):

BOOL = DbType('bool')
SMALLINT = DbType('int2')
INTEGER = DbType('int2 int4 int8 serial')
LONG = DbType('int8')
FLOAT = DbType('float4 float8')
NUMERIC = DbType('numeric')
MONEY = DbType('money')
DATE = DbType('date')
TIME = DbType('time timetz')
TIMESTAMP = DbType('timestamp timestamptz')
INTERVAL = DbType('interval')
UUID = DbType('uuid')
HSTORE = DbType('hstore')
JSON = DbType('json jsonb')

# Type object for arrays (also equate to their base types):

ARRAY = ArrayType()

# Type object for records (encompassing all composite types):

RECORD = RecordType()


# Mandatory type helpers defined by DB-API 2 specs:

def Date(year: int, month: int, day: int) -> date:  # noqa: N802
    """Construct an object holding a date value."""
    return date(year, month, day)


def Time(hour: int, minute: int = 0,  # noqa: N802
         second: int = 0, microsecond: int = 0,
         tzinfo: tzinfo | None = None) -> time:
    """Construct an object holding a time value."""
    return time(hour, minute, second, microsecond, tzinfo)


def Timestamp(year: int, month: int, day: int,  # noqa: N802
              hour: int = 0, minute: int = 0,
              second: int = 0, microsecond: int = 0,
              tzinfo: tzinfo | None = None) -> datetime:
    """Construct an object holding a time stamp value."""
    return datetime(year, month, day, hour, minute,
                    second, microsecond, tzinfo)


def DateFromTicks(ticks: float | None) -> date:  # noqa: N802
    """Construct an object holding a date value from the given ticks value."""
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: float | None) -> time:  # noqa: N802
    """Construct an object holding a time value from the given ticks value."""
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float | None) -> datetime:  # noqa: N802
    """Construct an object holding a time stamp from the given ticks value."""
    return Timestamp(*localtime(ticks)[:6])


class Binary(bytes):
    """Construct an object capable of holding a binary (long) string value."""


# Additional type helpers for PyGreSQL:

def Interval(days: int | float,  # noqa: N802
             hours: int | float = 0, minutes: int | float = 0,
             seconds: int | float = 0, microseconds: int | float = 0
             ) -> timedelta:
    """Construct an object holding a time interval value."""
    return timedelta(days, hours=hours, minutes=minutes,
                     seconds=seconds, microseconds=microseconds)


Uuid = Uuid  # Construct an object holding a UUID value


class Hstore(dict):
    """Wrapper class for marking hstore values."""

    _re_quote = regex('^[Nn][Uu][Ll][Ll]$|[ ,=>]')
    _re_escape = regex(r'(["\\])')

    @classmethod
    def _quote(cls, s: Any) -> Any:
        if s is None:
            return 'NULL'
        if not isinstance(s, str):
            s = str(s)
        if not s:
            return '""'
        quote = cls._re_quote.search(s)
        s = cls._re_escape.sub(r'\\\1', s)
        if quote:
            s = f'"{s}"'
        return s

    def __str__(self) -> str:
        """Create a printable representation of the hstore value."""
        q = self._quote
        return ','.join(f'{q(k)}=>{q(v)}' for k, v in self.items())


class Json:
    """Construct a wrapper for holding an object serializable to JSON."""

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


class Literal:
    """Construct a wrapper for holding a literal SQL string."""

    def __init__(self, sql: str) -> None:
        """Initialize literal SQL string."""
        self.sql = sql

    def __str__(self) -> str:
        """Return a printable representation of the SQL string."""
        return self.sql

    __pg_repr__ = __str__


# If run as script, print some information:

if __name__ == '__main__':
    print('PyGreSQL version', version)
    print('')
    print(__doc__)
