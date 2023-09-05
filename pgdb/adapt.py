"""Type helpers for adaptation of parameters."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, tzinfo
from json import dumps as jsonencode
from re import compile as regex
from time import localtime
from typing import Any, Callable, Iterable
from uuid import UUID as Uuid  # noqa: N811

from .typecode import TypeCode

__all__ = [
    'DbType', 'ArrayType', 'RecordType',
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'BOOL', 'SMALLINT',
    'INTEGER', 'LONG', 'FLOAT', 'NUMERIC', 'MONEY', 'DATE', 'TIME',
    'TIMESTAMP', 'INTERVAL', 'UUID', 'HSTORE', 'JSON', 'ARRAY', 'RECORD',
    'Date', 'Time', 'Timestamp',
    'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks'

]


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
        """Check whether arrays are equal."""
        if isinstance(other, str):
            return other.startswith('_')
        return isinstance(other, ArrayType)

    def __ne__(self, other: Any) -> bool:
        """Check whether arrays are different."""
        if isinstance(other, str):
            return not other.startswith('_')
        return not isinstance(other, ArrayType)


class RecordType:
    """Type class for PostgreSQL record types."""

    def __eq__(self, other: Any) -> bool:
        """Check whether records are equal."""
        if isinstance(other, TypeCode):
            return other.type == 'c'
        if isinstance(other, str):
            return other == 'record'
        return isinstance(other, RecordType)

    def __ne__(self, other: Any) -> bool:
        """Check whether records are different."""
        if isinstance(other, TypeCode):
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