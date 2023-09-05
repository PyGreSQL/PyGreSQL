"""Typecasting mechanisms."""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime, timedelta
from functools import partial
from inspect import signature
from re import compile as regex
from typing import Any, Callable, ClassVar, Sequence
from uuid import UUID

from .attrs import AttrDict
from .core import (
    Connection,
    cast_array,
    cast_hstore,
    cast_record,
    get_bool,
    get_decimal,
    get_decimal_point,
    get_jsondecode,
    unescape_bytea,
)
from .tz import timezone_as_offset

__all__ = [
    'cast_bool', 'cast_json', 'cast_num', 'cast_money', 'cast_int2vector',
    'cast_date', 'cast_time', 'cast_timetz', 'cast_interval',
    'cast_timestamp','cast_timestamptz',
    'Typecasts', 'get_typecast', 'set_typecast'
]

def get_args(func: Callable) -> list:
    """Get the arguments of a function."""
    return list(signature(func).parameters)


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


def cast_date(value: str, connection: Connection) -> Any:
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
    value += timezone_as_offset(tz)
    format += '%z'
    return datetime.strptime(value, format).timetz()


def cast_timestamp(value: str, connection: Connection) -> Any:
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


def cast_timestamptz(value: str, connection: Connection) -> Any:
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
    values.append(timezone_as_offset(tz))
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

    connection: Connection | None = None  # set in connection specific instance

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
