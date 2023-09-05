"""Internal type handling."""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime, time, timedelta
from decimal import Decimal as _Decimal
from functools import partial
from inspect import signature
from json import loads as jsondecode
from re import compile as regex
from typing import Any, Callable, ClassVar, Sequence
from uuid import UUID as Uuid  # noqa: N811

from pg.core import Connection as Cnx
from pg.core import (
    ProgrammingError,
    cast_array,
    cast_hstore,
    cast_record,
    unescape_bytea,
)

from .typecode import TypeCode

__all__ = [
    'Decimal', 'decimal_type', 'cast_bool', 'cast_money',
    'cast_int2vector', 'cast_date', 'cast_time', 'cast_interval',
    'cast_timetz', 'cast_timestamp', 'cast_timestamptz',
    'get_typecast', 'set_typecast', 'reset_typecast',
    'Typecasts', 'LocalTypecasts', 'TypeCache', 'FieldInfo'
]


Decimal: type = _Decimal


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


def cast_money(value: str) -> _Decimal | None:
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
        r = res[0]
        type_code = TypeCode.create(
            int(r[0]), r[1], int(r[2]), r[3], r[4], r[5], int(r[6]))
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