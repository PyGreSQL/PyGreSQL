"""Internal type handling."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal as _Decimal
from functools import partial
from inspect import signature
from json import loads as jsondecode
from keyword import iskeyword
from re import compile as regex
from typing import Any, ClassVar, NamedTuple
from uuid import UUID as Uuid  # noqa: N811

from pg.cast import cast_cidr, cast_inet
from pg.core import Connection as Cnx
from pg.core import (
    cast_array,
    cast_hstore,
    cast_record,
    unescape_bytea,
)
from pg.helpers import is_array_type, type_lookup_condition

from .typecode import TypeCode

__all__ = [
    'Decimal',
    'FieldInfo',
    'LocalTypecasts',
    'TypeCache',
    'Typecasts',
    'cast_bool',
    'cast_cidr',
    'cast_date',
    'cast_inet',
    'cast_int2vector',
    'cast_interval',
    'cast_money',
    'cast_time',
    'cast_timestamp',
    'cast_timestamptz',
    'cast_timetz',
    'decimal_type',
    'get_typecast',
    'reset_typecast',
    'set_typecast'
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
_re_placeholder = regex(r'\$(\d)')
_re_placeholder_bytes = regex(rb'\$(\d)')


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
    if value == 'infinity':  # possible since PostgreSQL 17
        return timedelta.max
    if value == '-infinity':
        return timedelta.min
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
        'oid8': int,
        'hstore': cast_hstore, 'json': jsondecode, 'jsonb': jsondecode,
        'float4': float, 'float8': float,
        'numeric': Decimal, 'money': cast_money,
        'date': cast_date, 'interval': cast_interval,
        'time': cast_time, 'timetz': cast_timetz,
        'timestamp': cast_timestamp, 'timestamptz': cast_timestamptz,
        'int2vector': cast_int2vector, 'uuid': Uuid,
        'anyarray': cast_array, 'record': cast_record}

    cnx: Cnx | None = None  # for local connection specific instances

    # incremented when the global typecasts change, so that connections can
    # drop the typecasts they have derived from the old global typecasts
    defaults_changed: ClassVar[int] = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the typecasts."""
        super().__init__(*args, **kwargs)
        self._records: set[str] = set()  # types with cached record casts

    def _drop_record_casts(self) -> None:
        """Drop the cached casts for composite types.

        These casts use the casts for their fields, which may change.
        They will be created again when needed.
        """
        for typ in self._records:
            self.pop(typ, None)
        self._records.clear()

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
            # create array cast, but do not cache it, since
            # the name might not be the name of an array type
            cast = self.create_array_cast(self[typ[1:]])
        return cast

    @staticmethod
    def _connection_arg(func: Callable) -> str | None:
        """Get the name of the connection argument of a typecast function.

        Like in the pg module, the argument is named 'connection'. The name
        'cnx' used by the built-in typecast functions is also accepted.
        """
        try:
            args = get_args(func)[1:]
        except (TypeError, ValueError):
            return None
        for name in ('connection', 'cnx'):
            if name in args:
                return name
        return None

    def _add_connection(self, cast: Callable) -> Callable:
        """Add a connection argument to the typecast function if necessary."""
        if not self.cnx:
            return cast
        name = self._connection_arg(cast)
        if not name:
            return cast
        return partial(cast, **{name: self.cnx})

    def get(self, typ: str, default: Callable | None = None  # type: ignore[override]
            ) -> Callable | None:
        """Get the typecast function for the given database type."""
        return self[typ] or default

    def set(self, typ: str | Sequence[str], cast: Callable | None) -> None:
        """Set a typecast function for the specified database type(s)."""
        self._drop_record_casts()
        if isinstance(typ, str):
            typ = [typ]
        if cast is None:
            for t in typ:
                self.pop(t, None)
        else:
            if not callable(cast):
                raise TypeError("Cast parameter must be callable")
            for t in typ:
                self[t] = self._add_connection(cast)

    def reset(self, typ: str | Sequence[str] | None = None) -> None:
        """Reset the typecasts for the specified type(s) to their defaults.

        When no type is specified, all typecasts will be reset.
        """
        self._drop_record_casts()
        if typ is None:
            # the defaults will be fetched and bound again when needed
            self.clear()
        else:
            if isinstance(typ, str):
                typ = [typ]
            for t in typ:
                cast = self._get_default(t)
                if cast:
                    self[t] = self._add_connection(cast)
                else:
                    self.pop(t, None)

    def _get_default(self, typ: str) -> Callable | None:
        """Get the default typecast that has been set for the given type.

        Array casts are not derived from the names of the element types,
        since the names might not be the names of array types.
        """
        defaults = self.defaults
        if typ in defaults:
            return defaults[typ]
        if typ.startswith('_'):
            return None
        return defaults.get(typ)

    def create_array_cast(self, basecast: Callable | None,
                          delim: str = ',') -> Callable:
        """Create an array typecast for the given base cast and delimiter."""
        cast_array = self['anyarray']
        # values that cannot be decoded are passed as bytes, keep them
        elemcast = None if basecast is str else basecast

        if delim != ',':
            delim_bytes = delim.encode('ascii')

            def cast(v: Any) -> list:
                return cast_array(v, elemcast, delim_bytes)
        else:
            def cast(v: Any) -> list:
                return cast_array(v, elemcast)
        return cast

    def create_record_cast(self, name: str, fields: Sequence[str],
                           casts: Sequence[Callable | None]) -> Callable:
        """Create a named record typecast for the given fields and casts."""
        cast_record = self['record']
        if not name.isidentifier() or iskeyword(name):
            name = 'Record'  # the type name cannot be used in Python
        # field names that cannot be used in Python are renamed
        record = namedtuple(name, fields, rename=True)  # type: ignore[misc]
        # values that cannot be decoded are passed as bytes, keep them
        fieldcasts = [None if c is str else c for c in casts]

        def cast(v: Any) -> record:
            # noinspection PyArgumentList
            return record(*cast_record(v, fieldcasts))
        return cast


_typecasts = Typecasts()  # this is the global typecast dictionary


def is_builtin_typecast(typ: str, cast: Callable | None) -> bool:
    """Check whether this is the built-in default typecast for the type.

    The built-in default typecasts are made for the base types of
    PostgreSQL. They should not be used for composite types, domains,
    enums and ranges that happen to have the same name in another schema,
    unlike typecasts that have been set explicitly for the name.
    """
    if isinstance(cast, partial):  # with an added connection
        cast = cast.func
    return cast is not None and cast is Typecasts.defaults.get(typ)


def get_typecast(typ: str) -> Callable | None:
    """Get the global typecast function for the given database type."""
    return _typecasts.get(typ)


def set_typecast(typ: str | Sequence[str], cast: Callable | None) -> None:
    """Set a global typecast function for the given database type(s).

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call con.type_cache.reset_typecast().
    """
    _typecasts.set(typ, cast)
    Typecasts.defaults_changed += 1


def reset_typecast(typ: str | Sequence[str] | None = None) -> None:
    """Reset the global typecasts for the given type(s) to their default.

    When no type is specified, all typecasts will be reset.

    Note that connections cache cast functions. To be sure a global change
    is picked up by a running connection, call con.type_cache.reset_typecast().
    """
    _typecasts.reset(typ)
    Typecasts.defaults_changed += 1


class LocalTypecasts(Typecasts):
    """Map typecasts, including local composite types, to cast functions."""

    defaults = _typecasts

    cnx: Cnx | None = None  # set in connection specific instances

    def __missing__(self, typ: str) -> Callable | None:
        """Create a cast function if it is not cached."""
        cast: Callable | None = self._get_default(typ)
        if cast:
            cast = self._add_connection(cast)
            self[typ] = cast
        elif typ.startswith('_'):
            # create array cast, but do not cache it, since
            # the name might not be the name of an array type
            cast = self.create_array_cast(self[typ[1:]])
        else:
            fields = self.get_fields(typ)
            if fields:
                casts = [self[field.type] for field in fields]
                field_names = [field.name for field in fields]
                cast = self.create_record_cast(typ, field_names, casts)
                self[typ] = cast
                self._records.add(typ)
        return cast

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_fields(self, typ: str) -> list[FieldInfo]:
        """Return the fields for the given record type.

        This method can be overridden in subclasses to support casting
        composite types by name. Connections do not use it, since the
        TypeCache creates the typecasts for composite types from their
        type codes.
        """
        return []


class FieldInfo(NamedTuple):
    """Name and type of a field of a composite type."""

    name: str
    type: TypeCode | None


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
        self._typecasts.cnx = cnx
        # typecasts for type codes, cached by OID
        self._type_casts: dict[int, Callable | None] = {}
        self._defaults_changed = Typecasts.defaults_changed
        self._query_pg_type = (
            "SELECT t.oid, t.typname, t.typlen, t.typtype, t.typcategory,"
            " t.typdelim, t.typrelid, t.typbasetype, t.typelem,"
            " (SELECT e.typtype FROM pg_catalog.pg_type e"
            " WHERE e.oid OPERATOR(pg_catalog.=) t.typelem)"
            " FROM pg_catalog.pg_type t WHERE {}")

    def __missing__(self, key: int | str | bytes) -> TypeCode:
        """Get the type info from the database if it is not cached."""
        try:
            condition, params = type_lookup_condition(key)
        except ValueError:
            res = None
        else:
            # the source object does not support parameters
            cmd = self._query_pg_type.format(condition)
            if isinstance(key, bytes):  # raw name in the client encoding
                raw_values = [b"'" + self._escape_string(param) + b"'"
                              for param in params if isinstance(param, bytes)]
                self._src.execute(_re_placeholder_bytes.sub(
                    lambda m: raw_values[int(m.group(1)) - 1],
                    cmd.encode('ascii')))
            else:
                values = [str(param) if isinstance(param, int)
                          else f"'{self._escape_string(param)}'"
                          for param in params if not isinstance(param, bytes)]
                self._src.execute(_re_placeholder.sub(
                    lambda m: values[int(m.group(1)) - 1], cmd))
            res = self._src.fetch(1)
        if not res:
            raise KeyError(f'Type {key!r} could not be found')
        r = res[0]
        oid = int(r[0])
        if oid in self:  # already cached under another name
            type_code = self[oid]
        else:
            delim, relid, elem, base = r[5], int(r[6]), int(r[8]), None
            elem_type = r[9] or ''  # the type of the element type
            if r[3] == 'd' and int(r[7]):
                # domains are handled like their base types
                base = self[int(r[7])]
                base = base.base or base  # resolve domains over domains
                delim, relid, elem = base.delim, base.relid, base.elem
                elem_type = base._elem_type
            type_code = TypeCode.create(
                oid, r[1], int(r[2]), r[3], r[4], delim, relid, base, elem)
            type_code._elem_type = elem_type
        # a type found by OID is not cached under its name, since this name
        # could refer to a different type that comes first in the search path
        self[type_code.oid] = self[key] = type_code
        return type_code

    def get(self, key: int | str,  # type: ignore[override]
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

    def _get_named_cast(self, typ: str) -> Callable | None:
        """Get the typecast function set for the given type name.

        Returns None if no typecast and no default has been set for it.
        Typecasts for arrays are never derived from the name here.
        """
        typecasts = self._typecasts
        if typ in typecasts:
            return typecasts[typ]
        cast = typecasts._get_default(typ)
        return typecasts[typ] if cast else None  # bind and cache it

    def _get_cast(self, typ: TypeCode) -> Callable | None:
        """Get the typecast function for the given type code.

        A typecast set for the name of the type is used first, but the
        built-in default typecasts only for base types. Otherwise,
        the typecast is created from the base type of domains, the fields
        of composite types or the element type of arrays. These are
        looked up by their OIDs, not by their names, which might be
        ambiguous. The result is cached under the OID of the type until
        typecasts are set or reset.
        """
        type_casts = self._type_casts
        if self._defaults_changed != Typecasts.defaults_changed:
            # the typecasts may depend on changed global typecasts
            type_casts.clear()
            self._defaults_changed = Typecasts.defaults_changed
        oid = typ.oid
        try:
            return type_casts[oid]
        except KeyError:
            pass
        name = str(typ)
        cast = self._get_named_cast(name)
        if cast and typ.type not in 'bp' and is_builtin_typecast(name, cast):
            # built-in typecasts are only used for base types
            cast = None
        if not cast:
            cast = self._create_cast(typ)
        type_casts[oid] = cast
        return cast

    def _create_cast(self, typ: TypeCode) -> Callable | None:
        """Create a typecast function from the structure of the given type."""
        if typ.base:
            # domains are cast like their base types,
            # unless a typecast has been set for the domain
            return self._get_cast(typ.base)
        typecasts = self._typecasts
        if typ.relid:
            fields = self.get_fields(typ)
            if not fields:
                return None
            casts = [self._get_cast(field.type) if field.type else None
                     for field in fields]
            field_names = [field.name for field in fields]
            return typecasts.create_record_cast(str(typ), field_names, casts)
        if is_array_type(str(typ), typ.len, typ.elem):
            elem_type = self.get(typ.elem)
            if not elem_type:
                return typecasts.create_array_cast(None)
            return typecasts.create_array_cast(
                self._get_cast(elem_type), elem_type.delim)
        return None

    def get_typecast(self, typ: int | str | TypeCode) -> Callable | None:
        """Get the typecast function for the given database type."""
        if isinstance(typ, TypeCode) and hasattr(typ, 'oid'):
            return self._get_cast(typ)
        if isinstance(typ, str):
            cast = self._get_named_cast(typ)
            if cast:
                return cast
        type_code = self.get(typ)
        if type_code:
            return self._get_cast(type_code)
        return self._typecasts[typ] if isinstance(typ, str) else None

    def set_typecast(self, typ: str | Sequence[str],
                     cast: Callable | None) -> None:
        """Set a typecast function for the specified database type(s)."""
        self._typecasts.set(typ, cast)
        self._type_casts.clear()

    def reset_typecast(self, typ: str | Sequence[str] | None = None) -> None:
        """Reset the typecast function for the specified database type(s)."""
        self._typecasts.reset(typ)
        self._type_casts.clear()

    def typecast(self, value: Any, typ: int | str | TypeCode) -> Any:
        """Cast the given value according to the given database type."""
        if value is None:
            # for NULL values, no typecast is necessary
            return None
        cast = self.get_typecast(typ)
        if cast is None or cast is str:
            # no typecast is necessary
            return value
        return cast(value)

    def get_row_caster(self, types: Sequence[int | str | TypeCode]
                       ) -> Callable:
        """Get a typecast function for a complete row of values."""
        get_typecast = self.get_typecast
        casts = [get_typecast(typ) for typ in types]
        casts = [cast if cast is not str else None for cast in casts]

        def row_caster(row: Sequence) -> Sequence:
            return [value if cast is None or value is None else cast(value)
                    for cast, value in zip(casts, row, strict=True)]

        return row_caster
