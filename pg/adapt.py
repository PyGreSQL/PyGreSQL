"""Adaptation of parameters."""

from __future__ import annotations

import weakref
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import partial
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network
from json import dumps as jsonencode
from math import isinf, isnan
from re import compile as regex
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, List  # noqa: UP035
from uuid import UUID

from .attrs import AttrDict
from .cast import Typecasts, is_builtin_typecast
from .core import Connection, InterfaceError
from .helpers import (
    format_text,
    is_array_type,
    join_text,
    type_lookup_condition,
)

if TYPE_CHECKING:
    from .db import DB

__all__ = [
    'UUID',
    'Adapter',
    'Bytea',
    'DbType',
    'DbTypes',
    'Hstore',
    'Json',
    'Literal'
]


class Bytea(bytes):
    """Wrapper class for marking Bytea values."""


def _reject_buffer(value: Any) -> None:
    """Raise a TypeError for bytearray and memoryview objects.

    These cannot be adapted yet, and their str() must not be sent instead,
    like it is done for objects of other types.
    """
    if isinstance(value, (bytearray, memoryview)):
        raise TypeError(f'Cannot adapt {type(value).__name__},'
                        ' convert it to bytes or Bytea')


class Hstore(dict):
    """Wrapper class for marking hstore values."""

    _re_quote = regex(r'^[Nn][Uu][Ll][Ll]$|[\s,=>]')
    _re_escape = regex(r'(["\\])')
    _re_quote_bytes = regex(rb'^[Nn][Uu][Ll][Ll]$|[\s,=>]')
    _re_escape_bytes = regex(rb'(["\\])')

    @classmethod
    def _quote(cls, s: Any) -> str | bytes:
        if s is None:
            return 'NULL'
        if isinstance(s, bytes):  # raw text in the client encoding
            if not s:
                return b'""'
            quote_bytes = cls._re_quote_bytes.search(s)
            s = cls._re_escape_bytes.sub(rb'\\\1', s)
            return b'"' + s + b'"' if quote_bytes else s
        if not isinstance(s, str):
            _reject_buffer(s)
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
        value = self._literal(lambda: 'utf-8')
        if isinstance(value, bytes):  # show raw bytes with escapes
            value = value.decode('utf-8', 'backslashreplace')
        return value

    def _literal(self, encoding: Callable[[], str]) -> str | bytes:
        """Create the hstore literal, as bytes if it contains bytes.

        Keys and values given as bytes are raw text in the client encoding,
        and the encoding function returns the codec for this encoding.
        """
        q = self._quote
        return join_text([join_text([q(k), q(v)], '=>', encoding)
                          for k, v in self.items()], ',', encoding)

    def _pg_text(self, codec: str) -> str | bytes:
        """Create the hstore literal for the given Python codec.

        This is used by the C extension for values that are sent as text.
        """
        return self._literal(lambda: codec)


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


class Literal(str):
    """Wrapper class for marking literal SQL values."""



class _SimpleTypes(dict):
    """Dictionary mapping pg_type names to simple type names.

    The corresponding Python types and simple names are also mapped.
    """

    _type_aliases: Mapping[str, list[str | type]] = MappingProxyType({
        'bool': [bool],
        'bytea': [Bytea, bytes],
        'date': ['interval', 'time', 'timetz', 'timestamp', 'timestamptz',
                 'abstime', 'reltime',  # these are very old
                 'datetime', 'timedelta',  # these do not really exist
                 date, time, datetime, timedelta],
        'float': ['float4', 'float8', float],
        'int': ['cid', 'int2', 'int4', 'int8', 'oid', 'oid8', 'xid', int],
        'hstore': [Hstore], 'json': ['jsonb', Json], 'uuid': [UUID],
        'num': ['numeric', Decimal], 'money': [],
        'text': ['bpchar', 'char', 'name', 'varchar', str]
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
                    # support builtin and legacy typing generics
                    array = f'{typ}[]'
                    self[list[key]] = array  # type: ignore[valid-type]
                    self[List[key]] = array  # type: ignore[valid-type]  # noqa: UP006

    def __missing__(self, key: Any) -> str:
        """Unmapped types are interpreted as text.

        Array type names like "int[]" or "_int4" are mapped to arrays of
        the simple type of the element, arrays of records to "record[]".
        """
        if isinstance(key, str):
            if key.endswith('[]'):
                base = key[:-2]
                while base.endswith('[]'):
                    base = base[:-2]
                if base == 'record':
                    return 'record[]'
                simple = self[base]
                return simple if simple.endswith('[]') else f'{simple}[]'
            if key.startswith('_'):  # PostgreSQL name of an array type
                return self[f'{key[1:]}[]']
        return 'text'

    def get_type_dict(self) -> dict[type, str]:
        """Get a plain dictionary of only the types."""
        return {key: typ for key, typ in self.items()
                if not isinstance(key, (str, tuple))}


_simpletypes = _SimpleTypes()
_simple_type_dict = _simpletypes.get_type_dict()


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
        elem: the type OID of the elements for array types
        base: the base type for domains, None for other types
        attnames: attributes for composite types

    Domains take simple, delim, relid and elem from their base type.
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
    elem: int
    base: DbType | None = None

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

    _num_types = frozenset([
        'int', 'float', 'num', 'money', 'int2', 'int4', 'int8',
        'float4', 'float8', 'numeric', 'money'])

    def __init__(self, db: DB) -> None:
        """Initialize type cache for connection."""
        super().__init__()
        self._db = weakref.proxy(db)
        self._regtypes = False
        self._typecasts = Typecasts()
        self._typecasts.connection = self._db.db
        # typecasts for type objects, cached by OID
        self._type_casts: dict[int, Callable | None] = {}
        self._defaults_changed = Typecasts.defaults_changed
        self._query_pg_type = (
            "SELECT t.oid, t.typname, t.oid::pg_catalog.regtype,"
            " t.typlen, t.typtype, t.typcategory, t.typdelim, t.typrelid,"
            " t.typelem, t.typbasetype"
            " FROM pg_catalog.pg_type t WHERE {}")

    def add(self, oid: int, pgtype: str, regtype: str,
            typlen: int, typtype: str, category: str, delim: str, relid: int,
            elem: int = 0, basetype: int = 0) -> DbType:
        """Create a PostgreSQL type name with additional info."""
        if oid in self:
            return self[oid]
        base = None
        if typtype == 'd' and basetype:
            # domains are handled like their base types
            base = self[basetype]
            base = base.base or base  # resolve domains over domains
            simple = base.simple
            delim, relid, elem = base.delim, base.relid, base.elem
        elif relid:
            simple = 'record'
        elif typtype not in 'bp':
            # enums and ranges, the names of built-in types do not apply
            simple = 'text'
        else:
            simple = _simpletypes[pgtype]
            if simple.endswith('[]'):
                if category != 'A' or not elem:
                    simple = 'text'  # not an array, only named like one
                else:
                    # the element type might not be the type with the name
                    # of the array type without the underscore
                    elem_type = self.get(elem)
                    if elem_type:
                        if elem_type.relid:
                            simple = 'record[]'
                        elif elem_type.base:
                            # arrays of domains over arrays are not
                            # multidimensional arrays, keep them as text
                            simple = ('text[]' if elem_type.base.elem
                                      else f'{elem_type.simple}[]')
                        elif elem_type.typtype not in 'bp':
                            simple = 'text[]'
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
        typ.elem = elem
        typ.base = base
        typ._get_attnames = self.get_attnames  # type: ignore[assignment]
        return typ

    def __missing__(self, key: int | str | bytes) -> DbType:
        """Get the type info from the database if it is not cached."""
        try:
            condition, params = type_lookup_condition(key)
        except ValueError:
            res = None
        else:
            cmd = self._query_pg_type.format(condition)
            res = self._db.query(cmd, params).getresult()
        if not res:
            raise KeyError(f'Type {key!r} could not be found')
        res = res[0]
        typ = self.add(*res)
        # a type found by OID is not cached under its name, since this name
        # could refer to a different type that comes first in the search path
        self[typ.oid] = self[key] = typ
        return typ

    def get(self, key: int | str,  # type: ignore[override]
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
        return self._db.get_attnames(typ.relid)

    def _get_cast(self, typ: DbType) -> Callable | None:
        """Get the typecast function for the given type object.

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
            # the typecasts may depend on changed defaults
            type_casts.clear()
            self._defaults_changed = Typecasts.defaults_changed
        oid = typ.oid
        try:
            return type_casts[oid]
        except KeyError:
            pass
        typecasts = self._typecasts
        name = typ.pgtype
        cast = None
        if name in typecasts or name in typecasts.defaults:
            cast = typecasts[name]
            if typ.typtype not in 'bp' and is_builtin_typecast(name, cast):
                # built-in typecasts are only used for base types
                cast = None
        if cast is None:
            cast = self._create_cast(typ)
        type_casts[oid] = cast
        return cast

    def _create_cast(self, typ: DbType) -> Callable | None:
        """Create a typecast function from the structure of the given type."""
        if typ.base:
            # domains are cast like their base types,
            # unless a typecast has been set for the domain
            return self._get_cast(typ.base)
        typecasts = self._typecasts
        if typ.relid:
            attnames = self.get_attnames(typ)
            if not attnames:
                return None
            casts = [self._get_cast(t) for t in attnames.values()]
            return typecasts.create_record_cast(typ.pgtype, attnames, casts)
        if is_array_type(typ.pgtype, typ.typlen, typ.elem):
            elem_type = self.get(typ.elem)
            if not elem_type:
                return typecasts.create_array_cast(None)
            return typecasts.create_array_cast(
                self._get_cast(elem_type), elem_type.delim)
        return None

    def get_typecast(self, typ: Any) -> Callable | None:
        """Get the typecast function for the given database type."""
        if isinstance(typ, DbType) and hasattr(typ, 'oid'):
            return self._get_cast(typ)
        typecasts = self._typecasts
        if isinstance(typ, str) and (
                typ in typecasts or typ in typecasts.defaults):
            return typecasts[typ]
        db_type = self.get(typ)
        if db_type:
            return self._get_cast(db_type)
        return typecasts.get(typ) if isinstance(typ, str) else None

    def set_typecast(self, typ: str | Sequence[str], cast: Callable) -> None:
        """Set a typecast function for the specified database type(s)."""
        self._typecasts.set(typ, cast)
        self._type_casts.clear()

    def _clear_type_casts(self) -> None:
        """Clear the typecasts created for type objects.

        These depend on the fields of composite types, which may change.
        """
        self._type_casts.clear()

    def _set_connection(self, cnx: Connection) -> None:
        """Use a new database connection, e.g. after reopening it.

        Types are looked up again, since they may differ in the new session,
        e.g. temporary types, and typecasts that have been bound to the old
        connection are bound to the new one.
        """
        self.clear()
        self._type_casts.clear()
        typecasts = self._typecasts
        old_cnx, typecasts.connection = typecasts.connection, cnx
        for typ, cast in list(typecasts.items()):
            if (isinstance(cast, partial)
                    and cast.keywords.get('connection') is old_cnx):
                typecasts[typ] = partial(
                    cast.func, *cast.args, **{**cast.keywords,
                                              'connection': cnx})

    def reset_typecast(self, typ: str | Sequence[str] | None = None) -> None:
        """Reset the typecast function for the specified database type(s)."""
        self._typecasts.reset(typ)
        self._type_casts.clear()

    def typecast(self, value: Any, typ: Any) -> Any:
        """Cast the given value according to the given database type."""
        if value is None:
            # for NULL values, no typecast is necessary
            return None
        db_type = typ if isinstance(typ, DbType) and hasattr(
            typ, 'oid') else self.get(typ)
        if db_type:
            cast = self._get_cast(db_type)
        elif isinstance(typ, str) and typ:
            cast = self._typecasts.get(typ)
        else:
            cast = None
        if not cast or cast is str:
            # no typecast is necessary
            return value
        return cast(value)


class Adapter:
    """Class providing methods for adapting parameters to the database."""

    _bool_true_values = frozenset(['t', 'true', '1', 'y', 'yes', 'on'])

    _date_literals = frozenset([
        'current_date', 'current_time', 'current_timestamp',
        'localtime', 'localtimestamp'])

    _re_array_quote = regex(r'[{},"\\\s]|^[Nn][Uu][Ll][Ll]$')
    _re_record_quote = regex(r'[(),"\\]')
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
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, Json):
            return str(v)
        return self.db.encode_json(v)

    def _adapt_hstore(self, v: Any) -> str | bytes | None:
        """Adapt a hstore parameter."""
        if v is None:
            return None
        if isinstance(v, (str, bytes)):
            return v
        if isinstance(v, dict):
            if not isinstance(v, Hstore):
                v = Hstore(v)
            return v._literal(self._encoding)
        raise TypeError(f'Hstore parameter {v} has wrong type')

    def _adapt_uuid(self, v: Any) -> str | None:
        """Adapt a UUID parameter."""
        if not v:
            return None
        if isinstance(v, str):
            return v
        return str(v)

    def _encoding(self) -> str:
        """Get the Python codec name of the client encoding."""
        return self.db.encoding

    def _join(self, parts: list[str | bytes], sep: str = ',',
              start: str = '', end: str = '') -> str | bytes:
        """Join parts of a literal, as bytes if any of the parts is bytes."""
        return join_text(parts, sep, self._encoding, start, end)

    def _to_text(self, v: Any) -> str | bytes:
        """Convert a value to text, like the C extension does.

        Hstore values are converted to their literal, as bytes if they
        contain raw text given as bytes, all other values with str().
        """
        if isinstance(v, Hstore):
            return v._literal(self._encoding)
        return str(v)

    _re_array_quote_bytes = regex(rb'[{},"\\\s]|^[Nn][Uu][Ll][Ll]$')
    _re_record_quote_bytes = regex(rb'[(),"\\]')
    _re_escape_bytes = regex(rb'(["\\])')

    @classmethod
    def _quote_array_element(cls, v: str | bytes) -> str | bytes:
        """Quote an adapted element of an array if necessary."""
        if isinstance(v, bytes):
            if not v:
                return b'""'
            if cls._re_array_quote_bytes.search(v):
                v = b'"' + cls._re_escape_bytes.sub(rb'\\\1', v) + b'"'
            return v
        if not v:
            return '""'
        if cls._re_array_quote.search(v):
            v = cls._re_array_escape.sub(r'\\\1', v)
            v = f'"{v}"'
        return v

    @classmethod
    def _quote_record_field(cls, v: str | bytes) -> str | bytes:
        """Quote an adapted field of a record if necessary."""
        if isinstance(v, bytes):
            if not v:
                return b'""'
            if cls._re_record_quote_bytes.search(v):
                v = b'"' + cls._re_escape_bytes.sub(rb'\\\1', v) + b'"'
            return v
        if not v:
            return '""'
        if cls._re_record_quote.search(v):
            v = cls._re_record_escape.sub(r'\\\1', v)
            v = f'"{v}"'
        return v

    def _adapt_array(self, v: Any,
                     adapt: Callable[[Any], str | bytes | None],
                     reject_buffers: bool = True) -> str | bytes:
        """Adapt an array parameter using the given element adapter.

        Elements given as bytes are raw text in the client encoding,
        which is passed unchanged. Elements of the types bytearray and
        memoryview raise a TypeError, unless reject_buffers is false.
        """
        if isinstance(v, list):
            return self._join(
                [self._adapt_array(v, adapt, reject_buffers) for v in v],
                ',', '{', '}')
        if v is None:
            return 'null'
        if isinstance(v, bytes):
            return self._quote_array_element(v)
        if reject_buffers:
            _reject_buffer(v)
        v = adapt(v)
        return 'null' if v is None else v

    def _adapt_text_array(self, v: Any) -> str | bytes:
        """Adapt a text type array parameter."""
        return self._adapt_array(
            v, lambda v: self._quote_array_element(self._to_text(v)))

    _adapt_date_array = _adapt_text_array

    def _adapt_bool_array(self, v: Any) -> str | bytes:
        """Adapt a boolean array parameter."""
        def adapt(v: Any) -> str | None:
            if isinstance(v, str):
                if not v:
                    return None
                v = v.lower() in self._bool_true_values
            return 't' if v else 'f'
        return self._adapt_array(v, adapt)

    def _adapt_num_array(self, v: Any) -> str | bytes:
        """Adapt a numeric array parameter."""
        return self._adapt_array(
            v, lambda v: None if not v and v != 0 else str(v))

    _adapt_int_array = _adapt_float_array = _adapt_money_array = \
        _adapt_num_array

    def _adapt_bytea_array(self, v: Any) -> bytes:
        """Adapt a bytea array parameter."""
        if isinstance(v, list):
            return b'{' + b','.join(
                self._adapt_bytea_array(v) for v in v) + b'}'
        if v is None:
            return b'null'
        _reject_buffer(v)
        return self.db.escape_bytea(v).replace(b'\\', b'\\\\')

    def _adapt_json_array(self, v: Any) -> str | bytes:
        """Adapt a json array parameter."""
        def adapt(v: Any) -> str | bytes | None:
            if v == '':  # empty strings are adapted as null
                return None
            v = self._adapt_json(v)
            return None if v is None else self._quote_array_element(v)
        # the JSON encoder may handle bytearray and memoryview objects
        return self._adapt_array(v, adapt, reject_buffers=False)

    def _adapt_hstore_array(self, v: Any) -> str | bytes:
        """Adapt a hstore array parameter."""
        def adapt(v: Any) -> str | bytes | None:
            v = self._adapt_hstore(v)
            return None if v is None else self._quote_array_element(v)
        return self._adapt_array(v, adapt)

    def _adapt_uuid_array(self, v: Any) -> str | bytes:
        """Adapt a UUID array parameter."""
        def adapt(v: Any) -> str | bytes | None:
            v = self._adapt_uuid(v)
            return None if v is None else self._quote_array_element(v)
        return self._adapt_array(v, adapt)

    def _adapt_record_array(self, v: Any, typ: Any) -> str | bytes:
        """Adapt a record array parameter with given type."""
        elem = getattr(typ, 'elem', None)
        elem_type = self.db.dbtypes.get(elem) if elem else None

        def adapt(v: Any) -> str | bytes:
            if isinstance(v, tuple):
                v = self._adapt_record(
                    v, elem_type or self.guess_simple_type(v))
            else:
                v = self._to_text(v)
            return self._quote_array_element(v)
        return self._adapt_array(v, adapt)

    def _adapt_record(self, v: Any, typ: Any) -> str | bytes:
        """Adapt a record parameter with given type."""
        typ = self.get_attnames(typ).values()
        if len(typ) != len(v):
            raise TypeError(f'Record parameter {v} has wrong size')
        adapt = self.adapt
        value: list[str | bytes] = []
        for v, t in zip(v, typ, strict=True):  # noqa: B020
            v = adapt(v, t)
            if v is None:
                v = ''
            else:
                if not isinstance(v, (str, bytes)):
                    v = self._to_text(v)
                v = self._quote_record_field(v)
            value.append(v)
        return self._join(value, ',', '(', ')')

    def adapt(self, value: Any, typ: Any =  None) -> Any:
        """Adapt a value with known database type.

        Values given as bytes for other types than bytea are raw text in
        the client encoding, which is passed unchanged.
        """
        if value is not None and not isinstance(value, Literal):
            if typ:
                simple = self.get_simple_name(typ)
            else:
                typ = simple = self.guess_simple_type(value) or 'text'
            if isinstance(value, bytes) and simple != 'bytea':
                return value
            pg_str = getattr(value, '__pg_str__', None)
            if pg_str:
                value = pg_str(typ)
            if not (simple == 'json' or (
                    simple == 'json[]' and isinstance(value, list))):
                # the JSON encoder may handle bytearray and memoryview
                _reject_buffer(value)
            if simple == 'text':
                pass
            elif simple == 'record':
                if isinstance(value, tuple):
                    value = self._adapt_record(value, typ)
            elif simple.endswith('[]'):
                if isinstance(value, list):
                    if simple == 'record[]':
                        value = self._adapt_record_array(value, typ)
                    else:
                        adapt = getattr(self, f'_adapt_{simple[:-2]}_array')
                        value = adapt(value)
            else:
                adapt = getattr(self, f'_adapt_{simple}')
                value = adapt(value)
        return value

    def _adapt_parameter(self, value: Any, typ: Any = None) -> Any:
        """Adapt a value that is passed as a separate parameter.

        Binary data for bytea is passed as Bytea, which is sent to the
        server in binary format. Values nested in other values and values
        put into the SQL inline are still adapted as text.
        """
        if isinstance(value, bytes) and not hasattr(value, '__pg_str__'):
            simple = (self.get_simple_name(typ) if typ
                      else self.guess_simple_type(value))
            if simple == 'bytea':
                return value if isinstance(value, Bytea) else Bytea(value)
        return self.adapt(value, typ)

    @staticmethod
    def simple_type(name: str) -> DbType:
        """Create a simple database type object with the given name."""
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
        if isinstance(value, str):
            return 'text'
        if isinstance(value, bytes):
            return 'bytea'
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
        if isinstance(value, bytes):
            value = self.db.escape_bytea(value).decode('ascii')
        elif isinstance(value, (datetime, date, time, timedelta)):
            value = str(value)
        if isinstance(value, str):
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
            return self._join(
                [self._inline_part(q(v, nested=True)) for v in value],
                ',', '[' if nested else 'ARRAY[', ']')
        if isinstance(value, tuple):
            q = self.adapt_inline
            return self._join(
                [self._inline_part(q(v)) for v in value], ',', '(', ')')
        if isinstance(value, Json):
            value = self.db.escape_string(str(value))
            return f"'{value}'::json"
        if isinstance(value, Hstore):
            try:
                value = value._literal(self._encoding)
            except TypeError as e:  # e.g. bytearray as key or value
                raise InterfaceError(str(e)) from e
            if isinstance(value, bytes):
                return b"'" + self.db.escape_string(value) + b"'::hstore"
            value = self.db.escape_string(value)
            return f"'{value}'::hstore"
        if isinstance(value, UUID):
            return f"'{value}'::uuid"
        if isinstance(value, (IPv4Address, IPv6Address)):  # or interface
            value = self.db.escape_string(str(value))
            return f"'{value}'::inet"
        if isinstance(value, (IPv4Network, IPv6Network)):
            value = self.db.escape_string(str(value))
            return f"'{value}'::cidr"
        pg_repr = getattr(value, '__pg_repr__', None)
        if not pg_repr:
            raise InterfaceError(
                f'Do not know how to adapt type {type(value)}')
        value = pg_repr()
        if isinstance(value, (tuple, list)):
            value = self.adapt_inline(value)
        return value

    @staticmethod
    def _inline_part(value: Any) -> str | bytes:
        """Convert an adapted inline value to a part of a literal."""
        return value if isinstance(value, (str, bytes)) else str(value)

    def parameter_list(self) -> _ParameterList:
        """Return a parameter list for parameters with known database types.

        The list has an add(value, typ) method that will build up the
        list and return either the literal value or a placeholder.
        """
        params = _ParameterList()
        params.adapt = self._adapt_parameter
        return params

    def format_query(self, command: str,
                     values: list | tuple | dict | None = None,
                     types: list | tuple | dict | None = None,
                     inline: bool=False
                     ) -> tuple[str | bytes, _ParameterList]:
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
                    seq_literals = [
                        add(value, typ)
                        for value, typ in zip(values, types, strict=True)]
                else:
                    seq_literals = [add(value) for value in values]
            formatted = format_text(
                command, tuple(seq_literals), self._encoding)
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
                    used_values[key] = values[key]
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
            formatted = format_text(command, map_literals, self._encoding)
        else:
            raise TypeError('The values must be passed as tuple, list or dict')
        return formatted, params
