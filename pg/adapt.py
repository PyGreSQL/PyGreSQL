"""Adaptation of parameters."""

from __future__ import annotations

import weakref
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from json import dumps as jsonencode
from math import isinf, isnan
from re import compile as regex
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, List, Mapping, Sequence
from uuid import UUID

from .attrs import AttrDict
from .cast import Typecasts
from .core import InterfaceError, ProgrammingError
from .helpers import quote_if_unqualified

if TYPE_CHECKING:
    from .db import DB

__all__ = [
    'Adapter', 'Bytea', 'DbType', 'DbTypes',
    'Hstore', 'Literal', 'Json', 'UUID'
]


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


class Literal(str):
    """Wrapper class for marking literal SQL values."""



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
        self._typecasts.connection = self._db.db
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
            cmd = self._query_pg_type.format(quote_if_unqualified('$1', key))
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
            else:
                if isinstance(v, bytes):
                    v = v.decode('ascii')
                elif not isinstance(v, str):
                    v = str(v)
                if v:
                    if self._re_record_quote.search(v):
                        v = self._re_record_escape.sub(r'\\\1', v)
                        v = f'"{v}"'
                else:
                    v = '""'
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
