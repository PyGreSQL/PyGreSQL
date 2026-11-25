"""Helper functions."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Callable, Iterator, Sequence
from decimal import Decimal
from functools import lru_cache
from json import loads as jsondecode
from typing import Any, NamedTuple, TypeAlias

from .core import Query, set_decimal, set_jsondecode, set_query_helpers

SomeNamedTuple: TypeAlias = Any  # alias for accessing arbitrary named tuples

__all__ = [
    'QuoteDict',
    'RowCache',
    'dictiter',
    'format_text',
    'join_text',
    'namediter',
    'namednext',
    'scalariter'
]


# Small helper functions

def format_text(command: str, params: tuple | dict,
                encoding: Callable[[], str]) -> str | bytes:
    """Format a command with literals, as bytes if any literal is bytes.

    Literals given as bytes are raw text in the client encoding. In this
    case, the command and the other literals are encoded with the codec
    returned by the given encoding function.
    """
    values = params.values() if isinstance(params, dict) else params
    if not any(isinstance(value, bytes) for value in values):
        return command % params
    codec = encoding()

    def encode(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode(codec)

    if isinstance(params, dict):
        params = {key.encode(codec): encode(value)
                  for key, value in params.items()}
    else:
        params = tuple(map(encode, params))
    return command.encode(codec) % params


def join_text(parts: Sequence[str | bytes], sep: str,
              encoding: Callable[[], str],
              start: str = '', end: str = '') -> str | bytes:
    """Join parts of a literal, as bytes if any of the parts is bytes.

    Parts given as bytes are raw text in the client encoding. In this case,
    the parts given as str are encoded with the codec returned by the given
    encoding function (raising a UnicodeEncodeError if that is impossible).
    """
    if not any(isinstance(part, bytes) for part in parts):
        return start + sep.join(parts) + end  # type: ignore[arg-type]
    codec = encoding()
    return start.encode() + sep.encode().join(
        part if isinstance(part, bytes) else part.encode(codec)
        for part in parts) + end.encode()


def split_qualified_name(name: str) -> list[str]:
    """Split a possibly schema-qualified name into its parts.

    The name is split at dots. A part can be enclosed in double quotes,
    with a double quote inside written twice; it can then contain dots.
    All parts are taken literally (case is preserved), so that they can
    be escaped individually. Empty parts, unterminated quotes and text
    after a closing quote raise a ValueError.
    """
    parts: list[str] = []
    end = len(name)
    pos = 0
    while True:
        if name.startswith('"', pos):
            chars: list[str] = []
            pos += 1
            while True:
                quote = name.find('"', pos)
                if quote < 0:
                    raise ValueError(
                        f'Unterminated quoted part in name {name!r}')
                chars.append(name[pos:quote])
                pos = quote + 1
                if not name.startswith('"', pos):
                    break
                chars.append('"')
                pos += 1
            if pos < end and name[pos] != '.':
                raise ValueError(
                    f'Unexpected text after quoted part in name {name!r}')
            part = ''.join(chars)
        else:
            dot = name.find('.', pos)
            if dot < 0:
                dot = end
            part = name[pos:dot]
            pos = dot
        if not part:
            raise ValueError(f'Empty part in name {name!r}')
        parts.append(part)
        if pos >= end:
            return parts
        pos += 1  # skip the dot

class QuoteDict(dict):
    """Dictionary with auto quoting of its items.

    The quote attribute must be set to the desired quote function.
    """

    quote: Callable[[Any], Any]
    quoted: dict[str, Any]  # the quoted values that have been accessed

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the dictionary."""
        super().__init__(*args, **kwargs)
        self.quoted = {}

    def __getitem__(self, key: str) -> Any:
        """Get a quoted value."""
        value = self.quoted[key] = self.quote(super().__getitem__(key))
        return value


class RowCache:
    """Global cache for the named tuples used for table rows.

    The result rows for database operations are returned as named tuples
    by default. Since creating namedtuple classes is a somewhat expensive
    operation, we cache up to 1024 of these classes by default.
    """

    @staticmethod
    @lru_cache(maxsize=1024)
    def row_factory(names: Sequence[str]) -> Callable[[Sequence], NamedTuple]:
        """Get a namedtuple factory for row results with the given names."""
        try:
            return namedtuple('Row', names, rename=True)._make  # type: ignore[attr-defined]
        except ValueError:  # there is still a problem with the field names
            names = [f'column_{n}' for n in range(len(names))]
            return namedtuple('Row', names)._make  # type: ignore[attr-defined]

    @classmethod
    def clear(cls) -> None:
        """Clear the namedtuple factory cache."""
        cls.row_factory.cache_clear()

    @classmethod
    def change_size(cls, maxsize: int | None) -> None:
        """Change the size of the namedtuple factory cache.

        If maxsize is set to None, the cache can grow without bound.
        """
        row_factory = cls.row_factory.__wrapped__
        cls.row_factory = lru_cache(maxsize)(row_factory)  # type: ignore[method-assign]


# Helper functions used by the query object

def is_array_type(name: str, typlen: int, elem: int) -> bool:
    """Check whether a type is an array type with the usual text format.

    Some types like int2vector, oidvector, name and point also have an
    element type, but are not cast like arrays. PostgreSQL always names
    array types with a leading underscore, while composite types or
    domains with such names have no element type or are handled first.
    The category is not checked, since it is 'P' for _record.
    """
    return bool(elem) and typlen == -1 and name.startswith('_')


def type_lookup_condition(key: int | str | bytes
                          ) -> tuple[str, Sequence[int | str | bytes]]:
    """Get a condition for looking up a type in pg_type by OID or name.

    Returns the condition for the pg_type table aliased as t, with the
    placeholders $1 and $2, and the list of the values for these.

    Integer keys are type OIDs. Names are split like table names with
    split_qualified_name(), so that parts are taken literally. Names
    without a schema are looked up in the search path, the schema pg_temp
    means the temporary schema of the session. Since the name is not
    parsed by the server, the lookup cannot raise an error there, which
    would abort the current transaction. Names given as bytes are raw
    names in the client encoding.

    Invalid names and OIDs out of range raise a ValueError, keys of other
    types raise a TypeError.
    """
    if isinstance(key, int) and not isinstance(key, bool):
        if not 0 <= key <= 0xffffffff:
            raise ValueError(f'Invalid type OID {key!r}')
        return 't.oid OPERATOR(pg_catalog.=) $1', [key]
    parts: Sequence[str | bytes]
    if isinstance(key, str):
        parts = split_qualified_name(key)
    elif isinstance(key, bytes):
        # quotes and dots are ASCII, so the raw name can be split this way
        parts = [part.encode('latin-1') for part in
                 split_qualified_name(key.decode('latin-1'))]
    else:
        raise TypeError(f'Invalid type key {key!r}')
    if len(parts) == 1:
        return ('t.typname OPERATOR(pg_catalog.=) $1::pg_catalog.name'
                ' AND pg_catalog.pg_type_is_visible(t.oid)'), parts
    if len(parts) == 2:
        return ("t.typnamespace OPERATOR(pg_catalog.=) CASE"
                " WHEN $1::pg_catalog.name OPERATOR(pg_catalog.=) 'pg_temp'"
                " THEN pg_catalog.pg_my_temp_schema()"
                " ELSE (SELECT n.oid FROM pg_catalog.pg_namespace n"
                " WHERE n.nspname OPERATOR(pg_catalog.=) $1::pg_catalog.name)"
                " END AND t.typname OPERATOR(pg_catalog.=) $2::pg_catalog.name"
                ), parts
    raise ValueError(f'Invalid type name {key!r}')


def dictiter(q: Query) -> Iterator[dict[str, Any]]:
    """Get query result as an iterator of dictionaries."""
    fields: tuple[str, ...] = q.listfields()
    for r in q:
        yield dict(zip(fields, r, strict=True))


def namediter(q: Query) -> Iterator[SomeNamedTuple]:
    """Get query result as an iterator of named tuples."""
    row = RowCache.row_factory(q.listfields())
    for r in q:
        yield row(r)


def namednext(q: Query) -> SomeNamedTuple:
    """Get next row from query result as a named tuple."""
    return RowCache.row_factory(q.listfields())(next(q))


def scalariter(q: Query) -> Iterator[Any]:
    """Get query result as an iterator of scalar values."""
    for r in q:
        yield r[0]


# Initialization

def init_core() -> None:
    """Initialize the C extension module."""
    # not at the top since it imports this module
    from .adapt import Bytea, Hstore
    set_decimal(Decimal)
    set_jsondecode(jsondecode)
    set_query_helpers(
        dictiter, namediter, namednext, scalariter, Bytea, Hstore)
