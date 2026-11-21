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
    'oid_key',
    'quote_if_unqualified',
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


def quote_if_unqualified(param: str, name: int | str) -> str:
    """Quote parameter representing a qualified name.

    Puts a quote_ident() call around the given parameter unless
    the name contains a dot, in which case the name is ambiguous
    (could be a qualified name or just a name with a dot in it)
    and must be quoted manually by the caller.
    """
    if isinstance(name, str) and '.' not in name:
        return f'quote_ident({param})'
    return param

def oid_key(table: str) -> str:
    """Build oid key from a table name."""
    return f'oid({table})'

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
    set_decimal(Decimal)
    set_jsondecode(jsondecode)
    set_query_helpers(dictiter, namediter, namednext, scalariter)
