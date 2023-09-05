"""Helper functions."""

from __future__ import annotations

from collections import namedtuple
from decimal import Decimal
from functools import lru_cache
from json import loads as jsondecode
from typing import Any, Callable, Generator, NamedTuple, Sequence

from .core import Query, set_decimal, set_jsondecode, set_query_helpers

SomeNamedTuple = Any  # alias for accessing arbitrary named tuples

__all__ = [
    'quote_if_unqualified', 'oid_key', 'QuoteDict', 'RowCache',
    'dictiter', 'namediter', 'namednext', 'scalariter'
]


# Small helper functions

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

    quote: Callable[[str], str]

    def __getitem__(self, key: str) -> str:
        """Get a quoted value."""
        return self.quote(super().__getitem__(key))


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
            return namedtuple('Row', names, rename=True)._make  # type: ignore
        except ValueError:  # there is still a problem with the field names
            names = [f'column_{n}' for n in range(len(names))]
            return namedtuple('Row', names)._make  # type: ignore

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
        cls.row_factory = lru_cache(maxsize)(row_factory)  # type: ignore


# Helper functions used by the query object

def dictiter(q: Query) -> Generator[dict[str, Any], None, None]:
    """Get query result as an iterator of dictionaries."""
    fields: tuple[str, ...] = q.listfields()
    for r in q:
        yield dict(zip(fields, r))


def namediter(q: Query) -> Generator[SomeNamedTuple, None, None]:
    """Get query result as an iterator of named tuples."""
    row = RowCache.row_factory(q.listfields())
    for r in q:
        yield row(r)


def namednext(q: Query) -> SomeNamedTuple:
    """Get next row from query result as a named tuple."""
    return RowCache.row_factory(q.listfields())(next(q))


def scalariter(q: Query) -> Generator[Any, None, None]:
    """Get query result as an iterator of scalar values."""
    for r in q:
        yield r[0]


# Initialization

def init_core() -> None:
    """Initialize the C extension module."""
    set_decimal(Decimal)
    set_jsondecode(jsondecode)
    set_query_helpers(dictiter, namediter, namednext, scalariter)
