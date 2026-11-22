"""Support for DB API 2 type codes."""

from __future__ import annotations

__all__ = ['TypeCode']


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
    base: TypeCode | None = None

    # noinspection PyShadowingBuiltins
    @classmethod
    def create(cls, oid: int, name: str, len: int, type: str, category: str,
               delim: str, relid: int, base: TypeCode | None = None
               ) -> TypeCode:
        """Create a type code for a PostgreSQL data type."""
        self = cls(name)
        self.oid = oid
        self.len = len
        self.type = type
        self.category = category
        self.delim = delim
        self.relid = relid
        self.base = base
        return self
