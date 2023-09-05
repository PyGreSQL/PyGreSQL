"""Error helpers."""

from __future__ import annotations

from typing import TypeVar

from .core import (
    DatabaseError,
    Error,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

__all__ = [
    'error', 'db_error', 'if_error', 'int_error', 'op_error', 'prg_error'
]

# Error messages

E = TypeVar('E', bound=Error)

def error(msg: str, cls: type[E]) -> E:
    """Return specified error object with empty sqlstate attribute."""
    error = cls(msg)
    if isinstance(error, DatabaseError):
        error.sqlstate = None
    return error


def db_error(msg: str) -> DatabaseError:
    """Return DatabaseError."""
    return error(msg, DatabaseError)


def int_error(msg: str) -> InternalError:
    """Return InternalError."""
    return error(msg, InternalError)


def prg_error(msg: str) -> ProgrammingError:
    """Return ProgrammingError."""
    return error(msg, ProgrammingError)


def if_error(msg: str) -> InterfaceError:
    """Return InterfaceError."""
    return error(msg, InterfaceError)


def op_error(msg: str) -> OperationalError:
    """Return OperationalError."""
    return error(msg, OperationalError)
