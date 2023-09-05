"""The DB API 2 Connection objects."""

from __future__ import annotations

from contextlib import suppress
from typing import Any, Sequence

from pg.core import Connection as Cnx
from pg.core import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from pg.error import op_error

from .cast import TypeCache
from .constants import shortcutmethods
from .cursor import Cursor

__all__ = ['Connection']

class Connection:
    """Connection object."""

    # expose the exceptions as attributes on the connection object
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def __init__(self, cnx: Cnx) -> None:
        """Create a database connection object."""
        self._cnx: Cnx | None = cnx  # connection
        self._tnx = False  # transaction state
        self.type_cache = TypeCache(cnx)
        self.cursor_type = Cursor
        self.autocommit = False
        try:
            self._cnx.source()
        except Exception as e:
            raise op_error("Invalid connection") from e

    def __enter__(self) -> Connection:
        """Enter the runtime context for the connection object.

        The runtime context can be used for running transactions.

        This also starts a transaction in autocommit mode.
        """
        if self.autocommit:
            cnx = self._cnx
            if not cnx:
                raise op_error("Connection has been closed")
            try:
                cnx.source().execute("BEGIN")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise op_error("Can't start transaction") from e
            else:
                self._tnx = True
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context for the connection object.

        This does not close the connection, but it ends a transaction.
        """
        if et is None and ev is None and tb is None:
            self.commit()
        else:
            self.rollback()

    def close(self) -> None:
        """Close the connection object."""
        if not self._cnx:
            raise op_error("Connection has been closed")
        if self._tnx:
            with suppress(DatabaseError):
                self.rollback()
        self._cnx.close()
        self._cnx = None

    @property
    def closed(self) -> bool:
        """Check whether the connection has been closed or is broken."""
        try:
            return not self._cnx or self._cnx.status != 1
        except TypeError:
            return True

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        if not self._cnx:
            raise op_error("Connection has been closed")
        if self._tnx:
            self._tnx = False
            try:
                self._cnx.source().execute("COMMIT")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise op_error("Can't commit transaction") from e

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction."""
        if not self._cnx:
            raise op_error("Connection has been closed")
        if self._tnx:
            self._tnx = False
            try:
                self._cnx.source().execute("ROLLBACK")
            except DatabaseError:
                raise  # database provides error message
            except Exception as e:
                raise op_error("Can't rollback transaction") from e

    def cursor(self) -> Cursor:
        """Return a new cursor object using the connection."""
        if not self._cnx:
            raise op_error("Connection has been closed")
        try:
            return self.cursor_type(self)
        except Exception as e:
            raise op_error("Invalid connection") from e

    if shortcutmethods:  # otherwise do not implement and document this

        def execute(self, operation: str,
                    parameters: Sequence | None = None) -> Cursor:
            """Shortcut method to run an operation on an implicit cursor."""
            cursor = self.cursor()
            cursor.execute(operation, parameters)
            return cursor

        def executemany(self, operation: str,
                        seq_of_parameters: Sequence[Sequence | None]
                        ) -> Cursor:
            """Shortcut method to run an operation against a sequence."""
            cursor = self.cursor()
            cursor.executemany(operation, seq_of_parameters)
            return cursor