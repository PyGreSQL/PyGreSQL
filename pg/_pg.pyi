"""Type hints for the PyGreSQL C extension."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Sequence, TypeVar

try:
    AnyStr = TypeVar('AnyStr', str, bytes, str | bytes)
except TypeError:  # Python < 3.10
    AnyStr = Any  # type: ignore
SomeNamedTuple = Any  # alias for accessing arbitrary named tuples

version: str
__version__: str

RESULT_EMPTY: int
RESULT_DML: int
RESULT_DDL: int
RESULT_DQL: int

TRANS_IDLE: int
TRANS_ACTIVE: int
TRANS_INTRANS: int
TRANS_INERROR: int
TRANS_UNKNOWN: int

POLLING_OK: int
POLLING_FAILED: int
POLLING_READING: int
POLLING_WRITING: int

INV_READ: int
INV_WRITE: int

SEEK_SET: int
SEEK_CUR: int
SEEK_END: int


class Error(Exception):
    """Exception that is the base class of all other error exceptions."""


class Warning(Exception):  # noqa: N818
    """Exception raised for important warnings."""


class InterfaceError(Error):
    """Exception raised for errors related to the database interface."""


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    sqlstate: str | None


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""


class OperationalError(DatabaseError):
    """Exception raised for errors related to the operation of the database."""


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors."""


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity is affected."""


class DataError(DatabaseError):
    """Exception raised for errors  due to problems with the processed data."""


class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is not supported."""


class InvalidResultError(DataError):
    """Exception when a database operation produced an invalid result."""


class NoResultError(InvalidResultError):
    """Exception when a database operation did not produce any result."""


class MultipleResultsError(InvalidResultError):
    """Exception when a database operation produced multiple results."""


class Source:
    """Source object."""

    arraysize: int
    resulttype: int
    ntuples: int
    nfields: int

    def execute(self, sql: str) -> int | None:
        """Execute a SQL statement."""
        ...

    def fetch(self, num: int) -> list[tuple]:
        """Return the next num rows from the last result in a list."""
        ...

    def listinfo(self) -> tuple[tuple[int, str, int, int, int], ...]:
        """Get information for all fields."""
        ...

    def oidstatus(self) -> int | None:
        """Return oid of last inserted row (if available)."""
        ...

    def putdata(self, buffer: str | bytes | BaseException | None
                ) -> int | None:
        """Send data to server during copy from stdin."""
        ...

    def getdata(self, decode: bool | None = None) -> str | bytes | int:
        """Receive data to server during copy to stdout."""
        ...

    def close(self) -> None:
        """Close query object without deleting it."""
        ...


class LargeObject:
    """Large object."""

    oid: int
    pgcnx: Connection
    error: str

    def open(self, mode: int) -> None:
        """Open a large object.

        The valid values for 'mode' parameter are defined as the module level
        constants INV_READ and INV_WRITE.
        """
        ...

    def close(self) -> None:
        """Close a large object."""
        ...

    def read(self, size: int) -> bytes:
        """Read data from large object."""
        ...

    def write(self, data: bytes) -> None:
        """Write data to large object."""
        ...

    def seek(self, offset: int, whence: int) -> int:
        """Change current position in large object.

        The valid values for the 'whence' parameter are defined as the
        module level constants SEEK_SET, SEEK_CUR and SEEK_END.
        """
        ...

    def unlink(self) -> None:
        """Delete large object."""
        ...

    def size(self) -> int:
        """Return the large object size."""
        ...

    def export(self, filename: str) -> None:
        """Export a large object to a file."""
        ...


class Connection:
    """Connection object.

    This object handles a connection to a PostgreSQL database.
    It embeds and hides all the parameters that define this connection,
    thus just leaving really significant parameters in function calls.
    """

    host: str
    port: int
    db: str
    options: str
    error: str
    status: int
    user : str
    protocol_version: int
    server_version: int
    socket: int
    backend_pid: int
    ssl_in_use: bool
    ssl_attributes: dict[str, str | None]

    def source(self) -> Source:
        """Create a new source object for this connection."""
        ...

    def query(self, cmd: str, args: Sequence | None = None) -> Query:
        """Create a new query object for this connection.

        Note that if the command is something other than DQL, this method
        can return an int, str or None instead of a Query.
        """
        ...

    def send_query(self, cmd: str, args: Sequence | None = None) -> Query:
        """Create a new asynchronous query object for this connection."""
        ...

    def query_prepared(self, name: str, args: Sequence | None = None) -> Query:
        """Execute a prepared statement."""
        ...

    def prepare(self, name: str, cmd: str) -> None:
        """Create a prepared statement."""
        ...

    def describe_prepared(self, name: str) -> Query:
        """Describe a prepared statement."""
        ...

    def poll(self) -> int:
        """Complete an asynchronous connection and get its state."""
        ...

    def reset(self) -> None:
        """Reset the connection."""
        ...

    def cancel(self) -> None:
        """Abandon processing of current SQL command."""
        ...

    def close(self) -> None:
        """Close the database connection."""
        ...

    def fileno(self) -> int:
        """Get the socket used to connect to the database."""
        ...

    def get_cast_hook(self) -> Callable | None:
        """Get the function that handles all external typecasting."""
        ...

    def set_cast_hook(self, hook: Callable | None) -> None:
        """Set a function that will handle all external typecasting."""
        ...

    def get_notice_receiver(self) -> Callable | None:
        """Get the current notice receiver."""
        ...

    def set_notice_receiver(self, receiver: Callable | None) -> None:
        """Set a custom notice receiver."""
        ...

    def getnotify(self) -> tuple[str, int, str] | None:
        """Get the last notify from the server."""
        ...

    def inserttable(self, table: str, values: Sequence[list|tuple],
                    columns: list[str] | tuple[str, ...] | None = None) -> int:
        """Insert a Python iterable into a database table."""
        ...

    def transaction(self) -> int:
        """Get the current in-transaction status of the server.

        The status returned by this method can be TRANS_IDLE (currently idle),
        TRANS_ACTIVE (a command is in progress), TRANS_INTRANS (idle, in a
        valid transaction block), or TRANS_INERROR (idle, in a failed
        transaction block).  TRANS_UNKNOWN is reported if the connection is
        bad.  The status TRANS_ACTIVE is reported only when a query has been
        sent to the server and not yet completed.
        """
        ...

    def parameter(self, name: str) -> str | None:
        """Look up a current parameter setting of the server."""
        ...

    def date_format(self) -> str:
        """Look up the date format currently being used by the database."""
        ...

    def escape_literal(self, s: AnyStr) -> AnyStr:
        """Escape a literal constant for use within SQL."""
        ...

    def escape_identifier(self, s: AnyStr) -> AnyStr:
        """Escape an identifier for use within SQL."""
        ...

    def escape_string(self, s: AnyStr) -> AnyStr:
        """Escape a string for use within SQL."""
        ...

    def escape_bytea(self, s: AnyStr) -> AnyStr:
        """Escape binary data for use within SQL as type 'bytea'."""
        ...

    def putline(self, line: str) -> None:
        """Write a line to the server socket."""
        ...

    def getline(self) -> str:
        """Get a line from server socket."""
        ...

    def endcopy(self) -> None:
        """Synchronize client and server."""
        ...

    def set_non_blocking(self, nb: bool) -> None:
        """Set the non-blocking mode of the connection."""
        ...

    def is_non_blocking(self) -> bool:
        """Get the non-blocking mode of the connection."""
        ...

    def locreate(self, mode: int) -> LargeObject:
        """Create a large object in the database.

        The valid values for 'mode' parameter are defined as the module level
        constants INV_READ and INV_WRITE.
        """
        ...

    def getlo(self, oid: int) -> LargeObject:
        """Build a large object from given oid."""
        ...

    def loimport(self, filename: str) -> LargeObject:
        """Import a file to a large object."""
        ...


class Query:
    """Query object.

    The Query object returned by Connection.query and DB.query can be used
    as an iterable returning rows as tuples.  You can also directly access
    row tuples using their index, and get the number of rows with the
    len() function.  The Query class also provides the several methods
    for accessing the results of the query.
    """

    def __len__(self) -> int:
        ...

    def __getitem__(self, key: int) -> object:
        ...

    def __iter__(self) -> Query:
        ...

    def __next__(self) -> tuple:
        ...

    def getresult(self) -> list[tuple]:
        """Get query values as list of tuples."""
        ...

    def dictresult(self) -> list[dict[str, object]]:
        """Get query values as list of dictionaries."""
        ...

    def dictiter(self) -> Iterable[dict[str, object]]:
        """Get query values as iterable of dictionaries."""
        ...

    def namedresult(self) -> list[SomeNamedTuple]:
        """Get query values as list of named tuples."""
        ...

    def namediter(self) -> Iterable[SomeNamedTuple]:
        """Get query values as iterable of named tuples."""
        ...

    def one(self) -> tuple | None:
        """Get one row from the result of a query as a tuple."""
        ...

    def single(self) -> tuple:
        """Get single row from the result of a query as a tuple."""
        ...

    def onedict(self) -> dict[str, object] | None:
        """Get one row from the result of a query as a dictionary."""
        ...

    def singledict(self) -> dict[str, object]:
        """Get single row from the result of a query as a dictionary."""
        ...

    def onenamed(self) -> SomeNamedTuple | None:
        """Get one row from the result of a query as named tuple."""
        ...

    def singlenamed(self) -> SomeNamedTuple:
        """Get single row from the result of a query as named tuple."""
        ...

    def scalarresult(self) -> list:
        """Get first fields from query result as list of scalar values."""

    def scalariter(self) -> Iterable:
        """Get first fields from query result as iterable of scalar values."""
        ...

    def onescalar(self) -> object | None:
        """Get one row from the result of a query as scalar value."""
        ...

    def singlescalar(self) -> object:
        """Get single row from the result of a query as scalar value."""
        ...

    def fieldname(self, num: int) -> str:
        """Get field name from its number."""
        ...

    def fieldnum(self, name: str) -> int:
        """Get field number from its name."""
        ...

    def listfields(self) -> tuple[str, ...]:
        """List field names of query result."""
        ...

    def fieldinfo(self, column: int | str | None) -> tuple[str, int, int, int]:
        """Get information on one or all fields of the query.

        The four-tuples contain the following information:
        The field name, the internal OID number of the field type,
        the size in bytes of the column or a negative value if it is
        of variable size, and a type-specific modifier value.
        """
        ...

    def memsize(self) -> int:
        """Return number of bytes allocated by query result."""
        ...


def connect(dbname: str | None = None,
            host: str | None = None,
            port: int | None = None,
            opt: str | None = None,
            user: str | None = None,
            passwd: str | None = None,
            nowait: int | None = None) -> Connection:
    """Connect to a PostgreSQL database."""
    ...


def cast_array(s: str, cast: Callable | None = None,
               delim: bytes | None = None) -> list:
    """Cast a string representing a PostgreSQL array to a Python list."""
    ...


def cast_record(s: str,
                cast: Callable | list[Callable | None] |
                      tuple[Callable | None, ...] | None = None,
                delim: bytes | None = None) -> tuple:
    """Cast a string representing a PostgreSQL record to a Python tuple."""
    ...


def cast_hstore(s: str) -> dict[str, str | None]:
    """Cast a string as a hstore."""
    ...


def escape_bytea(s: AnyStr) -> AnyStr:
    """Escape binary data for use within SQL as type 'bytea'."""
    ...


def unescape_bytea(s: AnyStr) -> bytes:
    """Unescape 'bytea' data that has been retrieved as text."""
    ...


def escape_string(s: AnyStr) -> AnyStr:
    """Escape a string for use within SQL."""
    ...


def get_pqlib_version() -> int:
    """Get the version of libpq that is being used by PyGreSQL."""
    ...


def get_array() -> bool:
    """Check whether arrays are returned as list objects."""
    ...


def set_array(on: bool) -> None:
    """Set whether arrays are returned as list objects."""
    ...


def get_bool() -> bool:
    """Check whether boolean values are returned as bool objects."""
    ...


def set_bool(on: bool | int) -> None:
    """Set whether boolean values are returned as bool objects."""
    ...


def get_bytea_escaped() -> bool:
    """Check whether 'bytea' values are returned as escaped strings."""
    ...


def set_bytea_escaped(on: bool | int) -> None:
    """Set whether 'bytea' values are returned as escaped strings."""
    ...


def get_datestyle() -> str | None:
    """Get the assumed date style for typecasting."""
    ...


def set_datestyle(datestyle: str | None) -> None:
    """Set a fixed date style that shall be assumed when typecasting."""
    ...


def get_decimal() -> type:
    """Get the decimal type to be used for numeric values."""
    ...


def set_decimal(cls: type) -> None:
    """Set a fixed date style that shall be assumed when typecasting."""
    ...


def get_decimal_point() -> str | None:
    """Get the decimal mark used for monetary values."""
    ...


def set_decimal_point(mark: str | None) -> None:
    """Specify which decimal mark is used for interpreting monetary values."""
    ...


def get_jsondecode() -> Callable[[str], object] | None:
    """Get the function that deserializes JSON formatted strings."""
    ...


def set_jsondecode(decode: Callable[[str], object] | None) -> None:
    """Set a function that will deserialize JSON formatted strings."""
    ...


def get_defbase() -> str | None:
    """Get the default database name."""
    ...


def set_defbase(base: str | None) -> None:
    """Set the default database name."""
    ...


def get_defhost() -> str | None:
    """Get the default host."""
    ...


def set_defhost(host: str | None) -> None:
    """Set the default host."""
    ...


def get_defport() -> int | None:
    """Get the default host."""
    ...


def set_defport(port: int | None) -> None:
    """Set the default port."""
    ...


def get_defopt() -> str | None:
    """Get the default connection options."""
    ...


def set_defopt(opt: str | None) -> None:
    """Set the default connection options."""
    ...


def get_defuser() -> str | None:
    """Get the default database user."""
    ...


def set_defuser(user: str | None) -> None:
    """Set the default database user."""
    ...


def get_defpasswd() -> str | None:
    """Get the default database password."""
    ...


def set_defpasswd(passwd: str | None) -> None:
    """Set the default database password."""
    ...


def set_query_helpers(*helpers: Callable) -> None:
    """Set internal query helper functions."""
    ...
