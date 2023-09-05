"""The DB API 2 Cursor object."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from math import isinf, isnan
from typing import TYPE_CHECKING, Any, Callable, Generator, Mapping, Sequence
from uuid import UUID as Uuid  # noqa: N811

from pg.core import (
    RESULT_DQL,
    DatabaseError,
    Error,
    InterfaceError,
    NotSupportedError,
)
from pg.core import Connection as Cnx
from pg.error import db_error, if_error, op_error
from pg.helpers import QuoteDict, RowCache

from .adapt import Binary, Hstore, Json, Literal
from .cast import TypeCache
from .typecode import TypeCode

if TYPE_CHECKING:
    from .connection import Connection

__all__ = ['Cursor', 'CursorDescription']


class Cursor:
    """Cursor object."""

    def __init__(self, connection: Connection) -> None:
        """Create a cursor object for the database connection."""
        self.connection = self._connection = connection
        cnx = connection._cnx
        if not cnx:
            raise op_error("Connection has been closed")
        self._cnx: Cnx = cnx
        self.type_cache: TypeCache = connection.type_cache
        self._src = self._cnx.source()
        # the official attribute for describing the result columns
        self._description: list[CursorDescription] | bool | None = None
        if self.row_factory is Cursor.row_factory:
            # the row factory needs to be determined dynamically
            self.row_factory = None  # type: ignore
        else:
            self.build_row_factory = None  # type: ignore
        self.rowcount: int | None = -1
        self.arraysize: int = 1
        self.lastrowid: int | None = None

    def __iter__(self) -> Cursor:
        """Make cursor compatible to the iteration protocol."""
        return self

    def __enter__(self) -> Cursor:
        """Enter the runtime context for the cursor object."""
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context for the cursor object."""
        self.close()

    def _quote(self, value: Any) -> Any:
        """Quote value depending on its type."""
        if value is None:
            return 'NULL'
        if isinstance(value, (Hstore, Json)):
            value = str(value)
        if isinstance(value, (bytes, str)):
            cnx = self._cnx
            if isinstance(value, Binary):
                value = cnx.escape_bytea(value).decode('ascii')
            else:
                value = cnx.escape_string(value)
            return f"'{value}'"
        if isinstance(value, float):
            if isinf(value):
                return "'-Infinity'" if value < 0 else "'Infinity'"
            if isnan(value):
                return "'NaN'"
            return value
        if isinstance(value, (int, Decimal, Literal)):
            return value
        if isinstance(value, datetime):
            if value.tzinfo:
                return f"'{value}'::timestamptz"
            return f"'{value}'::timestamp"
        if isinstance(value, date):
            return f"'{value}'::date"
        if isinstance(value, time):
            if value.tzinfo:
                return f"'{value}'::timetz"
            return f"'{value}'::time"
        if isinstance(value, timedelta):
            return f"'{value}'::interval"
        if isinstance(value, Uuid):
            return f"'{value}'::uuid"
        if isinstance(value, list):
            # Quote value as an ARRAY constructor. This is better than using
            # an array literal because it carries the information that this is
            # an array and not a string.  One issue with this syntax is that
            # you need to add an explicit typecast when passing empty arrays.
            # The ARRAY keyword is actually only necessary at the top level.
            if not value:  # exception for empty array
                return "'{}'"
            q = self._quote
            v = ','.join(str(q(v)) for v in value)
            return f'ARRAY[{v}]'
        if isinstance(value, tuple):
            # Quote as a ROW constructor.  This is better than using a record
            # literal because it carries the information that this is a record
            # and not a string.  We don't use the keyword ROW in order to make
            # this usable with the IN syntax as well.  It is only necessary
            # when the records has a single column which is not really useful.
            q = self._quote
            v = ','.join(str(q(v)) for v in value)
            return f'({v})'
        try:  # noinspection PyUnresolvedReferences
            value = value.__pg_repr__()
        except AttributeError as e:
            raise InterfaceError(
                f'Do not know how to adapt type {type(value)}') from e
        if isinstance(value, (tuple, list)):
            value = self._quote(value)
        return value

    def _quoteparams(self, string: str,
                     parameters: Mapping | Sequence | None) -> str:
        """Quote parameters.

        This function works for both mappings and sequences.

        The function should be used even when there are no parameters,
        so that we have a consistent behavior regarding percent signs.
        """
        if not parameters:
            try:
                return string % ()  # unescape literal quotes if possible
            except (TypeError, ValueError):
                return string  # silently accept unescaped quotes
        if isinstance(parameters, dict):
            parameters = QuoteDict(parameters)
            parameters.quote = self._quote
        else:
            parameters = tuple(map(self._quote, parameters))
        return string % parameters

    def _make_description(self, info: tuple[int, str, int, int, int]
                          ) -> CursorDescription:
        """Make the description tuple for the given field info."""
        name, typ, size, mod = info[1:]
        type_code = self.type_cache[typ]
        if mod > 0:
            mod -= 4
        precision: int | None
        scale: int | None
        if type_code == 'numeric':
            precision, scale = mod >> 16, mod & 0xffff
            size = precision
        else:
            if not size:
                size = type_code.size
            if size == -1:
                size = mod
            precision = scale = None
        return CursorDescription(
            name, type_code, None, size, precision, scale, None)

    @property
    def description(self) -> list[CursorDescription] | None:
        """Read-only attribute describing the result columns."""
        description = self._description
        if description is None:
            return None
        if not isinstance(description, list):
            make = self._make_description
            description = [make(info) for info in self._src.listinfo()]
            self._description = description
        return description

    @property
    def colnames(self) -> Sequence[str] | None:
        """Unofficial convenience method for getting the column names."""
        description = self.description
        return None if description is None else [d[0] for d in description]

    @property
    def coltypes(self) -> Sequence[TypeCode] | None:
        """Unofficial convenience method for getting the column types."""
        description = self.description
        return None if description is None else [d[1] for d in description]

    def close(self) -> None:
        """Close the cursor object."""
        self._src.close()

    def execute(self, operation: str, parameters: Sequence | None = None
                ) -> Cursor:
        """Prepare and execute a database operation (query or command)."""
        # The parameters may also be specified as list of tuples to e.g.
        # insert multiple rows in a single operation, but this kind of
        # usage is deprecated.  We make several plausibility checks because
        # tuples can also be passed with the meaning of ROW constructors.
        if (parameters and isinstance(parameters, list)
                and len(parameters) > 1
                and all(isinstance(p, tuple) for p in parameters)
                and all(len(p) == len(parameters[0]) for p in parameters[1:])):
            return self.executemany(operation, parameters)
        # not a list of tuples
        return self.executemany(operation, [parameters])

    def executemany(self, operation: str,
                    seq_of_parameters: Sequence[Sequence | None]) -> Cursor:
        """Prepare operation and execute it against a parameter sequence."""
        if not seq_of_parameters:
            # don't do anything without parameters
            return self
        self._description = None
        self.rowcount = -1
        # first try to execute all queries
        rowcount = 0
        sql = "BEGIN"
        try:
            if not self._connection._tnx and not self._connection.autocommit:
                try:
                    self._src.execute(sql)
                except DatabaseError:
                    raise  # database provides error message
                except Exception as e:
                    raise op_error("Can't start transaction") from e
                else:
                    self._connection._tnx = True
            for parameters in seq_of_parameters:
                sql = operation
                sql = self._quoteparams(sql, parameters)
                rows = self._src.execute(sql)
                if rows:  # true if not DML
                    rowcount += rows
                else:
                    self.rowcount = -1
        except DatabaseError:
            raise  # database provides error message
        except Error as err:
            # noinspection PyTypeChecker
            raise if_error(f"Error in '{sql}': '{err}'") from err
        except Exception as err:
            raise op_error(f"Internal error in '{sql}': {err}") from err
        # then initialize result raw count and description
        if self._src.resulttype == RESULT_DQL:
            self._description = True  # fetch on demand
            self.rowcount = self._src.ntuples
            self.lastrowid = None
            build_row_factory = self.build_row_factory
            if build_row_factory:  # type: ignore
                self.row_factory = build_row_factory()  # type: ignore
        else:
            self.rowcount = rowcount
            self.lastrowid = self._src.oidstatus()
        # return the cursor object, so you can write statements such as
        # "cursor.execute(...).fetchall()" or "for row in cursor.execute(...)"
        return self

    def fetchone(self) -> Sequence | None:
        """Fetch the next row of a query result set."""
        res = self.fetchmany(1, False)
        try:
            return res[0]
        except IndexError:
            return None

    def fetchall(self) -> Sequence[Sequence]:
        """Fetch all (remaining) rows of a query result."""
        return self.fetchmany(-1, False)

    def fetchmany(self, size: int | None = None, keep: bool = False
                  ) -> Sequence[Sequence]:
        """Fetch the next set of rows of a query result.

        The number of rows to fetch per call is specified by the
        size parameter. If it is not given, the cursor's arraysize
        determines the number of rows to be fetched. If you set
        the keep parameter to true, this is kept as new arraysize.
        """
        if size is None:
            size = self.arraysize
        if keep:
            self.arraysize = size
        try:
            result = self._src.fetch(size)
        except DatabaseError:
            raise
        except Error as err:
            raise db_error(str(err)) from err
        row_factory = self.row_factory
        coltypes = self.coltypes
        if coltypes is None:
            # cannot determine column types, return raw result
            return [row_factory(row) for row in result]
        if len(result) > 5:
            # optimize the case where we really fetch many values
            # by looking up all type casting functions upfront
            cast_row = self.type_cache.get_row_caster(coltypes)
            return [row_factory(cast_row(row)) for row in result]
        cast_value = self.type_cache.typecast
        return [row_factory([cast_value(value, typ)
                for typ, value in zip(coltypes, row)]) for row in result]

    def callproc(self, procname: str, parameters: Sequence | None = None
                 ) -> Sequence | None:
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each input
        argument that the procedure expects. The result of the call is the
        same as this input sequence; replacement of output and input/output
        parameters in the return value is currently not supported.

        The procedure may also provide a result set as output. These can be
        requested through the standard fetch methods of the cursor.
        """
        n = len(parameters) if parameters else 0
        s = ','.join(n * ['%s'])
        query = f'select * from "{procname}"({s})'  # noqa: S608
        self.execute(query, parameters)
        return parameters

    # noinspection PyShadowingBuiltins
    def copy_from(self, stream: Any, table: str,
                  format: str | None = None, sep: str | None = None,
                  null: str | None = None, size: int | None  = None,
                  columns: Sequence[str] | None = None) -> Cursor:
        """Copy data from an input stream to the specified table.

        The input stream can be a file-like object with a read() method or
        it can also be an iterable returning a row or multiple rows of input
        on each iteration.

        The format must be 'text', 'csv' or 'binary'. The sep option sets the
        column separator (delimiter) used in the non binary formats.
        The null option sets the textual representation of NULL in the input.

        The size option sets the size of the buffer used when reading data
        from file-like objects.

        The copy operation can be restricted to a subset of columns. If no
        columns are specified, all of them will be copied.
        """
        binary_format = format == 'binary'
        try:
            read = stream.read
        except AttributeError as e:
            if size:
                raise ValueError(
                    "Size must only be set for file-like objects") from e
            input_type: type | tuple[type, ...]
            type_name: str
            if binary_format:
                input_type = bytes
                type_name = 'byte strings'
            else:
                input_type = (bytes, str)
                type_name = 'strings'

            if isinstance(stream, (bytes, str)):
                if not isinstance(stream, input_type):
                    raise ValueError(f"The input must be {type_name}") from e
                if not binary_format:
                    if isinstance(stream, str):
                        if not stream.endswith('\n'):
                            stream += '\n'
                    else:
                        if not stream.endswith(b'\n'):
                            stream += b'\n'

                def chunks() -> Generator:
                    yield stream

            elif isinstance(stream, Iterable):

                def chunks() -> Generator:
                    for chunk in stream:
                        if not isinstance(chunk, input_type):
                            raise ValueError(
                                f"Input stream must consist of {type_name}")
                        if isinstance(chunk, str):
                            if not chunk.endswith('\n'):
                                chunk += '\n'
                        else:
                            if not chunk.endswith(b'\n'):
                                chunk += b'\n'
                        yield chunk

            else:
                raise TypeError("Need an input stream to copy from") from e
        else:
            if size is None:
                size = 8192
            elif not isinstance(size, int):
                raise TypeError("The size option must be an integer")
            if size > 0:

                def chunks() -> Generator:
                    while True:
                        buffer = read(size)
                        yield buffer
                        if not buffer or len(buffer) < size:
                            break

            else:

                def chunks() -> Generator:
                    yield read()

        if not table or not isinstance(table, str):
            raise TypeError("Need a table to copy to")
        if table.lower().startswith('select '):
            raise ValueError("Must specify a table, not a query")
        cnx = self._cnx
        table = '.'.join(map(cnx.escape_identifier, table.split('.', 1)))
        operation_parts = [f'copy {table}']
        options = []
        parameters = []
        if format is not None:
            if not isinstance(format, str):
                raise TypeError("The format option must be be a string")
            if format not in ('text', 'csv', 'binary'):
                raise ValueError("Invalid format")
            options.append(f'format {format}')
        if sep is not None:
            if not isinstance(sep, str):
                raise TypeError("The sep option must be a string")
            if format == 'binary':
                raise ValueError(
                    "The sep option is not allowed with binary format")
            if len(sep) != 1:
                raise ValueError(
                    "The sep option must be a single one-byte character")
            options.append('delimiter %s')
            parameters.append(sep)
        if null is not None:
            if not isinstance(null, str):
                raise TypeError("The null option must be a string")
            options.append('null %s')
            parameters.append(null)
        if columns:
            if not isinstance(columns, str):
                columns = ','.join(map(cnx.escape_identifier, columns))
            operation_parts.append(f'({columns})')
        operation_parts.append("from stdin")
        if options:
            operation_parts.append(f"({','.join(options)})")
        operation = ' '.join(operation_parts)

        putdata = self._src.putdata
        self.execute(operation, parameters)

        try:
            for chunk in chunks():
                putdata(chunk)
        except BaseException as error:
            self.rowcount = -1
            # the following call will re-raise the error
            putdata(error)
        else:
            rowcount = putdata(None)
            self.rowcount = -1 if rowcount is None else rowcount

        # return the cursor object, so you can chain operations
        return self

    # noinspection PyShadowingBuiltins
    def copy_to(self, stream: Any, table: str,
                format: str | None = None, sep: str | None = None,
                null: str | None = None, decode: bool | None = None,
                columns: Sequence[str] | None = None) -> Cursor | Generator:
        """Copy data from the specified table to an output stream.

        The output stream can be a file-like object with a write() method or
        it can also be None, in which case the method will return a generator
        yielding a row on each iteration.

        Output will be returned as byte strings unless you set decode to true.

        Note that you can also use a select query instead of the table name.

        The format must be 'text', 'csv' or 'binary'. The sep option sets the
        column separator (delimiter) used in the non binary formats.
        The null option sets the textual representation of NULL in the output.

        The copy operation can be restricted to a subset of columns. If no
        columns are specified, all of them will be copied.
        """
        binary_format = format == 'binary'
        if stream is None:
            write = None
        else:
            try:
                write = stream.write
            except AttributeError as e:
                raise TypeError("Need an output stream to copy to") from e
        if not table or not isinstance(table, str):
            raise TypeError("Need a table to copy to")
        cnx = self._cnx
        if table.lower().startswith('select '):
            if columns:
                raise ValueError("Columns must be specified in the query")
            table = f'({table})'
        else:
            table = '.'.join(map(cnx.escape_identifier, table.split('.', 1)))
        operation_parts = [f'copy {table}']
        options = []
        parameters = []
        if format is not None:
            if not isinstance(format, str):
                raise TypeError("The format option must be a string")
            if format not in ('text', 'csv', 'binary'):
                raise ValueError("Invalid format")
            options.append(f'format {format}')
        if sep is not None:
            if not isinstance(sep, str):
                raise TypeError("The sep option must be a string")
            if binary_format:
                raise ValueError(
                    "The sep option is not allowed with binary format")
            if len(sep) != 1:
                raise ValueError(
                    "The sep option must be a single one-byte character")
            options.append('delimiter %s')
            parameters.append(sep)
        if null is not None:
            if not isinstance(null, str):
                raise TypeError("The null option must be a string")
            options.append('null %s')
            parameters.append(null)
        if decode is None:
            decode = format != 'binary'
        else:
            if not isinstance(decode, (int, bool)):
                raise TypeError("The decode option must be a boolean")
            if decode and binary_format:
                raise ValueError(
                    "The decode option is not allowed with binary format")
        if columns:
            if not isinstance(columns, str):
                columns = ','.join(map(cnx.escape_identifier, columns))
            operation_parts.append(f'({columns})')

        operation_parts.append("to stdout")
        if options:
            operation_parts.append(f"({','.join(options)})")
        operation = ' '.join(operation_parts)

        getdata = self._src.getdata
        self.execute(operation, parameters)

        def copy() -> Generator:
            self.rowcount = 0
            while True:
                row = getdata(decode)
                if isinstance(row, int):
                    if self.rowcount != row:
                        self.rowcount = row
                    break
                self.rowcount += 1
                yield row

        if write is None:
            # no input stream, return the generator
            return copy()

        # write the rows to the file-like input stream
        for row in copy():
            # noinspection PyUnboundLocalVariable
            write(row)

        # return the cursor object, so you can chain operations
        return self

    def __next__(self) -> Sequence:
        """Return the next row (support for the iteration protocol)."""
        res = self.fetchone()
        if res is None:
            raise StopIteration
        return res

    # Note that the iterator protocol now uses __next()__ instead of next(),
    # but we keep it for backward compatibility of pgdb.
    next = __next__

    @staticmethod
    def nextset() -> bool | None:
        """Not supported."""
        raise NotSupportedError("The nextset() method is not supported")

    @staticmethod
    def setinputsizes(sizes: Sequence[int]) -> None:
        """Not supported."""
        pass  # unsupported, but silently passed

    @staticmethod
    def setoutputsize(size: int, column: int = 0) -> None:
        """Not supported."""
        pass  # unsupported, but silently passed

    @staticmethod
    def row_factory(row: Sequence) -> Sequence:
        """Process rows before they are returned.

        You can overwrite this statically with a custom row factory, or
        you can build a row factory dynamically with build_row_factory().

        For example, you can create a Cursor class that returns rows as
        Python dictionaries like this:

            class DictCursor(pgdb.Cursor):

                def row_factory(self, row):
                    return {desc[0]: value
                        for desc, value in zip(self.description, row)}

            cur = DictCursor(con)  # get one DictCursor instance or
            con.cursor_type = DictCursor  # always use DictCursor instances
        """
        raise NotImplementedError

    def build_row_factory(self) -> Callable[[Sequence], Sequence] | None:
        """Build a row factory based on the current description.

        This implementation builds a row factory for creating named tuples.
        You can overwrite this method if you want to dynamically create
        different row factories whenever the column description changes.
        """
        names = self.colnames
        return RowCache.row_factory(tuple(names)) if names else None


CursorDescription = namedtuple('CursorDescription', (
    'name', 'type_code', 'display_size', 'internal_size',
    'precision', 'scale', 'null_ok'))
