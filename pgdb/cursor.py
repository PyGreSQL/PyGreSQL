"""The DB API 2 Cursor object."""

from __future__ import annotations

from collections.abc import (
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network
from math import isinf, isnan
from typing import TYPE_CHECKING, Any, AnyStr, NamedTuple
from uuid import UUID as Uuid  # noqa: N811

from pg.core import (
    RESULT_DQL,
    DatabaseError,
    DataError,
    Error,
    InterfaceError,
    NotSupportedError,
)
from pg.core import Connection as Cnx
from pg.error import db_error, error, if_error, op_error, prg_error
from pg.helpers import (
    QuoteDict,
    RowCache,
    format_text,
    join_text,
    split_qualified_name,
)

from .adapt import Hstore, Json, Literal
from .cast import TypeCache
from .typecode import TypeCode

if TYPE_CHECKING:
    from .connection import Connection

__all__ = ['Cursor', 'CursorDescription']

# query for looking up a routine, needs a condition with placeholders
_query_pg_proc = (
    "SELECT p.prokind, p.pronargs, p.pronargdefaults,"
    " p.provariadic OPERATOR(pg_catalog.<>) 0,"
    " pg_catalog.array_to_string(p.proargmodes, '')"
    " FROM pg_catalog.pg_proc p WHERE {}")


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
            self.row_factory = None  # type: ignore[assignment, method-assign]
        else:
            self.build_row_factory = None  # type: ignore[assignment, method-assign]
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
        match value:
            case None:
                return 'NULL'
            case bytes():
                return f"'{self._cnx.escape_bytea(value).decode('ascii')}'"
            case str():
                return f"'{self._escape_text(value)}'"
            case Hstore():
                v = value._literal(self._encoding)
                if isinstance(v, bytes):  # contains raw text
                    return b"'" + self._escape_text(v) + b"'"
                return f"'{self._escape_text(v)}'"
            case Json():
                return f"'{self._escape_text(str(value))}'"
            case float():
                if isinf(value):
                    return "'-Infinity'" if value < 0 else "'Infinity'"
                if isnan(value):
                    return "'NaN'"
                return value
            case int() | Decimal() | Literal():
                return value
            case datetime(tzinfo=None):  # must come before date
                return f"'{value}'::timestamp"
            case datetime():
                return f"'{value}'::timestamptz"
            case date():
                return f"'{value}'::date"
            case time(tzinfo=None):
                return f"'{value}'::time"
            case time():
                return f"'{value}'::timetz"
            case timedelta():
                return f"'{value}'::interval"
            case Uuid():
                return f"'{value}'::uuid"
            case IPv4Address() | IPv6Address():  # or interface
                return f"'{self._escape_text(str(value))}'::inet"
            case IPv4Network() | IPv6Network():
                return f"'{self._escape_text(str(value))}'::cidr"
            case list():
                # Quote value as an ARRAY constructor. This is better than
                # using an array literal because it carries the information
                # that this is an array and not a string.  One issue with this
                # syntax is that you need to add an explicit typecast when
                # passing empty arrays. The ARRAY keyword is actually only
                # necessary at the top level.
                if not value:  # exception for empty array
                    return "'{}'"
                return self._join(value, 'ARRAY[', ']')
            case tuple():
                # Quote as a ROW constructor.  This is better than using a
                # record literal because it carries the information that this
                # is a record and not a string.  We don't use the keyword ROW
                # in order to make this usable with the IN syntax as well.
                # It is only necessary when the records has a single column
                # which is not really useful.
                return self._join(value, '(', ')')
        try:  # noinspection PyUnresolvedReferences
            value = value.__pg_repr__()
        except AttributeError as e:
            raise InterfaceError(
                f'Do not know how to adapt type {type(value)}') from e
        if isinstance(value, (tuple, list)):
            value = self._quote(value)
        return value

    def _encoding(self) -> str:
        """Get the Python codec name of the client encoding."""
        return self._cnx.encoding

    def _join(self, values: Sequence, start: str, end: str) -> str | bytes:
        """Quote and join values, as bytes if any of them is quoted as bytes.

        Values quoted as bytes contain raw text in the client encoding.
        """
        parts = [v if isinstance(v, (str, bytes)) else str(v)
                 for v in map(self._quote, values)]
        return join_text(parts, ',', self._encoding, start, end)

    def _decode_sql(self, sql: str | bytes) -> str:
        """Decode a query containing raw text for an error message."""
        if isinstance(sql, str):
            return sql
        try:
            codec = self._encoding()
        except Exception:  # e.g. the connection has been lost
            codec = 'utf-8'
        return sql.decode(codec, 'backslashreplace')

    def _escape_text(self, value: AnyStr) -> AnyStr:
        """Escape a text value, which must not contain NUL characters."""
        if (b'\0' in value if isinstance(value, bytes)
                else '\0' in value):
            raise error('Text values cannot contain NUL characters', DataError)
        return self._cnx.escape_string(value)

    def _quoteparams(self, string: str,
                     parameters: Mapping | Sequence | None) -> str | bytes:
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
            quote_dict = QuoteDict(parameters)
            quote_dict.quote = self._quote
            formatted = string % quote_dict
            quoted = quote_dict.quoted
            if any(isinstance(value, bytes) for value in quoted.values()):
                # some values contain raw text, the query must be bytes
                return format_text(string, quoted, self._encoding)
            return formatted
        return format_text(
            string, tuple(map(self._quote, parameters)), self._encoding)

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
        sql: str | bytes = "BEGIN"
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
                sql = self._quoteparams(operation, parameters)
                if (b'\0' in sql if isinstance(sql, bytes)
                        else '\0' in sql):
                    raise error(
                        'Queries cannot contain NUL characters', DataError)
                rows = self._src.execute(sql)
                if rows:  # true if not DML
                    rowcount += rows
                else:
                    self.rowcount = -1
        except DatabaseError:
            raise  # database provides error message
        except UnicodeError as err:  # cannot encode the query or parameters
            sql = self._decode_sql(sql)
            raise error(f"Error in '{sql}': {err}", DataError) from err
        except Error as err:
            sql = self._decode_sql(sql)
            # noinspection PyTypeChecker
            raise if_error(f"Error in '{sql}': '{err}'") from err
        except Exception as err:
            sql = self._decode_sql(sql)
            raise op_error(f"Internal error in '{sql}': {err}") from err
        # then initialize result raw count and description
        if self._src.resulttype == RESULT_DQL:
            self._description = True  # fetch on demand
            self.rowcount = self._src.ntuples
            self.lastrowid = None
            build_row_factory = self.build_row_factory
            if build_row_factory:  # type: ignore[truthy-function]
                self.row_factory = build_row_factory()  # type: ignore[assignment, method-assign]
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
        return [row_factory([
            cast_value(value, typ)
            for typ, value in zip(coltypes, row, strict=True)])
            for row in result]

    def callproc(self, procname: str, parameters: Sequence | None = None
                 ) -> Sequence | None:
        """Call a stored database function or procedure with the given name.

        The name can be schema-qualified. It is split at dots, and its
        parts are taken literally, like table names.

        The sequence of parameters must contain one entry for each argument
        that the function or procedure expects. For a function, these are
        its input arguments, and the parameters are returned unchanged.
        For a procedure, these are all its arguments, including output
        arguments (pass None for them), and a copy of the parameters is
        returned where output and input/output parameters are replaced
        with the values returned by the procedure.

        The function or procedure may also provide a result set as output.
        This can be requested through the standard fetch methods.
        """
        if not isinstance(procname, str):
            raise TypeError("The procedure name must be a string")
        parts = split_qualified_name(procname)
        n = len(parameters) if parameters else 0
        is_procedure, outputs = self._get_routine(procname, parts, n)
        cnx = self._cnx
        # percent signs in the name must not be taken as placeholders
        name = '.'.join(map(cnx.escape_identifier, parts)).replace('%', '%%')
        args = ','.join(n * ['%s'])
        if is_procedure:
            query = f'call {name}({args})'
        else:
            query = f'select * from {name}({args})'  # noqa: S608
        self.execute(query, parameters)
        if not is_procedure or parameters is None:
            return parameters
        values = list(parameters)
        if outputs and self._src.resulttype == RESULT_DQL:
            row = self._src.fetch(1)
            if row:
                coltypes = self.coltypes
                if coltypes is not None:
                    typecast = self.type_cache.typecast
                    row_values = [typecast(value, typ) for typ, value in
                                  zip(coltypes, row[0], strict=True)]
                    # values of output parameters with defaults that were
                    # omitted can only be fetched from the cursor
                    for pos, value in zip(outputs, row_values, strict=False):
                        if pos < n:
                            values[pos] = value
                # the row can still be fetched from the cursor
                self._src.movefirst()
        return tuple(values) if isinstance(parameters, tuple) else values

    def _get_routine(self, procname: str, parts: Sequence[str], n: int
                     ) -> tuple[bool, list[int]]:
        """Check whether the routine to be called is a procedure.

        Returns whether it is a procedure that must be called with CALL,
        and the positions of its output and input/output arguments.
        Routines that cannot be called with n arguments are disregarded
        if other routines with the same name can be called. If the name is
        not found, it is assumed to be a function, so that the database
        reports the error when calling it.
        """
        if len(parts) == 1:
            condition = ('p.proname OPERATOR(pg_catalog.=)'
                         ' %(name)s::pg_catalog.name'
                         ' AND pg_catalog.pg_function_is_visible(p.oid)')
            params = {'name': parts[0]}
        else:
            # the schema pg_temp means the temporary schema of the session;
            # a database name in front of the schema is checked by the call
            condition = (
                "p.pronamespace OPERATOR(pg_catalog.=) CASE"
                " WHEN %(schema)s::pg_catalog.name"
                " OPERATOR(pg_catalog.=) 'pg_temp'"
                " THEN pg_catalog.pg_my_temp_schema()"
                " ELSE (SELECT n.oid FROM pg_catalog.pg_namespace n"
                " WHERE n.nspname OPERATOR(pg_catalog.=)"
                " %(schema)s::pg_catalog.name) END"
                " AND p.proname OPERATOR(pg_catalog.=)"
                " %(name)s::pg_catalog.name")
            params = {'schema': parts[-2], 'name': parts[-1]}
        # run the lookup with the source object, like the type cache,
        # so that the values are not cast and errors are raised as usual
        cmd = _query_pg_proc.format(condition)
        sql: str | bytes = cmd
        try:
            sql = self._quoteparams(cmd, params)
            self._src.execute(sql)
            routines = self._src.fetch(-1)
        except DatabaseError:
            raise  # database provides error message
        except UnicodeError as err:  # cannot encode the name
            sql = self._decode_sql(sql)
            raise error(f"Error in '{sql}': {err}", DataError) from err
        except Error as err:
            sql = self._decode_sql(sql)
            raise if_error(f"Error in '{sql}': '{err}'") from err
        except Exception as err:
            sql = self._decode_sql(sql)
            raise op_error(f"Internal error in '{sql}': {err}") from err
        candidates = []
        for kind, nargs_str, ndefaults_str, variadic, modes in routines:
            is_procedure = kind == 'p'
            nargs, ndefaults = int(nargs_str), int(ndefaults_str)
            if is_procedure and modes:
                # output arguments must also be passed to procedures
                nargs = len(modes)
            min_args = nargs - ndefaults
            # a variadic argument without default needs at least one value
            callable_with_n = min_args <= n and (
                variadic == 't' or n <= nargs)
            outputs = [i for i, mode in enumerate(modes or '')
                       if mode in 'bo'] if is_procedure else []
            candidates.append((callable_with_n, is_procedure, outputs))
        if any(candidate[0] for candidate in candidates):
            candidates = [candidate for candidate in candidates
                          if candidate[0]]
        if not candidates or not all(
                is_procedure for _, is_procedure, _ in candidates):
            # functions are called with SELECT as before
            return False, []
        outputs = candidates[0][2]
        if any(candidate[2] != outputs for candidate in candidates[1:]):
            raise prg_error(
                f"Cannot determine the output parameters of procedure"
                f" {procname!r} with {n} parameters")
        return True, outputs

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

                def chunks() -> Iterator[str | bytes]:
                    yield stream

            elif isinstance(stream, Iterable):

                def chunks() -> Iterator[str | bytes]:
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

                def chunks() -> Iterator[str | bytes]:
                    while True:
                        buffer = read(size)
                        yield buffer
                        if not buffer or len(buffer) < size:
                            break

            else:

                def chunks() -> Iterator[str | bytes]:
                    yield read()

        if not table or not isinstance(table, str):
            raise TypeError("Need a table to copy to")
        if table.lower().startswith('select '):
            raise ValueError("Must specify a table, not a query")
        cnx = self._cnx
        table = '.'.join(map(cnx.escape_identifier,
                             split_qualified_name(table)))
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
                columns: Sequence[str] | None = None
                ) -> Cursor | Generator[str | bytes, None, None]:
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
            table = '.'.join(map(cnx.escape_identifier,
                                 split_qualified_name(table)))
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

        def copy() -> Generator[str | bytes, None, None]:
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


class CursorDescription(NamedTuple):
    """Description of a result column as specified by the DB-API 2."""

    name: str
    type_code: TypeCode
    display_size: int | None
    internal_size: int
    precision: int | None
    scale: int | None
    null_ok: bool | None
