"""Connection wrapper."""

from __future__ import annotations

from contextlib import suppress
from json import dumps as jsonencode
from json import loads as jsondecode
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Sequence,
    TypeVar,
    overload,
)

from . import Connection, connect
from .adapt import Adapter, DbTypes
from .attrs import AttrDict
from .core import (
    InternalError,
    LargeObject,
    ProgrammingError,
    Query,
    get_bool,
    get_jsondecode,
    unescape_bytea,
)
from .error import db_error, int_error, prg_error
from .helpers import namediter, oid_key, quote_if_unqualified
from .notify import NotificationHandler

if TYPE_CHECKING:
    from pgdb.connection import Connection as DbApi2Connection

try:
    AnyStr = TypeVar('AnyStr', str, bytes, str | bytes)
except TypeError:  # Python < 3.10
    AnyStr = Any  # type: ignore

__all__ = ['DB']


# The actual PostgreSQL database connection interface:

class DB:
    """Wrapper class for the core connection type."""

    dbname: str
    host: str
    port: int
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

    db: Connection | None = None  # invalid fallback for underlying connection
    _db_args: Any  # either the connect args or the underlying connection

    @overload
    def __init__(self, dbname: str | None = None,
                 host: str | None = None, port: int = -1,
                 opt: str | None = None,
                 user: str | None = None, passwd: str | None = None,
                 nowait: bool = False) -> None:
        ...  # create a new connection using the specified parameters

    @overload
    def __init__(self, db: Connection | DB | DbApi2Connection) -> None:
        ...  # create a connection wrapper based on an existing connection

    def __init__(self, *args: Any, **kw: Any) -> None: 
        """Create a new connection.

        You can pass either the connection parameters or an existing
        pg or pgdb Connection. This allows you to use the methods
        of the classic pg interface with a DB-API 2 pgdb Connection.
        """
        if kw:
            db = kw.get('db')
            if db is not None and (args or len(kw) > 1):
                raise TypeError("Conflicting connection parameters")
        elif len(args) == 1 and not isinstance(args[0], str):
            db = args[0]
        else:
            db = None
        if db:
            if isinstance(db, DB):
                db = db.db  # allow db to be a wrapped Connection
            else:
                with suppress(AttributeError):
                    db = db._cnx  # allow db to be a pgdb Connection
            if not isinstance(db, Connection):
                raise TypeError(
                    "The 'db' argument must be a valid database connection.")
            self._db_args = db
            self._closeable = False
        else:
            db = connect(*args, **kw)
            self._db_args = args, kw
            self._closeable = True
        self.db = db
        self.dbname = db.db
        self._regtypes = False
        self._attnames: dict[str, AttrDict] = {}
        self._generated: dict[str, frozenset[str]] = {}
        self._pkeys: dict[str, str | tuple[str, ...]] = {}
        self._privileges: dict[tuple[str, str], bool] = {}
        self.adapter = Adapter(self)
        self.dbtypes = DbTypes(self)
        self._query_attnames = (
            "SELECT a.attname,"
            " t.oid, t.typname, t.oid::pg_catalog.regtype,"
            " t.typlen, t.typtype, t.typcategory, t.typdelim, t.typrelid"
            " FROM pg_catalog.pg_attribute a"
            " JOIN pg_catalog.pg_type t"
            " ON t.oid OPERATOR(pg_catalog.=) a.atttypid"
            " WHERE a.attrelid OPERATOR(pg_catalog.=)"
            " {}::pg_catalog.regclass"
            " AND {} AND NOT a.attisdropped ORDER BY a.attnum")
        if db.server_version < 120000:
            self._query_generated = (
                "a.attidentity OPERATOR(pg_catalog.=) 'a'"
            )
        else:
            self._query_generated = (
                "(a.attidentity OPERATOR(pg_catalog.=) 'a' OR"
                " a.attgenerated OPERATOR(pg_catalog.!=) '')"
            )
        db.set_cast_hook(self.dbtypes.typecast)
        # For debugging scripts, self.debug can be set
        # * to a string format specification (e.g. in CGI set to "%s<BR>"),
        # * to a file object to write debug statements or
        # * to a callable object which takes a string argument
        # * to any other true value to just print debug statements
        self.debug: Any = None

    def __getattr__(self, name: str) -> Any:
        """Get the specified attribute of the connection."""
        # All undefined members are same as in underlying connection:
        if self.db:
            return getattr(self.db, name)
        else:
            raise int_error('Connection is not valid')

    def __dir__(self) -> list[str]:
        """List all attributes of the connection."""
        # Custom dir function including the attributes of the connection:
        attrs = set(self.__class__.__dict__)
        attrs.update(self.__dict__)
        attrs.update(dir(self.db))
        return sorted(attrs)

    # Context manager methods

    def __enter__(self) -> DB:
        """Enter the runtime context. This will start a transaction."""
        self.begin()
        return self

    def __exit__(self, et: type[BaseException] | None,
                 ev: BaseException | None, tb: Any) -> None:
        """Exit the runtime context. This will end the transaction."""
        if et is None and ev is None and tb is None:
            self.commit()
        else:
            self.rollback()

    def __del__(self) -> None:
        """Delete the connection."""
        try:
            db = self.db
        except AttributeError:
            db = None
        if db:
            with suppress(TypeError):  # when already closed
                db.set_cast_hook(None)
            if self._closeable:
                with suppress(InternalError):  # when already closed
                    db.close()

    # Auxiliary methods

    def _do_debug(self, *args: Any) -> None:
        """Print a debug message."""
        if self.debug:
            s = '\n'.join(str(arg) for arg in args)
            if isinstance(self.debug, str):
                print(self.debug % s)
            elif hasattr(self.debug, 'write'):
                # noinspection PyCallingNonCallable
                self.debug.write(s + '\n')
            elif callable(self.debug):
                self.debug(s)
            else:
                print(s)

    def _escape_qualified_name(self, s: str) -> str:
        """Escape a qualified name.

        Escapes the name for use as an SQL identifier, unless the
        name contains a dot, in which case the name is ambiguous
        (could be a qualified name or just a name with a dot in it)
        and must be quoted manually by the caller.
        """
        if '.' not in s:
            s = self.escape_identifier(s)
        return s

    @staticmethod
    def _make_bool(d: Any) -> bool | str:
        """Get boolean value corresponding to d."""
        return bool(d) if get_bool() else ('t' if d else 'f')

    @staticmethod
    def _list_params(params: Sequence) -> str:
        """Create a human readable parameter list."""
        return ', '.join(f'${n}={v!r}' for n, v in enumerate(params, 1))

    @property
    def _valid_db(self) -> Connection:
        """Get underlying connection and make sure it is not closed."""
        db = self.db
        if not db:
            raise int_error('Connection already closed')
        return db

    # Public methods

    # escape_string and escape_bytea exist as methods,
    # so we define unescape_bytea as a method as well
    unescape_bytea = staticmethod(unescape_bytea)

    @staticmethod
    def decode_json(s: str) -> Any:
        """Decode a JSON string coming from the database."""
        return (get_jsondecode() or jsondecode)(s)

    @staticmethod
    def encode_json(d: Any) -> str:
        """Encode a JSON string for use within SQL."""
        return jsonencode(d)

    def close(self) -> None:
        """Close the database connection."""
        # Wraps shared library function so we can track state.
        db = self._valid_db
        with suppress(TypeError):  # when already closed
            db.set_cast_hook(None)
        if self._closeable:
            db.close()
        self.db = None

    def reset(self) -> None:
        """Reset connection with current parameters.

        All derived queries and large objects derived from this connection
        will not be usable after this call.
        """
        self._valid_db.reset()

    def reopen(self) -> None:
        """Reopen connection to the database.

        Used in case we need another connection to the same database.
        Note that we can still reopen a database that we have closed.
        """
        # There is no such shared library function.
        if self._closeable:
            args, kw = self._db_args
            db = connect(*args, **kw)
            if self.db:
                self.db.set_cast_hook(None)
                self.db.close()
            db.set_cast_hook(self.dbtypes.typecast)
            self.db = db
        else:
            self.db = self._db_args

    def begin(self, mode: str | None = None) -> Query:
        """Begin a transaction."""
        qstr = 'BEGIN'
        if mode:
            qstr += ' ' + mode
        return self.query(qstr)

    start = begin

    def commit(self) -> Query:
        """Commit the current transaction."""
        return self.query('COMMIT')

    end = commit

    def rollback(self, name: str | None = None) -> Query:
        """Roll back the current transaction."""
        qstr = 'ROLLBACK'
        if name:
            qstr += ' TO ' + name
        return self.query(qstr)

    abort = rollback

    def savepoint(self, name: str) -> Query:
        """Define a new savepoint within the current transaction."""
        return self.query('SAVEPOINT ' + name)

    def release(self, name: str) -> Query:
        """Destroy a previously defined savepoint."""
        return self.query('RELEASE ' + name)

    def get_parameter(self,
                      parameter: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str] | dict[str, Any]
                      ) -> str | list[str] | dict[str, str]:
        """Get the value of a run-time parameter.

        If the parameter is a string, the return value will also be a string
        that is the current setting of the run-time parameter with that name.

        You can get several parameters at once by passing a list, set or dict.
        When passing a list of parameter names, the return value will be a
        corresponding list of parameter settings.  When passing a set of
        parameter names, a new dict will be returned, mapping these parameter
        names to their settings.  Finally, if you pass a dict as parameter,
        its values will be set to the current parameter settings corresponding
        to its keys.

        By passing the special name 'all' as the parameter, you can get a dict
        of all existing configuration parameters.
        """
        values: Any
        if isinstance(parameter, str):
            parameter = [parameter]
            values = None
        elif isinstance(parameter, (list, tuple)):
            values = []
        elif isinstance(parameter, (set, frozenset)):
            values = {}
        elif isinstance(parameter, dict):
            values = parameter
        else:
            raise TypeError(
                'The parameter must be a string, list, set or dict')
        if not parameter:
            raise TypeError('No parameter has been specified')
        query = self._valid_db.query
        params: Any = {} if isinstance(values, dict) else []
        for param_key in parameter:
            param = param_key.strip().lower() if isinstance(
                param_key, (bytes, str)) else None
            if not param:
                raise TypeError('Invalid parameter')
            if param == 'all':
                cmd = 'SHOW ALL'
                values = query(cmd).getresult()
                values = {value[0]: value[1] for value in values}
                break
            if isinstance(params, dict):
                params[param] = param_key
            else:
                params.append(param)
        else:
            for param in params:
                cmd = f'SHOW {param}'
                value = query(cmd).singlescalar()
                if values is None:
                    values = value
                elif isinstance(values, list):
                    values.append(value)
                else:
                    values[params[param]] = value
        return values

    def set_parameter(self,
                      parameter: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str] | dict[str, Any],
                      value: str | list[str] | tuple[str, ...] |
                      set[str] | frozenset[str]| None = None,
                      local: bool = False) -> None:
        """Set the value of a run-time parameter.

        If the parameter and the value are strings, the run-time parameter
        will be set to that value.  If no value or None is passed as a value,
        then the run-time parameter will be restored to its default value.

        You can set several parameters at once by passing a list of parameter
        names, together with a single value that all parameters should be
        set to or with a corresponding list of values.  You can also pass
        the parameters as a set if you only provide a single value.
        Finally, you can pass a dict with parameter names as keys.  In this
        case, you should not pass a value, since the values for the parameters
        will be taken from the dict.

        By passing the special name 'all' as the parameter, you can reset
        all existing settable run-time parameters to their default values.

        If you set local to True, then the command takes effect for only the
        current transaction.  After commit() or rollback(), the session-level
        setting takes effect again.  Setting local to True will appear to
        have no effect if it is executed outside a transaction, since the
        transaction will end immediately.
        """
        if isinstance(parameter, str):
            parameter = {parameter: value}
        elif isinstance(parameter, (list, tuple)):
            if isinstance(value, (list, tuple)):
                parameter = dict(zip(parameter, value))
            else:
                parameter = dict.fromkeys(parameter, value)
        elif isinstance(parameter, (set, frozenset)):
            if isinstance(value, (list, tuple, set, frozenset)):
                value = set(value)
                if len(value) == 1:
                    value = next(iter(value))
            if not (value is None or isinstance(value, str)):
                raise ValueError(
                    'A single value must be specified'
                    ' when parameter is a set')
            parameter = dict.fromkeys(parameter, value)
        elif isinstance(parameter, dict):
            if value is not None:
                raise ValueError(
                    'A value must not be specified'
                    ' when parameter is a dictionary')
        else:
            raise TypeError(
                'The parameter must be a string, list, set or dict')
        if not parameter:
            raise TypeError('No parameter has been specified')
        params: dict[str, str | None] = {}
        for param, param_value in parameter.items():
            param = param.strip().lower() if isinstance(param, str) else None
            if not param:
                raise TypeError('Invalid parameter')
            if param == 'all':
                if param_value is not None:
                    raise ValueError(
                        'A value must not be specified'
                        " when parameter is 'all'")
                params = {'all': None}
                break
            params[param] = param_value
        local_clause = ' LOCAL' if local else ''
        for param, param_value in params.items():
            cmd = (f'RESET{local_clause} {param}'
                   if param_value is None else
                   f'SET{local_clause} {param} TO {param_value}')
            self._do_debug(cmd)
            self._valid_db.query(cmd)

    def query(self, command: str, *args: Any) -> Query:
        """Execute a SQL command string.

        This method simply sends a SQL query to the database.  If the query is
        an insert statement that inserted exactly one row into a table that
        has OIDs, the return value is the OID of the newly inserted row.
        If the query is an update or delete statement, or an insert statement
        that did not insert exactly one row in a table with OIDs, then the
        number of rows affected is returned as a string.  If it is a statement
        that returns rows as a result (usually a select statement, but maybe
        also an "insert/update ... returning" statement), this method returns
        a Query object that can be accessed via getresult() or dictresult()
        or simply printed.  Otherwise, it returns `None`.

        The query can contain numbered parameters of the form $1 in place
        of any data constant.  Arguments given after the query string will
        be substituted for the corresponding numbered parameter.  Parameter
        values can also be given as a single list or tuple argument.
        """
        # Wraps shared library function for debugging.
        db = self._valid_db
        if args:
            self._do_debug(command, args)
            return db.query(command, args)
        self._do_debug(command)
        return db.query(command)

    def query_formatted(self, command: str,
                        parameters: tuple | list | dict | None = None,
                        types: tuple | list | dict | None = None,
                        inline: bool =False) -> Query:
        """Execute a formatted SQL command string.

        Similar to query, but using Python format placeholders of the form
        %s or %(names)s instead of PostgreSQL placeholders of the form $1.
        The parameters must be passed as a tuple, list or dict.  You can
        also pass a corresponding tuple, list or dict of database types in
        order to format the parameters properly in case there is ambiguity.

        If you set inline to True, the parameters will be sent to the database
        embedded in the SQL command, otherwise they will be sent separately.
        """
        return self.query(*self.adapter.format_query(
            command, parameters, types, inline))

    def query_prepared(self, name: str, *args: Any) -> Query:
        """Execute a prepared SQL statement.

        This works like the query() method, except that instead of passing
        the SQL command, you pass the name of a prepared statement.  If you
        pass an empty name, the unnamed statement will be executed.
        """
        if name is None:
            name = ''
        db = self._valid_db
        if args:
            self._do_debug('EXECUTE', name, args)
            return db.query_prepared(name, args)
        self._do_debug('EXECUTE', name)
        return db.query_prepared(name)

    def prepare(self, name: str, command: str) -> None:
        """Create a prepared SQL statement.

        This creates a prepared statement for the given command with the
        given name for later execution with the query_prepared() method.

        The name can be empty to create an unnamed statement, in which case
        any pre-existing unnamed statement is automatically replaced;
        otherwise it is an error if the statement name is already
        defined in the current database session. We recommend always using
        named queries, since unnamed queries have a limited lifetime and
        can be automatically replaced or destroyed by various operations.
        """
        if name is None:
            name = ''
        self._do_debug('prepare', name, command)
        self._valid_db.prepare(name, command)

    def describe_prepared(self, name: str | None = None) -> Query:
        """Describe a prepared SQL statement.

        This method returns a Query object describing the result columns of
        the prepared statement with the given name. If you omit the name,
        the unnamed statement will be described if you created one before.
        """
        if name is None:
            name = ''
        return self._valid_db.describe_prepared(name)

    def delete_prepared(self, name: str | None = None) -> Query:
        """Delete a prepared SQL statement.

        This deallocates a previously prepared SQL statement with the given
        name, or deallocates all prepared statements if you do not specify a
        name. Note that prepared statements are also deallocated automatically
        when the current session ends.
        """
        if not name:
            name = 'ALL'
        cmd = f"DEALLOCATE {name}"
        self._do_debug(cmd)
        return self._valid_db.query(cmd)

    def pkey(self, table: str, composite: bool = False, flush: bool = False
             ) -> str | tuple[str, ...]:
        """Get the primary key of a table.

        Single primary keys are returned as strings unless you
        set the composite flag.  Composite primary keys are always
        represented as tuples.  Note that this raises a KeyError
        if the table does not have a primary key.

        If flush is set then the internal cache for primary keys will
        be flushed.  This may be necessary after the database schema or
        the search path has been changed.
        """
        pkeys = self._pkeys
        if flush:
            pkeys.clear()
            self._do_debug('The pkey cache has been flushed')
        try:  # cache lookup
            pkey = pkeys[table]
        except KeyError as e:  # cache miss, check the database
            cmd = ("SELECT"  # noqa: S608
                   " a.attname, a.attnum, i.indkey"
                   " FROM pg_catalog.pg_index i"
                   " JOIN pg_catalog.pg_attribute a"
                   " ON a.attrelid OPERATOR(pg_catalog.=) i.indrelid"
                   " AND a.attnum OPERATOR(pg_catalog.=) ANY(i.indkey)"
                   " AND NOT a.attisdropped"
                   " WHERE i.indrelid OPERATOR(pg_catalog.=)"
                   " {}::pg_catalog.regclass"
                   " AND i.indisprimary ORDER BY a.attnum").format(
                  quote_if_unqualified('$1', table))
            res = self._valid_db.query(cmd, (table,)).getresult()
            if not res:
                raise KeyError(f'Table {table} has no primary key') from e
            # we want to use the order defined in the primary key index here,
            # not the order as defined by the columns in the table
            if len(res) > 1:
                indkey = res[0][2]
                pkey = tuple(row[0] for row in sorted(
                    res, key=lambda row: indkey.index(row[1])))
            else:
                pkey = res[0][0]
            pkeys[table] = pkey  # cache it
        if composite and not isinstance(pkey, tuple):
            pkey = (pkey,)
        return pkey

    def pkeys(self, table: str) -> tuple[str, ...]:
        """Get the primary key of a table as a tuple.

        Same as pkey() with 'composite' set to True.
        """
        return self.pkey(table, True)  # type: ignore

    def get_databases(self) -> list[str]:
        """Get list of databases in the system."""
        return [r[0] for r in self._valid_db.query(
            'SELECT datname FROM pg_catalog.pg_database').getresult()]

    def get_relations(self, kinds: str | Sequence[str] | None = None,
                      system: bool = False) -> list[str]:
        """Get list of relations in connected database of specified kinds.

        If kinds is None or empty, all kinds of relations are returned.
        Otherwise, kinds can be a string or sequence of type letters
        specifying which kind of relations you want to list.

        Set the system flag if you want to get the system relations as well.
        """
        where_parts = []
        if kinds:
            where_parts.append(
                "r.relkind IN ({})".format(','.join(f"'{k}'" for k in kinds)))
        if not system:
            where_parts.append("s.nspname NOT SIMILAR"
                               " TO 'pg/_%|information/_schema' ESCAPE '/'")
        where = " WHERE " + ' AND '.join(where_parts) if where_parts else ''
        cmd = ("SELECT"  # noqa: S608
               " pg_catalog.quote_ident(s.nspname) OPERATOR(pg_catalog.||)"
               " '.' OPERATOR(pg_catalog.||) pg_catalog.quote_ident(r.relname)"
               " FROM pg_catalog.pg_class r"
               " JOIN pg_catalog.pg_namespace s"
               f" ON s.oid OPERATOR(pg_catalog.=) r.relnamespace{where}"
               " ORDER BY s.nspname, r.relname")
        return [r[0] for r in self._valid_db.query(cmd).getresult()]

    def get_tables(self, system: bool = False) -> list[str]:
        """Return list of tables in connected database.

        Set the system flag if you want to get the system tables as well.
        """
        return self.get_relations('r', system)

    def get_attnames(self, table: str, with_oid: bool=True, flush: bool=False
                     ) -> AttrDict:
        """Given the name of a table, dig out the set of attribute names.

        Returns a read-only dictionary of attribute names (the names are
        the keys, the values are the names of the attributes' types)
        with the column names in the proper order if you iterate over it.

        If flush is set, then the internal cache for attribute names will
        be flushed. This may be necessary after the database schema or
        the search path has been changed.

        By default, only a limited number of simple types will be returned.
        You can get the registered types after calling use_regtypes(True).
        """
        attnames = self._attnames
        if flush:
            attnames.clear()
            self._do_debug('The attnames cache has been flushed')
        try:  # cache lookup
            names = attnames[table]
        except KeyError:  # cache miss, check the database
            cmd = "a.attnum OPERATOR(pg_catalog.>) 0"
            if with_oid:
                cmd = f"({cmd} OR a.attname OPERATOR(pg_catalog.=) 'oid')"
            cmd = self._query_attnames.format(
                quote_if_unqualified('$1', table), cmd)
            res = self._valid_db.query(cmd, (table,)).getresult()
            types = self.dbtypes
            names = AttrDict((name[0], types.add(*name[1:])) for name in res)
            attnames[table] = names  # cache it
        return names

    def get_generated(self, table: str, flush: bool = False) -> frozenset[str]:
        """Given the name of a table, dig out the set of generated columns.

        Returns a set of column names that are generated and unalterable.

        If flush is set, then the internal cache for generated columns will
        be flushed. This may be necessary after the database schema or
        the search path has been changed.
        """
        generated = self._generated
        if flush:
            generated.clear()
            self._do_debug('The generated cache has been flushed')
        try:  # cache lookup
            names = generated[table]
        except KeyError:  # cache miss, check the database
            cmd = "a.attnum OPERATOR(pg_catalog.>) 0"
            cmd = f"{cmd} AND {self._query_generated}"
            cmd = self._query_attnames.format(
                quote_if_unqualified('$1', table), cmd)
            res = self._valid_db.query(cmd, (table,)).getresult()
            names = frozenset(name[0] for name in res)
            generated[table] = names  # cache it
        return names

    def use_regtypes(self, regtypes: bool | None = None) -> bool:
        """Use registered type names instead of simplified type names."""
        if regtypes is None:
            return self.dbtypes._regtypes
        regtypes = bool(regtypes)
        if regtypes != self.dbtypes._regtypes:
            self.dbtypes._regtypes = regtypes
            self._attnames.clear()
            self.dbtypes.clear()
        return regtypes

    def has_table_privilege(self, table: str, privilege: str = 'select',
                            flush: bool = False) -> bool:
        """Check whether current user has specified table privilege.

        If flush is set, then the internal cache for table privileges will
        be flushed. This may be necessary after privileges have been changed.
        """
        privileges = self._privileges
        if flush:
            privileges.clear()
            self._do_debug('The privileges cache has been flushed')
        privilege = privilege.lower()
        try:  # ask cache
            ret = privileges[table, privilege]
        except KeyError:  # cache miss, ask the database
            cmd = "SELECT pg_catalog.has_table_privilege({}, $2)".format(
                quote_if_unqualified('$1', table))
            query = self._valid_db.query(cmd, (table, privilege))
            ret = query.singlescalar() == self._make_bool(True)
            privileges[table, privilege] = ret  # cache it
        return ret

    def get(self, table: str, row: Any,
            keyname: str | tuple[str, ...] | None = None) -> dict[str, Any]:
        """Get a row from a database table or view.

        This method is the basic mechanism to get a single row.  It assumes
        that the keyname specifies a unique row.  It must be the name of a
        single column or a tuple of column names.  If the keyname is not
        specified, then the primary key for the table is used.

        If row is a dictionary, then the value for the key is taken from it.
        Otherwise, the row must be a single value or a tuple of values
        corresponding to the passed keyname or primary key.  The fetched row
        from the table will be returned as a new dictionary or used to replace
        the existing values when row was passed as a dictionary.

        The OID is also put into the dictionary if the table has one, but
        in order to allow the caller to work with multiple tables, it is
        munged as "oid(table)" using the actual name of the table.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        attnames = self.get_attnames(table)
        qoid = oid_key(table) if 'oid' in attnames else None
        if keyname and isinstance(keyname, str):
            keyname = (keyname,)
        if qoid and isinstance(row, dict) and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if not keyname:
            try:  # if keyname is not specified, try using the primary key
                keyname = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                # try using the oid instead
                if qoid and isinstance(row, dict) and 'oid' in row:
                    keyname = ('oid',)
                else:
                    raise prg_error(
                        f'Table {table} has no primary key') from e
            else:  # the table has a primary key
                # check whether all key columns have values
                if isinstance(row, dict) and not set(keyname).issubset(row):
                    # try using the oid instead
                    if qoid and 'oid' in row:
                        keyname = ('oid',)
                    else:
                        raise KeyError(
                            'Missing value in row for specified keyname')
        if not isinstance(row, dict):
            if not isinstance(row, (tuple, list)):
                row = [row]
            if len(keyname) != len(row):
                raise KeyError(
                    'Differing number of items in keyname and row')
            row = dict(zip(keyname, row))
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        what = 'oid, *' if qoid else '*'
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keyname)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        t = self._escape_qualified_name(table)
        cmd = f'SELECT {what} FROM {t} WHERE {where} LIMIT 1'  # noqa: S608s
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if not res:
            # make where clause in error message better readable
            where = where.replace('OPERATOR(pg_catalog.=)', '=')
            raise db_error(
                f'No such record in {table}\nwhere {where}\nwith '
                + self._list_params(params))
        for n, value in res[0].items():
            if qoid and n == 'oid':
                n = qoid
            row[n] = value
        return row

    def insert(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> dict[str, Any]:
        """Insert a row into a database table.

        This method inserts a row into a table.  The name of the table must
        be passed as the first parameter.  The other parameters are used for
        providing the data of the row that shall be inserted into the table.
        If a dictionary is supplied as the second parameter, it starts with
        that.  Otherwise, it uses a blank dictionary.
        Either way the dictionary is updated from the keywords.

        The dictionary is then reloaded with the values actually inserted in
        order to pick up values modified by rules, triggers, etc.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        if row is None:
            row = {}
        row.update(kw)
        if 'oid' in row:
            del row['oid']  # do not insert oid
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = oid_key(table) if 'oid' in attnames else None
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        name_list, value_list = [], []
        for n in attnames:
            if n in row and n not in generated:
                name_list.append(col(n))
                value_list.append(adapt(row[n], attnames[n]))
        if not name_list:
            raise prg_error('No column found that can be inserted')
        names, values = ', '.join(name_list), ', '.join(value_list)
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'INSERT INTO {t} ({names})'  # noqa: S608
               f' VALUES ({values}) RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # this should always be true
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        return row

    def update(self, table: str, row: dict[str, Any] | None = None, **kw : Any
               ) -> dict[str, Any]:
        """Update an existing row in a database table.

        Similar to insert, but updates an existing row.  The update is based
        on the primary key of the table or the OID value as munged by get()
        or passed as keyword.  The OID will take precedence if provided, so
        that it is possible to update the primary key itself.

        The dictionary is then modified to reflect any changes caused by the
        update due to triggers, rules, default values, etc.
        """
        if table.endswith('*'):
            table = table[:-1].rstrip()  # need parent table name
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = oid_key(table) if 'oid' in attnames else None
        if row is None:
            row = {}
        elif 'oid' in row:
            del row['oid']  # only accept oid key from named args for safety
        row.update(kw)
        if qoid and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if qoid and 'oid' in row:  # try using the oid
            keynames: tuple[str, ...] = ('oid',)
            keyset = set(keynames)
        else:  # try using the primary key
            try:
                keynames = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                raise prg_error(f'Table {table} has no primary key') from e
            keyset = set(keynames)
            # check whether all key columns have values
            if not keyset.issubset(row):
                raise KeyError('Missing value for primary key in row')
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keynames)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        values_list = []
        for n in attnames:
            if n in row and n not in keyset and n not in generated:
                values_list.append(f'{col(n)} = {adapt(row[n], attnames[n])}')
        if not values_list:
            return row
        values = ', '.join(values_list)
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'UPDATE {t} SET {values}'  # noqa: S608
               f' WHERE {where} RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # may be empty when row does not exist
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        return row

    def upsert(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> dict[str, Any]:
        """Insert a row into a database table with conflict resolution.

        This method inserts a row into a table, but instead of raising a
        ProgrammingError exception in case a row with the same primary key
        already exists, an update will be executed instead.  This will be
        performed as a single atomic operation on the database, so race
        conditions can be avoided.

        Like the insert method, the first parameter is the name of the
        table and the second parameter can be used to pass the values to
        be inserted as a dictionary.

        Unlike the insert und update statement, keyword parameters are not
        used to modify the dictionary, but to specify which columns shall
        be updated in case of a conflict, and in which way:

        A value of False or None means the column shall not be updated,
        a value of True means the column shall be updated with the value
        that has been proposed for insertion, i.e. has been passed as value
        in the dictionary.  Columns that are not specified by keywords but
        appear as keys in the dictionary are also updated like in the case
        keywords had been passed with the value True.

        So if in the case of a conflict you want to update every column
        that has been passed in the dictionary row, you would call
        upsert(table, row). If you don't want to do anything in case
        of a conflict, i.e. leave the existing row as it is, call
        upsert(table, row, **dict.fromkeys(row)).

        If you need more fine-grained control of what gets updated, you can
        also pass strings in the keyword parameters.  These strings will
        be used as SQL expressions for the update columns.  In these
        expressions you can refer to the value that already exists in
        the table by prefixing the column name with "included.", and to
        the value that has been proposed for insertion by prefixing the
        column name with the "excluded."

        The dictionary is modified in any case to reflect the values in
        the database after the operation has completed.

        Note: The method uses the PostgreSQL "upsert" feature which is
        only available since PostgreSQL 9.5.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        if row is None:
            row = {}
        if 'oid' in row:
            del row['oid']  # do not insert oid
        if 'oid' in kw:
            del kw['oid']  # do not update oid
        attnames = self.get_attnames(table)
        generated = self.get_generated(table)
        qoid = oid_key(table) if 'oid' in attnames else None
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        name_list, value_list = [], []
        for n in attnames:
            if n in row and n not in generated:
                name_list.append(col(n))
                value_list.append(adapt(row[n], attnames[n]))
        names, values = ', '.join(name_list), ', '.join(value_list)
        try:
            keynames = self.pkeys(table)
        except KeyError as e:
            raise prg_error(f'Table {table} has no primary key') from e
        target = ', '.join(col(k) for k in keynames)
        update = []
        keyset = set(keynames)
        keyset.add('oid')
        for n in attnames:
            if n not in keyset and n not in generated:
                value = kw.get(n, n in row)
                if value:
                    if not isinstance(value, str):
                        value = f'excluded.{col(n)}'
                    update.append(f'{col(n)} = {value}')
        if not values:
            return row
        do = 'update set ' + ', '.join(update) if update else 'nothing'
        ret = 'oid, *' if qoid else '*'
        t = self._escape_qualified_name(table)
        cmd = (f'INSERT INTO {t} AS included ({names})'  # noqa: S608
               f' VALUES ({values})'
               f' ON CONFLICT ({target}) DO {do} RETURNING {ret}')
        self._do_debug(cmd, params)
        query = self._valid_db.query(cmd, params)
        res = query.dictresult()
        if res:  # may be empty with "do nothing"
            for n, value in res[0].items():
                if qoid and n == 'oid':
                    n = qoid
                row[n] = value
        else:
            self.get(table, row)
        return row

    def clear(self, table: str, row: dict[str, Any] | None = None
              ) -> dict[str, Any]:
        """Clear all the attributes to values determined by the types.

        Numeric types are set to 0, Booleans are set to false, and everything
        else is set to the empty string.  If the row argument is present,
        it is used as the row dictionary and any entries matching attribute
        names are cleared with everything else left unchanged.
        """
        # At some point we will need a way to get defaults from a table.
        if row is None:
            row = {}  # empty if argument is not present
        attnames = self.get_attnames(table)
        for n, t in attnames.items():
            if n == 'oid':
                continue
            t = t.simple
            if t in DbTypes._num_types:
                row[n] = 0
            elif t == 'bool':
                row[n] = self._make_bool(False)
            else:
                row[n] = ''
        return row

    def delete(self, table: str, row: dict[str, Any] | None = None, **kw: Any
               ) -> int:
        """Delete an existing row in a database table.

        This method deletes the row from a table.  It deletes based on the
        primary key of the table or the OID value as munged by get() or
        passed as keyword.  The OID will take precedence if provided.

        The return value is the number of deleted rows (i.e. 0 if the row
        did not exist and 1 if the row was deleted).

        Note that if the row cannot be deleted because e.g. it is still
        referenced by another table, this method raises a ProgrammingError.
        """
        if table.endswith('*'):  # hint for descendant tables can be ignored
            table = table[:-1].rstrip()
        attnames = self.get_attnames(table)
        qoid = oid_key(table) if 'oid' in attnames else None
        if row is None:
            row = {}
        elif 'oid' in row:
            del row['oid']  # only accept oid key from named args for safety
        row.update(kw)
        if qoid and qoid in row and 'oid' not in row:
            row['oid'] = row[qoid]
        if qoid and 'oid' in row:  # try using the oid
            keynames: tuple[str, ...] = ('oid',)
        else:  # try using the primary key
            try:
                keynames = self.pkeys(table)
            except KeyError as e:  # the table has no primary key
                raise prg_error(f'Table {table} has no primary key') from e
            # check whether all key columns have values
            if not set(keynames).issubset(row):
                raise KeyError('Missing value for primary key in row')
        params = self.adapter.parameter_list()
        adapt = params.add
        col = self.escape_identifier
        where = ' AND '.join('{} OPERATOR(pg_catalog.=) {}'.format(
            col(k), adapt(row[k], attnames[k])) for k in keynames)
        if 'oid' in row:
            if qoid:
                row[qoid] = row['oid']
            del row['oid']
        t = self._escape_qualified_name(table)
        cmd = f'DELETE FROM {t} WHERE {where}'  # noqa: S608
        self._do_debug(cmd, params)
        res = self._valid_db.query(cmd, params)
        return int(res)  # type: ignore

    def truncate(self, table: str | list[str] | tuple[str, ...] |
                 set[str] | frozenset[str], restart: bool = False,
                 cascade: bool = False, only: bool = False) -> Query:
        """Empty a table or set of tables.

        This method quickly removes all rows from the given table or set
        of tables.  It has the same effect as an unqualified DELETE on each
        table, but since it does not actually scan the tables it is faster.
        Furthermore, it reclaims disk space immediately, rather than requiring
        a subsequent VACUUM operation. This is most useful on large tables.

        If restart is set to True, sequences owned by columns of the truncated
        table(s) are automatically restarted.  If cascade is set to True, it
        also truncates all tables that have foreign-key references to any of
        the named tables.  If the parameter 'only' is not set to True, all the
        descendant tables (if any) will also be truncated. Optionally, a '*'
        can be specified after the table name to explicitly indicate that
        descendant tables are included.
        """
        if isinstance(table, str):
            table_only = {table: only}
            table = [table]
        elif isinstance(table, (list, tuple)):
            if isinstance(only, (list, tuple)):
                table_only = dict(zip(table, only))
            else:
                table_only = dict.fromkeys(table, only)
        elif isinstance(table, (set, frozenset)):
            table_only = dict.fromkeys(table, only)
        else:
            raise TypeError('The table must be a string, list or set')
        if not (restart is None or isinstance(restart, (bool, int))):
            raise TypeError('Invalid type for the restart option')
        if not (cascade is None or isinstance(cascade, (bool, int))):
            raise TypeError('Invalid type for the cascade option')
        tables = []
        for t in table:
            u = table_only.get(t)
            if not (u is None or isinstance(u, (bool, int))):
                raise TypeError('Invalid type for the only option')
            if t.endswith('*'):
                if u:
                    raise ValueError(
                        'Contradictory table name and only options')
                t = t[:-1].rstrip()
            t = self._escape_qualified_name(t)
            if u:
                t = f'ONLY {t}'
            tables.append(t)
        cmd_parts = ['TRUNCATE', ', '.join(tables)]
        if restart:
            cmd_parts.append('RESTART IDENTITY')
        if cascade:
            cmd_parts.append('CASCADE')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        return self._valid_db.query(cmd)

    def get_as_list(
            self, table: str,
            what: str | list[str] | tuple[str, ...] | None = None,
            where: str | list[str] | tuple[str, ...] | None = None,
            order: str | list[str] | tuple[str, ...] | bool | None = None,
            limit: int | None = None, offset: int | None = None,
            scalar: bool = False) -> list:
        """Get a table as a list.

        This gets a convenient representation of the table as a list
        of named tuples in Python.  You only need to pass the name of
        the table (or any other SQL expression returning rows).  Note that
        by default this will return the full content of the table which
        can be huge and overflow your memory.  However, you can control
        the amount of data returned using the other optional parameters.

        The parameter 'what' can restrict the query to only return a
        subset of the table columns.  It can be a string, list or a tuple.

        The parameter 'where' can restrict the query to only return a
        subset of the table rows.  It can be a string, list or a tuple
        of SQL expressions that all need to be fulfilled.

        The parameter 'order' specifies the ordering of the rows.  It can
        also be a string, list or a tuple.  If no ordering is specified,
        the result will be ordered by the primary key(s) or all columns if
        no primary key exists.  You can set 'order' to False if you don't
        care about the ordering.  The parameters 'limit' and 'offset' can be
        integers specifying the maximum number of rows returned and a number
        of rows skipped over.

        If you set the 'scalar' option to True, then instead of the
        named tuples you will get the first items of these tuples.
        This is useful if the result has only one column anyway.
        """
        if not table:
            raise TypeError('The table name is missing')
        if what:
            if isinstance(what, (list, tuple)):
                what = ', '.join(map(str, what))
            if order is None:
                order = what
        else:
            what = '*'
        cmd_parts = ['SELECT', what, 'FROM', table]
        if where:
            if isinstance(where, (list, tuple)):
                where = ' AND '.join(map(str, where))
            cmd_parts.extend(['WHERE', where])
        if order is None or order is True:
            try:
                order = self.pkeys(table)
            except (KeyError, ProgrammingError):
                with suppress(KeyError, ProgrammingError):
                    order = list(self.get_attnames(table))
        if order and not isinstance(order, bool):
            if isinstance(order, (list, tuple)):
                order = ', '.join(map(str, order))
            cmd_parts.extend(['ORDER BY', order])
        if limit:
            cmd_parts.append(f'LIMIT {limit}')
        if offset:
            cmd_parts.append(f'OFFSET {offset}')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        query = self._valid_db.query(cmd)
        res = query.namedresult()
        if res and scalar:
            res = [row[0] for row in res]
        return res

    def get_as_dict(
            self, table: str,
            keyname: str | list[str] | tuple[str, ...] | None = None, 
            what: str | list[str] | tuple[str, ...] | None = None,
            where: str | list[str] | tuple[str, ...] | None = None,
            order: str | list[str] | tuple[str, ...] | bool | None = None,
            limit: int | None = None, offset: int | None = None,
            scalar: bool = False) -> dict:
        """Get a table as a dictionary.

        This method is similar to get_as_list(), but returns the table
        as a Python dict instead of a Python list, which can be even
        more convenient. The primary key column(s) of the table will
        be used as the keys of the dictionary, while the other column(s)
        will be the corresponding values.  The keys will be named tuples
        if the table has a composite primary key.  The rows will be also
        named tuples unless the 'scalar' option has been set to True.
        With the optional parameter 'keyname' you can specify an alternative
        set of columns to be used as the keys of the dictionary.  It must
        be set as a string, list or a tuple.

        The dictionary will be ordered using the order specified with the
        'order' parameter or the key column(s) if not specified.  You can
        set 'order' to False if you don't care about the ordering.
        """
        if not table:
            raise TypeError('The table name is missing')
        if not keyname:
            try:
                keyname = self.pkeys(table)
            except (KeyError, ProgrammingError) as e:
                raise prg_error(f'Table {table} has no primary key') from e
        if isinstance(keyname, str):
            keynames: list[str] | tuple[str, ...] = (keyname,)
        elif isinstance(keyname, (list, tuple)):
            keynames = keyname
        else:
            raise KeyError('The keyname must be a string, list or tuple')
        if what:
            if isinstance(what, (list, tuple)):
                what = ', '.join(map(str, what))
            if order is None:
                order = what
        else:
            what = '*'
        cmd_parts = ['SELECT', what, 'FROM', table]
        if where:
            if isinstance(where, (list, tuple)):
                where = ' AND '.join(map(str, where))
            cmd_parts.extend(['WHERE', where])
        if order is None or order is True:
            order = keyname
        if order and not isinstance(order, bool):
            if isinstance(order, (list, tuple)):
                order = ', '.join(map(str, order))
            cmd_parts.extend(['ORDER BY', order])
        if limit:
            cmd_parts.append(f'LIMIT {limit}')
        if offset:
            cmd_parts.append(f'OFFSET {offset}')
        cmd = ' '.join(cmd_parts)
        self._do_debug(cmd)
        query = self._valid_db.query(cmd)
        res = query.getresult()
        if not res:
            return {}
        keyset = set(keynames)
        fields = query.listfields()
        if not keyset.issubset(fields):
            raise KeyError('Missing keyname in row')
        key_index: list[int] = []
        row_index: list[int] = []
        for i, f in enumerate(fields):
            (key_index if f in keyset else row_index).append(i)
        key_tuple = len(key_index) > 1
        get_key = itemgetter(*key_index)
        keys = map(get_key, res)
        if scalar:
            row_index = row_index[:1]
            row_is_tuple = False
        else:
            row_is_tuple = len(row_index) > 1
        if scalar or row_is_tuple:
            get_row: Callable[[tuple], tuple] = itemgetter(  # pyright: ignore
                *row_index)
        else:
            frst_index = row_index[0]

            def get_row(row : tuple) -> tuple:
                return row[frst_index],  # tuple with one item

            row_is_tuple = True
        rows = map(get_row, res)
        if key_tuple or row_is_tuple:
            if key_tuple:
                keys = namediter(_MemoryQuery(keys, keynames))  # type: ignore
            if row_is_tuple:
                fields = tuple(f for f in fields if f not in keyset)
                rows = namediter(_MemoryQuery(rows, fields))  # type: ignore
        # noinspection PyArgumentList
        return dict(zip(keys, rows))

    def notification_handler(self, event: str, callback: Callable,
                             arg_dict: dict | None = None,
                             timeout: int | float | None = None,
                             stop_event: str | None = None
                             ) -> NotificationHandler:
        """Get notification handler that will run the given callback."""
        return NotificationHandler(self, event, callback,
                                   arg_dict, timeout, stop_event)

    # immediately wrapped methods

    def send_query(self, cmd: str, args: Sequence | None = None) -> Query:
        """Create a new asynchronous query object for this connection."""
        if args is None:
            return self._valid_db.send_query(cmd)
        return self._valid_db.send_query(cmd, args)

    def poll(self) -> int:
        """Complete an asynchronous connection and get its state."""
        return self._valid_db.poll()

    def cancel(self) -> None:
        """Abandon processing of current SQL command."""
        self._valid_db.cancel()

    def fileno(self) -> int:
        """Get the socket used to connect to the database."""
        return self._valid_db.fileno()

    def get_cast_hook(self) -> Callable | None:
        """Get the function that handles all external typecasting."""
        return self._valid_db.get_cast_hook()

    def set_cast_hook(self, hook: Callable | None) -> None:
        """Set a function that will handle all external typecasting."""
        self._valid_db.set_cast_hook(hook)

    def get_notice_receiver(self) -> Callable | None:
        """Get the current notice receiver."""
        return self._valid_db.get_notice_receiver()

    def set_notice_receiver(self, receiver: Callable | None) -> None:
        """Set a custom notice receiver."""
        self._valid_db.set_notice_receiver(receiver)

    def getnotify(self) -> tuple[str, int, str] | None:
        """Get the last notify from the server."""
        return self._valid_db.getnotify()

    def inserttable(self, table: str, values: Sequence[list|tuple],
                    columns: list[str] | tuple[str, ...] | None = None) -> int:
        """Insert a Python iterable into a database table."""
        if columns is None:
            return self._valid_db.inserttable(table, values)
        return self._valid_db.inserttable(table, values, columns)

    def transaction(self) -> int:
        """Get the current in-transaction status of the server.

        The status returned by this method can be TRANS_IDLE (currently idle),
        TRANS_ACTIVE (a command is in progress), TRANS_INTRANS (idle, in a
        valid transaction block), or TRANS_INERROR (idle, in a failed
        transaction block).  TRANS_UNKNOWN is reported if the connection is
        bad.  The status TRANS_ACTIVE is reported only when a query has been
        sent to the server and not yet completed.
        """
        return self._valid_db.transaction()

    def parameter(self, name: str) -> str | None:
        """Look up a current parameter setting of the server."""
        return self._valid_db.parameter(name)


    def date_format(self) -> str:
        """Look up the date format currently being used by the database."""
        return self._valid_db.date_format()

    def escape_literal(self, s: AnyStr) -> AnyStr:
        """Escape a literal constant for use within SQL."""
        return self._valid_db.escape_literal(s)

    def escape_identifier(self, s: AnyStr) -> AnyStr:
        """Escape an identifier for use within SQL."""
        return self._valid_db.escape_identifier(s)

    def escape_string(self, s: AnyStr) -> AnyStr:
        """Escape a string for use within SQL."""
        return self._valid_db.escape_string(s)

    def escape_bytea(self, s: AnyStr) -> AnyStr:
        """Escape binary data for use within SQL as type 'bytea'."""
        return self._valid_db.escape_bytea(s)

    def putline(self, line: str) -> None:
        """Write a line to the server socket."""
        self._valid_db.putline(line)

    def getline(self) -> str:
        """Get a line from server socket."""
        return self._valid_db.getline()

    def endcopy(self) -> None:
        """Synchronize client and server."""
        self._valid_db.endcopy()

    def set_non_blocking(self, nb: bool) -> None:
        """Set the non-blocking mode of the connection."""
        self._valid_db.set_non_blocking(nb)

    def is_non_blocking(self) -> bool:
        """Get the non-blocking mode of the connection."""
        return self._valid_db.is_non_blocking()

    def locreate(self, mode: int) -> LargeObject:
        """Create a large object in the database.

        The valid values for 'mode' parameter are defined as the module level
        constants INV_READ and INV_WRITE.
        """
        return self._valid_db.locreate(mode)

    def getlo(self, oid: int) -> LargeObject:
        """Build a large object from given oid."""
        return self._valid_db.getlo(oid)

    def loimport(self, filename: str) -> LargeObject:
        """Import a file to a large object."""
        return self._valid_db.loimport(filename)


class _MemoryQuery:
    """Class that embodies a given query result."""

    result: Any
    fields: tuple[str, ...]

    def __init__(self, result: Any, fields: Sequence[str]) -> None:
        """Create query from given result rows and field names."""
        self.result = result
        self.fields = tuple(fields)

    def listfields(self) -> tuple[str, ...]:
        """Return the stored field names of this query."""
        return self.fields

    def getresult(self) -> Any:
        """Return the stored result of this query."""
        return self.result

    def __iter__(self) -> Iterator[Any]:
        return iter(self.result)