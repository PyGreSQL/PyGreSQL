Connection -- The connection object
===================================

.. currentmodule:: pg

.. class:: Connection

This object handles a connection to a PostgreSQL database. It embeds and
hides all the parameters that define this connection, thus just leaving really
significant parameters in function calls.

.. note::

    Some methods give direct access to the connection socket.
    *Do not use them unless you really know what you are doing.*
    Some other methods give access to large objects.
    Refer to the PostgreSQL user manual for more information about these.

A connection must not be used concurrently by different threads. While a
command is running on the connection, trying to run another command on it
or to close or reset it raises a :exc:`pg.InterfaceError`, e.g. when this
is done by another thread or by a notice receiver. The only method that
may be called by another thread while a command is running is
:meth:`Connection.cancel`. Code that is called back while a command is
running in the same thread, i.e. a notice receiver or the iterable passed
to :meth:`Connection.inserttable`, may also use the methods that only
return local information, such as :meth:`Connection.transaction`,
:meth:`Connection.parameter` and the escape methods.

.. versionchanged:: 6.3
    Using a connection while one of its commands is running raises an
    error instead of hanging or crashing.

query -- execute a SQL command string
-------------------------------------

.. method:: Connection.query(command, [args])

    Execute a SQL command string

    :param command: SQL command
    :type command: str or bytes
    :param args: optional parameter values
    :returns: result values
    :rtype: :class:`Query`, str or None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: empty SQL query
    :raises pg.DatabaseError: error in query (a subclass depending on the
        ``SQLSTATE``, e.g. :exc:`pg.ProgrammingError`)
    :raises pg.OperationalError: the connection has been lost

This method simply sends a SQL query to the database. If the query is an
insert, update or delete statement, the number of rows affected is returned
as a string. If it is a statement that returns rows as a result (usually a
select statement, but maybe also an ``"insert/update ... returning"``
statement), this method returns a :class:`Query`. Otherwise, it returns
``None``.

You can use the :class:`Query` object as an iterator that yields all results
as tuples, or call :meth:`Query.getresult` to get the result as a list
of tuples. Alternatively, you can call :meth:`Query.dictresult` or
:meth:`Query.dictiter` if you want to get the rows as dictionaries,
or :meth:`Query.namedresult` or :meth:`Query.namediter` if you want to
get the rows as named tuples. You can also simply print the :class:`Query`
object to show the query results on the console.

The SQL command may optionally contain positional parameters of the form
``$1``, ``$2``, etc instead of literal data, in which case the values
must be supplied separately as a tuple.  The values are substituted by
the database in such a way that they don't need to be escaped, making this
an effective way to pass arbitrary or unknown data without worrying about
SQL injection or syntax errors.
``None`` is passed as NULL. Strings are encoded with the client encoding,
bytes are passed unchanged as the text representation of the value in the
client encoding. Values wrapped with :func:`Hstore` are converted to hstore
literals, in which keys and values given as bytes are also passed unchanged.
All other objects are converted to strings with ``str()`` and then also
encoded with the client encoding, except for objects of the types
:class:`bytearray` and :class:`memoryview`, which raise a :exc:`TypeError`
since their string representation is not their content.
Binary data must be wrapped with :func:`Bytea`. It is then sent unchanged
in binary format as a parameter of type ``bytea`` and may contain any
bytes, including NUL bytes, which are not allowed in the other parameters.
Other values like lists and dictionaries are not adapted to PostgreSQL
types, see :ref:`query_parameters` for how to pass such values.

.. versionchanged:: 6.3
    Values wrapped with :func:`Bytea` are sent as binary data. Before, they
    were passed as text like plain bytes. Objects of the types
    :class:`bytearray` and :class:`memoryview` raise a :exc:`TypeError`.
    Before, their string representation was sent.

If you don't pass any parameters, the command string can also include
multiple SQL commands (separated by semicolons). You will only get the
return value for the last command in this case.

When the database could not process the query, a subclass of
:exc:`pg.DatabaseError` corresponding to the ``SQLSTATE`` error code is
raised, e.g. a :exc:`pg.ProgrammingError` for a syntax error, a
:exc:`pg.DataError` for a division by zero or a :exc:`pg.IntegrityError`
for a unique constraint violation. You can check the ``SQLSTATE`` error code
of this error by reading its :attr:`sqlstate` attribute.

Example::

    name = input("Name? ")
    phone = con.query("select phone from employees where name=$1",
        (name,)).getresult()


send_query -- executes a SQL command string asynchronously
----------------------------------------------------------

.. method:: Connection.send_query(command, [args])

    Submits a command to the server without waiting for the result(s).

    :param command: SQL command
    :type command: str or bytes
    :param args: optional parameter values
    :returns: a query object, as described below
    :rtype: :class:`Query`
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises pg.OperationalError: the connection has been lost

This method is much the same as :meth:`Connection.query`, except that it
returns without waiting for the query to complete. The application can do
other things in the meantime, including executing queries using other
database connections. The application can call ``select()`` using the
``fileno``  obtained by the connection's :meth:`Connection.fileno` method
to determine when the query has results to return.

This method always returns a :class:`Query` object. This object differs
from the :class:`Query` object returned by :meth:`Connection.query` in a
few ways. Most importantly, the results of all statements are returned,
one after the other, and errors in the query (such as an empty query) are
only raised when the result of the failing statement is fetched.

The methods that return the whole result, i.e. :meth:`Query.getresult`,
:meth:`Query.dictresult`, :meth:`Query.namedresult`,
:meth:`Query.scalarresult` and the corresponding iterator methods, move to
the next result and return it. They return ``None`` when all results have
been returned, and the application should call them until they return
``None`` or raise an exception. Otherwise, the connection stays busy until
another command is executed (see below). The iterators returned by these
methods only iterate over the result that was current when they were
created.

In cases when :meth:`Connection.query` would return something other than
a :class:`Query` object, that result will be returned by these methods.
There's one important difference in these result codes: if
:meth:`Connection.query` returns ``None``, these methods will return an
empty string (``''``) instead, since ``None`` marks the end of the results.

All other methods and operations, i.e. getting single rows with
:meth:`Query.one`, :meth:`Query.single` and their variants, iterating over
the query, getting rows by index, :func:`len` and the methods that return
information about the fields, access the current result without moving to
the next result, like they do for queries returned by
:meth:`Connection.query`. When no result has been fetched yet, they fetch
the first result, which will then also be returned by the next call of one
of the methods returning the whole result. When the current result is not
a query result, there are no fields and no rows.

When another command is executed on the connection, e.g. with
:meth:`Connection.query` or :meth:`Connection.send_query`, before all
results have been fetched, the remaining results are fetched and kept in
the query object, so that they can still be returned afterwards. Note that
this waits for the asynchronous query to complete. If the connection is
closed or reset before all results have been fetched, these results are
lost and trying to fetch them raises an error.

If a statement is a COPY command, an empty string is returned as its
result. Before the next result can be fetched, the data must be sent
using :meth:`Connection.putline` and :meth:`Connection.endcopy`, or
received using :meth:`Connection.getline` until it returns ``None``.
The COPY must also be completed before another command can be executed
on the connection, otherwise a :exc:`pg.InterfaceError` is raised.

A query object and its connection must not be used concurrently by
different threads. If a query object is used again while its results are
being fetched or converted, e.g. by another thread or by a cast hook or
notice receiver, a :exc:`pg.InterfaceError` is raised.

.. versionchanged:: 6.3
    The row accessors, :func:`len` and iteration do not move to the next
    result any more, and results are not discarded any more when another
    command is executed on the connection.

.. versionadded:: 5.2

Examples::

    name = input("Name? ")
    query = con.send_query("select phone from employees where name=$1",
                           (name,))
    phone = query.getresult()
    query.getresult()  # to close the query

    # Run two queries in one round trip:
    # (Note that you cannot use a union here
    # when the result sets have different row types.)
    query = con.send_query("select a,b,c from x where d=e;"
                           " select e,f from y where g")
    result_x = query.dictresult()
    result_y = query.dictresult()
    query.dictresult()  # to close the query

    # Using select() to wait for the query to be ready:
    query = con.send_query("select pg_sleep(20)")
    r, w, e = select([con.fileno(), other, sockets], [], [])
    if con.fileno() in r:
        results = query.getresult()
        query.getresult()  # to close the query

    # Concurrent queries on separate connections:
    con1 = connect()
    con2 = connect()
    s = con1.query("begin; set transaction isolation level repeatable read;"
                   "select pg_export_snapshot();").single()
    con2.query("begin; set transaction isolation level repeatable read;"
               f"set transaction snapshot '{s}'")
    q1 = con1.send_query("select a,b,c from x where d=e")
    q2 = con2.send_query("select e,f from y where g")
    r1 = q1.getresult()
    q1.getresult()
    r2 = q2.getresult()
    q2.getresult()
    con1.query("commit")
    con2.query("commit")


query_prepared -- execute a prepared statement
----------------------------------------------

.. method:: Connection.query_prepared(name, [args])

    Execute a prepared statement

    :param name: name of the prepared statement
    :type name: str or bytes
    :param args: optional parameter values
    :returns: result values
    :rtype: :class:`Query`, str or None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: empty SQL query
    :raises pg.DatabaseError: error in query (a subclass depending on the
        ``SQLSTATE``, e.g. :exc:`pg.ProgrammingError`)
    :raises pg.OperationalError: prepared statement does not exist,
        or the connection has been lost

This method works exactly like :meth:`Connection.query` except that instead
of passing the command itself, you pass the name of a prepared statement.
An empty name corresponds to the unnamed statement.  You must have previously
created the corresponding named or unnamed statement with
:meth:`Connection.prepare`, or an :exc:`pg.OperationalError` will be raised.

The types of the parameters have been determined when the statement was
prepared. Values wrapped with :func:`Bytea` are sent as binary data, so
they can only be passed for parameters of type ``bytea``. Use a cast like
``$1::bytea`` in the prepared statement if the type cannot be derived from
the context.

.. versionadded:: 5.1

prepare -- create a prepared statement
--------------------------------------

.. method:: Connection.prepare(name, command, [types])

    Create a prepared statement

    :param name: name of the prepared statement
    :type name: str or bytes
    :param command: SQL command
    :type command: str or bytes
    :param types: optional OIDs of the parameter types
    :type types: list or tuple of int or None
    :rtype: None
    :raises TypeError: bad argument types, or wrong number of arguments
    :raises TypeError: invalid connection
    :raises ValueError: name or command contains NUL characters
    :raises ValueError: invalid type OID
    :raises pg.ProgrammingError: error in query or duplicate query

This method creates a prepared statement with the specified name for the
given command for later execution with the :meth:`Connection.query_prepared`
method. The name can be empty to create an unnamed statement, in which case
any pre-existing unnamed statement is automatically replaced; otherwise a
:exc:`pg.ProgrammingError` is raised if the statement name is already defined
in the current database session.

The SQL command may optionally contain positional parameters of the form
``$1``, ``$2``, etc instead of literal data.  The corresponding values
must then later be passed to the :meth:`Connection.query_prepared` method
separately as a tuple.

The server infers the types of the parameters from the context. If this is
not possible or not what you want, you can pass the types of the parameters
as a list or tuple of type OIDs with the *types* argument, which can also be
passed as a keyword argument. Types given as ``None`` or 0, and types of
parameters beyond the length of the list, are still inferred by the server.
If you pass more types than the command has parameters, the statement
expects that many values when it is executed. For instance, the following
statement adds two integers::

    con.prepare('add', "select $1 + $2", [23, 23])

The OID of a type can be found in the ``pg_type`` system catalog, e.g. with
a query like ``select 'int'::regtype::oid``. :meth:`DB.prepare` also accepts
type names.

.. versionadded:: 5.1

.. versionchanged:: 6.3
    The optional *types* argument has been added.

describe_prepared -- describe a prepared statement
--------------------------------------------------

.. method:: Connection.describe_prepared(name)

    Describe a prepared statement

    :param name: name of the prepared statement
    :type name: str or bytes
    :rtype: :class:`Query`
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: name contains NUL characters
    :raises pg.OperationalError: prepared statement does not exist

This method returns a :class:`Query` object describing the prepared
statement with the given name.  You can also pass an empty name in order
to describe the unnamed statement.  Information on the fields of the
corresponding query can be obtained through the :meth:`Query.listfields`,
:meth:`Query.fieldname`, :meth:`Query.fieldnum` and :meth:`Query.fieldinfo`
methods.

.. versionadded:: 5.1

reset -- reset the connection
-----------------------------

.. method:: Connection.reset()

    Reset the :mod:`pg` connection

    :rtype: None
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection
    :raises pg.InterfaceError: the connection is busy

This method resets the current database connection, i.e. it closes the
connection to the server and opens a new one with the same parameters.
The state of the session on the server, such as open transactions, prepared
statements, temporary tables and open large objects, is lost. Results of an
asynchronous query that have not yet been fetched are discarded, but the
results of other queries remain available.

poll -- completes an asynchronous connection
--------------------------------------------

.. method:: Connection.poll()

    Complete an asynchronous :mod:`pg` connection and get its state

    :returns: state of the connection
    :rtype: int
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection
    :raises pg.InternalError: some error occurred during pg connection

The database connection can be performed without any blocking calls.
This allows the application mainline to perform other operations or perhaps
connect to multiple databases concurrently. Once the connection is established,
it's no different from a connection made using blocking calls.

The required steps are to pass the parameter ``nowait=True`` to  the
:meth:`pg.connect` call, then call :meth:`Connection.poll` until it either
returns :const:`POLLING_OK` or raises an exception. To avoid blocking
in :meth:`Connection.poll`, use `select()` or `poll()` to wait for the
connection to be readable or writable, depending on the return code of the
previous call to :meth:`Connection.poll`. The initial state of the connection
is :const:`POLLING_WRITING`. The possible states are defined as constants in
the :mod:`pg` module (:const:`POLLING_OK`, :const:`POLLING_FAILED`,
:const:`POLLING_READING` and :const:`POLLING_WRITING`).

.. versionadded:: 5.2

Example::

    con = pg.connect('testdb', nowait=True)
    fileno = con.fileno()
    rd = []
    wt = [fileno]
    rc = pg.POLLING_WRITING
    while rc not in (pg.POLLING_OK, pg.POLLING_FAILED):
        ra, wa, xa = select(rd, wt, [], timeout)
        if not ra and not wa:
            timedout()
        rc = con.poll()
        if rc == pg.POLLING_READING:
            rd = [fileno]
            wt = []
        else:
            rd = []
            wt = [fileno]


cancel -- abandon processing of current SQL command
---------------------------------------------------

.. method:: Connection.cancel()

    Abandon processing of the current SQL command

    :returns: 1 (the request has been sent)
    :rtype: int
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection
    :raises pg.OperationalError: the request could not be sent
    :raises pg.InterfaceError: the connection is being established

This method requests that the server abandon processing
of the current SQL command.  It can be called by another thread while
the command is running.  If the request has been sent, the command fails
with an error if it was still running, but the server may also have
finished processing it already.  If the request could not be sent, a
:exc:`pg.OperationalError` with the reason given by libpq is raised.
The method always returns 1 otherwise, for backward compatibility.

The request is sent over a new connection to the server, and other threads
can run while it is sent.  If PyGreSQL has been built with libpq 17 or
newer, the request uses the same encryption settings as the connection,
and connecting is limited by the ``connect_timeout`` of the connection.
Otherwise, the request is sent unencrypted, and without a timeout.

.. versionchanged:: 6.3
    The request is sent without blocking other threads and, with libpq 17
    or newer, encrypted like the connection.  If the request could not be
    sent, an error is raised instead of returning 0.  Before, the method
    used the deprecated ``PQrequestCancel()`` function of libpq, which is
    not thread-safe.

close -- close the database connection
--------------------------------------

.. method:: Connection.close()

    Close the :mod:`pg` connection

    :rtype: None
    :raises TypeError: too many (any) arguments
    :raises pg.InternalError: the connection has already been closed
    :raises pg.InterfaceError: the connection is busy

This method closes the database connection. The connection will
be closed in any case when the connection is deleted but this
allows you to explicitly close it. After the connection has been closed,
its other methods and its attributes cannot be used any more and raise a
:exc:`TypeError`, and large objects of the connection cannot be used
either. Results of an asynchronous query that have not yet been fetched
are discarded, but the results of other queries remain available.

transaction -- get the current transaction state
------------------------------------------------

.. method:: Connection.transaction()

    Get the current in-transaction status of the server

    :returns: the current in-transaction status
    :rtype: int
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

The status returned by this method can be :const:`TRANS_IDLE` (currently idle),
:const:`TRANS_ACTIVE` (a command is in progress), :const:`TRANS_INTRANS` (idle,
in a valid transaction block), or :const:`TRANS_INERROR` (idle, in a failed
transaction block).  :const:`TRANS_UNKNOWN` is reported if the connection is
bad.  The status :const:`TRANS_ACTIVE` is reported only when a query has been
sent to the server and not yet completed.

parameter -- get a current server parameter setting
---------------------------------------------------

.. method:: Connection.parameter(name)

    Look up a current parameter setting of the server

    :param name: the name of the parameter to look up
    :type name: str or bytes
    :returns: the current setting of the specified parameter
    :rtype: str or None
    :raises TypeError: bad argument type, or wrong number of arguments
    :raises TypeError: invalid connection

Certain parameter values are reported by the server automatically at
connection startup or whenever their values change.  This method can be used
to interrogate these settings.  It returns the current value of a parameter
if known, or *None* if the parameter is not known.

You can use this method to check the settings of important parameters such as
`server_version`, `server_encoding`, `client_encoding`, `application_name`,
`is_superuser`, `session_authorization`, `DateStyle`, `IntervalStyle`,
`TimeZone`, `integer_datetimes`, and `standard_conforming_strings`.

Values that are not reported by this method can be requested using
:meth:`DB.get_parameter`.

.. versionadded:: 4.0

date_format -- get the currently used date format
-------------------------------------------------

.. method:: Connection.date_format()

    Look up the date format currently being used by the database

    :returns: the current date format
    :rtype: str
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

This method returns the current date format used by the server.  Note that
it is cheap to call this method, since there is no database query involved
and the setting is also cached internally.  You will need the date format
when you want to manually typecast dates and timestamps coming from the
database instead of using the built-in typecast functions.  The date format
returned by this method can be directly used with date formatting functions
such as :meth:`datetime.strptime <datetime.datetime.strptime>`.  It is
derived from the current setting of the database parameter ``DateStyle``.

.. versionadded:: 5.0

escape_literal/identifier/string/bytea -- escape for SQL
--------------------------------------------------------

The connection also has the methods ``escape_literal()``,
``escape_identifier()``, ``escape_string()`` and ``escape_bytea()``,
which escape strings for direct use in SQL commands, taking the properties
of the connection such as the client encoding into account. They work
exactly like the methods of the same name of the :class:`DB` wrapper,
see :meth:`DB.escape_literal`, :meth:`DB.escape_identifier`,
:meth:`DB.escape_string` and :meth:`DB.escape_bytea`.

encrypt_password -- encrypt a password
--------------------------------------

.. method:: Connection.encrypt_password(password, user, algorithm=None)

    Encrypt a password for the given user

    :param password: the password in clear text
    :type password: str or bytes
    :param user: the name of the user
    :type user: str or bytes
    :param algorithm: the encryption algorithm, or None for the server setting
    :type algorithm: str, bytes or None
    :returns: the encrypted password
    :rtype: str
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: an argument contains NUL characters
    :raises ValueError: unknown algorithm
    :raises UnicodeEncodeError: the user name cannot be encoded
    :raises pg.InterfaceError: the connection is busy
    :raises pg.OperationalError: the connection has been lost
    :raises pg.ProgrammingError: the server setting cannot be read

This method returns the password in the encrypted form that is stored in
the server, e.g. ``SCRAM-SHA-256$4096:...`` or ``md5...``. It can be used in
the ``PASSWORD`` clause of the SQL commands ``CREATE ROLE`` and
``ALTER ROLE``, so that the password itself is not sent to the server,
where it could appear in the server log or in ``pg_stat_activity``. Use
the method ``escape_literal()`` (see :meth:`DB.escape_literal`) to insert
it into the SQL command.

The *algorithm* can be ``'scram-sha-256'`` or ``'md5'``. Note that MD5
encrypted passwords are deprecated since PostgreSQL 18. If *algorithm* is
None, the server setting ``password_encryption`` is used, which is queried
from the server. The encryption itself is done on the client by libpq
(``PQencryptPasswordConn()``). The user name is only used by the MD5
algorithm, but must always be passed.

A *password* given as a string is always encoded as UTF-8, regardless of the
client encoding, since passwords are also sent as UTF-8 when connecting with
:func:`pg.connect` and SCRAM expects UTF-8 passwords. A *user* name given
as a string is encoded with the client encoding. Both can also be passed
as bytes, which are used unchanged. Note that the MD5 algorithm uses the
user name as salt, while the server uses the name in the server encoding,
so with MD5, non-ASCII user names only work if the client encoding is the
same as the server encoding (libpq has the same limitation).

Example::

    encrypted = con.encrypt_password(password, 'bob')
    con.query(f"CREATE ROLE bob LOGIN PASSWORD"
              f" {con.escape_literal(encrypted)}")

.. versionadded:: 6.3

change_password -- change the password of a user
------------------------------------------------

.. method:: Connection.change_password(user, password)

    Change the password of the given user

    :param user: the name of the user
    :type user: str or bytes
    :param password: the new password in clear text
    :type password: str or bytes
    :rtype: None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: an argument contains NUL characters
    :raises UnicodeEncodeError: the user name cannot be encoded
    :raises pg.InterfaceError: the connection is busy
    :raises pg.OperationalError: the connection has been lost
    :raises pg.ProgrammingError: the password could not be changed

This method encrypts the password like :meth:`Connection.encrypt_password`
with the algorithm from the server setting ``password_encryption`` and sends
it with an ``ALTER USER`` command to the server, so that the password itself
is never sent to the server. With libpq 17 or newer, this is done by the
libpq function ``PQchangePassword()``.

The password and the user name are encoded like in
:meth:`Connection.encrypt_password`. The command runs in the current
transaction, if one is open, so that the change only takes effect after
it is committed. Users can always change their own password. Changing the
password of another user needs the ``CREATEROLE`` privilege (and since
PostgreSQL 16 the ``ADMIN`` option on that role), or superuser privileges
if the other user is a superuser.
The method raises a :exc:`pg.ProgrammingError` if the command fails, e.g.
because the user does not exist, which you can check with the
:attr:`sqlstate` attribute of the error.

.. versionadded:: 6.3

fileno -- get the socket used to connect to the database
--------------------------------------------------------

.. method:: Connection.fileno()

    Get the socket used to connect to the database

    :returns: the socket id of the database connection
    :rtype: int
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

This method returns the underlying socket id used to connect
to the database. This is useful for use in select calls, etc.

set_non_blocking -- set the non-blocking status of the connection
-----------------------------------------------------------------

.. method:: set_non_blocking(nb)

    Set the non-blocking mode of the connection

    :param bool nb: True to put the connection into non-blocking mode.
                    False to put it into blocking mode.
    :raises TypeError: too many parameters
    :raises TypeError: invalid connection

Puts the socket connection into non-blocking mode or into blocking mode.
This affects copy commands and large object operations, but not queries.

.. versionadded:: 5.2

is_non_blocking -- report the blocking status of the connection
---------------------------------------------------------------

.. method:: is_non_blocking()

    get the non-blocking mode of the connection

    :returns: True if the connection is in non-blocking mode.
              False if it is in blocking mode.
    :rtype: bool
    :raises TypeError: too many parameters
    :raises TypeError: invalid connection

Returns True if the connection is in non-blocking mode, False otherwise.

.. versionadded:: 5.2

getnotify -- get the last notify from the server
------------------------------------------------

.. method:: Connection.getnotify()

    Get the last notify from the server

    :returns: last notify from server
    :rtype: tuple, None
    :raises TypeError: too many parameters
    :raises TypeError: invalid connection

This method tries to get a notify from the server (from the SQL statement
NOTIFY). If the server returns no notify, the method returns None.
Otherwise, it returns a tuple (triplet) *(relname, pid, extra)*, where
*relname* is the name of the notification channel, *pid* is the process id
of the connection that triggered the notify, and *extra* is a payload string
that has been sent with the notification. Remember to do a listen query
first, otherwise :meth:`Connection.getnotify` will always return ``None``.

The name and the payload are decoded with the client encoding. Like text
values, a payload that cannot be decoded (e.g. non-ASCII text with the client
encoding ``SQL_ASCII``) is returned as ``bytes``.

.. versionchanged:: 4.1
    Support for payload strings was added in version 4.1.

.. versionchanged:: 6.3
    The name and the payload are decoded with the client encoding.

inserttable -- insert an iterable into a table
----------------------------------------------

.. method:: Connection.inserttable(table, values, [columns], *, freeze=False)

    Insert a Python iterable into a database table

    :param table: the table name
    :type table: str or bytes
    :param list values: iterable of row values, which must be lists or tuples
    :param list columns: list or tuple of column names (*None* = all columns)
    :param bool freeze: if True, immediately freeze the inserted rows
    :rtype: int
    :raises TypeError: invalid connection, bad argument type, or too many
        arguments
    :raises MemoryError: insert buffer could not be allocated
    :raises ValueError: unsupported values, values or names containing NUL
        characters, or an error reported by the server
    :raises UnicodeEncodeError: a string that cannot be encoded in the
        client encoding
    :raises OSError: the data could not be sent to the server

This method allows you to *quickly* insert large blocks of data in a table.
Internally, it uses the COPY command of the PostgreSQL database.
The method takes an iterable of row values which must be tuples or lists
of the same size, containing the values for each inserted row.
These may contain strings, bytes, ``None`` for NULL values, and any other
objects whose string representation is a valid input for the column type,
such as integers, floats, booleans, :class:`decimal.Decimal`,
:class:`uuid.UUID`, date, time, datetime and timedelta objects, objects
of the :mod:`ipaddress` module, and values wrapped with :func:`Json`
or :func:`Hstore`.
Strings are encoded with the client encoding, bytes are passed unchanged
as the text representation of the value in the client encoding.
Other objects are converted like query parameters (see
:meth:`Connection.query`), i.e. to hstore literals or with ``str()``,
and then also encoded with the client encoding. Objects of the types
:class:`bytearray` and :class:`memoryview` raise a :exc:`TypeError`.
The values must not contain NUL characters, which PostgreSQL does not
support in text. Binary data for columns of type ``bytea`` must be
wrapped with :func:`Bytea`; it is then sent in the hex format of bytea
and may contain any bytes, including NUL bytes.
``table`` is the table name, optionally qualified with a schema name
(see :ref:`table_names`).
The table and column names can also be passed as bytes, which are not
encoded, e.g. for non-ASCII names with the client encoding SQL_ASCII.
``columns`` is an optional tuple or list of column names to be passed on
to the COPY command.
The number of rows affected is returned.

.. warning::

    This method doesn't type check the fields according to the table
    definition; it just looks whether or not it knows how to handle such
    types.

get/set_cast_hook -- fallback typecast function
-----------------------------------------------

.. method:: Connection.get_cast_hook()

    Get the function that handles all external typecasting

    :returns: the current external typecast function
    :rtype: callable, None
    :raises TypeError: too many (any) arguments

This returns the callback function used by PyGreSQL to provide plug-in
Python typecast functions for the connection.

.. versionadded:: 5.0

.. method:: Connection.set_cast_hook(func)

    Set a function that will handle all external typecasting

    :param func: the function to be used as a callback
    :rtype: None
    :raises TypeError: the specified function is neither callable nor None

This method allows setting a custom fallback function for providing
Python typecast functions for the connection to supplement the C
extension module.  If you set this function to *None*, then only the typecast
functions implemented in the C extension module are enabled.  You normally
would not want to change this.  Instead, you can use :func:`get_typecast` and
:func:`set_typecast` to add or change the plug-in Python typecast functions.

.. versionadded:: 5.0

get/set_notice_receiver -- custom notice receiver
-------------------------------------------------

.. method:: Connection.get_notice_receiver()

    Get the current notice receiver

    :returns: the current notice receiver callable
    :rtype: callable, None
    :raises TypeError: too many (any) arguments

This method gets the custom notice receiver callback function that has
been set with :meth:`Connection.set_notice_receiver`, or ``None`` if no
custom notice receiver has been set on the connection, or if it has been
reset to ``None``.

.. versionadded:: 4.1

.. method:: Connection.set_notice_receiver(func)

    Set a custom notice receiver

    :param func: the custom notice receiver callback function or ``None``
    :rtype: None
    :raises TypeError: the specified notice receiver is neither callable
        nor None

This method allows setting a custom notice receiver callback function.
When a notice or warning message is received from the server,
or generated internally by libpq, and the message level is not below
the one set with ``client_min_messages``, the specified notice receiver
function will be called. This function must take one parameter,
the :class:`Notice` object, which provides the following read-only
attributes:

    .. attribute:: Notice.pgcnx

        the connection (``None`` after the receiver has returned)

    .. attribute:: Notice.message

        the full message with a trailing newline

    .. attribute:: Notice.severity

        the level of the message, e.g. 'NOTICE' or 'WARNING'

    .. attribute:: Notice.primary

        the primary human-readable error message

    .. attribute:: Notice.detail

        an optional secondary error message

    .. attribute:: Notice.hint

        an optional suggestion what to do about the problem

``str(notice)`` returns the full message. The notice object can be kept
and used after the receiver has returned, but its :attr:`Notice.pgcnx`
attribute is then ``None``. Exceptions raised by the receiver cannot be
propagated to the code that caused the notice, so they are passed to
``sys.unraisablehook()`` instead, which by default prints them to
``sys.stderr``.

The receiver is called in the middle of a libpq operation on the
connection, e.g. while a query is running. Therefore it must not run
queries or other commands on the connection, and must not close it;
trying this raises a :exc:`pg.InterfaceError`. It may set another notice
receiver, though.

If you set the notice receiver to ``None``, the default notice receiver
of libpq is restored, which prints the messages to ``sys.stderr``.
To ignore all notices, set a receiver that does nothing instead.

.. versionadded:: 4.1

.. versionchanged:: 6.3
    The notice can be used after the receiver has returned, exceptions
    raised by the receiver are reported as unraisable, setting the
    receiver to ``None`` restores the default receiver, and using the
    connection in the receiver raises an error instead of hanging.

putline -- write a line to the server socket
--------------------------------------------

.. method:: Connection.putline(line)

    Write a line to the server socket

    :param line: line to be written
    :type line: str or bytes
    :rtype: None
    :raises TypeError: invalid connection, bad parameter type, or too many
        parameters
    :raises OSError: no COPY in progress or other error sending the line

This method allows you to directly write a string to the server socket.
A string is encoded with the client encoding, bytes are sent unchanged
(including NUL bytes, e.g. for the binary COPY format).

.. versionchanged:: 6.3
    Strings are encoded with the client encoding instead of UTF-8.

getline -- get a line from server socket
----------------------------------------

.. method:: Connection.getline()

    Get a line from server socket

    :returns: the line read, or None at the end of the copy operation
    :rtype: str, bytes or None
    :raises TypeError: invalid connection
    :raises TypeError: too many parameters
    :raises OSError: no COPY in progress or other error reading the line
    :raises pg.DatabaseError: the COPY failed (at the end)

This method allows you to directly read a string from the server socket.
The line is decoded with the client encoding. Like text values, a line that
cannot be decoded is returned as ``bytes``. A final newline is removed, so
this method is only suited for the text and CSV formats of COPY. Use the
``getdata()`` method of the source object for the binary format.

At the end of the COPY, the final result of the command is fetched, so that
the connection can be used again, and an error is raised if the COPY failed.
If the COPY was started with :meth:`Connection.send_query`, the final result
is returned by the query object instead.

.. versionchanged:: 6.3
    The line is decoded with the client encoding instead of UTF-8.
    An :exc:`OSError` instead of a :exc:`MemoryError` is raised when no
    COPY is in progress, and the connection is idle after the COPY.

endcopy -- synchronize client and server
----------------------------------------

.. method:: Connection.endcopy()

    Synchronize client and server

    :rtype: None
    :raises TypeError: invalid connection
    :raises TypeError: too many parameters
    :raises OSError: no COPY in progress or other error ending the COPY
    :raises pg.DatabaseError: the COPY failed

The use of direct access methods may desynchronize client and server.
This method ensures that client and server will be synchronized.

After the data has been sent with :meth:`Connection.putline`, this method
ends the COPY and fetches the final result of the command, so that the
connection can be used again, and raises an error if the COPY failed, e.g.
because of invalid data. If the COPY was started with
:meth:`Connection.send_query`, the final result is returned by the query
object instead.

.. versionchanged:: 6.3
    Errors of the COPY are raised, and the connection is idle afterwards.

locreate -- create a large object in the database
-------------------------------------------------

.. method:: Connection.locreate(mode)

    Create a large object in the database

    :param int mode: large object create mode
    :returns: object handling the PostgreSQL large object
    :rtype: :class:`LargeObject`
    :raises TypeError: invalid connection, bad parameter type, or too many
        parameters
    :raises pg.OperationalError: creation error

This method creates a large object in the database. The mode can be defined
by OR-ing the constants defined in the :mod:`pg` module (:const:`INV_READ`,
and :const:`INV_WRITE`). Please refer to PostgreSQL user manual for a
description of the mode values.

getlo -- build a large object from given oid
--------------------------------------------

.. method:: Connection.getlo(oid)

    Build a large object from the given OID

    :param int oid: OID of the existing large object
    :returns: object handling the PostgreSQL large object
    :rtype: :class:`LargeObject`
    :raises TypeError: invalid connection, bad parameter type, or too many
        parameters
    :raises ValueError: bad OID value (0 is invalid_oid)

This method allows reusing a previously created large object through the
:class:`LargeObject` interface, provided the user has its OID.

loimport -- import a file to a large object
-------------------------------------------

.. method:: Connection.loimport(name)

    Import a file to a large object

    :param name: the name of the file to be imported
    :type name: str, bytes or path-like object
    :returns: object handling the PostgreSQL large object
    :rtype: :class:`LargeObject`
    :raises TypeError: invalid connection, bad argument type, or too many
        arguments
    :raises pg.OperationalError: error during file import

This method allows you to create large objects in a very simple way. You just
give the name of a file containing the data to be used. The file is read on
the host running the PyGreSQL interface, so the name is encoded with the
file system encoding like :func:`os.fsencode` does.

.. versionchanged:: 6.3
    Path-like objects and bytes are accepted as file names.

Object attributes
-----------------
Every :class:`Connection` defines a set of read-only attributes that describe
the connection and its status. These attributes are:

.. attribute:: Connection.host

    the host name of the server (str)

This is ``'localhost'`` if the connection uses a Unix-domain socket.

.. attribute:: Connection.hostaddr

    the IP address of the server (str)

This is the address the host name has been resolved to, or the address given
with the ``hostaddr`` connection parameter. It is empty if the connection uses
a Unix-domain socket.

.. versionadded:: 6.3

.. attribute:: Connection.port

    the port of the server (int)

.. attribute:: Connection.db

    the selected database (str)

.. attribute:: Connection.options

    the connection options (str)

.. attribute:: Connection.user

    user name on the database system (str)

.. attribute:: Connection.protocol_version

    the major version of the frontend/backend protocol being used (int)

.. versionadded:: 4.0

.. attribute:: Connection.full_protocol_version

    the full version of the frontend/backend protocol being used (int)

The version is given as the major version times 10000 plus the minor version,
e.g. 30002 for version 3.2, which can be used with PostgreSQL 18 and newer if
it is allowed with the ``max_protocol_version`` connection parameter. If
PyGreSQL has been built with a libpq older than version 18, the minor version
cannot be determined and is returned as 0.

.. versionadded:: 6.3

.. attribute:: Connection.server_version

    the backend version (int, e.g. 150004 for 15.4)

.. versionadded:: 4.0

.. attribute:: Connection.encoding

    the name of the Python codec used for the client encoding (str)

This is the codec that is used to encode text sent to the server and to
decode text received from the server, e.g. ``'utf-8'`` for the client
encoding ``UTF8``. For ``SQL_ASCII`` and for client encodings that are not
supported by Python, this is ``'ascii'``.

.. versionadded:: 6.3

.. attribute:: Connection.status

    the status of the connection (int: 1 = OK, 0 = bad)

.. attribute:: Connection.error

    the last warning/error message from the server (str)

.. attribute:: Connection.socket

    the file descriptor number of the connection socket to the server (int)

.. versionadded:: 5.1

.. attribute:: Connection.backend_pid

     the PID of the backend process handling this connection (int)

.. versionadded:: 5.1

.. attribute:: Connection.ssl_in_use

     this is True if the connection uses SSL, False if not

.. versionadded:: 5.1

.. attribute:: Connection.ssl_attributes

     SSL-related information about the connection (dict)

.. versionadded:: 5.1
