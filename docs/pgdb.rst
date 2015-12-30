----------------------------------------------
:mod:`pgdb` --- The DB-API Compliant Interface
----------------------------------------------

.. module:: pgdb

.. contents:: Contents


Introduction
============
You may either choose to use the "classic" PyGreSQL interface
provided by the :mod:`pg` module or else the
DB-API 2.0 compliant interface provided by the :mod:`pgdb` module.

`DB-API 2.0 <http://www.python.org/dev/peps/pep-0249/>`_
(Python Database API Specification v2.0)
is a specification for connecting to databases (not only PostGreSQL)
from Python that has been developed by the Python DB-SIG in 1999.

The following documentation covers only the newer :mod:`pgdb` API.

The authoritative programming information for the DB-API is :pep:`0249`

A useful tutorial-like `introduction to the DB-API
<http://www2.linuxjournal.com/lj-issues/issue49/2605.html>`_
has been written by Andrew M. Kuchling for the LINUX Journal in 1998.


Module functions and constants
==============================

The :mod:`pgdb` module defines a :func:`connect` function that allows to
connect to a database, some global constants describing the capabilities
of the module as well as several exception classes.

connect -- Open a PostgreSQL connection
---------------------------------------

.. function:: connect([dsn], [user], [password], [host], [database])

    Return a new connection to the database

    :param str dsn: data source name as string
    :param str user: the database user name
    :param str password: the database password
    :param str host: the hostname of the database
    :param database: the name of the database
    :returns: a connection object
    :rtype: :class:`pgdbCnx`
    :raises pgdb.OperationalError: error connecting to the database

This function takes parameters specifying how to connect to a PostgreSQL
database and returns a :class:`pgdbCnx` object using these parameters.
If specified, the *dsn* parameter must be a string with the format
``'host:base:user:passwd:opt:tty'``. All of the parts specified in the *dsn*
are optional. You can also specify the parameters individually using keyword
arguments, which always take precedence. The *host* can also contain a port
if specified in the format ``'host:port'``. In the *opt* part of the *dsn*
you can pass command-line options to the server, the *tty* part is used to
send server debug output.

Example::

    con = connect(dsn='myhost:mydb', user='guido', password='234$')


Module constants
----------------

.. data:: apilevel

    The string constant ``'2.0'``, stating that the module is DB-API 2.0 level
    compliant.

.. data:: threadsafety

    The integer constant 1, stating that the module itself is thread-safe,
    but the connections are not thread-safe, and therefore must be protected
    with a lock if you want to use them from different threads.

.. data:: paramstyle

   The string constant ``pyformat``, stating that parameters should be passed
   using Python extended format codes, e.g. ``" ... WHERE name=%(name)s"``.

Errors raised by this module
----------------------------

The errors that can be raised by the :mod:`pgdb` module are the following:

.. exception:: Warning

    Exception raised for important warnings like data truncations while
    inserting.

.. exception:: Error

    Exception that is the base class of all other error exceptions. You can
    use this to catch all errors with one single except statement.
    Warnings are not considered errors and thus do not use this class as base.

.. exception:: InterfaceError

    Exception raised for errors that are related to the database interface
    rather than the database itself.

.. exception:: DatabaseError

    Exception raised for errors that are related to the database.

.. exception:: DataError

    Exception raised for errors that are due to problems with the processed
    data like division by zero or numeric value out of range.

.. exception:: OperationalError

    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction could
    not be processed, or a memory allocation error occurred during processing.

.. exception:: IntegrityError

    Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails.

.. exception:: ProgrammingError

    Exception raised for programming errors, e.g. table not found or already
    exists, syntax error in the SQL statement or wrong number of parameters
    specified.

.. exception:: NotSupportedError

    Exception raised in case a method or database API was used which is not
    supported by the database.


pgdbCnx -- The connection object
================================

.. class:: pgdbCnx

These connection objects respond to the following methods.

Note that ``pgdb.pgdbCnx`` objects also implement the context manager protocol,
i.e. you can use them in a ``with`` statement.

close -- close the connection
-----------------------------

.. method:: pgdbCnx.close()

    Close the connection now (rather than whenever it is deleted)

    :rtype: None

The connection will be unusable from this point forward; an :exc:`Error`
(or subclass) exception will be raised if any operation is attempted with
the connection. The same applies to all cursor objects trying to use the
connection. Note that closing a connection without committing the changes
first will cause an implicit rollback to be performed.

commit -- commit the connection
-------------------------------

.. method:: pgdbCnx.commit()

    Commit any pending transaction to the database

    :rtype: None

Note that connections always use a transaction, there is no auto-commit.

rollback -- roll back the connection
------------------------------------

.. method:: pgdbCnx.rollback()

    Roll back any pending transaction to the database

    :rtype: None

This method causes the database to roll back to the start of any pending
transaction. Closing a connection without committing the changes first will
cause an implicit rollback to be performed.

cursor -- return a new cursor object
------------------------------------

.. method:: pgdbCnx.cusor()

    Return a new cursor object using the connection

    :returns: a connection object
    :rtype: :class:`pgdbCursor`

This method returns a new :class:`pgdbCursor` object that can be used to
operate on the database in the way described in the next section.


pgdbCursor -- The cursor object
===============================

.. class:: pgdb.Cursor

These objects represent a database cursor, which is used to manage the context
of a fetch operation. Cursors created from the same connection are not
isolated, i.e., any changes done to the database by a cursor are immediately
visible by the other cursors. Cursors created from different connections can
or can not be isolated, depending on the level of transaction isolation.
The default PostgreSQL transaction isolation level is "read committed".

Cursor objects respond to the following methods and attributes.

Note that ``pgdb.Cursor`` objects also implement both the iterator and the
context manager protocol, i.e. you can iterate over them and you can use them
in a ``with`` statement.

description -- details regarding the result columns
---------------------------------------------------

.. attribute:: Cursor.description

    This read-only attribute is a sequence of 7-item sequences.

    Each of these sequences contains information describing one result column:

    - *name*
    - *type_code*
    - *display_size*
    - *internal_size*
    - *precision*
    - *scale*
    - *null_ok*

    Note that *precision*, *scale* and *null_ok* are not implemented.

    This attribute will be ``None`` for operations that do not return rows
    or if the cursor has not had an operation invoked via the
    :meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` method yet.

rowcount -- number of rows of the result
----------------------------------------

.. attribute:: Cursor.rowcount

    This read-only attribute specifies the number of rows that the last
    :meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` call produced
    (for DQL statements like SELECT) or affected (for DML statements like
    UPDATE or INSERT ). The attribute is -1 in case no such method call has
    been performed on the cursor or the rowcount of the last operation
    cannot be determined by the interface.

close -- close the cursor
-------------------------

.. method:: pgdbCursor.close()

    Close the cursor now (rather than whenever it is deleted)

    :rtype: None

The cursor will be unusable from this point forward; an :exc:`Error`
(or subclass) exception will be raised if any operation is attempted
with the cursor.

execute -- execute a database operation
---------------------------------------

.. method:: pgdbCursor.execute(operation, [parameters])

    Prepare and execute a database operation (query or command)

    :param str operation: the database operation
    :param parameters: a sequence or mapping of parameters
    :returns: the cursor, so you can chain commands

Parameters may be provided as sequence or mapping and will be bound to
variables in the operation. Variables are specified using Python extended
format codes, e.g. ``" ... WHERE name=%(name)s"``.

A reference to the operation will be retained by the cursor. If the same
operation object is passed in again, then the cursor can optimize its behavior.
This is most effective for algorithms where the same operation is used,
but different parameters are bound to it (many times).

The parameters may also be specified as list of tuples to e.g. insert multiple
rows in a single operation, but this kind of usage is deprecated:
:meth:`pgdbCursor.executemany` should be used instead.

executemany -- execute many similar database operations
-------------------------------------------------------

.. method:: pgdbCursor.executemany(operation, [seq_of_parameters])

    Prepare and execute many similar database operations (queries or commands)

    :param str operation: the database operation
    :param seq_of_parameters: a sequence or mapping of parameter tuples or mappings
    :returns: the cursor, so you can chain commands

Prepare a database operation (query or command) and then execute it against
all parameter tuples or mappings found in the sequence *seq_of_parameters*.

Parameters are bounded to the query using Python extended format codes,
e.g. ``" ... WHERE name=%(name)s"``.

fetchone -- fetch next row of the query result
----------------------------------------------

.. method:: pgdbCursor.fetchone()

    Fetch the next row of a query result set

    :returns: the next row of the query result set
    :rtype: tuple or None

Fetch the next row of a query result set, returning a single tuple,
or ``None`` when no more data is available.

An :exc:`Error` (or subclass) exception is raised if the previous call to
:meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` did not produce
any result set or no call was issued yet.

fetchmany -- fetch next set of rows of the query result
-------------------------------------------------------

.. method:: pgdbCursor.fetchmany([size=None], [keep=False])

    Fetch the next set of rows of a query result

    :param size: the number of rows to be fetched
    :type size: int or None
    :param keep: if set to true, will keep the passed arraysize
    :tpye keep: bool
    :returns: the next set of rows of the query result
    :rtype: list of tuples

Fetch the next set of rows of a query result, returning a list of tuples.
An empty sequence is returned when no more rows are available.

The number of rows to fetch per call is specified by the *size* parameter.
If it is not given, the cursor's :attr:`arraysize` determines the number of
rows to be fetched. If you set the *keep* parameter to True, this is kept as
new :attr:`arraysize`.

The method tries to fetch as many rows as indicated by the *size* parameter.
If this is not possible due to the specified number of rows not being
available, fewer rows may be returned.

An :exc:`Error` (or subclass) exception is raised if the previous call to
:meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` did not produce
any result set or no call was issued yet.

Note there are performance considerations involved with the *size* parameter.
For optimal performance, it is usually best to use the :attr:`arraysize`
attribute. If the *size* parameter is used, then it is best for it to retain
the same value from one :meth:`pgdbCursor.fetchmany` call to the next.

fetchall -- fetch all rows of the query result
----------------------------------------------

.. method:: pgdbCursor.fetchall()

    Fetch all (remaining) rows of a query result

    :returns: the set of all rows of the query result
    :rtype: list of tuples

Fetch all (remaining) rows of a query result, returning them as list of tuples.
Note that the cursor's :attr:`arraysize` attribute can affect the performance
of this operation.

row_factory -- process a row of the query result
------------------------------------------------

.. method:: pgdbCursor.row_factory(row)

    Process rows before they are returned

    :param tuple row: the currently processed row of the result set
    :returns: the transformed row that the cursor methods shall return

Note that this method is not part of the DB-API 2 standard.

You can overwrite this method with a custom row factory, e.g.
if you want to return rows as dicts instead of tuples::

    class DictCursor(pgdb.pgdbCursor):

        def row_factory(self, row):
            return {desc[0]:value
                for desc, value in zip(self.description, row)}

    cur = DictCursor(con)

arraysize - the number of rows to fetch at a time
-------------------------------------------------

.. attribute:: pgdbCursor.arraysize

    The number of rows to fetch at a time

This read/write attribute specifies the number of rows to fetch at a time with
:meth:`pgdbCursor.fetchmany`. It defaults to 1 meaning to fetch a single row
at a time.


pgdbType -- Type objects and constructors
=========================================

The :attr:`pgdbCursor.description` attribute returns information about each
of the result columns of a query. The *type_code* must compare equal to one
of the :class:`pgdbType` objects defined below. Type objects can be equal to
more than one type code (e.g. :class:`DATETIME` is equal to the type codes
for date, time and timestamp columns).

The :mod:`pgdb` module exports the following constructors and singletons:

.. function:: Date(year, month, day)

    Construct an object holding a date value

.. function:: Time(hour, minute=0, second=0, microsecond=0)

    Construct an object holding a time value

.. function:: Timestamp(year, month, day, hour=0, minute=0, second=0, microsecond=0)

    Construct an object holding a time stamp value

.. function:: DateFromTicks(ticks)

    Construct an object holding a date value from the given *ticks* value

.. function:: TimeFromTicks(ticks)

    Construct an object holding a time value from the given *ticks* value

.. function:: TimestampFromTicks(ticks)

    Construct an object holding a time stamp from the given *ticks* value

.. function:: Binary(bytes)

    Construct an object capable of holding a (long) binary string value

.. class:: STRING

    Used to describe columns that are string-based (e.g. ``char``, ``varchar``, ``text``)

.. class:: BINARY type

    Used to describe (long) binary columns (``bytea``)

.. class:: NUMBER

    Used to describe numeric columns (e.g. ``int``, ``float``, ``numeric``, ``money``)

.. class:: DATETIME

    Used to describe date/time columns (e.g. ``date``, ``time``, ``timestamp``, ``interval``)

.. class:: ROWID

    Used to describe the ``oid`` column of PostgreSQL database tables

The following more specific types are not part of the DB-API 2 standard:

.. class:: BOOL

    Used to describe ``boolean`` columns

.. class:: SMALLINT

    Used to describe ``smallint`` columns

.. class:: INTEGER

    Used to describe ``integer`` columns

.. class:: LONG

    Used to describe ``bigint`` columns

.. class:: FLOAT

    Used to describe ``float`` columns

.. class:: NUMERIC

    Used to describe ``numeric`` columns

.. class:: MONEY

    Used to describe ``money`` columns

.. class:: DATE

    Used to describe ``date`` columns

.. class:: TIME

    Used to describe ``time`` columns

.. class:: TIMESTAMP

    Used to describe ``timestamp`` columns

.. class:: INTERVAL

    Used to describe date and time ``interval`` columns
