pgdbCursor -- The cursor object
===============================

.. py:currentmodule:: pgdb

.. class:: pgdbCursor

These objects represent a database cursor, which is used to manage the context
of a fetch operation. Cursors created from the same connection are not
isolated, i.e., any changes done to the database by a cursor are immediately
visible by the other cursors. Cursors created from different connections can
or can not be isolated, depending on the level of transaction isolation.
The default PostgreSQL transaction isolation level is "read committed".

Cursor objects respond to the following methods and attributes.

Note that ``pgdbCursor`` objects also implement both the iterator and the
context manager protocol, i.e. you can iterate over them and you can use them
in a ``with`` statement.

description -- details regarding the result columns
---------------------------------------------------

.. attribute:: pgdbCursor.description

    This read-only attribute is a sequence of 7-item tuples.

    Each of these tuples contains information describing one result column:

    - *name*
    - *type_code*
    - *display_size*
    - *internal_size*
    - *precision*
    - *scale*
    - *null_ok*

    Note that *display_size*, *precision*, *scale* and *null_ok*
    are not implemented.

    This attribute will be ``None`` for operations that do not return rows
    or if the cursor has not had an operation invoked via the
    :meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` method yet.

rowcount -- number of rows of the result
----------------------------------------

.. attribute:: pgdbCursor.rowcount

    This read-only attribute specifies the number of rows that the last
    :meth:`pgdbCursor.execute` or :meth:`pgdbCursor.executemany` call produced
    (for DQL statements like SELECT) or affected (for DML statements like
    UPDATE or INSERT). The attribute is -1 in case no such method call has
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

Note that in case this method raises a :exc:`DatabaseError`, you can get
information about the error condition that has occurred by introspecting
its :attr:`DatabaseError.sqlstate` attribute, which will be the ``SQLSTATE``
error code associated with the error.  Applications that need to know which
error condition has occurred should usually test the error code, rather than
looking at the textual error message.

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
    :rtype: list or None

Fetch the next row of a query result set, returning a single list,
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
    :rtype: list of lists

Fetch the next set of rows of a query result, returning a list of lists.
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
    :rtype: list of list

Fetch all (remaining) rows of a query result, returning them as list of lists.
Note that the cursor's :attr:`arraysize` attribute can affect the performance
of this operation.

row_factory -- process a row of the query result
------------------------------------------------

.. method:: pgdbCursor.row_factory(row)

    Process rows before they are returned

    :param list row: the currently processed row of the result set
    :returns: the transformed row that the fetch methods shall return

.. note::

    This method is not part of the DB-API 2 standard.

You can overwrite this method with a custom row factory, e.g.
if you want to return rows as dicts instead of lists::

    class DictCursor(pgdb.pgdbCursor):

        def row_factory(self, row):
            return dict((d[0], v) for d, v in zip(self.description, row))

    cur = DictCursor(con)

.. versionadded:: 4.0

arraysize - the number of rows to fetch at a time
-------------------------------------------------

.. attribute:: pgdbCursor.arraysize

    The number of rows to fetch at a time

This read/write attribute specifies the number of rows to fetch at a time with
:meth:`pgdbCursor.fetchmany`. It defaults to 1 meaning to fetch a single row
at a time.
