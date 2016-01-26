Connection -- The connection object
===================================

.. py:currentmodule:: pgdb

.. class:: Connection

These connection objects respond to the following methods.

Note that ``pgdb.Connection`` objects also implement the context manager protocol,
i.e. you can use them in a ``with`` statement.

close -- close the connection
-----------------------------

.. method:: Connection.close()

    Close the connection now (rather than whenever it is deleted)

    :rtype: None

The connection will be unusable from this point forward; an :exc:`Error`
(or subclass) exception will be raised if any operation is attempted with
the connection. The same applies to all cursor objects trying to use the
connection. Note that closing a connection without committing the changes
first will cause an implicit rollback to be performed.

commit -- commit the connection
-------------------------------

.. method:: Connection.commit()

    Commit any pending transaction to the database

    :rtype: None

Note that connections always use a transaction, there is no auto-commit.

rollback -- roll back the connection
------------------------------------

.. method:: Connection.rollback()

    Roll back any pending transaction to the database

    :rtype: None

This method causes the database to roll back to the start of any pending
transaction. Closing a connection without committing the changes first will
cause an implicit rollback to be performed.

cursor -- return a new cursor object
------------------------------------

.. method:: Connection.cursor()

    Return a new cursor object using the connection

    :returns: a connection object
    :rtype: :class:`Cursor`

This method returns a new :class:`Cursor` object that can be used to
operate on the database in the way described in the next section.

Attributes that are not part of the standard
--------------------------------------------

.. note::

   The following attributes are not part of the DB-API 2 standard.

.. attribute:: Connection.cursor_type

    The default cursor type used by the connection

If you want to use your own custom subclass of the :class:`Cursor` class
with he connection, set this attribute to your custom cursor class. You will
then get your custom cursor whenever you call :meth:`Connection.cursor`.

.. versionadded:: 5.0

.. attribute:: Connection.type_cache

    A dictionary with type information on the PostgreSQL types

You can request the dictionary either via type names or type OIDs.

The values are named tuples containing the following fields:

        - *oid* -- the OID of the type
        - *name*  -- the type's name
        - *len*  -- the internal size
        - *type*  -- ``'b'`` = base, ``'c'`` = composite, ...
        - *category*  -- ``'A'`` = Array, ``'B'`` = Boolean, ...
        - *delim*  -- delimiter to be used when parsing arrays
        - *relid*  -- the table OID for composite types

For details, see the PostgreSQL documentation on `pg_type
<http://www.postgresql.org/docs/current/static/catalog-pg-type.html>`_.

The :attr:`Connection.type_cache` also provides a method :meth:`columns`
that returns the names and type OIDs of the columns of composite types.

.. versionadded:: 5.0
