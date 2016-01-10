pgdbCnx -- The connection object
================================

.. py:currentmodule:: pgdb

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

.. method:: pgdbCnx.cursor()

    Return a new cursor object using the connection

    :returns: a connection object
    :rtype: :class:`pgdbCursor`

This method returns a new :class:`pgdbCursor` object that can be used to
operate on the database in the way described in the next section.
