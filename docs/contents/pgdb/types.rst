pgdbType -- Type objects and constructors
=========================================

.. py:currentmodule:: pgdb

Type constructors
-----------------

For binding to an operation's input parameters, PostgreSQL needs to have
the input in a particular format.  However, from the parameters to the
:meth:`pgdbCursor.execute` and :meth:`pgdbCursor.executemany` methods it
is not always obvious as which PostgreSQL data types they shall be bound.
For instance, a Python string could be bound as a simple ``char`` value,
or also as a ``date`` or a ``time``.  To make the intention clear in such
cases, you can wrap the parameters in type helper objects.  PyGreSQL provides
the constructors defined below to create such objects that can hold special
values.  When passed to the cursor methods, PyGreSQL can then detect the
proper type of the input parameter and bind it accordingly.

The :mod:`pgdb` module exports the following type constructors as part of
the DB-API 2 standard:

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

.. note::

    SQL ``NULL`` values are always represented by the Python *None* singleton
    on input and output.

Type objects
------------

.. class:: pgdbType

The :attr:`pgdbCursor.description` attribute returns information about each
of the result columns of a query.  The *type_code* must compare equal to one
of the :class:`pgdbType` objects defined below.  Type objects can be equal to
more than one type code (e.g. :class:`DATETIME` is equal to the type codes
for date, time and timestamp columns).

The pgdb module exports the following :class:`Type` objects as part of the
DB-API 2 standard:

.. object:: STRING

    Used to describe columns that are string-based (e.g. ``char``, ``varchar``, ``text``)

.. object:: BINARY

    Used to describe (long) binary columns (``bytea``)

.. object:: NUMBER

    Used to describe numeric columns (e.g. ``int``, ``float``, ``numeric``, ``money``)

.. object:: DATETIME

    Used to describe date/time columns (e.g. ``date``, ``time``, ``timestamp``, ``interval``)

.. object:: ROWID

    Used to describe the ``oid`` column of PostgreSQL database tables

.. note::

  The following more specific type objects are not part of the DB-API 2 standard.

.. object:: BOOL

    Used to describe ``boolean`` columns

.. object:: SMALLINT

    Used to describe ``smallint`` columns

.. object:: INTEGER

    Used to describe ``integer`` columns

.. object:: LONG

    Used to describe ``bigint`` columns

.. object:: FLOAT

    Used to describe ``float`` columns

.. object:: NUMERIC

    Used to describe ``numeric`` columns

.. object:: MONEY

    Used to describe ``money`` columns

.. object:: DATE

    Used to describe ``date`` columns

.. object:: TIME

    Used to describe ``time`` columns

.. object:: TIMESTAMP

    Used to describe ``timestamp`` columns

.. object:: INTERVAL

    Used to describe date and time ``interval`` columns

