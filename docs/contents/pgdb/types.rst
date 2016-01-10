pgdbType -- Type objects and constructors
=========================================

.. py:currentmodule:: pgdb

.. class:: pgdbType

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

.. note::

    The following more specific types are not part of the DB-API 2 standard.

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
