#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# This file contains the DB-API 2 compatible pgdb module.
#
# Copyright (c) 2023 by the PyGreSQL Development Team
#
# Please see the LICENSE.TXT file for specific restrictions.

"""pgdb - DB-API 2.0 compliant module for PyGreSQL.

(c) 1999, Pascal Andre <andre@via.ecp.fr>.
See package documentation for further information on copyright.

Inline documentation is sparse.
See DB-API 2.0 specification for usage information:
http://www.python.org/peps/pep-0249.html

Basic usage:

    pgdb.connect(connect_string) # open a connection
    # connect_string = 'host:database:user:password:opt'
    # All parts are optional. You may also pass host through
    # password as keyword arguments. To pass a port,
    # pass it in the host keyword parameter:
    connection = pgdb.connect(host='localhost:5432')

    cursor = connection.cursor() # open a cursor

    cursor.execute(query[, params])
    # Execute a query, binding params (a dictionary) if they are
    # passed. The binding syntax is the same as the % operator
    # for dictionaries, and no quoting is done.

    cursor.executemany(query, list of params)
    # Execute a query many times, binding each param dictionary
    # from the list.

    cursor.fetchone() # fetch one row, [value, value, ...]

    cursor.fetchall() # fetch all rows, [[value, value, ...], ...]

    cursor.fetchmany([size])
    # returns size or cursor.arraysize number of rows,
    # [[value, value, ...], ...] from result set.
    # Default cursor.arraysize is 1.

    cursor.description # returns information about the columns
    #	[(column_name, type_name, display_size,
    #		internal_size, precision, scale, null_ok), ...]
    # Note that display_size, precision, scale and null_ok
    # are not implemented.

    cursor.rowcount # number of rows available in the result set
    # Available after a call to execute.

    connection.commit() # commit transaction

    connection.rollback() # or rollback transaction

    cursor.close() # close the cursor

    connection.close() # close the connection
"""

from pg.core import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    version,
)

from .adapt import (
    ARRAY,
    BINARY,
    BOOL,
    DATE,
    DATETIME,
    FLOAT,
    HSTORE,
    INTEGER,
    INTERVAL,
    JSON,
    LONG,
    MONEY,
    NUMBER,
    NUMERIC,
    RECORD,
    ROWID,
    SMALLINT,
    STRING,
    TIME,
    TIMESTAMP,
    UUID,
    Binary,
    Date,
    DateFromTicks,
    DbType,
    Hstore,
    Interval,
    Json,
    Literal,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    Uuid,
)
from .cast import get_typecast, reset_typecast, set_typecast
from .connect import connect
from .connection import Connection
from .constants import apilevel, paramstyle, shortcutmethods, threadsafety
from .cursor import Cursor

__all__ = [
    'Connection', 'Cursor',
    'Date', 'Time', 'Timestamp',
    'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary', 'Interval', 'Uuid',
    'Hstore', 'Json', 'Literal', 'DbType',
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'BOOL',
    'SMALLINT', 'INTEGER', 'LONG', 'FLOAT', 'NUMERIC', 'MONEY',
    'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
    'UUID', 'HSTORE', 'JSON', 'ARRAY', 'RECORD',
    'Error', 'Warning',
    'InterfaceError', 'DatabaseError', 'DataError', 'OperationalError',
    'IntegrityError', 'InternalError', 'ProgrammingError', 'NotSupportedError',
    'get_typecast', 'set_typecast', 'reset_typecast',
    'apilevel', 'connect', 'paramstyle', 'shortcutmethods', 'threadsafety',
    'version', '__version__',
]

__version__ = version
