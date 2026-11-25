#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# This file contains the DB-API 2 compatible pgdb module.
#
# Copyright (c) 2026 by the PyGreSQL Development Team
#
# Please see the LICENSE.TXT file for specific restrictions.

"""pgdb - DB-API 2.0 compliant module for PyGreSQL.

(c) 1999, Pascal Andre <andre@via.ecp.fr>.
See package documentation for further information on copyright.

This module implements the Python Database API Specification v2.0
(PEP 249, https://peps.python.org/pep-0249/) on top of the classic
pg module. Basic usage:

    con = pgdb.connect(host='localhost:5432', database='test')
    cur = con.cursor()
    cur.execute("SELECT * FROM fruits WHERE name = %(name)s",
                {'name': 'apple'})
    rows = cur.fetchall()  # list of named tuples
    con.commit()
    con.close()

Parameters are passed separately as a sequence or a mapping and are
properly quoted by the module (paramstyle 'pyformat').

See the documentation at https://pygresql.github.io/contents/
for details and for the extensions to the DB-API 2.0.
"""

from pg.core import (
    ConnectError,
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
    'ARRAY',
    'BINARY',
    'BOOL',
    'DATE',
    'DATETIME',
    'FLOAT',
    'HSTORE',
    'INTEGER',
    'INTERVAL',
    'JSON',
    'LONG',
    'MONEY',
    'NUMBER',
    'NUMERIC',
    'RECORD',
    'ROWID',
    'SMALLINT',
    'STRING',
    'TIME',
    'TIMESTAMP',
    'UUID',
    'Binary',
    'ConnectError',
    'Connection',
    'Cursor',
    'DataError',
    'DatabaseError',
    'Date',
    'DateFromTicks',
    'DbType',
    'Error',
    'Hstore',
    'IntegrityError',
    'InterfaceError',
    'InternalError',
    'Interval',
    'Json',
    'Literal',
    'NotSupportedError',
    'OperationalError',
    'ProgrammingError',
    'Time',
    'TimeFromTicks',
    'Timestamp',
    'TimestampFromTicks',
    'Uuid',
    'Warning',
    '__version__',
    'apilevel',
    'connect',
    'get_typecast',
    'paramstyle',
    'reset_typecast',
    'set_typecast',
    'shortcutmethods',
    'threadsafety',
    'version',
]

__version__ = version
