#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# This file contains the classic pg module.
#
# Copyright (c) 2023 by the PyGreSQL Development Team
#
# The notification handler is based on pgnotify which is
# Copyright (c) 2001 Ng Pheng Siong. All rights reserved.
#
# Please see the LICENSE.TXT file for specific restrictions.

"""PyGreSQL classic interface.

This pg module implements some basic database management stuff.
It includes the _pg module and builds on it, providing the higher
level wrapper class named DB with additional functionality.
This is known as the "classic" ("old style") PyGreSQL interface.
For a DB-API 2 compliant interface use the newer pgdb module.
"""

from __future__ import annotations

from .adapt import Adapter, Bytea, Hstore, Json, Literal
from .cast import Typecasts, get_typecast, set_typecast
from .core import (
    INV_READ,
    INV_WRITE,
    POLLING_FAILED,
    POLLING_OK,
    POLLING_READING,
    POLLING_WRITING,
    RESULT_DDL,
    RESULT_DML,
    RESULT_DQL,
    RESULT_EMPTY,
    SEEK_CUR,
    SEEK_END,
    SEEK_SET,
    TRANS_ACTIVE,
    TRANS_IDLE,
    TRANS_INERROR,
    TRANS_INTRANS,
    TRANS_UNKNOWN,
    Connection,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    InvalidResultError,
    MultipleResultsError,
    NoResultError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Query,
    Warning,
    cast_array,
    cast_hstore,
    cast_record,
    connect,
    escape_bytea,
    escape_string,
    get_array,
    get_bool,
    get_bytea_escaped,
    get_datestyle,
    get_decimal,
    get_decimal_point,
    get_defbase,
    get_defhost,
    get_defopt,
    get_defport,
    get_defuser,
    get_jsondecode,
    get_pqlib_version,
    set_array,
    set_bool,
    set_bytea_escaped,
    set_datestyle,
    set_decimal,
    set_decimal_point,
    set_defbase,
    set_defhost,
    set_defopt,
    set_defpasswd,
    set_defport,
    set_defuser,
    set_jsondecode,
    set_query_helpers,
    unescape_bytea,
    version,
)
from .db import DB
from .helpers import RowCache, init_core
from .notify import NotificationHandler

__all__ = [
    'DB', 'Adapter',
    'NotificationHandler', 'Typecasts',
    'Bytea', 'Hstore', 'Json', 'Literal',
    'Error', 'Warning',
    'DataError', 'DatabaseError',
    'IntegrityError', 'InterfaceError', 'InternalError',
    'InvalidResultError', 'MultipleResultsError',
    'NoResultError', 'NotSupportedError',
    'OperationalError', 'ProgrammingError',
    'Connection', 'Query', 'RowCache',
    'INV_READ', 'INV_WRITE',
    'POLLING_OK', 'POLLING_FAILED', 'POLLING_READING', 'POLLING_WRITING',
    'RESULT_DDL', 'RESULT_DML', 'RESULT_DQL', 'RESULT_EMPTY',
    'SEEK_CUR', 'SEEK_END', 'SEEK_SET',
    'TRANS_ACTIVE', 'TRANS_IDLE', 'TRANS_INERROR',
    'TRANS_INTRANS', 'TRANS_UNKNOWN',
    'cast_array', 'cast_hstore', 'cast_record',
    'connect', 'escape_bytea', 'escape_string', 'unescape_bytea',
    'get_array', 'get_bool', 'get_bytea_escaped',
    'get_datestyle', 'get_decimal', 'get_decimal_point',
    'get_defbase', 'get_defhost', 'get_defopt', 'get_defport', 'get_defuser',
    'get_jsondecode', 'get_pqlib_version', 'get_typecast',
    'set_array', 'set_bool', 'set_bytea_escaped',
    'set_datestyle', 'set_decimal', 'set_decimal_point',
    'set_defbase', 'set_defhost', 'set_defopt',
    'set_defpasswd', 'set_defport', 'set_defuser',
    'set_jsondecode', 'set_query_helpers', 'set_typecast',
    'version', '__version__',
]

__version__ = version

init_core()
