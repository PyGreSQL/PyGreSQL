"""Core functionality from extension module."""

try:
    from ._pg import version
except ImportError as e:  # noqa: F841
    import os
    libpq = 'libpq.'
    if os.name == 'nt':
        libpq += 'dll'
        import sys
        paths = [path for path in os.environ["PATH"].split(os.pathsep)
                 if os.path.exists(os.path.join(path, libpq))]
        if sys.version_info >= (3, 8):
            # see https://docs.python.org/3/whatsnew/3.8.html#ctypes
            add_dll_dir = os.add_dll_directory  # type: ignore
            for path in paths:
                with add_dll_dir(os.path.abspath(path)):
                    try:
                        from ._pg import version
                    except ImportError:
                        pass
                    else:
                        del version
                        e = None  # type: ignore
                        break
        if paths:
            libpq = 'compatible ' + libpq
    else:
        libpq += 'so'
    if e:
        raise ImportError(
            "Cannot import shared library for PyGreSQL,\n"
            f"probably because no {libpq} is installed.\n{e}") from e
else:
    del version

# import objects from extension module
from ._pg import (
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
    LargeObject,
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

__all__ = [
    'Error', 'Warning',
    'DataError', 'DatabaseError',
    'IntegrityError', 'InterfaceError', 'InternalError',
    'InvalidResultError', 'MultipleResultsError',
    'NoResultError', 'NotSupportedError',
    'OperationalError', 'ProgrammingError',
    'Connection', 'Query', 'LargeObject',
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
    'get_jsondecode', 'get_pqlib_version',
    'set_array', 'set_bool', 'set_bytea_escaped',
    'set_datestyle', 'set_decimal', 'set_decimal_point',
    'set_defbase', 'set_defhost', 'set_defopt',
    'set_defpasswd', 'set_defport', 'set_defuser',
    'set_jsondecode', 'set_query_helpers',
    'version',
]
