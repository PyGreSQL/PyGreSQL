"""The DB API 2 connect function."""

from __future__ import annotations

from typing import Any

from pg.core import connect as get_cnx

from .connection import Connection

__all__ = ['connect']

def connect(dsn: str | None = None,
            user: str | None = None, password: str | None = None,
            host: str | None = None, database: str | None = None,
            **kwargs: Any) -> Connection:
    """Connect to a database."""
    # first get params from DSN
    dbport = -1
    dbhost: str | None = ""
    dbname: str | None = ""
    dbuser: str | None = ""
    dbpasswd: str | None = ""
    dbopt: str | None = ""
    if dsn:
        try:
            params = dsn.split(":", 4)
            dbhost = params[0]
            dbname = params[1]
            dbuser = params[2]
            dbpasswd = params[3]
            dbopt = params[4]
        except (AttributeError, IndexError, TypeError):
            pass

    # override if necessary
    if user is not None:
        dbuser = user
    if password is not None:
        dbpasswd = password
    if database is not None:
        dbname = database
    if host:
        try:
            params = host.split(":", 1)
            dbhost = params[0]
            dbport = int(params[1])
        except (AttributeError, IndexError, TypeError, ValueError):
            pass

    # empty host is localhost
    if dbhost == "":
        dbhost = None
    if dbuser == "":
        dbuser = None

    # pass keyword arguments as connection info string
    if kwargs:
        kwarg_list = list(kwargs.items())
        kw_parts = []
        if dbname and '=' in dbname:
            kw_parts.append(dbname)
        else:
            kwarg_list.insert(0, ('dbname', dbname))
        for kw, value in kwarg_list:
            value = str(value)
            if not value or ' ' in value:
                value = value.replace('\\', '\\\\').replace("'", "\\'")
                value = f"'{value}'"
            kw_parts.append(f'{kw}={value}')
        dbname = ' '.join(kw_parts)
    # open the connection
    cnx = get_cnx(dbname, dbhost, dbport, dbopt, dbuser, dbpasswd)
    return Connection(cnx)
