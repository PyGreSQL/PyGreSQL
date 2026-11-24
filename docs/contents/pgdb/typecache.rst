TypeCache -- The internal cache for database types
==================================================

.. currentmodule:: pgdb

.. class:: TypeCache

.. versionadded:: 5.0

The internal :class:`TypeCache` of PyGreSQL is not part of the DB-API 2
standard, but is documented here in case you need full control and
understanding of the internal handling of database types.

The TypeCache is essentially a dictionary mapping PostgreSQL internal
type names and type OIDs to DB-API 2 "type codes" (which are also returned
as the *type_code* field of the :attr:`Cursor.description` attribute).

Types are looked up in the database when they are not yet cached. Type names
are taken literally, like the parts of table names in the classic module (see
:ref:`table_names`): they are the internal names in the ``pg_type`` catalog
such as ``int4`` or ``_int4``, not SQL type names such as ``integer`` or
``int[]``. They can be enclosed in double quotes and qualified with a schema
name, e.g. ``pg_catalog.int4``. Names without a schema are looked up in the
search path, and ``pg_temp`` stands for the temporary schema of the session.
Names can also be passed as bytes in the client encoding. Looking up a type
that does not exist never aborts the current transaction.

.. versionchanged:: 6.3
    Before, qualified type names were parsed as SQL type names, and looking up
    a type that did not exist aborted the current transaction.

These type codes are strings which are equal to the PostgreSQL internal
type name, but they are also carrying additional information about the
associated PostgreSQL type in the following attributes:

        - *oid* -- the OID of the type
        - *len*  -- the internal size
        - *type*  -- ``'b'`` = base, ``'c'`` = composite, ...
        - *category*  -- ``'A'`` = Array, ``'B'`` = Boolean, ...
        - *delim*  -- delimiter to be used when parsing arrays
        - *relid*  -- the table OID for composite types
        - *base*  -- the base type code for domains, None for other types
        - *elem*  -- the OID of the element type for array types

Domains take the attributes *delim*, *relid* and *elem* from their base type,
resolved through domains over domains. Values of domain types are cast like
values of their base types. A typecast set for the name of a domain is used
for elements of arrays and fields of composite types with that domain type,
but not for values of columns, since PostgreSQL reports these with the base
type, which is also the type code in :attr:`Cursor.description`.

.. versionchanged:: 6.3
    The *base* and *elem* attributes have been added.

For details, see the PostgreSQL documentation on `pg_type
<http://www.postgresql.org/docs/current/static/catalog-pg-type.html>`_.

In addition to the dictionary methods, the :class:`TypeCache` provides
the following methods:

.. method:: TypeCache.get_fields(typ)

    Get the names and types of the fields of composite types

    :param typ: PostgreSQL type name or OID of a composite type
    :type typ: str or int
    :returns: a list of pairs of field names and types
    :rtype: list

.. method:: TypeCache.get_typecast(typ)

    Get the cast function for the given database type

    :param typ: PostgreSQL type name, type code or OID
    :type typ: str or int
    :returns: the typecast function for the specified type
    :rtype: function or None

.. method:: TypeCache.set_typecast(typ, cast)

    Set a typecast function for the given database type(s)

    :param typ: PostgreSQL type name or type code, or list of such
    :type typ: str or list
    :param cast: the typecast function to be set for the specified type(s)
    :type typ: str or int

The typecast function must take one string object as argument and return a
Python object into which the PostgreSQL type shall be casted.  If the function
takes another parameter named *connection*, then the current database
connection will also be passed to the typecast function.  This may sometimes
be necessary to look up certain database settings.

Changing the typecast function for a type also affects fields of composite
types with that type, even if values of these composite types have already
been cast.

Typecasts are set for the names of types, so a typecast that you set applies to
all types with that name in all schemas. The built-in default typecasts,
however, are only used for base types, not for composite types, domains, enums
and ranges that have the name of a built-in type. For composite types, domains
and arrays without a typecast set for their name, the typecast is
created from the types of the fields, the base type or the element type. These
are looked up by their OIDs, so that this also works for types that are not in
the search path and for types with the same name in different schemas. The
typecasts for composite types are cached with their fields. After changing the
fields, call :meth:`TypeCache.reset_typecast`. Types that have been looked up
by name stay cached under that name. After changing the search path, call the
``clear()`` method. This only affects lookups by name, values in query results
are always cast according to the OIDs of their types.

.. method:: TypeCache.reset_typecast([typ])

    Reset the typecasts for the specified (or all) type(s) to their defaults

    :param str typ: PostgreSQL type name or type code, or list of such,
        or None to reset all typecast functions
    :type typ: str, list or None

.. method:: TypeCache.typecast(value, typ)

    Cast the given value according to the given database type

    :param typ: PostgreSQL type name, type code or OID
    :type typ: str or int
    :returns: the casted value

.. note::

    Note that the :class:`TypeCache` is always bound to a database connection.
    You can also get, set and reset typecast functions on a global level using
    the functions :func:`pgdb.get_typecast`, :func:`pgdb.set_typecast` and
    :func:`pgdb.reset_typecast`. If you do this, running connections may
    continue to use typecast functions they have already cached, particularly
    for type names they have already used. To be sure that a global change is
    picked up, call :meth:`TypeCache.reset_typecast` for these types on the
    :attr:`Connection.type_cache` objects of the running connections.
