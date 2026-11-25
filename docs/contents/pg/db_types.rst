DbTypes -- The internal cache for database types
================================================

.. currentmodule:: pg

.. class:: DbTypes

.. versionadded:: 5.0

The :class:`DbTypes` object is essentially a dictionary mapping PostgreSQL
internal type names and type OIDs to PyGreSQL "type names" (which are also
returned by :meth:`DB.get_attnames` as dictionary values).

Types are looked up in the database when they are not yet cached. Type names
are taken literally, like the parts of table names (see :ref:`table_names`):
they are the internal names in the ``pg_type`` catalog such as ``int4`` or
``_int4``, not SQL type names such as ``integer`` or ``int[]``. They can be
enclosed in double quotes and qualified with a schema name, e.g.
``pg_catalog.int4``. Names without a schema are looked up in the search path,
and ``pg_temp`` stands for the temporary schema of the session. Names can
also be passed as bytes in the client encoding. Looking up a type that does
not exist never aborts the current transaction. Note that the *types*
parameter of :meth:`DB.prepare` takes SQL type names instead.

.. versionchanged:: 6.3
    Before, qualified type names were parsed as SQL type names, and looking up
    a type that did not exist aborted the current transaction.

These type names are strings which are equal to either the simple PyGreSQL
names or to the more fine-grained registered PostgreSQL type names if these
have been enabled with :meth:`DB.use_regtypes`. Type names are strings that
are augmented with additional information about the associated PostgreSQL
type that can be inspected using the following attributes:

        - *oid* -- the PostgreSQL type OID
        - *pgtype* -- the internal PostgreSQL data type name
        - *regtype* -- the registered PostgreSQL data type name
        - *simple* -- the more coarse-grained PyGreSQL type name
        - *typlen* -- internal size of the type, negative if variable
        - *typtype* -- `b` = base type, `c` = composite type etc.
        - *category* -- `A` = Array, `b` = Boolean, `C` = Composite etc.
        - *delim* -- delimiter for array types
        - *relid* -- corresponding table for composite types
        - *elem* -- the type OID of the elements for array types
        - *base* -- the base type for domains, None for other types
        - *attnames* -- attributes for composite types

Domains are handled like their base types: their *simple* name and the
attributes *delim*, *relid*, *elem* and *attnames* are those of the base
type, which is available as *base* (resolved through domains over domains).
The other attributes describe the domain itself. Arrays of domains have the
simple name of arrays of the base type. Values of domain types are cast like
values of their base types. A typecast set for the name of a domain is used
for elements of arrays and fields of composite types with that domain type,
but not for values of columns, since PostgreSQL reports these with the base
type.

Arrays of domains over array types are not multidimensional arrays, but
arrays with arrays as elements. They have the simple name ``text[]``, and
their values must be passed as PostgreSQL array literals in strings.

.. versionchanged:: 6.3
    The *elem* attribute has been added. Arrays of composite types now
    have the simple PyGreSQL type name ``record[]`` instead of ``text``,
    and the pseudo types ``record`` and ``_record`` for anonymous records
    the simple names ``record`` and ``record[]``.
    The *base* attribute has been added. Domains and arrays of domains now
    have the simple PyGreSQL type name of their base type instead of
    ``text``.

For details, see the PostgreSQL documentation on `pg_type
<https://www.postgresql.org/docs/current/catalog-pg-type.html>`_.

In addition to the dictionary methods, the :class:`DbTypes` class also
provides the following methods.  Note that the ``get()`` method and item
access look up types in the database if they are not yet cached.  If the
type does not exist, ``get()`` returns the given default value (*None* if
not specified), while item access raises a :exc:`KeyError`.

.. method:: DbTypes.get_attnames(typ)

    Get the names and types of the fields of composite types

    :param typ: PostgreSQL type name, type object or OID of a composite type
    :type typ: str, int or DbType
    :returns: an ordered dictionary mapping field names to type names,
        or *None* if the type is not a composite type

.. method:: DbTypes.get_typecast(typ)

    Get the cast function for the given database type

    :param typ: PostgreSQL type name, type object or OID
    :type typ: str, int or DbType
    :returns: the typecast function for the specified type
    :rtype: function or None

.. method:: DbTypes.set_typecast(typ, cast)

    Set a typecast function for the given database type(s)

    :param typ: PostgreSQL type name or list of type names
    :type typ: str or list
    :param cast: the typecast function to be set for the specified type(s),
        or *None* to remove the typecast set for the type(s)
    :type cast: callable or None

The typecast function must take one string object as argument and return a
Python object into which the PostgreSQL type shall be cast.  If the function
takes another parameter named *connection*, then the current database
connection (the low-level :class:`Connection`) will also be passed to the
typecast function.  This may sometimes be necessary to look up certain
database settings.

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
fields, call :meth:`DB.get_attnames` for any table with ``flush=True``. Types
that have been looked up by name stay cached under that name. After changing
the search path, call the ``clear()`` method. This only affects lookups by
name, values in query results are always cast according to the OIDs of their
types.

.. method:: DbTypes.reset_typecast([typ])

    Reset the typecasts for the specified (or all) type(s) to their defaults

    :param str typ: PostgreSQL type name or list of type names,
        or None to reset all typecast functions
    :type typ: str, list or None

.. method:: DbTypes.typecast(value, typ)

    Cast the given value according to the given database type

    :param str value: the value as returned by the database
    :param typ: PostgreSQL type name, type object or OID
    :type typ: str, int or DbType
    :returns: the cast value

.. note::

    Note that a :class:`DbTypes` object is always bound to a database
    connection. You can also get and set typecast functions on a global level
    using the functions :func:`pg.get_typecast` and :func:`pg.set_typecast`. If
    you do this, running connections may continue to use typecast functions
    they have already cached, particularly for type names they have already
    used. To be sure that a global change is picked up, call
    :meth:`DbTypes.reset_typecast` for these types on the :attr:`DB.dbtypes`
    objects of the running connections.

    Also note that the typecasting for all of the basic types happens already
    in the C low-level extension module.  The typecast functions that can be
    set with the above methods are only called for the types that are not
    already supported by the C extension.
