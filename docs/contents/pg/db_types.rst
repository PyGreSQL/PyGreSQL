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
        - *category* -- `A` = Array, `b` =Boolean, `C` = Composite etc.
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
    have the simple PyGreSQL type name ``record[]`` instead of ``text``.
    The *base* attribute has been added. Domains and arrays of domains now
    have the simple PyGreSQL type name of their base type instead of
    ``text``.

For details, see the PostgreSQL documentation on `pg_type
<http://www.postgresql.org/docs/current/static/catalog-pg-type.html>`_.

In addition to the dictionary methods, the :class:`DbTypes` class also
provides the following methods:

.. method:: DbTypes.get_attnames(typ)

    Get the names and types of the fields of composite types

    :param typ: PostgreSQL type name or OID of a composite type
    :type typ: str or int
    :returns: an ordered dictionary mapping field names to type names

.. method:: DbTypes.get_typecast(typ)

    Get the cast function for the given database type

    :param str typ: PostgreSQL type name
    :returns: the typecast function for the specified type
    :rtype: function or None

.. method:: DbTypes.set_typecast(typ, cast)

    Set a typecast function for the given database type(s)

    :param typ: PostgreSQL type name or list of type names
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

.. method:: DbTypes.reset_typecast([typ])

    Reset the typecasts for the specified (or all) type(s) to their defaults

    :param str typ: PostgreSQL type name or list of type names,
        or None to reset all typecast functions
    :type typ: str, list or None

.. method:: DbTypes.typecast(value, typ)

    Cast the given value according to the given database type

    :param str typ: PostgreSQL type name or type code
    :returns: the casted value

.. note::

    Note that :class:`DbTypes` object is always bound to a database connection.
    You can also get and set and reset typecast functions on a global level
    using the functions :func:`pg.get_typecast` and :func:`pg.set_typecast`.
    If you do this, the current database connections will continue to use their
    already cached typecast functions unless you reset the typecast functions
    by calling the :meth:`DbTypes.reset_typecast` method on :attr:`DB.dbtypes`
    objects of the running connections.

    Also note that the typecasting for all of the basic types happens already
    in the C low-level extension module.  The typecast functions that can be
    set with the above methods are only called for the types that are not
    already supported by the C extension.
