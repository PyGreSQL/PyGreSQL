Query methods
=============

.. py:currentmodule:: pg

.. class:: Query

The :class:`Query` object returned by :meth:`Connection.query` and
:meth:`DB.query` can be used as an iterator returning rows as tuples.
You can also directly access row tuples using their index, and get
the number of rows with the :func:`len` function. The :class:`Query`
class also provides the following methods for accessing the results
of the query:

getresult -- get query values as list of tuples
-----------------------------------------------

.. method:: Query.getresult()

    Get query values as list of tuples

    :returns: result values as a list of tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as a list of tuples.
More information about this result may be accessed using
:meth:`Query.listfields`, :meth:`Query.fieldname`
and :meth:`Query.fieldnum` methods.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

Since PyGreSQL 5.1 the :class:`Query` can be also used directly as
an iterable sequence, i.e. you can iterate over the :class:`Query`
object to get the same tuples as returned by :meth:`Query.getresult`.
You can also call :func:`len` on a query to find the number of rows
in the result, and access row tuples using their index directly on
the :class:`Query` object.

dictresult -- get query values as list of dictionaries
------------------------------------------------------

.. method:: Query.dictresult()

    Get query values as list of dictionaries

    :returns: result values as a list of dictionaries
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as a list of dictionaries which have
the field names as keys.

If the query has duplicate field names, you will get the value for the
field with the highest index in the query.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

namedresult -- get query values as list of named tuples
-------------------------------------------------------

.. method:: Query.namedresult()

    Get query values as list of named tuples

    :returns: result values as a list of named tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises TypeError: named tuples not supported
    :raises MemoryError: internal memory error

This method returns query results as a list of named tuples with
proper field names.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

.. versionadded:: 4.1

dictiter -- get query values as iterator of dictionaries
--------------------------------------------------------

.. method:: Query.dictiter()

    Get query values as iterator of dictionaries

    :returns: result values as an iterator of dictionaries
    :rtype: iterator
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as an iterator of dictionaries which have
the field names as keys.

If the query has duplicate field names, you will get the value for the
field with the highest index in the query.

.. versionadded:: 5.1

namediter -- get query values as iterator of named tuples
---------------------------------------------------------

.. method:: Query.namediter()

    Get query values as iterator of named tuples

    :returns: result values as an iterator of named tuples
    :rtype: iterator
    :raises TypeError: too many (any) parameters
    :raises TypeError: named tuples not supported
    :raises MemoryError: internal memory error

This method returns query results as an iterator of named tuples with
proper field names.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

.. versionadded:: 5.1


listfields -- list fields names of previous query result
--------------------------------------------------------

.. method:: Query.listfields()

    List fields names of previous query result

    :returns: field names
    :rtype: list
    :raises TypeError: too many parameters

This method returns the list of field names defined for the
query result. The fields are in the same order as the result values.

fieldname, fieldnum -- field name/number conversion
---------------------------------------------------

.. method:: Query.fieldname(num)

    Get field name from its number

    :param int num: field number
    :returns: field name
    :rtype: str
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: invalid field number

This method allows to find a field name from its rank number. It can be
useful for displaying a result. The fields are in the same order as the
result values.

.. method:: Query.fieldnum(name)

    Get field number from its name

    :param str name: field name
    :returns: field number
    :rtype: int
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: unknown field name

This method returns a field number given its name. It can be used to
build a function that converts result list strings to their correct
type, using a hardcoded table definition. The number returned is the
field rank in the query result.

ntuples -- return number of tuples in query object
--------------------------------------------------

.. method:: Query.ntuples()

    Return number of tuples in query object

    :returns: number of tuples in :class:`Query`
    :rtype: int
    :raises TypeError: Too many arguments.

This method returns the number of tuples in the query result.

.. deprecated:: 5.1
   You can use the normal :func:`len` function instead.
