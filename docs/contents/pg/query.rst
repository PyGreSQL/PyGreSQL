pgqueryobject methods
=====================

.. py:currentmodule:: pg

.. class:: pgqueryobject

The :class:`pgqueryobject` returned by :meth:`pgobject.query` and
:meth:`DB.query` provides the following methods for accessing
the results of the query:

getresult -- get query values as list of tuples
-----------------------------------------------

.. method:: pgqueryobject.getresult()

    Get query values as list of tuples

    :returns: result values as a list of tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns the list of the values returned by the query.
More information about this result may be accessed using
:meth:`pgqueryobject.listfields`, :meth:`pgqueryobject.fieldname`
and :meth:`pgqueryobject.fieldnum` methods.

dictresult -- get query values as list of dictionaries
------------------------------------------------------

.. method:: pgqueryobject.dictresult()

    Get query values as list of dictionaries

    :returns: result values as a list of dictionaries
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns the list of the values returned by the query
with each tuple returned as a dictionary with the field names
used as the dictionary index.

namedresult -- get query values as list of named tuples
-------------------------------------------------------

.. method:: pgqueryobject.namedresult()

    Get query values as list of named tuples

    :returns: result values as a list of named tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises TypeError: named tuples not supported
    :raises MemoryError: internal memory error

This method returns the list of the values returned by the query
with each row returned as a named tuple with proper field names.

.. versionadded:: 4.1

listfields -- list fields names of previous query result
--------------------------------------------------------

.. method:: pgqueryobject.listfields()

    List fields names of previous query result

    :returns: field names
    :rtype: list
    :raises TypeError: too many parameters

This method returns the list of names of the fields defined for the
query result. The fields are in the same order as the result values.

fieldname, fieldnum -- field name/number conversion
---------------------------------------------------

.. method:: pgqueryobject.fieldname(num)

    Get field name from its number

    :param int num: field number
    :returns: field name
    :rtype: str
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: invalid field number

This method allows to find a field name from its rank number. It can be
useful for displaying a result. The fields are in the same order as the
result values.

.. method:: pgqueryobject.fieldnum(name)

    Get field number from its name

    :param str name: field name
    :returns: field number
    :rtype: int
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: unknown field name

This method returns a field number from its name. It can be used to
build a function that converts result list strings to their correct
type, using a hardcoded table definition. The number returned is the
field rank in the result values list.

ntuples -- return number of tuples in query object
--------------------------------------------------

.. method:: pgqueryobject.ntuples()

    Return number of tuples in query object

    :returns: number of tuples in :class:`pgqueryobject`
    :rtype: int
    :raises TypeError: Too many arguments.

This method returns the number of tuples found in a query.
