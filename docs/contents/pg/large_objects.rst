pglarge -- Large Objects
========================

.. py:currentmodule:: pg

.. class:: pglarge

Objects that are instances of the class :class:`pglarge` are used to handle
all the requests concerning a PostgreSQL large object. These objects embed
and hide all the "recurrent" variables (object OID and connection), exactly
in the same way :class:`pgobject` instances do, thus only keeping significant
parameters in function calls. The :class:`pglarge` object keeps a reference
to the :class:`pgobject` used for its creation, sending requests though with
its parameters. Any modification but dereferencing the :class:`pgobject`
will thus affect the :class:`pglarge` object. Dereferencing the initial
:class:`pgobject` is not a problem since Python won't deallocate it before
the :class:`pglarge` object dereferences it. All functions return a generic
error message on call error, whatever the exact error was. The :attr:`error`
attribute of the object allows to get the exact error message.

See also the PostgreSQL programmer's guide for more information about the
large object interface.

open -- open a large object
---------------------------

.. method:: pglarge.open(mode)

    Open a large object

    :param int mode: open mode definition
    :rtype: None
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises IOError: already opened object, or open error

This method opens a large object for reading/writing, in the same way than the
Unix open() function. The mode value can be obtained by OR-ing the constants
defined in the :mod:`pg` module (:const:`INV_READ`, :const:`INV_WRITE`).

close -- close a large object
-----------------------------

.. method:: pglarge.close()

    Close a large object

    :rtype: None
    :raises TypeError: invalid connection
    :raises TypeError: too many parameters
    :raises IOError: object is not opened, or close error

This method closes a previously opened large object, in the same way than
the Unix close() function.

read, write, tell, seek, unlink -- file-like large object handling
------------------------------------------------------------------

.. method:: pglarge.read(size)

    Read data from large object

    :param int size: maximal size of the buffer to be read
    :returns: the read buffer
    :rtype: str
    :raises TypeError: invalid connection, invalid object,
     bad parameter type, or too many parameters
    :raises ValueError: if `size` is negative
    :raises IOError: object is not opened, or read error

This function allows to read data from a large object, starting at current
position.

.. method:: pglarge.write(string)

    Read data to large object

    :param str string: string buffer to be written
    :rtype: None
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises IOError: object is not opened, or write error

This function allows to write data to a large object, starting at current
position.

.. method:: pglarge.seek(offset, whence)

    Change current position in large object

    :param int offset: position offset
    :param int whence: positional parameter
    :returns: new position in object
    :rtype: int
    :raises TypeError: invalid connection or invalid object,
     bad parameter type, or too many parameters
    :raises IOError: object is not opened, or seek error

This method allows to move the position cursor in the large object.
The valid values for the whence parameter are defined as constants in the
:mod:`pg` module (:const:`SEEK_SET`, :const:`SEEK_CUR`, :const:`SEEK_END`).

.. method:: pglarge.tell()

    Return current position in large object

    :returns: current position in large object
    :rtype: int
    :raises TypeError: invalid connection or invalid object
    :raises TypeError: too many parameters
    :raises IOError: object is not opened, or seek error

This method allows to get the current position in the large object.

.. method:: pglarge.unlink()

    Delete large object

    :rtype: None
    :raises TypeError: invalid connection or invalid object
    :raises TypeError: too many parameters
    :raises IOError: object is not closed, or unlink error

This methods unlinks (deletes) the PostgreSQL large object.

size -- get the large object size
---------------------------------

.. method:: pglarge.size()

    Return the large object size

    :returns: the large object size
    :rtype: int
    :raises TypeError: invalid connection or invalid object
    :raises TypeError: too many parameters
    :raises IOError: object is not opened, or seek/tell error

This (composite) method allows to get the size of a large object. It was
implemented because this function is very useful for a web interfaced
database. Currently, the large object needs to be opened first.

export -- save a large object to a file
---------------------------------------

.. method:: pglarge.export(name)

    Export a large object to a file

    :param str name: file to be created
    :rtype: None
    :raises TypeError: invalid connection or invalid object,
     bad parameter type, or too many parameters
    :raises IOError: object is not closed, or export error

This methods allows to dump the content of a large object in a very simple
way. The exported file is created on the host of the program, not the
server host.

Object attributes
-----------------
:class:`pglarge` objects define a read-only set of attributes that allow
to get some information about it. These attributes are:

.. attribute:: pglarge.oid

   the OID associated with the object (int)

.. attribute:: pglarge.pgcnx

   the :class:`pgobject` associated with the object

.. attribute:: pglarge.error

   the last warning/error message of the connection

.. warning::

    In multi-threaded environments, :attr:`pglarge.error` may be modified by
    another thread using the same :class:`pgobject`. Remember these object
    are shared, not duplicated. You should provide some locking to be able
    if you want to check this. The :attr:`pglarge.oid` attribute is very
    interesting, because it allows you to reuse the OID later, creating the
    :class:`pglarge` object with a :meth:`pgobject.getlo` method call.
