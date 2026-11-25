LargeObject -- Large Objects
============================

.. currentmodule:: pg

.. class:: LargeObject

.. versionchanged:: 6.3
    The class is now exported by the :mod:`pg` module.

Instances of the class :class:`LargeObject` are used to handle all the
requests concerning a PostgreSQL large object. These objects embed and hide
all the recurring variables (object OID and connection), in the same way
:class:`Connection` instances do, thus only keeping significant parameters
in function calls. The :class:`LargeObject` instance keeps a reference to
the :class:`Connection` object used for its creation, sending requests
through with its parameters. Any modification other than dereferencing the
:class:`Connection` object will thus affect the :class:`LargeObject` instance.
Dereferencing the initial :class:`Connection` object is not a problem since
Python won't deallocate it before the :class:`LargeObject` instance
dereferences it. All functions return a generic error message on error.
The exact error message is provided by the object's :attr:`error` attribute.

All methods raise a :exc:`pg.OperationalError` if the connection has been
closed, and a :exc:`pg.IntegrityError` if the large object is not valid any
more because it has been unlinked.

See also the PostgreSQL documentation for more information about the
`large object interface`__.

__ https://www.postgresql.org/docs/current/largeobjects.html

open -- open a large object
---------------------------

.. method:: LargeObject.open(mode)

    Open a large object

    :param int mode: open mode definition
    :rtype: None
    :raises TypeError: bad parameter type, or too many parameters
    :raises OSError: already opened object, or open error

This method opens a large object for reading/writing, in a similar manner as
the Unix open() function does for files. The mode value can be obtained by
OR-ing the constants defined in the :mod:`pg` module (:const:`INV_READ`,
:const:`INV_WRITE`).

close -- close a large object
-----------------------------

.. method:: LargeObject.close()

    Close a large object

    :rtype: None
    :raises TypeError: too many parameters
    :raises OSError: object is not opened, or close error

This method closes a previously opened large object, in a similar manner as
the Unix close() function.

read, write, tell, seek, unlink -- file-like large object handling
------------------------------------------------------------------

.. method:: LargeObject.read(size)

    Read data from large object

    :param int size: maximum size of the buffer to be read
    :returns: the read buffer
    :rtype: bytes
    :raises TypeError: bad parameter type, or too many parameters
    :raises ValueError: if *size* is not positive
    :raises OSError: object is not opened, or read error

This function allows reading data from a large object, starting at the
current position.

.. method:: LargeObject.write(data)

    Write data to large object

    :param data: buffer of bytes to be written
    :type data: bytes or str
    :rtype: None
    :raises TypeError: bad parameter type, or too many parameters
    :raises OSError: object is not opened, or write error

This function allows writing data to a large object, starting at the current
position. A string is encoded as UTF-8, independent of the client encoding,
since the server stores large objects without conversion.

.. method:: LargeObject.seek(offset, whence)

    Change current position in large object

    :param int offset: position offset
    :param int whence: positional parameter
    :returns: new position in object
    :rtype: int
    :raises TypeError: bad parameter type, or too many parameters
    :raises OSError: object is not opened, or seek error

This method updates the position offset in the large object. The valid values
for the whence parameter are defined as constants in the :mod:`pg` module
(:const:`SEEK_SET`, :const:`SEEK_CUR`, :const:`SEEK_END`).

.. method:: LargeObject.tell()

    Return current position in large object

    :returns: current position in large object
    :rtype: int
    :raises TypeError: too many parameters
    :raises OSError: object is not opened, or seek error

This method returns the current position offset in the large object.

.. method:: LargeObject.unlink()

    Delete large object

    :rtype: None
    :raises TypeError: too many parameters
    :raises OSError: object is not closed, or unlink error

This method unlinks (deletes) the PostgreSQL large object.

size -- get the large object size
---------------------------------

.. method:: LargeObject.size()

    Return the large object size

    :returns: the large object size
    :rtype: int
    :raises TypeError: too many parameters
    :raises OSError: object is not opened, or seek/tell error

This (composite) method returns the size of a large object.
The large object needs to be opened first.

export -- save a large object to a file
---------------------------------------

.. method:: LargeObject.export(name)

    Export a large object to a file

    :param name: file to be created
    :type name: str, bytes or path-like object
    :rtype: None
    :raises TypeError: bad parameter type, or too many parameters
    :raises OSError: object is not closed, or export error

This method allows saving the content of a large object to a file in a
very simple way. The file is created on the host running the PyGreSQL
interface, not on the server host, so the name is encoded with the file
system encoding like :func:`os.fsencode` does.

.. versionchanged:: 6.3
    Path-like objects and bytes are accepted as file names.

Object attributes
-----------------
:class:`LargeObject` objects define a read-only set of attributes exposing
some information about it. These attributes are:

.. attribute:: LargeObject.oid

    the OID associated with the large object (int)

The :attr:`LargeObject.oid` attribute allows you to reuse the large object
later, by creating a new :class:`LargeObject` instance for it with the
:meth:`Connection.getlo` method.

.. attribute:: LargeObject.pgcnx

    the :class:`Connection` object associated with the large object

Both attributes are ``None`` if the large object has been unlinked or the
connection has been closed.

.. attribute:: LargeObject.error

    the last warning/error message of the connection (str)

.. warning::

    In multi-threaded environments, :attr:`LargeObject.error` may be modified
    by another thread using the same :class:`Connection`. Remember these
    objects are shared, not duplicated. You should provide some locking if you
    want to use this information in a program in which it's shared between
    multiple threads.
