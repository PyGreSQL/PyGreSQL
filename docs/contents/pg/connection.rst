pgobject -- The connection object
=================================

.. class:: pgobject

This object handles a connection to a PostgreSQL database. It embeds and
hides all the parameters that define this connection, thus just leaving really
significant parameters in function calls.

.. note::

    Some methods give direct access to the connection socket.
    *Do not use them unless you really know what you are doing.*
    If you prefer disabling them,
    set the ``-DNO_DIRECT`` option in the Python setup file.
    These methods are specified by the tag [DA].

.. note::

    Some other methods give access to large objects
    (refer to PostgreSQL user manual for more information about these).
    If you want to forbid access to these from the module,
    set the ``-DNO_LARGE`` option in the Python setup file.
    These methods are specified by the tag [LO].

query -- execute a SQL command string
-------------------------------------

.. method:: pgobject.query(command, [args])

    Execute a SQL command string

    :param str command: SQL command
    :param args: optional positional arguments
    :returns: result values
    :rtype: :class:`pgqueryobject`, None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: empty SQL query or lost connection
    :raises pg.ProgrammingError: error in query
    :raises pg.InternalError: error during query processing

This method simply sends a SQL query to the database. If the query is an
insert statement that inserted exactly one row into a table that has OIDs, the
return value is the OID of the newly inserted row. If the query is an update
or delete statement, or an insert statement that did not insert exactly one
row in a table with OIDs, then the number of rows affected is returned as a
string. If it is a statement that returns rows as a result (usually a select
statement, but maybe also an ``"insert/update ... returning"`` statement),
this method returns a :class:`pgqueryobject` that can be accessed via the
:meth:`pgqueryobject.getresult`, :meth:`pgqueryobject.dictresult` or
:meth:`pgqueryobject.namedresult` methods or simply printed.
Otherwise, it returns ``None``.

The query may optionally contain positional parameters of the form ``$1``,
``$2``, etc instead of literal data, and the values supplied as a tuple.
The values are substituted by the database in such a way that they don't
need to be escaped, making this an effective way to pass arbitrary or
unknown data without worrying about SQL injection or syntax errors.

When the database could not process the query, a :exc:`pg.ProgrammingError` or
a :exc:`pg.InternalError` is raised. You can check the ``SQLSTATE`` code of
this error by reading its :attr:`sqlstate` attribute.

Example::

    name = raw_input("Name? ")
    phone = con.query("select phone from employees where name=$1",
        (name,)).getresult()

reset -- reset the connection
-----------------------------

.. method:: pgobject.reset()

    Reset the :mod:`pg` connection

    :rtype: None
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

This method resets the current database connection.

cancel -- abandon processing of current SQL command
---------------------------------------------------

.. method:: pgobject.cancel()

    :rtype: None
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

This method requests that the server abandon processing
of the current SQL command.

close -- close the database connection
--------------------------------------

.. method:: pgobject.close()

    Close the :mod:`pg` connection

    :rtype: None
    :raises TypeError: too many (any) arguments

This method closes the database connection. The connection will
be closed in any case when the connection is deleted but this
allows you to explicitly close it. It is mainly here to allow
the DB-SIG API wrapper to implement a close function.

fileno -- returns the socket used to connect to the database
------------------------------------------------------------

.. method:: pgobject.fileno()

    Return the socket used to connect to the database

    :returns: the socket id of the database connection
    :rtype: int
    :raises TypeError: too many (any) arguments
    :raises TypeError: invalid connection

This method returns the underlying socket id used to connect
to the database. This is useful for use in select calls, etc.

getnotify -- get the last notify from the server
------------------------------------------------

.. method:: pgobject.getnotify()

    Get the last notify from the server

    :returns: last notify from server
    :rtype: tuple, None
    :raises TypeError: too many parameters
    :raises TypeError: invalid connection

This method tries to get a notify from the server (from the SQL statement
NOTIFY). If the server returns no notify, the methods returns None.
Otherwise, it returns a tuple (triplet) *(relname, pid, extra)*, where
*relname* is the name of the notify, *pid* is the process id of the
connection that triggered the notify, and *extra* is a payload string
that has been sent with the notification. Remember to do a listen query
first, otherwise :meth:`pgobject.getnotify` will always return ``None``.

inserttable -- insert a list into a table
-----------------------------------------

.. method:: pgobject.inserttable(table, values)

    Insert a Python list into a database table

    :param str table: the table name
    :param list values: list of rows values
    :rtype: None
    :raises TypeError: invalid connection, bad argument type, or too many arguments
    :raises MemoryError: insert buffer could not be allocated
    :raises ValueError: unsupported values

This method allows to *quickly* insert large blocks of data in a table:
It inserts the whole values list into the given table. Internally, it
uses the COPY command of the PostgreSQL database. The list is a list
of tuples/lists that define the values for each inserted row. The rows
values may contain string, integer, long or double (real) values.

.. note::

   **Be very careful**:
   This method doesn't type check the fields according to the table definition;
   it just look whether or not it knows how to handle such types.

get/set_notice_receiver -- custom notice receiver
-------------------------------------------------

.. method:: pgobject.get_notice_receiver()

    Get the current notice receiver

    :returns: the current notice receiver callable
    :rtype: callable, None
    :raises TypeError: too many (any) arguments

This method gets the custom notice receiver callback function that has
been set with :meth:`pgobject.set_notice_receiver`, or ``None`` if no
custom notice receiver has ever been set on the connection.

.. method:: pgobject.set_notice_receiver(proc)

    Set a custom notice receiver

    :param proc: the custom notice receiver callback function
    :rtype: None
    :raises TypeError: the specified notice receiver is not callable

This method allows setting a custom notice receiver callback function.
When a notice or warning message is received from the server,
or generated internally by libpq, and the message level is below
the one set with ``client_min_messages``, the specified notice receiver
function will be called. This function must take one parameter,
the :class:`pgnotice` object, which provides the following read-only
attributes:

    .. attribute:: pgnotice.pgcnx

        the connection

    .. attribute:: pgnotice.message

        the full message with a trailing newline

    .. attribute:: pgnotice.severity

        the level of the message, e.g. 'NOTICE' or 'WARNING'

    .. attribute:: pgnotice.primary

        the primary human-readable error message

    .. attribute:: pgnotice.detail

        an optional secondary error message

    .. attribute:: pgnotice.hint

        an optional suggestion what to do about the problem

putline -- write a line to the server socket [DA]
-------------------------------------------------

.. method:: pgobject.putline(line)

    Write a line to the server socket

    :param str line: line to be written
    :rtype: None
    :raises TypeError: invalid connection, bad parameter type, or too many parameters

This method allows to directly write a string to the server socket.

getline -- get a line from server socket [DA]
---------------------------------------------

.. method:: pgobject.getline()

    Get a line from server socket

    :returns:  the line read
    :rtype: str
    :raises TypeError: invalid connection
    :raises TypeError: too many parameters
    :raises MemoryError: buffer overflow

This method allows to directly read a string from the server socket.

endcopy -- synchronize client and server [DA]
---------------------------------------------

.. method:: pgobject.endcopy()

    Synchronize client and server

    :rtype: None
    :raises TypeError: invalid connection
    :raises TypeError: too many parameters

The use of direct access methods may desynchronize client and server.
This method ensure that client and server will be synchronized.

locreate -- create a large object in the database [LO]
------------------------------------------------------

.. method:: pgobject.locreate(mode)

    Create a large object in the database

    :param int mode: large object create mode
    :returns: object handling the PostGreSQL large object
    :rtype: :class:`pglarge`
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises pg.OperationalError: creation error

This method creates a large object in the database. The mode can be defined
by OR-ing the constants defined in the :mod:`pg` module (:const:`INV_READ`,
:const:`INV_WRITE` and :const:`INV_ARCHIVE`). Please refer to PostgreSQL
user manual for a description of the mode values.

getlo -- build a large object from given oid [LO]
-------------------------------------------------

.. method:: pgobject.getlo(oid)

    Create a large object in the database

    :param int oid: OID of the existing large object
    :returns: object handling the PostGreSQL large object
    :rtype: :class:`pglarge`
    :raises TypeError:  invalid connection, bad parameter type, or too many parameters
    :raises ValueError: bad OID value (0 is invalid_oid)

This method allows to reuse a formerly created large object through the
:class:`pglarge` interface, providing the user have its OID.

loimport -- import a file to a large object [LO]
------------------------------------------------

.. method:: pgobject.loimport(name)

    Import a file to a large object

    :param str name: the name of the file to be imported
    :returns: object handling the PostGreSQL large object
    :rtype: :class:`pglarge`
    :raises TypeError: invalid connection, bad argument type, or too many arguments
    :raises pg.OperationalError: error during file import

This methods allows to create large objects in a very simple way. You just
give the name of a file containing the data to be used.

Object attributes
-----------------
Every :class:`pgobject` defines a set of read-only attributes that describe
the connection and its status. These attributes are:

.. attribute:: pgobject.host

   the host name of the server (str)

.. attribute:: pgobject.port

   the port of the server (int)

.. attribute:: pgobject.db

   the selected database (str)

.. attribute:: pgobject.options

   the connection options (str)

.. attribute:: pgobject.tty

   the connection debug terminal (str)

.. attribute:: pgobject.user

    user name on the database system (str)

.. attribute:: pgobject.protocol_version

   the frontend/backend protocol being used (int)

.. attribute:: pgobject.server_version

   the backend version (int, e.g. 80305 for 8.3.5)

.. attribute:: pgobject.status

   the status of the connection (int: 1 = OK, 0 = bad)

.. attribute:: pgobject.error

   the last warning/error message from the server (str)