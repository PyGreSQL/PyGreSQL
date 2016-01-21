Module functions and constants
==============================

.. py:currentmodule:: pg

The :mod:`pg` module defines a few functions that allow to connect
to a database and to define "default variables" that override
the environment variables used by PostgreSQL.

These "default variables" were designed to allow you to handle general
connection parameters without heavy code in your programs. You can prompt the
user for a value, put it in the default variable, and forget it, without
having to modify your environment. The support for default variables can be
disabled by setting the ``-DNO_DEF_VAR`` option in the Python setup file.
Methods relative to this are specified by the tag [DV].

All variables are set to ``None`` at module initialization, specifying that
standard environment variables should be used.

connect -- Open a PostgreSQL connection
---------------------------------------

.. function:: connect([dbname], [host], [port], [opt], [tty], [user], [passwd])

    Open a :mod:`pg` connection

    :param dbname: name of connected database (*None* = :data:`defbase`)
    :type str: str or None
    :param host: name of the server host (*None* = :data:`defhost`)
    :type host:  str or None
    :param port: port used by the database server (-1 = :data:`defport`)
    :type port: int
    :param opt: connection options (*None* = :data:`defopt`)
    :type opt: str or None
    :param tty: debug terminal (*None* = :data:`deftty`)
    :type tty: str or None
    :param user: PostgreSQL user (*None* = :data:`defuser`)
    :type user: str or None
    :param passwd: password for user (*None* = :data:`defpasswd`)
    :type passwd: str or None
    :returns: If successful, the :class:`pgobject` handling the connection
    :rtype: :class:`pgobject`
    :raises TypeError: bad argument type, or too many arguments
    :raises SyntaxError: duplicate argument definition
    :raises pg.InternalError: some error occurred during pg connection definition
    :raises Exception: (all exceptions relative to object allocation)

This function opens a connection to a specified database on a given
PostgreSQL server. You can use keywords here, as described in the
Python tutorial. The names of the keywords are the name of the
parameters given in the syntax line. For a precise description
of the parameters, please refer to the PostgreSQL user manual.

Example::

    import pg

    con1 = pg.connect('testdb', 'myhost', 5432, None, None, 'bob', None)
    con2 = pg.connect(dbname='testdb', host='localhost', user='bob')

get/set_defhost -- default server host [DV]
-------------------------------------------

.. function:: get_defhost(host)

    Get the default host

    :returns: the current default host specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default host specification,
or ``None`` if the environment variables should be used.
Environment variables won't be looked up.

.. function:: set_defhost(host)

    Set the default host

    :param host: the new default host specification
    :type host: str or None
    :returns: the previous default host specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This methods sets the default host value for new connections.
If ``None`` is supplied as parameter, environment variables will
be used in future connections. It returns the previous setting
for default host.

get/set_defport -- default server port [DV]
-------------------------------------------

.. function:: get_defport()

    Get the default port

    :returns: the current default port specification
    :rtype: int
    :raises TypeError: too many arguments

This method returns the current default port specification,
or ``None`` if the environment variables should be used.
Environment variables won't be looked up.

.. function::  set_defport(port)

    Set the default port

    :param port: the new default port
    :type port: int
    :returns: previous default port specification
    :rtype: int or None

This methods sets the default port value for new connections. If -1 is
supplied as parameter, environment variables will be used in future
connections. It returns the previous setting for default port.

get/set_defopt --  default connection options [DV]
--------------------------------------------------

.. function:: get_defopt()

    Get the default connection options

    :returns: the current default options specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default connection options specification,
or ``None`` if the environment variables should be used. Environment variables
won't be looked up.

.. function:: set_defopt(options)

    Set the default connection options

    :param options: the new default connection options
    :type options: str or None
    :returns: previous default options specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This methods sets the default connection options value for new connections.
If ``None`` is supplied as parameter, environment variables will be used in
future connections. It returns the previous setting for default options.

get/set_deftty -- default debug tty [DV]
----------------------------------------

.. function:: get_deftty()

    Get the default debug terminal

    :returns: the current default debug terminal specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default debug terminal specification, or
``None`` if the environment variables should be used. Environment variables
won't be looked up. Note that this is ignored in newer PostgreSQL versions.

.. function:: set_deftty(terminal)

    Set the default debug terminal

    :param terminal: the new default debug terminal
    :type terminal: str or None
    :returns: the previous default debug terminal specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This methods sets the default debug terminal value for new connections.
If ``None`` is supplied as parameter, environment variables will be used
in future connections. It returns the previous setting for default terminal.
Note that this is ignored in newer PostgreSQL versions.

get/set_defbase -- default database name [DV]
---------------------------------------------

.. function:: get_defbase()

    Get the default database name

    :returns: the current default database name specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default database name specification, or
``None`` if the environment variables should be used. Environment variables
won't be looked up.

.. function:: set_defbase(base)

    Set the default database name

    :param base: the new default base name
    :type base: str or None
    :returns: the previous default database name specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This method sets the default database name value for new connections. If
``None`` is supplied as parameter, environment variables will be used in
future connections. It returns the previous setting for default host.

get/set_defuser -- default database user [DV]
---------------------------------------------

.. function:: get_defuser()

    Get the default database user

    :returns: the current default database user specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default database user specification, or
``None`` if the environment variables should be used. Environment variables
won't be looked up.

.. function:: set_defuser(user)

    Set the default database user

    :param user: the new default database user
    :type base: str or None
    :returns: the previous default database user specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This method sets the default database user name for new connections. If
``None`` is supplied as parameter, environment variables will be used in
future connections. It returns the previous setting for default host.

get/set_defpasswd -- default database password [DV]
---------------------------------------------------

.. function:: get_defpasswd()

    Get the default database password

    :returns: the current default database password specification
    :rtype: str or None
    :raises TypeError: too many arguments

This method returns the current default database password specification, or
``None`` if the environment variables should be used. Environment variables
won't be looked up.

.. function:: set_defpasswd(passwd)

    Set the default database password

    :param passwd: the new default database password
    :type base: str or None
    :returns: the previous default database password specification
    :rtype: str or None
    :raises TypeError: bad argument type, or too many arguments

This method sets the default database password for new connections. If
``None`` is supplied as parameter, environment variables will be used in
future connections. It returns the previous setting for default host.

escape_string -- escape a string for use within SQL
---------------------------------------------------

.. function:: escape_string(string)

    Escape a string for use within SQL

    :param str string: the string that is to be escaped
    :returns: the escaped string
    :rtype: str
    :raises TypeError: bad argument type, or too many arguments

This function escapes a string for use within an SQL command.
This is useful when inserting data values as literal constants
in SQL commands. Certain characters (such as quotes and backslashes)
must be escaped to prevent them from being interpreted specially
by the SQL parser. :func:`escape_string` performs this operation.
Note that there is also a :class:`pgobject` method with the same name
which takes connection properties into account.

.. note::

   It is especially important to do proper escaping when
   handling strings that were received from an untrustworthy source.
   Otherwise there is a security risk: you are vulnerable to "SQL injection"
   attacks wherein unwanted SQL commands are fed to your database.

Example::

    name = raw_input("Name? ")
    phone = con.query("select phone from employees where name='%s'"
        % escape_string(name)).getresult()

escape_bytea -- escape binary data for use within SQL
-----------------------------------------------------

.. function:: escape_bytea(datastring)

    escape binary data for use within SQL as type ``bytea``

    :param str datastring: string containing the binary data that is to be escaped
    :returns: the escaped string
    :rtype: str
    :raises TypeError: bad argument type, or too many arguments

Escapes binary data for use within an SQL command with the type ``bytea``.
As with :func:`escape_string`, this is only used when inserting data directly
into an SQL command string.
Note that there is also a :class:`pgobject` method with the same name
which takes connection properties into account.

Example::

    picture = open('garfield.gif', 'rb').read()
    con.query("update pictures set img='%s' where name='Garfield'"
        % escape_bytea(picture))

unescape_bytea -- unescape data that has been retrieved as text
---------------------------------------------------------------

.. function:: unescape_bytea(string)

    Unescape ``bytea`` data that has been retrieved as text

    :param str datastring: the ``bytea`` data string that has been retrieved as text
    :returns: byte string containing the binary data
    :rtype: str
    :raises TypeError: bad argument type, or too many arguments

Converts an escaped string representation of binary data into binary
data -- the reverse of :func:`escape_bytea`. This is needed when retrieving
``bytea`` data with one of the :meth:`pgqueryobject.getresult`,
:meth:`pgqueryobject.dictresult` or :meth:`pgqueryobject.namedresult` methods.

Example::

    picture = unescape_bytea(con.query(
          "select img from pictures where name='Garfield'").getresult[0][0])
    open('garfield.gif', 'wb').write(picture)

get/set_decimal -- decimal type to be used for numeric values
-------------------------------------------------------------

.. function:: get_decimal()

    Get the decimal type to be used for numeric values

    :returns: the Python class used for PostgreSQL numeric values
    :rtype: class

This function returns the Python class that is used by PyGreSQL to hold
PostgreSQL numeric values. The default class is :class:`decimal.Decimal`
if available, otherwise the :class:`float` type is used.

.. function:: set_decimal(cls)

    Set a decimal type to be used for numeric values

    :param class cls: the Python class to be used for PostgreSQL numeric values

This function can be used to specify the Python class that shall
be used by PyGreSQL to hold PostgreSQL numeric values.
The default class is :class:`decimal.Decimal` if available,
otherwise the :class:`float` type is used.

get/set_decimal_point -- decimal mark used for monetary values
--------------------------------------------------------------

.. function:: get_decimal_point()

    Get the decimal mark used for monetary values

    :returns: string with one character representing the decimal mark
    :rtype: str

This function returns the decimal mark used by PyGreSQL to interpret
PostgreSQL monetary values when converting them to decimal numbers.
The default setting is ``'.'`` as a decimal point. This setting is not
adapted automatically to the locale used by PostGreSQL, but you can
use ``set_decimal()`` to set a different decimal mark manually. A return
value of ``None`` means monetary values are not interpreted as decimal
numbers, but returned as strings including the formatting and currency.

.. versionadded:: 4.1.1

.. function:: set_decimal_point(string)

    Specify which decimal mark is used for interpreting monetary values

    :param str string: string with one character representing the decimal mark

This function can be used to specify the decimal mark used by PyGreSQL
to interpret PostgreSQL monetary values. The default value is '.' as
a decimal point. This value is not adapted automatically to the locale
used by PostGreSQL, so if you are dealing with a database set to a
locale that uses a ``','`` instead of ``'.'`` as the decimal point,
then you need to call ``set_decimal(',')`` to have PyGreSQL interpret
monetary values correctly. If you don't want money values to be converted
to decimal numbers, then you can call ``set_decimal(None)``, which will
cause PyGreSQL to return monetary values as strings including their
formatting and currency.

.. versionadded:: 4.1.1

get/set_bool -- whether boolean values are returned as bool objects
-------------------------------------------------------------------

.. function:: get_bool()

    Check whether boolean values are returned as bool objects

    :returns: whether or not bool objects will be returned
    :rtype: bool

This function checks whether PyGreSQL returns PostgreSQL boolean
values converted to Python bool objects, or as ``'f'`` and ``'t'``
strings which are the values used internally by PostgreSQL. By default,
conversion to bool objects is not activated, but you can enable
this with the ``set_bool()`` method.

.. versionadded:: 4.2

.. function:: set_bool(on)

    Set whether boolean values are returned as bool objects

    :param on: whether or not bool objects shall be returned

This function can be used to specify whether PyGreSQL shall return
PostgreSQL boolean values converted to Python bool objects, or as
``'f'`` and ``'t'`` strings which are the values used internally by PostgreSQL.
By default, conversion to bool objects is not activated, but you can
enable this by calling ``set_bool(True)``.

.. versionadded:: 4.2

get/set_namedresult -- conversion to named tuples
-------------------------------------------------

.. function:: get_namedresult()

    Get the function that converts to named tuples

This returns the function used by PyGreSQL to construct the result of the
:meth:`pgqueryobject.namedresult` method.

.. versionadded:: 4.1

.. function:: set_namedresult(func)

    Set a function that will convert to named tuples

    :param func: the function to be used to convert results to named tuples

You can use this if you want to create different kinds of named tuples returned
by the :meth:`pgqueryobject.namedresult` method.  If you set this function to
*None*, then it will become equal to :meth:`pgqueryobject.getresult`.

.. versionadded:: 4.1


Module constants
----------------
Some constants are defined in the module dictionary.
They are intended to be used as parameters for methods calls.
You should refer to the libpq description in the PostgreSQL user manual
for more information about them. These constants are:

.. data:: version, __version__

    constants that give the current version

.. data:: INV_READ, INV_WRITE

    large objects access modes,
    used by :meth:`pgobject.locreate` and :meth:`pglarge.open`

.. data:: SEEK_SET, SEEK_CUR, SEEK_END:

    positional flags, used by :meth:`pglarge.seek`
