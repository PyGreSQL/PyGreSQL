The DB wrapper class
====================

.. py:currentmodule:: pg

.. class:: DB

The :class:`Connection` methods are wrapped in the class :class:`DB`.
The preferred way to use this module is as follows::

    import pg

    db = pg.DB(...)  # see below

    for r in db.query(  # just for example
        """SELECT foo,bar
         FROM foo_bar_table
         WHERE foo !~ bar"""
        ).dictresult():

        print '%(foo)s %(bar)s' % r

This class can be subclassed as in this example::

    import pg

    class DB_ride(pg.DB):
        """Ride database wrapper

        This class encapsulates the database functions and the specific
        methods for the ride database."""

    def __init__(self):
        """Open a database connection to the rides database"""
        pg.DB.__init__(self, dbname='ride')
        self.query("SET DATESTYLE TO 'ISO'")

    [Add or override methods here]

The following describes the methods and variables of this class.

Initialization
--------------
The :class:`DB` class is initialized with the same arguments as the
:func:`connect` function described above. It also initializes a few
internal variables. The statement ``db = DB()`` will open the local
database with the name of the user just like ``connect()`` does.

You can also initialize the DB class with an existing :mod:`pg` or :mod:`pgdb`
connection. Pass this connection as a single unnamed parameter, or as a
single parameter named ``db``. This allows you to use all of the methods
of the DB class with a DB-API 2 compliant connection. Note that the
:meth:`Connection.close` and :meth:`Connection.reopen` methods are inoperative
in this case.

pkey -- return the primary key of a table
-----------------------------------------

.. method:: DB.pkey(table)

    Return the primary key of a table

    :param str table: name of table
    :returns: Name of the field which is the primary key of the table
    :rtype: str
    :raises KeyError: the table does not have a primary key

This method returns the primary key of a table. For composite primary
keys, the return value will be a frozenset. Note that this raises a
KeyError if the table does not have a primary key.

get_databases -- get list of databases in the system
----------------------------------------------------

.. method:: DB.get_databases()

    Get the list of databases in the system

    :returns: all databases in the system
    :rtype: list

Although you can do this with a simple select, it is added here for
convenience.

get_relations -- get list of relations in connected database
------------------------------------------------------------

.. method:: DB.get_relations(kinds)

    Get the list of relations in connected database

    :param str kinds: a string or sequence of type letters
    :returns: all relations of the given kinds in the database
    :rtype: list

The type letters are ``r`` = ordinary table, ``i`` = index, ``S`` = sequence,
``v`` = view, ``c`` = composite type, ``s`` = special, ``t`` = TOAST table.
If `kinds` is None or an empty string, all relations are returned (this is
also the default). Although you can do this with a simple select, it is
added here for convenience.

get_tables -- get list of tables in connected database
------------------------------------------------------

.. method:: DB.get_tables()

    Get the list of tables in connected database

    :returns: all tables in connected database
    :rtype: list

This is a shortcut for ``get_relations('r')`` that has been added for
convenience.

get_attnames -- get the attribute names of a table
--------------------------------------------------

.. method:: DB.get_attnames(table)

    Get the attribute names of a table

    :param str table: name of table
    :returns: A dictionary -- the keys are the attribute names,
     the values are the type names of the attributes.

Given the name of a table, digs out the set of attribute names.

has_table_privilege -- check whether current user has specified table privilege
-------------------------------------------------------------------------------

.. method:: DB.has_table_privilege(table, privilege)

    Check whether current user has specified table privilege

    :param str table: the name of the table
    :param str privilege: privilege to be checked -- default is 'select'
    :returns: whether current user has specified table privilege
    :rtype: bool

Returns True if the current user has the specified privilege for the table.

.. versionadded:: 4.0

get -- get a row from a database table or view
----------------------------------------------

.. method:: DB.get(table, arg, [keyname])

    Get a row from a database table or view

    :param str table:  name of table or view
    :param arg:  either a dictionary or the value to be looked up
    :param str keyname: name of field to use as key (optional)
    :returns: A dictionary - the keys are the attribute names,
      the values are the row values.

This method is the basic mechanism to get a single row. It assumes
that the key specifies a unique row. If *keyname* is not specified,
then the primary key for the table is used. If *arg* is a dictionary
then the value for the key is taken from it and it is modified to
include the new values, replacing existing values where necessary.
For a composite key, *keyname* can also be a sequence of key names.
The OID is also put into the dictionary if the table has one, but in
order to allow the caller to work with multiple tables, it is munged
as ``oid(table)``.

insert -- insert a row into a database table
--------------------------------------------

.. method:: DB.insert(table, [d,] [key = val, ...])

    Insert a row into a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :returns: the inserted values
    :rtype: dict

This method inserts a row into a table.  If the optional dictionary is
not supplied then the required values must be included as keyword/value
pairs.  If a dictionary is supplied then any keywords provided will be
added to or replace the entry in the dictionary.

The dictionary is then reloaded with the values actually inserted in order
to pick up values modified by rules, triggers, etc.

Note: The method currently doesn't support insert into views
although PostgreSQL does.

update -- update a row in a database table
------------------------------------------

.. method:: DB.update(table, [d,] [key = val, ...])

    Update a row in a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :returns: the new row
    :rtype: dict

Similar to insert but updates an existing row.  The update is based on the
OID value as munged by get or passed as keyword, or on the primary key of
the table.  The dictionary is modified to reflect any changes caused by the
update due to triggers, rules, default values, etc.

Like insert, the dictionary is optional and updates will be performed
on the fields in the keywords.  There must be an OID or primary key
either in the dictionary where the OID must be munged, or in the keywords
where it can be simply the string 'oid'.

query -- execute a SQL command string
-------------------------------------

.. method:: DB.query(command, [arg1, [arg2, ...]])

    Execute a SQL command string

    :param str command: SQL command
    :param arg*: optional positional arguments
    :returns: result values
    :rtype: :class:`Query`, None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: empty SQL query or lost connection
    :raises pg.ProgrammingError: error in query
    :raises pg.InternalError: error during query processing

Similar to the :class:`Connection` function with the same name, except that
positional arguments can be passed either as a single list or tuple, or as
individual positional arguments.

Example::

    name = input("Name? ")
    phone = input("Phone? ")
    rows = db.query("update employees set phone=$2 where name=$1",
        (name, phone)).getresult()[0][0]
    # or
    rows = db.query("update employees set phone=$2 where name=$1",
         name, phone).getresult()[0][0]

clear -- clear row values in memory
-----------------------------------

.. method:: DB.clear(table, [a])

    Clear row values in memory

    :param str table: name of table
    :param dict a: optional dictionary of values
    :returns: an empty row
    :rtype: dict

This method clears all the attributes to values determined by the types.
Numeric types are set to 0, Booleans are set to ``'f'``, dates are set
to ``'now()'`` and everything else is set to the empty string.
If the array argument is present, it is used as the array and any entries
matching attribute names are cleared with everything else left unchanged.

If the dictionary is not supplied a new one is created.

delete -- delete a row from a database table
--------------------------------------------

.. method:: DB.delete(table, [d,] [key = val, ...])

    Delete a row from a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :rtype: None

This method deletes the row from a table.  It deletes based on the OID value
as munged by get or passed as keyword, or on the primary key of the table.
The return value is the number of deleted rows (i.e. 0 if the row did not
exist and 1 if the row was deleted).

escape_literal -- escape a literal string for use within SQL
------------------------------------------------------------

.. method:: DB.escape_literal(string)

    Escape a string for use within SQL as a literal constant

    :param str string: the string that is to be escaped
    :returns: the escaped string
    :rtype: str

This method escapes a string for use within an SQL command. This is useful
when inserting data values as literal constants in SQL commands. Certain
characters (such as quotes and backslashes) must be escaped to prevent them
from being interpreted specially by the SQL parser.

.. versionadded:: 4.1

escape_identifier -- escape an identifier string for use within SQL
-------------------------------------------------------------------

.. method:: DB.escape_identifier(string)

    Escape a string for use within SQL as an identifier

    :param str string: the string that is to be escaped
    :returns: the escaped string
    :rtype: str

This method escapes a string for use as an SQL identifier, such as a table,
column, or function name. This is useful when a user-supplied identifier
might contain special characters that would otherwise not be interpreted
as part of the identifier by the SQL parser, or when the identifier might
contain upper case characters whose case should be preserved.

.. versionadded:: 4.1

escape_string -- escape a string for use within SQL
---------------------------------------------------

.. method:: DB.escape_string(string)

    Escape a string for use within SQL

    :param str string: the string that is to be escaped
    :returns: the escaped string
    :rtype: str

Similar to the module function with the same name, but the
behavior of this method is adjusted depending on the connection properties
(such as character encoding).

escape_bytea -- escape binary data for use within SQL
-----------------------------------------------------

.. method:: DB.escape_bytea(datastring)

    Escape binary data for use within SQL as type ``bytea``

    :param str datastring: string containing the binary data that is to be escaped
    :returns: the escaped string
    :rtype: str

Similar to the module function with the same name, but the
behavior of this method is adjusted depending on the connection properties
(in particular, whether standard-conforming strings are enabled).

unescape_bytea -- unescape data that has been retrieved as text
---------------------------------------------------------------

.. method:: DB.unescape_bytea(string)

    Unescape ``bytea`` data that has been retrieved as text

    :param datastring: the ``bytea`` data string that has been retrieved as text
    :returns: byte string containing the binary data
    :rtype: bytes

See the module function with the same name.

use_regtypes -- determine use of regular type names
---------------------------------------------------

.. method:: DB.use_regtypes([regtypes])

    Determine whether regular type names shall be used

    :param bool regtypes: if passed, set whether regular type names shall be used
    :returns: whether regular type names are used

The :meth:`DB.get_attnames` method can return either simplified "classic"
type names (the default) or more specific "regular" type names. Which kind
of type names is used can be changed by calling :meth:`DB.get_regtypes`.
If you pass a boolean, it sets whether regular type names shall be used.
The method can also be used to check through its return value whether
currently regular type names are used.

.. versionadded:: 4.1
