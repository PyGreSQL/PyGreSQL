The DB wrapper class
====================

.. py:currentmodule:: pg

.. class:: DB

The :class:`pgobject` methods are wrapped in the class :class:`DB`.
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
:meth:`pgobject.close` and :meth:`pgobject.reopen` methods are inoperative
in this case.

pkey -- return the primary key of a table
-----------------------------------------

.. method:: DB.pkey(table)

    Return the primary key of a table

    :param str table: name of table
    :returns: Name of the field which is the primary key of the table
    :rtype: str
    :rtype: str
    :raises KeyError: the table does not have a primary key

This method returns the primary key of a table.  For composite primary
keys, the return value will be a frozenset.  Note that this raises a
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

.. method:: DB.get_relations([kinds], [system])

    Get the list of relations in connected database

    :param str kinds: a string or sequence of type letters
    :param bool system: whether system relations should be returned
    :returns: all relations of the given kinds in the database
    :rtype: list

This method returns the list of relations in the connected database.  Although
you can do this with a simple select, it is added here for convenience.  You
can select which kinds of relations you are interested in by passing type
letters in the `kinds` parameter.  The type letters are ``r`` = ordinary table,
``i`` = index, ``S`` = sequence, ``v`` = view, ``c`` = composite type,
``s`` = special, ``t`` = TOAST table.  If `kinds` is None or an empty string,
all relations are returned (this is also the default).  If `system` is set to
`True`, then system tables and views (temporary tables, toast tables, catalog
vies and tables) will be returned as well, otherwise they will be ignored.

get_tables -- get list of tables in connected database
------------------------------------------------------

.. method:: DB.get_tables([system])

    Get the list of tables in connected database

    :param bool system: whether system tables should be returned
    :returns: all tables in connected database
    :rtype: list

This is a shortcut for ``get_relations('r', system)`` that has been added for
convenience.

get_attnames -- get the attribute names of a table
--------------------------------------------------

.. method:: DB.get_attnames(table)

    Get the attribute names of a table

    :param str table: name of table
    :returns: a dictionary mapping attribute names to type names

Given the name of a table, digs out the set of attribute names.

Returns a dictionary of attribute names (the names are the keys,
the values are the names of the attributes' types).

By default, only a limited number of simple types will be returned.
You can get the regular types after enabling this by calling the
:meth:`DB.use_regtypes` method.

get/set_parameter -- get or set  run-time parameters
----------------------------------------------------

.. method:: DB.get_parameter(parameter)

    Get the value of run-time parameters

    :param parameter: the run-time parameter(s) to get
    :type param: str, tuple, list or dict
    :returns: the current value(s) of the run-time parameter(s)
    :rtype: str, list or dict
    :raises TypeError: Invalid parameter type(s)
    :raises pg.ProgrammingError: Invalid parameter name(s)

If the parameter is a string, the return value will also be a string
that is the current setting of the run-time parameter with that name.

You can get several parameters at once by passing a list, set or dict.
When passing a list of parameter names, the return value will be a
corresponding list of parameter settings.  When passing a set of
parameter names, a new dict will be returned, mapping these parameter
names to their settings.  Finally, if you pass a dict as parameter,
its values will be set to the current parameter settings corresponding
to its keys.

By passing the special name `'all'` as the parameter, you can get a dict
of all existing configuration parameters.

.. versionadded:: 4.2

.. method:: DB.set_parameter(parameter, [value], [local])

    Set the value of run-time parameters

    :param parameter: the run-time parameter(s) to set
    :type param: string, tuple, list or dict
    :param value: the value to set
    :type param: str or None
    :raises TypeError: Invalid parameter type(s)
    :raises ValueError: Invalid value argument(s)
    :raises pg.ProgrammingError: Invalid parameter name(s) or values

If the parameter and the value are strings, the run-time parameter
will be set to that value.  If no value or *None* is passed as a value,
then the run-time parameter will be restored to its default value.

You can set several parameters at once by passing a list of parameter
names, together with a single value that all parameters should be
set to or with a corresponding list of values.  You can also pass
the parameters as a set if you only provide a single value.
Finally, you can pass a dict with parameter names as keys.  In this
case, you should not pass a value, since the values for the parameters
will be taken from the dict.

By passing the special name `'all'` as the parameter, you can reset
all existing settable run-time parameters to their default values.

If you set *local* to `True`, then the command takes effect for only the
current transaction.  After :meth:`DB.commit` or :meth:`DB.rollback`,
the session-level setting takes effect again.  Setting *local* to `True`
will appear to have no effect if it is executed outside a transaction,
since the transaction will end immediately.

.. versionadded:: 4.2

has_table_privilege -- check table privilege
--------------------------------------------

.. method:: DB.has_table_privilege(table, privilege)

    Check whether current user has specified table privilege

    :param str table: the name of the table
    :param str privilege: privilege to be checked -- default is 'select'
    :returns: whether current user has specified table privilege
    :rtype: bool

Returns True if the current user has the specified privilege for the table.

.. versionadded:: 4.0

begin/commit/rollback/savepoint/release -- transaction handling
---------------------------------------------------------------

.. method:: DB.begin([mode])

    Begin a transaction

    :param str mode: an optional transaction mode such as 'READ ONLY'

    This initiates a transaction block, that is, all following queries
    will be executed in a single transaction until :meth:`DB.commit`
    or :meth:`DB.rollback` is called.

.. versionadded:: 4.1

.. method:: DB.start()

    This is the same as the :meth:`DB.begin` method.

.. method:: DB.commit()

    Commit a transaction

    This commits the current transaction. All changes made by the
    transaction become visible to others and are guaranteed to be
    durable if a crash occurs.

.. method:: DB.end()

    This is the same as the :meth:`DB.commit` method.

.. versionadded:: 4.1

.. method:: DB.rollback([name])

    Roll back a transaction

    :param str name: optionally, roll back to the specified savepoint

    This rolls back the current transaction and causes all the updates
    made by the transaction to be discarded.

.. versionadded:: 4.1

.. method:: DB.abort()

    This is the same as the :meth:`DB.rollback` method.

.. versionadded:: 4.2

.. method:: DB.savepoint(name)

    Define a new savepoint

    :param str name: the name to give to the new savepoint

    This establishes a new savepoint within the current transaction.

.. versionadded:: 4.1

.. method:: DB.release(name)

    Destroy a savepoint

    :param str name: the name of the savepoint to destroy

    This destroys a savepoint previously defined in the current transaction.

.. versionadded:: 4.1

get -- get a row from a database table or view
----------------------------------------------

.. method:: DB.get(table, arg, [keyname])

    Get a row from a database table or view

    :param str table:  name of table or view
    :param arg:  either a dictionary or the value to be looked up
    :param str keyname: name of field to use as key (optional)
    :returns: A dictionary - the keys are the attribute names,
      the values are the row values.
    :raises pg.ProgrammingError: no primary key or missing privilege

This method is the basic mechanism to get a single row.  It assumes
that the key specifies a unique row.  If *keyname* is not specified,
then the primary key for the table is used.  If *arg* is a dictionary
then the value for the key is taken from it and it is modified to
include the new values, replacing existing values where necessary.
For a composite key, *keyname* can also be a sequence of key names.
The OID is also put into the dictionary if the table has one, but in
order to allow the caller to work with multiple tables, it is munged
as ``oid(schema.table)``.

insert -- insert a row into a database table
--------------------------------------------

.. method:: DB.insert(table, [d], [key=val, ...])

    Insert a row into a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :returns: the inserted values in the database
    :rtype: dict
    :raises pg.ProgrammingError: missing privilege or conflict

This method inserts a row into a table.  If the optional dictionary is
not supplied then the required values must be included as keyword/value
pairs.  If a dictionary is supplied then any keywords provided will be
added to or replace the entry in the dictionary.

The dictionary is then, if possible, reloaded with the values actually
inserted in order to pick up values modified by rules, triggers, etc.

update -- update a row in a database table
------------------------------------------

.. method:: DB.update(table, [d], [key=val, ...])

    Update a row in a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :returns: the new row in the database
    :rtype: dict
    :raises pg.ProgrammingError: no primary key or missing privilege

Similar to insert but updates an existing row.  The update is based on the
OID value as munged by :meth:`DB.get` or passed as keyword, or on the primary
key of the table.  The dictionary is modified, if possible, to reflect any
changes caused by the update due to triggers, rules, default values, etc.

Like insert, the dictionary is optional and updates will be performed
on the fields in the keywords.  There must be an OID or primary key
either in the dictionary where the OID must be munged, or in the keywords
where it can be simply the string ``'oid'``.

query -- execute a SQL command string
-------------------------------------

.. method:: DB.query(command, [arg1, [arg2, ...]])

    Execute a SQL command string

    :param str command: SQL command
    :param arg*: optional positional arguments
    :returns: result values
    :rtype: :class:`pgqueryobject`, None
    :raises TypeError: bad argument type, or too many arguments
    :raises TypeError: invalid connection
    :raises ValueError: empty SQL query or lost connection
    :raises pg.ProgrammingError: error in query
    :raises pg.InternalError: error during query processing

Similar to the :class:`pgobject` function with the same name, except that
positional arguments can be passed either as a single list or tuple, or as
individual positional arguments.

Example::

    name = raw_input("Name? ")
    phone = raw_input("Phone? ")
    rows = db.query("update employees set phone=$2 where name=$1",
        (name, phone)).getresult()[0][0]
    # or
    rows = db.query("update employees set phone=$2 where name=$1",
         name, phone).getresult()[0][0]

clear -- clear row values in memory
-----------------------------------

.. method:: DB.clear(table, [d])

    Clear row values in memory

    :param str table: name of table
    :param dict d: optional dictionary of values
    :returns: an empty row
    :rtype: dict

This method clears all the attributes to values determined by the types.
Numeric types are set to 0, Booleans are set to ``'f'``, and everything
else is set to the empty string.  If the optional dictionary is present,
it is used as the row and any entries matching attribute names are cleared
with everything else left unchanged.

If the dictionary is not supplied a new one is created.

delete -- delete a row from a database table
--------------------------------------------

.. method:: DB.delete(table, [d], [key=val, ...])

    Delete a row from a database table

    :param str table: name of table
    :param dict d: optional dictionary of values
    :rtype: None
    :raises pg.ProgrammingError: table has no primary key,
        row is still referenced or missing privilege

This method deletes the row from a table.  It deletes based on the OID value
as munged by :meth:`DB.get` or passed as keyword, or on the primary key of
the table.  The return value is the number of deleted rows (i.e. 0 if the
row did not exist and 1 if the row was deleted).

truncate -- quickly empty database tables
-----------------------------------------

.. method:: DB.truncate(table, [restart], [cascade], [only])

    Empty a table or set of tables

    :param table: the name of the table(s)
    :type table: str, list or set
    :param bool restart: whether table sequences should be restarted
    :param bool cascade: whether referenced tables should also be truncated
    :param only: whether only parent tables should be truncated
    :type only: bool or list

This method quickly removes all rows from the given table or set
of tables.  It has the same effect as an unqualified DELETE on each
table, but since it does not actually scan the tables it is faster.
Furthermore, it reclaims disk space immediately, rather than requiring
a subsequent VACUUM operation. This is most useful on large tables.

If *restart* is set to `True`, sequences owned by columns of the truncated
table(s) are automatically restarted.  If *cascade* is set to `True`, it
also truncates all tables that have foreign-key references to any of
the named tables.  If the parameter *only* is not set to `True`, all the
descendant tables (if any) will also be truncated. Optionally, a ``*``
can be specified after the table name to explicitly indicate that
descendant tables are included.  If the parameter *table* is a list,
the parameter *only* can also be a list of corresponding boolean values.

.. versionadded:: 4.2

escape_literal/identifier/string/bytea -- escape for SQL
--------------------------------------------------------

The following methods escape text or binary strings so that they can be
inserted directly into an SQL command.  Except for :meth:`DB.escape_byte`,
you don't need to call these methods for the strings passed as parameters
to :meth:`DB.query`.  You also don't need to call any of these methods
when storing data using :meth:`DB.insert` and similar.

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

.. method:: DB.escape_bytea(datastring)

    Escape binary data for use within SQL as type ``bytea``

    :param str datastring: string containing the binary data that is to be escaped
    :returns: the escaped string
    :rtype: str

Similar to the module function :func:`pg.escape_string` with the same name,
but the behavior of this method is adjusted depending on the connection
properties (such as character encoding).

unescape_bytea -- unescape data retrieved from the database
-----------------------------------------------------------

.. method:: DB.unescape_bytea(string)

    Unescape ``bytea`` data that has been retrieved as text

    :param datastring: the ``bytea`` data string that has been retrieved as text
    :returns: byte string containing the binary data
    :rtype: str

See the module function :func:`pg.unescape_bytea` with the same name.

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

notification_handler -- create a notification handler
-----------------------------------------------------

.. class:: DB.notification_handler(event, callback, [arg_dict], [timeout], [stop_event])

    Create a notification handler instance

    :param str event: the name of an event to listen for
    :param callback: a callback function
    :param dict arg_dict: an optional dictionary for passing arguments
    :param timeout: the time-out when waiting for notifications
    :type timeout: int, float or None
    :param str stop_event: an optional different name to be used as stop event

This method creates a :class:`pg.NotificationHandler` object using the
:class:`DB` connection as explained under :doc:`notification`.

.. versionadded:: 4.1.1
