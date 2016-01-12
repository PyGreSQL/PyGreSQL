#! /usr/bin/python
#
# pg.py
#
# $Id$
#

"""PyGreSQL classic interface.

This pg module implements some basic database management stuff.
It includes the _pg module and builds on it, providing the higher
level wrapper class named DB with additional functionality.
This is known as the "classic" ("old style") PyGreSQL interface.
For a DB-API 2 compliant interface use the newer pgdb module.

"""

# Copyright (c) 1997-2013 by D'Arcy J.M. Cain.
#
# Contributions made by Ch. Zwerschke and others.
#
# The notification handler is based on pgnotify which is
# Copyright (c) 2001 Ng Pheng Siong. All rights reserved.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies and that
# both that copyright notice and this permission notice appear in
# supporting documentation.

from _pg import *

import select
import warnings
try:
    frozenset
except NameError:  # Python < 2.4, unsupported
    from sets import ImmutableSet as frozenset
try:
    from decimal import Decimal
    set_decimal(Decimal)
except ImportError:  # Python < 2.4, unsupported
    Decimal = float
try:
    from collections import namedtuple
except ImportError:  # Python < 2.6
    namedtuple = None


# Auxiliary functions that are independent from a DB connection:

def _is_quoted(s):
    """Check whether this string is a quoted identifier."""
    s = s.replace('_', 'a')
    return not s.isalnum() or s[:1].isdigit() or s != s.lower()


def _is_unquoted(s):
    """Check whether this string is an unquoted identifier."""
    s = s.replace('_', 'a')
    return s.isalnum() and not s[:1].isdigit()


def _split_first_part(s):
    """Split the first part of a dot separated string."""
    s = s.lstrip()
    if s[:1] == '"':
        p = []
        s = s.split('"', 3)[1:]
        p.append(s[0])
        while len(s) == 3 and s[1] == '':
            p.append('"')
            s = s[2].split('"', 2)
            p.append(s[0])
        p = [''.join(p)]
        s = '"'.join(s[1:]).lstrip()
        if s:
            if s[:0] == '.':
                p.append(s[1:])
            else:
                s = _split_first_part(s)
                p[0] += s[0]
                if len(s) > 1:
                    p.append(s[1])
    else:
        p = s.split('.', 1)
        s = p[0].rstrip()
        if _is_unquoted(s):
            s = s.lower()
        p[0] = s
    return p


def _split_parts(s):
    """Split all parts of a dot separated string."""
    q = []
    while s:
        s = _split_first_part(s)
        q.append(s[0])
        if len(s) < 2:
            break
        s = s[1]
    return q


def _join_parts(s):
    """Join all parts of a dot separated string."""
    return '.'.join([_is_quoted(p) and '"%s"' % p or p for p in s])


def _oid_key(qcl):
    """Build oid key from qualified class name."""
    return 'oid(%s)' % qcl


if namedtuple:

    def _namedresult(q):
        """Get query result as named tuples."""
        row = namedtuple('Row', q.listfields())
        return [row(*r) for r in q.getresult()]

    set_namedresult(_namedresult)


def _db_error(msg, cls=DatabaseError):
    """Returns DatabaseError with empty sqlstate attribute."""
    error = cls(msg)
    error.sqlstate = None
    return error


def _int_error(msg):
    """Returns InternalError."""
    return _db_error(msg, InternalError)


def _prg_error(msg):
    """Returns ProgrammingError."""
    return _db_error(msg, ProgrammingError)


class NotificationHandler(object):
    """A PostgreSQL client-side asynchronous notification handler."""

    def __init__(self, db, event, callback, arg_dict=None, timeout=None):
        """Initialize the notification handler.

        db       - PostgreSQL connection object.
        event    - Event (notification channel) to LISTEN for.
        callback - Event callback function.
        arg_dict - A dictionary passed as the argument to the callback.
        timeout  - Timeout in seconds; a floating point number denotes
                   fractions of seconds. If it is absent or None, the
                   callers will never time out.

        """
        if isinstance(db, DB):
            db = db.db
        self.db = db
        self.event = event
        self.stop_event = 'stop_%s' % event
        self.listening = False
        self.callback = callback
        if arg_dict is None:
            arg_dict = {}
        self.arg_dict = arg_dict
        self.timeout = timeout

    def __del__(self):
        self.close()

    def close(self):
        """Stop listening and close the connection."""
        if self.db:
            self.unlisten()
            self.db.close()
            self.db = None

    def listen(self):
        """Start listening for the event and the stop event."""
        if not self.listening:
            self.db.query('listen "%s"' % self.event)
            self.db.query('listen "%s"' % self.stop_event)
            self.listening = True

    def unlisten(self):
        """Stop listening for the event and the stop event."""
        if self.listening:
            self.db.query('unlisten "%s"' % self.event)
            self.db.query('unlisten "%s"' % self.stop_event)
            self.listening = False

    def notify(self, db=None, stop=False, payload=None):
        """Generate a notification.

        Note: If the main loop is running in another thread, you must pass
        a different database connection to avoid a collision.

        The payload parameter is only supported in PostgreSQL >= 9.0.

        """
        if not db:
            db = self.db
        if self.listening:
            q = 'notify "%s"' % (stop and self.stop_event or self.event)
            if payload:
                q += ", '%s'" % payload
            return db.query(q)

    def __call__(self, close=False):
        """Invoke the notification handler.

        The handler is a loop that actually LISTENs for two NOTIFY messages:

        <event> and stop_<event>.

        When either of these NOTIFY messages are received, its associated
        'pid' and 'event' are inserted into <arg_dict>, and the callback is
        invoked with <arg_dict>. If the NOTIFY message is stop_<event>, the
        handler UNLISTENs both <event> and stop_<event> and exits.

        Note: If you run this loop in another thread, don't use the same
        database connection for database operations in the main thread.

        """
        self.listen()
        _ilist = [self.db.fileno()]

        while self.listening:
            ilist, _olist, _elist = select.select(_ilist, [], [], self.timeout)
            if ilist:
                while self.listening:
                    notice = self.db.getnotify()
                    if not notice:  # no more messages
                        break
                    event, pid, extra = notice
                    if event not in (self.event, self.stop_event):
                        self.unlisten()
                        raise _db_error(
                            'listening for "%s" and "%s", but notified of "%s"'
                            % (self.event, self.stop_event, event))
                    if event == self.stop_event:
                        self.unlisten()
                    self.arg_dict['pid'] = pid
                    self.arg_dict['event'] = event
                    self.arg_dict['extra'] = extra
                    self.callback(self.arg_dict)
            else:   # we timed out
                self.unlisten()
                self.callback(None)


def pgnotify(*args, **kw):
    """Same as NotificationHandler, under the traditional name."""
    warnings.warn("pgnotify is deprecated, use NotificationHandler instead.",
        DeprecationWarning, stacklevel=2)
    return NotificationHandler(*args, **kw)


# The actual PostGreSQL database connection interface:

class DB(object):
    """Wrapper class for the _pg connection type."""

    def __init__(self, *args, **kw):
        """Create a new connection.

        You can pass either the connection parameters or an existing
        _pg or pgdb connection. This allows you to use the methods
        of the classic pg interface with a DB-API 2 pgdb connection.

        """
        if not args and len(kw) == 1:
            db = kw.get('db')
        elif not kw and len(args) == 1:
            db = args[0]
        else:
            db = None
        if db:
            if isinstance(db, DB):
                db = db.db
            else:
                try:
                    db = db._cnx
                except AttributeError:
                    pass
        if not db or not hasattr(db, 'db') or not hasattr(db, 'query'):
            db = connect(*args, **kw)
            self._closeable = True
        else:
            self._closeable = False
        self.db = db
        self.dbname = db.db
        self._regtypes = False
        self._attnames = {}
        self._pkeys = {}
        self._privileges = {}
        self._args = args, kw
        self.debug = None  # For debugging scripts, this can be set
            # * to a string format specification (e.g. in CGI set to "%s<BR>"),
            # * to a file object to write debug statements or
            # * to a callable object which takes a string argument
            # * to any other true value to just print debug statements

    def __getattr__(self, name):
        # All undefined members are same as in underlying connection:
        if self.db:
            return getattr(self.db, name)
        else:
            raise _int_error('Connection is not valid')

    # Context manager methods

    def __enter__(self):
        """Enter the runtime context. This will start a transaction."""
        self.begin()
        return self

    def __exit__(self, et, ev, tb):
        """Exit the runtime context. This will end the transaction."""
        if et is None and ev is None and tb is None:
            self.commit()
        else:
            self.rollback()

    # Auxiliary methods

    def _do_debug(self, s):
        """Print a debug message."""
        if self.debug:
            if isinstance(self.debug, basestring):
                print(self.debug % s)
            elif isinstance(self.debug, file):
                self.debug.write(s + '\n')
            elif callable(self.debug):
                self.debug(s)
            else:
                print(s)

    def _make_bool(d):
        """Get boolean value corresponding to d."""
        if get_bool():
            return bool(d)
        return d and 't' or 'f'
    _make_bool = staticmethod(_make_bool)

    def _quote_text(self, d):
        """Quote text value."""
        if not isinstance(d, basestring):
            d = str(d)
        return "'%s'" % self.escape_string(d)

    _bool_true = frozenset('t true 1 y yes on'.split())

    def _quote_bool(self, d):
        """Quote boolean value."""
        if isinstance(d, basestring):
            if not d:
                return 'NULL'
            d = d.lower() in self._bool_true
        return d and "'t'" or "'f'"

    _date_literals = frozenset('current_date current_time'
        ' current_timestamp localtime localtimestamp'.split())

    def _quote_date(self, d):
        """Quote date value."""
        if not d:
            return 'NULL'
        if isinstance(d, basestring) and d.lower() in self._date_literals:
            return d
        return self._quote_text(d)

    def _quote_num(self, d):
        """Quote numeric value."""
        if not d and d != 0:
            return 'NULL'
        return str(d)

    def _quote_money(self, d):
        """Quote money value."""
        if d is None or d == '':
            return 'NULL'
        if not isinstance(d, basestring):
            d = str(d)
        return d

    _quote_funcs = dict(  # quote methods for each type
        text=_quote_text, bool=_quote_bool, date=_quote_date,
        int=_quote_num, num=_quote_num, float=_quote_num,
        money=_quote_money)

    def _quote(self, d, t):
        """Return quotes if needed."""
        if d is None:
            return 'NULL'
        try:
            quote_func = self._quote_funcs[t]
        except KeyError:
            quote_func = self._quote_funcs['text']
        return quote_func(self, d)

    def _split_schema(self, cl):
        """Return schema and name of object separately.

        This auxiliary function splits off the namespace (schema)
        belonging to the class with the name cl. If the class name
        is not qualified, the function is able to determine the schema
        of the class, taking into account the current search path.

        """
        s = _split_parts(cl)
        if len(s) > 1:  # name already qualified?
            # should be database.schema.table or schema.table
            if len(s) > 3:
                raise _prg_error('Too many dots in class name %s' % cl)
            schema, cl = s[-2:]
        else:
            cl = s[0]
            # determine search path
            q = 'SELECT current_schemas(TRUE)'
            schemas = self.db.query(q).getresult()[0][0][1:-1].split(',')
            if schemas:  # non-empty path
                # search schema for this object in the current search path
                # (we could also use unnest with ordinality here to spare
                # one query, but this is only possible since PostgreSQL 9.4)
                q = ' UNION '.join(
                    ["SELECT %d::integer AS n, '%s'::name AS nspname"
                        % s for s in enumerate(schemas)])
                q = ("SELECT nspname FROM pg_class r"
                    " JOIN pg_namespace s ON r.relnamespace = s.oid"
                    " JOIN (%s) AS p USING (nspname)"
                    " WHERE r.relname = $1 ORDER BY n LIMIT 1" % q)
                schema = self.db.query(q, (cl,)).getresult()
                if schema:  # schema found
                    schema = schema[0][0]
                else:  # object not found in current search path
                    schema = 'public'
            else:  # empty path
                schema = 'public'
        return schema, cl

    def _add_schema(self, cl):
        """Ensure that the class name is prefixed with a schema name."""
        return _join_parts(self._split_schema(cl))

    # Public methods

    # escape_string and escape_bytea exist as methods,
    # so we define unescape_bytea as a method as well
    unescape_bytea = staticmethod(unescape_bytea)

    def close(self):
        """Close the database connection."""
        # Wraps shared library function so we can track state.
        if self._closeable:
            if self.db:
                self.db.close()
                self.db = None
            else:
                raise _int_error('Connection already closed')

    def reset(self):
        """Reset connection with current parameters.

        All derived queries and large objects derived from this connection
        will not be usable after this call.

        """
        if self.db:
            self.db.reset()
        else:
            raise _int_error('Connection already closed')

    def reopen(self):
        """Reopen connection to the database.

        Used in case we need another connection to the same database.
        Note that we can still reopen a database that we have closed.

        """
        # There is no such shared library function.
        if self._closeable:
            db = connect(*self._args[0], **self._args[1])
            if self.db:
                self.db.close()
            self.db = db

    def begin(self, mode=None):
        """Begin a transaction."""
        qstr = 'BEGIN'
        if mode:
            qstr += ' ' + mode
        return self.query(qstr)

    start = begin

    def commit(self):
        """Commit the current transaction."""
        return self.query('COMMIT')

    end = commit

    def rollback(self, name=None):
        """Rollback the current transaction."""
        qstr = 'ROLLBACK'
        if name:
            qstr += ' TO ' + name
        return self.query(qstr)

    def savepoint(self, name=None):
        """Define a new savepoint within the current transaction."""
        qstr = 'SAVEPOINT'
        if name:
            qstr += ' ' + name
        return self.query(qstr)

    def release(self, name):
        """Destroy a previously defined savepoint."""
        return self.query('RELEASE ' + name)

    def query(self, qstr, *args):
        """Executes a SQL command string.

        This method simply sends a SQL query to the database. If the query is
        an insert statement that inserted exactly one row into a table that
        has OIDs, the return value is the OID of the newly inserted row.
        If the query is an update or delete statement, or an insert statement
        that did not insert exactly one row in a table with OIDs, then the
        number of rows affected is returned as a string. If it is a statement
        that returns rows as a result (usually a select statement, but maybe
        also an "insert/update ... returning" statement), this method returns
        a pgqueryobject that can be accessed via getresult() or dictresult()
        or simply printed. Otherwise, it returns `None`.

        The query can contain numbered parameters of the form $1 in place
        of any data constant. Arguments given after the query string will
        be substituted for the corresponding numbered parameter. Parameter
        values can also be given as a single list or tuple argument.

        Note that the query string must not be passed as a unicode value,
        but you can pass arguments as unicode values if they can be decoded
        using the current client encoding.

        """
        # Wraps shared library function for debugging.
        if not self.db:
            raise _int_error('Connection is not valid')
        self._do_debug(qstr)
        return self.db.query(qstr, args)

    def pkey(self, cl, newpkey=None):
        """This method gets or sets the primary key of a class.

        Composite primary keys are represented as frozensets. Note that
        this raises an exception if the table does not have a primary key.

        If newpkey is set and is not a dictionary then set that
        value as the primary key of the class.  If it is a dictionary
        then replace the internal cache of primary keys with a copy of it.

        """
        # First see if the caller is supplying a dictionary
        if isinstance(newpkey, dict):
            # make sure that all classes have a namespace
            self._pkeys = dict([
                ('.' in cl and cl or 'public.' + cl, pkey)
                for cl, pkey in newpkey.items()])
            return self._pkeys

        qcl = self._add_schema(cl)  # build fully qualified class name
        # Check if the caller is supplying a new primary key for the class
        if newpkey:
            self._pkeys[qcl] = newpkey
            return newpkey

        # Get all the primary keys at once
        if qcl not in self._pkeys:
            # if not found, check again in case it was added after we started
            self._pkeys = {}
            if self.server_version >= 80200:
                # the ANY syntax works correctly only with PostgreSQL >= 8.2
                any_indkey = "= ANY (i.indkey)"
            else:
                any_indkey = "IN (%s)" % ', '.join(
                    ['i.indkey[%d]' % i for i in range(16)])
            q = ("SELECT s.nspname, r.relname, a.attname"
                " FROM pg_class r"
                " JOIN pg_namespace s ON s.oid = r.relnamespace"
                " AND s.nspname NOT SIMILAR"
                " TO 'pg/_%|information/_schema' ESCAPE '/'"
                " JOIN pg_attribute a ON a.attrelid = r.oid"
                " AND NOT a.attisdropped"
                " JOIN pg_index i ON i.indrelid = r.oid"
                " AND i.indisprimary AND a.attnum " + any_indkey)
            for r in self.db.query(q).getresult():
                cl, pkey = _join_parts(r[:2]), r[2]
                self._pkeys.setdefault(cl, []).append(pkey)
            # (only) for composite primary keys, the values will be frozensets
            for cl, pkey in self._pkeys.items():
                self._pkeys[cl] = len(pkey) > 1 and frozenset(pkey) or pkey[0]
            self._do_debug(self._pkeys)

        # will raise an exception if primary key doesn't exist
        return self._pkeys[qcl]

    def get_databases(self):
        """Get list of databases in the system."""
        return [s[0] for s in
            self.db.query('SELECT datname FROM pg_database').getresult()]

    def get_relations(self, kinds=None):
        """Get list of relations in connected database of specified kinds.

        If kinds is None or empty, all kinds of relations are returned.
        Otherwise kinds can be a string or sequence of type letters
        specifying which kind of relations you want to list.

        """
        where = kinds and " AND r.relkind IN (%s)" % ','.join(
            ["'%s'" % k for k in kinds]) or ''
        q = ("SELECT s.nspname, r.relname"
            " FROM pg_class r"
            " JOIN pg_namespace s ON s.oid = r.relnamespace"
            " WHERE s.nspname NOT SIMILAR"
            " TO 'pg/_%%|information/_schema' ESCAPE '/' %s"
            " ORDER BY 1, 2") % where
        return [_join_parts(r) for r in self.db.query(q).getresult()]

    def get_tables(self):
        """Return list of tables in connected database."""
        return self.get_relations('r')

    def get_attnames(self, cl, newattnames=None):
        """Given the name of a table, digs out the set of attribute names.

        Returns a dictionary of attribute names (the names are the keys,
        the values are the names of the attributes' types).
        If the optional newattnames exists, it must be a dictionary and
        will become the new attribute names dictionary.

        By default, only a limited number of simple types will be returned.
        You can get the regular types after calling use_regtypes(True).

        """
        if isinstance(newattnames, dict):
            self._attnames = newattnames
            return
        elif newattnames:
            raise _prg_error('If supplied, newattnames must be a dictionary')
        cl = self._split_schema(cl)  # split into schema and class
        qcl = _join_parts(cl)  # build fully qualified name
        # May as well cache them:
        if qcl in self._attnames:
            return self._attnames[qcl]
        if qcl not in self.get_relations('rv'):
            raise _prg_error('Class %s does not exist' % qcl)

        q = ("SELECT a.attname, t.typname%s"
            " FROM pg_class r"
            " JOIN pg_namespace s ON r.relnamespace = s.oid"
            " JOIN pg_attribute a ON a.attrelid = r.oid"
            " JOIN pg_type t ON t.oid = a.atttypid"
            " WHERE s.nspname = $1 AND r.relname = $2"
            " AND (a.attnum > 0 OR a.attname = 'oid')"
            " AND NOT a.attisdropped") % (
                self._regtypes and '::regtype' or '',)
        q = self.db.query(q, cl).getresult()

        if self._regtypes:
            t = dict(q)
        else:
            t = {}
            for att, typ in q:
                if typ.startswith('bool'):
                    typ = 'bool'
                elif typ.startswith('abstime'):
                    typ = 'date'
                elif typ.startswith('date'):
                    typ = 'date'
                elif typ.startswith('interval'):
                    typ = 'date'
                elif typ.startswith('timestamp'):
                    typ = 'date'
                elif typ.startswith('oid'):
                    typ = 'int'
                elif typ.startswith('int'):
                    typ = 'int'
                elif typ.startswith('float'):
                    typ = 'float'
                elif typ.startswith('numeric'):
                    typ = 'num'
                elif typ.startswith('money'):
                    typ = 'money'
                else:
                    typ = 'text'
                t[att] = typ

        self._attnames[qcl] = t  # cache it
        return self._attnames[qcl]

    def use_regtypes(self, regtypes=None):
        """Use regular type names instead of simplified type names."""
        if regtypes is None:
            return self._regtypes
        else:
            regtypes = bool(regtypes)
            if regtypes != self._regtypes:
                self._regtypes = regtypes
                self._attnames.clear()
            return regtypes

    def has_table_privilege(self, cl, privilege='select'):
        """Check whether current user has specified table privilege."""
        qcl = self._add_schema(cl)
        privilege = privilege.lower()
        try:
            return self._privileges[(qcl, privilege)]
        except KeyError:
            q = "SELECT has_table_privilege($1, $2)"
            q = self.db.query(q, (qcl, privilege))
            ret = q.getresult()[0][0] == self._make_bool(True)
            self._privileges[(qcl, privilege)] = ret
            return ret

    def get(self, cl, arg, keyname=None):
        """Get a row from a database table or view.

        This method is the basic mechanism to get a single row.  The keyname
        that the key specifies a unique row.  If keyname is not specified
        then the primary key for the table is used.  If arg is a dictionary
        then the value for the key is taken from it and it is modified to
        include the new values, replacing existing values where necessary.
        For a composite key, keyname can also be a sequence of key names.
        The OID is also put into the dictionary if the table has one, but
        in order to allow the caller to work with multiple tables, it is
        munged as oid(schema.table).

        """
        if cl.endswith('*'):  # scan descendant tables?
            cl = cl[:-1].rstrip()  # need parent table name
        # build qualified class name
        qcl = self._add_schema(cl)
        # To allow users to work with multiple tables,
        # we munge the name of the "oid" the key
        qoid = _oid_key(qcl)
        if not keyname:
            # use the primary key by default
            try:
                keyname = self.pkey(qcl)
            except KeyError:
                raise _prg_error('Class %s has no primary key' % qcl)
        # We want the oid for later updates if that isn't the key
        if keyname == 'oid':
            if isinstance(arg, dict):
                if qoid not in arg:
                    raise _db_error('%s not in arg' % qoid)
            else:
                arg = {qoid: arg}
            where = 'oid = %s' % arg[qoid]
            attnames = '*'
        else:
            attnames = self.get_attnames(qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            if not isinstance(arg, dict):
                if len(keyname) > 1:
                    raise _prg_error('Composite key needs dict as arg')
                arg = dict([(k, arg) for k in keyname])
            where = ' AND '.join(['%s = %s'
                % (k, self._quote(arg[k], attnames[k])) for k in keyname])
            attnames = ', '.join(attnames)
        q = 'SELECT %s FROM %s WHERE %s LIMIT 1' % (attnames, qcl, where)
        self._do_debug(q)
        res = self.db.query(q).dictresult()
        if not res:
            raise _db_error('No such record in %s where %s' % (qcl, where))
        for att, value in res[0].items():
            arg[att == 'oid' and qoid or att] = value
        return arg

    def insert(self, cl, d=None, **kw):
        """Insert a row into a database table.

        This method inserts a row into a table.  The name of the table must
        be passed as the first parameter.  The other parameters are used for
        providing the data of the row that shall be inserted into the table.
        If a dictionary is supplied as the second parameter, it starts with
        that.  Otherwise it uses a blank dictionary. Either way the dictionary
        is updated from the keywords.

        The dictionary is then, if possible, reloaded with the values actually
        inserted in order to pick up values modified by rules, triggers, etc.

        Note: The method currently doesn't support insert into views
        although PostgreSQL does.

        """
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if d is None:
            d = {}
        d.update(kw)
        attnames = self.get_attnames(qcl)
        names, values = [], []
        for n in attnames:
            if n != 'oid' and n in d:
                names.append('"%s"' % n)
                values.append(self._quote(d[n], attnames[n]))
        names, values = ', '.join(names), ', '.join(values)
        selectable = self.has_table_privilege(qcl)
        if selectable and self.server_version >= 80200:
            ret = ' RETURNING %s*' % ('oid' in attnames and 'oid, ' or '')
        else:
            ret = ''
        q = 'INSERT INTO %s (%s) VALUES (%s)%s' % (qcl, names, values, ret)
        self._do_debug(q)
        res = self.db.query(q)
        if ret:
            res = res.dictresult()
            for att, value in res[0].items():
                d[att == 'oid' and qoid or att] = value
        elif isinstance(res, int):
            d[qoid] = res
            if selectable:
                self.get(qcl, d, 'oid')
        elif selectable:
            if qoid in d:
                self.get(qcl, d, 'oid')
            else:
                try:
                    self.get(qcl, d)
                except ProgrammingError:
                    pass  # table has no primary key
        return d

    def update(self, cl, d=None, **kw):
        """Update an existing row in a database table.

        Similar to insert but updates an existing row.  The update is based
        on the OID value as munged by get or passed as keyword, or on the
        primary key of the table.  The dictionary is modified, if possible,
        to reflect any changes caused by the update due to triggers, rules,
        default values, etc.

        """
        # Update always works on the oid which get returns if available,
        # otherwise use the primary key.  Fail if neither.
        # Note that we only accept oid key from named args for safety
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if 'oid' in kw:
            kw[qoid] = kw['oid']
            del kw['oid']
        if d is None:
            d = {}
        d.update(kw)
        attnames = self.get_attnames(qcl)
        if qoid in d:
            where = 'oid = %s' % d[qoid]
            keyname = ()
        else:
            try:
                keyname = self.pkey(qcl)
            except KeyError:
                raise _prg_error('Class %s has no primary key' % qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            try:
                where = ' AND '.join(['%s = %s'
                    % (k, self._quote(d[k], attnames[k])) for k in keyname])
            except KeyError:
                raise _prg_error('Update needs primary key or oid.')
        values = []
        for n in attnames:
            if n in d and n not in keyname:
                values.append('%s = %s' % (n, self._quote(d[n], attnames[n])))
        if not values:
            return d
        values = ', '.join(values)
        selectable = self.has_table_privilege(qcl)
        if selectable and self.server_version >= 80200:
            ret = ' RETURNING %s*' % ('oid' in attnames and 'oid, ' or '')
        else:
            ret = ''
        q = 'UPDATE %s SET %s WHERE %s%s' % (qcl, values, where, ret)
        self._do_debug(q)
        res = self.db.query(q)
        if ret:
            res = res.dictresult()[0]
            for att, value in res.items():
                d[att == 'oid' and qoid or att] = value
        else:
            if selectable:
                if qoid in d:
                    self.get(qcl, d, 'oid')
                else:
                    self.get(qcl, d)
        return d

    def clear(self, cl, a=None):
        """Clear all the attributes to values determined by the types.

        Numeric types are set to 0, Booleans are set to false, and everything
        else is set to the empty string.  If the array argument is present,
        it is used as the array and any entries matching attribute names are
        cleared with everything else left unchanged.

        """
        # At some point we will need a way to get defaults from a table.
        qcl = self._add_schema(cl)
        if a is None:
            a = {}  # empty if argument is not present
        attnames = self.get_attnames(qcl)
        for n, t in attnames.items():
            if n == 'oid':
                continue
            if t in ('int', 'integer', 'smallint', 'bigint',
                    'float', 'real', 'double precision',
                    'num', 'numeric', 'money'):
                a[n] = 0
            elif t in ('bool', 'boolean'):
                a[n] = self._make_bool(False)
            else:
                a[n] = ''
        return a

    def delete(self, cl, d=None, **kw):
        """Delete an existing row in a database table.

        This method deletes the row from a table.  It deletes based on the
        OID value as munged by get or passed as keyword, or on the primary
        key of the table.  The return value is the number of deleted rows
        (i.e. 0 if the row did not exist and 1 if the row was deleted).

        """
        # Like update, delete works on the oid.
        # One day we will be testing that the record to be deleted
        # isn't referenced somewhere (or else PostgreSQL will).
        # Note that we only accept oid key from named args for safety
        qcl = self._add_schema(cl)
        qoid = _oid_key(qcl)
        if 'oid' in kw:
            kw[qoid] = kw['oid']
            del kw['oid']
        if d is None:
            d = {}
        d.update(kw)
        if qoid in d:
            where = 'oid = %s' % d[qoid]
        else:
            try:
                keyname = self.pkey(qcl)
            except KeyError:
                raise _prg_error('Class %s has no primary key' % qcl)
            if isinstance(keyname, basestring):
                keyname = (keyname,)
            attnames = self.get_attnames(qcl)
            try:
                where = ' AND '.join(['%s = %s'
                    % (k, self._quote(d[k], attnames[k])) for k in keyname])
            except KeyError:
                raise _prg_error('Delete needs primary key or oid.')
        q = 'DELETE FROM %s WHERE %s' % (qcl, where)
        self._do_debug(q)
        return int(self.db.query(q))

    def notification_handler(self, event, callback, arg_dict={}, timeout=None):
        """Get notification handler that will run the given callback."""
        return NotificationHandler(self.db, event, callback, arg_dict, timeout)


# if run as script, print some information

if __name__ == '__main__':
    print('PyGreSQL version' + version)
    print('')
    print(__doc__)
