#!/usr/bin/env python
#
# pg.py
#
# Written by D'Arcy J.M. Cain
# Improved by Christoph Zwerschke
#
# $Id: pg.py,v 1.66 2008-11-25 18:15:23 darcy Exp $
#

"""PyGreSQL classic interface.

This pg module implements some basic database management stuff.
It includes the _pg module and builds on it, providing the higher
level wrapper class named DB with addtional functionality.
This is known as the "classic" ("old style") PyGreSQL interface.
For a DB-API 2 compliant interface use the newer pgdb module.

"""

from _pg import *
try:
    frozenset
except NameError: # Python < 2.4
    from sets import ImmutableSet as frozenset
try:
    from decimal import Decimal
    set_decimal(Decimal)
except ImportError:
    pass # Python < 2.4


# Auxiliary functions which are independent from a DB connection:

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


# The PostGreSQL database connection interface:

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
            self._closeable = 1
        else:
            self._closeable = 0
        self.db = db
        self.dbname = db.db
        self._attnames = {}
        self._pkeys = {}
        self._args = args, kw
        self.debug = None # For debugging scripts, this can be set
            # * to a string format specification (e.g. in CGI set to "%s<BR>"),
            # * to a file object to write debug statements or
            # * to a callable object which takes a string argument.

    def __getattr__(self, name):
        # All undefined members are the same as in the underlying pg connection:
        if self.db:
            return getattr(self.db, name)
        else:
            raise InternalError('Connection is not valid')

    # Auxiliary methods

    def _do_debug(self, s):
        """Print a debug message."""
        if self.debug:
            if isinstance(self.debug, basestring):
                print self.debug % s
            elif isinstance(self.debug, file):
                print >> self.debug, s
            elif callable(self.debug):
                self.debug(s)

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
        else:
            d = bool(d)
        return ("'f'", "'t'")[d]

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
        if not d:
            return 'NULL'
        return str(d)

    def _quote_money(self, d):
        """Quote money value."""
        if not d:
            return 'NULL'
        return "'%.2f'" % float(d)

    _quote_funcs = dict( # quote methods for each type
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
        if len(s) > 1: # name already qualfied?
            # should be database.schema.table or schema.table
            if len(s) > 3:
                raise ProgrammingError('Too many dots in class name %s' % cl)
            schema, cl = s[-2:]
        else:
            cl = s[0]
            # determine search path
            query = 'SELECT current_schemas(TRUE)'
            schemas = self.db.query(query).getresult()[0][0][1:-1].split(',')
            if schemas: # non-empty path
                # search schema for this object in the current search path
                query = ' UNION '.join(
                    ["SELECT %d::integer AS n, '%s'::name AS nspname"
                        % s for s in enumerate(schemas)])
                query = ("SELECT nspname FROM pg_class"
                    " JOIN pg_namespace ON pg_class.relnamespace=pg_namespace.oid"
                    " JOIN (%s) AS p USING (nspname)"
                    " WHERE pg_class.relname='%s'"
                    " ORDER BY n LIMIT 1" % (query, cl))
                schema = self.db.query(query).getresult()
                if schema: # schema found
                    schema = schema[0][0]
                else: # object not found in current search path
                    schema = 'public'
            else: # empty path
                schema = 'public'
        return schema, cl

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
                raise InternalError('Connection already closed')

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

    def query(self, qstr):
        """Executes a SQL command string.

        This method simply sends a SQL query to the database. If the query is
        an insert statement, the return value is the OID of the newly
        inserted row.  If it is otherwise a query that does not return a result
        (ie. is not a some kind of SELECT statement), it returns None.
        Otherwise, it returns a pgqueryobject that can be accessed via the
        getresult or dictresult method or simply printed.

        """
        # Wraps shared library function for debugging.
        if not self.db:
            raise InternalError('Connection is not valid')
        self._do_debug(qstr)
        return self.db.query(qstr)

    def pkey(self, cl, newpkey=None):
        """This method gets or sets the primary key of a class.

        If newpkey is set and is not a dictionary then set that
        value as the primary key of the class.  If it is a dictionary
        then replace the _pkeys dictionary with it.

        """
        # First see if the caller is supplying a dictionary
        if isinstance(newpkey, dict):
            # make sure that we have a namespace
            self._pkeys = {}
            for x in newpkey.keys():
                if x.find('.') == -1:
                    self._pkeys['public.' + x] = newpkey[x]
                else:
                    self._pkeys[x] = newpkey[x]
            return self._pkeys

        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        if newpkey:
            self._pkeys[qcl] = newpkey
            return newpkey

        # Get all the primary keys at once
        if qcl not in self._pkeys:
            # if not found, check again in case it was added after we started
            self._pkeys = dict([
                (_join_parts(r[:2]), r[2]) for r in self.db.query(
                "SELECT pg_namespace.nspname, pg_class.relname"
                    ",pg_attribute.attname FROM pg_class"
                " JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace"
                    " AND pg_namespace.nspname NOT LIKE 'pg_%'"
                " JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid"
                    " AND pg_attribute.attisdropped='f'"
                " JOIN pg_index ON pg_index.indrelid=pg_class.oid"
                    " AND pg_index.indisprimary='t'"
                    " AND pg_index.indkey[0]=pg_attribute.attnum"
                ).getresult()])
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
        where = kinds and "pg_class.relkind IN (%s) AND" % ','.join(
            ["'%s'" % x for x in kinds]) or ''
        return map(_join_parts, self.db.query(
            "SELECT pg_namespace.nspname, pg_class.relname "
            "FROM pg_class "
            "JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace "
            "WHERE %s pg_class.relname !~ '^Inv' AND "
                "pg_class.relname !~ '^pg_' "
            "ORDER BY 1, 2" % where).getresult())

    def get_tables(self):
        """Return list of tables in connected database."""
        return self.get_relations('r')

    def get_attnames(self, cl, newattnames=None):
        """Given the name of a table, digs out the set of attribute names.

        Returns a dictionary of attribute names (the names are the keys,
        the values are the names of the attributes' types).
        If the optional newattnames exists, it must be a dictionary and
        will become the new attribute names dictionary.

        """
        if isinstance(newattnames, dict):
            self._attnames = newattnames
            return
        elif newattnames:
            raise ProgrammingError(
                'If supplied, newattnames must be a dictionary')
        cl = self._split_schema(cl) # split into schema and cl
        qcl = _join_parts(cl) # build qualified name
        # May as well cache them:
        if qcl in self._attnames:
            return self._attnames[qcl]
        if qcl not in self.get_relations('rv'):
            raise ProgrammingError('Class %s does not exist' % qcl)
        t = {}
        for att, typ in self.db.query("SELECT pg_attribute.attname"
            ",pg_type.typname FROM pg_class"
            " JOIN pg_namespace ON pg_class.relnamespace=pg_namespace.oid"
            " JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid"
            " JOIN pg_type ON pg_type.oid=pg_attribute.atttypid"
            " WHERE pg_namespace.nspname='%s' AND pg_class.relname='%s'"
            " AND (pg_attribute.attnum>0 or pg_attribute.attname='oid')"
            " AND pg_attribute.attisdropped='f'"
                % cl).getresult():
            if typ.startswith('bool'):
                t[att] = 'bool'
            elif typ.startswith('abstime'):
                t[att] = 'date'
            elif typ.startswith('date'):
                t[att] = 'date'
            elif typ.startswith('interval'):
                t[att] = 'date'
            elif typ.startswith('timestamp'):
                t[att] = 'date'
            elif typ.startswith('oid'):
                t[att] = 'int'
            elif typ.startswith('int'):
                t[att] = 'int'
            elif typ.startswith('float'):
                t[att] = 'float'
            elif typ.startswith('numeric'):
                t[att] = 'num'
            elif typ.startswith('money'):
                t[att] = 'money'
            else:
                t[att] = 'text'
        self._attnames[qcl] = t # cache it
        return self._attnames[qcl]

    def get(self, cl, arg, keyname=None, view=0):
        """Get a tuple from a database table or view.

        This method is the basic mechanism to get a single row.  It assumes
        that the key specifies a unique row.  If keyname is not specified
        then the primary key for the table is used.  If arg is a dictionary
        then the value for the key is taken from it and it is modified to
        include the new values, replacing existing values where necessary.
        The OID is also put into the dictionary, but in order to allow the
        caller to work with multiple tables, it is munged as oid(schema.table).

        """
        if cl.endswith('*'): # scan descendant tables?
            cl = cl[:-1].rstrip() # need parent table name
        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        # To allow users to work with multiple tables,
        # we munge the name when the key is "oid"
        foid = 'oid(%s)' % qcl # build mangled name
        if keyname is None: # use the primary key by default
            keyname = self.pkey(qcl)
        fnames = self.get_attnames(qcl)
        if isinstance(arg, dict):
            k = arg[keyname == 'oid' and foid or keyname]
        else:
            k = arg
            arg = {}
        # We want the oid for later updates if that isn't the key
        if keyname == 'oid':
            q = 'SELECT * FROM %s WHERE oid=%s LIMIT 1' % (qcl, k)
        elif view:
            q = 'SELECT * FROM %s WHERE %s=%s LIMIT 1' % \
                (qcl, keyname, self._quote(k, fnames[keyname]))
        else:
            q = 'SELECT %s FROM %s WHERE %s=%s LIMIT 1' % \
                (','.join(fnames.keys()), qcl, \
                    keyname, self._quote(k, fnames[keyname]))
        self._do_debug(q)
        res = self.db.query(q).dictresult()
        if not res:
            raise DatabaseError('No such record in %s where %s=%s'
                % (qcl, keyname, self._quote(k, fnames[keyname])))
        for k, d in res[0].items():
            if k == 'oid':
                k = foid
            arg[k] = d
        return arg

    def insert(self, cl, d=None, **kw):
        """Insert a tuple into a database table.

        This method inserts values into the table specified filling in the
        values from the dictionary.  It then reloads the dictionary with the
        values from the database.  This causes the dictionary to be updated
        with values that are modified by rules, triggers, etc.

        Note: The method currently doesn't support insert into views
        although PostgreSQL does.

        """
        if d is None:
            a = {}
        else:
            a = d
        a.update(kw)

        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        foid = 'oid(%s)' % qcl # build mangled name
        fnames = self.get_attnames(qcl)
        t = []
        n = []
        for f in fnames.keys():
            if f != 'oid' and f in a:
                t.append(self._quote(a[f], fnames[f]))
                n.append('"%s"' % f)
        q = 'INSERT INTO %s (%s) VALUES (%s)' % \
            (qcl, ','.join(n), ','.join(t))
        self._do_debug(q)
        a[foid] = self.db.query(q)
        # Reload the dictionary to catch things modified by engine.
        # Note that get() changes 'oid' below to oid_schema_table.
        # If no read perms (it can and does happen), return None.
        try:
            return self.get(qcl, a, 'oid')
        except Exception:
            return None

    def update(self, cl, d=None, **kw):
        """Update an existing row in a database table.

        Similar to insert but updates an existing row.  The update is based
        on the OID value as munged by get.  The array returned is the
        one sent modified to reflect any changes caused by the update due
        to triggers, rules, defaults, etc.

        """
        # Update always works on the oid which get returns if available,
        # otherwise use the primary key.  Fail if neither.
        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        foid = 'oid(%s)' % qcl # build mangled oid

        # Note that we only accept oid key from named args for safety
        if 'oid' in kw:
            kw[foid] = kw['oid']
            del kw['oid']

        if d is None:
            a = {}
        else:
            a = d
        a.update(kw)

        if foid in a:
            where = "oid=%s" % a[foid]
        else:
            try:
                pk = self.pkey(qcl)
            except Exception:
                raise ProgrammingError(
                    'Update needs primary key or oid as %s' % foid)
            where = "%s='%s'" % (pk, a[pk])
        v = []
        fnames = self.get_attnames(qcl)
        for ff in fnames.keys():
            if ff != 'oid' and ff in a:
                v.append('%s=%s' % (ff, self._quote(a[ff], fnames[ff])))
        if v == []:
            return None
        q = 'UPDATE %s SET %s WHERE %s' % (qcl, ','.join(v), where)
        self._do_debug(q)
        self.db.query(q)
        # Reload the dictionary to catch things modified by engine:
        if foid in a:
            return self.get(qcl, a, 'oid')
        else:
            return self.get(qcl, a)

    def clear(self, cl, a=None):
        """

        This method clears all the attributes to values determined by the types.
        Numeric types are set to 0, Booleans are set to 'f', and everything
        else is set to the empty string.  If the array argument is present,
        it is used as the array and any entries matching attribute names are
        cleared with everything else left unchanged.

        """
        # At some point we will need a way to get defaults from a table.
        if a is None:
            a = {} # empty if argument is not present
        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        fnames = self.get_attnames(qcl)
        for k, t in fnames.items():
            if k == 'oid':
                continue
            if t in ('int', 'float', 'num', 'money'):
                a[k] = 0
            elif t == 'bool':
                a[k] = 'f'
            else:
                a[k] = ''
        return a

    def delete(self, cl, d=None, **kw):
        """Delete an existing row in a database table.

        This method deletes the row from a table.
        It deletes based on the OID munged as described above.

        """
        # Like update, delete works on the oid.
        # One day we will be testing that the record to be deleted
        # isn't referenced somewhere (or else PostgreSQL will).
        qcl = _join_parts(self._split_schema(cl)) # build qualified name
        foid = 'oid(%s)' % qcl # build mangled oid

        # Note that we only accept oid key from named args for safety
        if 'oid' in kw:
            kw[foid] = kw['oid']
            del kw['oid']

        if d is None:
            a = {}
        else:
            a = d
        a.update(kw)

        q = 'DELETE FROM %s WHERE oid=%s' % (qcl, a[foid])
        self._do_debug(q)
        self.db.query(q)


# if run as script, print some information

if __name__ == '__main__':
    print 'PyGreSQL version', version
    print
    print __doc__
