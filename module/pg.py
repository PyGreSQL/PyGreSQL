# pg.py
# Written by D'Arcy J.M. Cain
# $Id: pg.py,v 1.31 2005-05-26 13:58:36 cito Exp $

"""PyGreSQL Classic Interface.

This library implements some basic database management stuff.
It includes the pg module and builds on it.  This is known as the
"Classic" interface.  For DB-API compliance use the pgdb module.
"""

from _pg import *
from types import *

def _quote(d, t):
	"""Return quotes if needed (utility function)."""
	if d == None:
		return "NULL"
	if t in ('int', 'seq'):
		if d == "": return "NULL"
		return "%d" % long(d)
	if t == 'decimal':
		if d == "": return "NULL"
		return "%f" % float(d)
	if t == 'money':
		if d == "": return "NULL"
		return "'%.2f'" % float(d)
	if t == 'bool':
		d = str(d).lower() in ('t', 'true', '1', 'y', 'yes', 'on')
		return ("'f'", "'t'")[d]
	if t in ('date', 'inet', 'cidr') and d == '': return "NULL"
	return "'%s'" % str(d).replace('\\','\\\\').replace('\'','\\\'')

class DB:
	"""This class wraps the pg connection type."""

	def __init__(self, *args, **kw):
		self.db = connect(*args, **kw)

		# Create convenience methods, in a way that is still overridable
		# (members are not copied because they are actually functions):
		for e in self.db.__methods__:
			if e not in ("close", "query"): # These are wrapped separately
				setattr(self, e, getattr(self.db, e))

		self.__attnames = {}
		self.__pkeys = {}
		self.__args = args, kw
		self.debug = None # For debugging scripts, this can be set
			# * to a string format specification (e.g. in a CGI set to "%s<BR>"),
			# * to a function which takes a string argument or
			# * to a file object to write debug statements to.

	def _do_debug(self, s):
		"""Print a debug message."""
		if not self.debug: return
		if isinstance(self.debug, StringType): print self.debug % s
		if isinstance(self.debug, FunctionType): self.debug(s)
		if isinstance(self.debug, FileType): print >> self.debug, s

	def close(self,):
		"""Close the database connection."""
		# Wraps shared library function so we can track state.
		self.db.close()
		self.db = None

	def reopen(self):
		"""Reopen connection to the database.

		Used in case we need another connection to the same database.
		Note that we can still reopen a database that we have closed.

		"""
		if self.db: self.close()
		try: self.db = connect(*self.__args[0], **self.__args[1])
		except:
			self.db = None
			raise

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
		self._do_debug(qstr)
		return self.db.query(qstr)

	def split_schema(self, cl):
		"""Return schema and name of object separately.

		If name is not qualified, determine first schema in search path
		contaning the object, otherwise split qualified name.

		"""
		if cl.find('.') < 0:
			# determine search path
			query = "SELECT current_schemas(TRUE)"
			schemas = self.db.query(query).getresult()[0][0][1:-1].split(',')
			if schemas: # non-empty path
				# search schema for this object in the current search path
				query = " UNION ".join(["SELECT %d AS n, '%s' AS nspname" % s
					for s in enumerate(schemas)])
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
		else: # name already qualfied?
			# should be database.schema.table or schema.table
			schema, cl = cl.split('.', 2)[-2:]
		return schema, cl

	def pkey(self, cl, newpkey = None):
		"""This method gets or sets the primary key of a class.

		If newpkey is set and is not a dictionary then set that
		value as the primary key of the class.  If it is a dictionary
		then replace the __pkeys dictionary with it.

		"""
		# Get all the primary keys at once
		if isinstance(newpkey, DictType):
			self.__pkeys = newpkey
			return newpkey
		qcl = "%s.%s"  % self.split_schema(cl) # determine qualified name
		if newpkey:
			self.__pkeys[qcl] = newpkey
			return newpkey
		if self.__pkeys == {} or not self.__pkeys.has_key(qcl):
			# if not found, determine pkey again in case it was added after we started
			for q, a in self.db.query("SELECT pg_namespace.nspname||'.'||"
				"pg_class.relname,pg_attribute.attname FROM pg_class"
				" JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace"
				" AND pg_namespace.nspname NOT LIKE 'pg_%'"
				" JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid"
				" AND pg_attribute.attisdropped='f'"
				" JOIN pg_index ON pg_index.indrelid=pg_class.oid"
				" AND pg_index.indisprimary='t'"
				" AND pg_index.indkey[0]=pg_attribute.attnum").getresult():
				self.__pkeys[q] = a
			self._do_debug(self.__pkeys)
		# will raise an exception if primary key doesn't exist
		return self.__pkeys[qcl]

	def get_databases(self):
		"""Get list of databases in the system."""
		return [x[0] for x in
			self.db.query("SELECT datname FROM pg_database").getresult()]

	def get_tables(self):
		"""Get list of tables in connected database."""
		return [x[0] for x in
			self.db.query("SELECT pg_namespace.nspname||'.'||"
				"pg_class.relname FROM pg_class"
				" JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace"
				" WHERE pg_class.relkind='r' AND"
				" pg_class.relname !~ '^Inv' AND "
				" pg_class.relname !~ '^pg_' order by 1").getresult()]

	def get_attnames(self, cl, newattnames = None):
		"""Given the name of a table, digs out the set of attribute names.

		Returns a dictionary of attribute names (the names are the keys,
		the values are the names of the attributes' types).
		If the optional newattnames exists, it must be a dictionary and
		will become the new attribute names dictionary.

		"""
		if isinstance(newattnames, DictType):
			self.__attnames = newattnames
			return
		elif newattnames:
			raise ProgrammingError, \
				"If supplied, newattnames must be a dictionary"
		cl = self.split_schema(cl) # split into schema and cl
		qcl = '%s.%s' % cl # build qualified name
		# May as well cache them:
		if self.__attnames.has_key(qcl):
			return self.__attnames[qcl]
		query = ("SELECT pg_attribute.attname,pg_type.typname FROM pg_class"
			" JOIN pg_namespace ON pg_class.relnamespace=pg_namespace.oid"
			" JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid"
			" JOIN pg_type ON pg_type.oid=pg_attribute.atttypid"
			" WHERE pg_namespace.nspname='%s' AND pg_class.relname='%s'"
			" AND pg_attribute.attnum>0 AND pg_attribute.attisdropped='f'" % cl)
		l = {}
		for att, typ in self.db.query(query).getresult():
			if typ.startswith("interval"):
				l[att] = 'date'
			elif typ.startswith("int"):
				l[att] = 'int'
			elif typ.startswith("oid"):
				l[att] = 'int'
			elif typ.startswith("text"):
				l[att] = 'text'
			elif typ.startswith("char"):
				l[att] = 'text'
			elif typ.startswith("name"):
				l[att] = 'text'
			elif typ.startswith("abstime"):
				l[att] = 'date'
			elif typ.startswith("date"):
				l[att] = 'date'
			elif typ.startswith("timestamp"):
				l[att] = 'date'
			elif typ.startswith("bool"):
				l[att] = 'bool'
			elif typ.startswith("float"):
				l[att] = 'decimal'
			elif typ.startswith("money"):
				l[att] = 'money'
			else:
				l[att] = 'text'
		l['oid'] = 'int' # every table has this
		self.__attnames[qcl] = l # cache it
		return self.__attnames[qcl]

	def get(self, cl, arg, keyname = None, view = 0):
		"""Get a tuple from a database table.

		This method is the basic mechanism to get a single row.  It assumes
		that the key specifies a unique row.  If keyname is not specified
		then the primary key for the table is used.  If arg is a dictionary
		then the value for the key is taken from it and it is modified to
		include the new values, replacing existing values where necessary.
		The oid is also put into the dictionary but in order to allow the
		caller to work with multiple tables, the attribute name is munge
		as "oid_schema_table" to make it unique.

		"""
		if cl.endswith('*'): # scan descendant tables?
			xcl = cl[:-1].rstrip() # need parent table name
		else:
			xcl = cl
		xcl = self.split_schema(xcl) # split into schema and cl
		qcl = '%s.%s' % xcl # build qualified name
		if keyname == None:	# use the primary key by default
			keyname = self.pkey(qcl)
		fnames = self.get_attnames(qcl)
		if isinstance(arg, DictType):
			# To allow users to work with multiple tables,
			# we munge the name when the key is "oid"
			if keyname == 'oid':
				foid = 'oid_%s_%s' % xcl # build mangled name
				k = arg[foid]
			else:
				k = arg[keyname]
		else:
			k = arg
			arg = {}
		# We want the oid for later updates if that isn't the key
		if keyname == 'oid':
			q = "SELECT * FROM %s WHERE oid=%s" % (cl, k)
		elif view:
			q = "SELECT * FROM %s WHERE %s=%s" % \
				(cl, keyname, _quote(k, fnames[keyname]))
		else:
			foid = 'oid_%s_%s' % xcl # build mangled name
			q = "SELECT oid AS %s,%s FROM %s WHERE %s=%s" % \
				(foid, ','.join(fnames.keys()), cl, \
					keyname, _quote(k, fnames[keyname]))
		self._do_debug(q)
		res = self.db.query(q).dictresult()
		if res == []:
			raise DatabaseError, \
				"No such record in %s where %s is %s" % \
					(cl, keyname, _quote(k, fnames[keyname]))
			return None
		for k in res[0].keys():
			arg[k] = res[0][k]
		return arg

	def insert(self, cl, a):
		"""Insert a tuple into a database table.

		This method inserts values into the table specified filling in the
		values from the dictionary.  It then reloads the dictionary with the
		values from the database.  This causes the dictionary to be updated
		with values that are modified by rules, triggers, etc.

		Note: The method currently doesn't support insert into views
		although PostgreSQL does.

		"""
		cl = self.split_schema(cl) # split into schema and cl
		qcl = '%s.%s' % cl # build qualified name
		foid = 'oid_%s_%s' % cl # build mangled name
		fnames = self.get_attnames(qcl)
		l = []
		n = []
		for f in fnames.keys():
			if f != 'oid' and a.has_key(f):
				l.append(_quote(a[f], fnames[f]))
				n.append(f)
		q = "INSERT INTO %s (%s) VALUES (%s)" % \
			(qcl, ','.join(n), ','.join(l))
		self._do_debug(q)
		a[foid] = self.db.query(q)
		# Reload the dictionary to catch things modified by engine.
		# Note that get() changes 'oid' below to oid_schema_table.
		# If no read perms (it can and does happen), return None.
		try:
			return self.get(qcl, a, 'oid')
		except:
			return None

	def update(self, cl, a):
		"""Update an existing row in a database table.

		Similar to insert but updates an existing row.  The update is based
		on the OID value as munged by get.  The array returned is the
		one sent modified to reflect any changes caused by the update due
		to triggers, rules, defaults, etc.

		"""
		# Update always works on the oid which get returns if available,
		# otherwise use the primary key.  Fail if neither.
		cl = self.split_schema(cl) # split into schema and cl
		qcl = '%s.%s' % cl # build qualified name
		foid = 'oid_%s_%s' % cl # build mangled oid
		if a.has_key(foid):
			where = "oid=%s" % a[foid]
		else:
			try:
				pk = self.pkey(qcl)
			except:
				raise ProgrammingError, \
					"Update needs primary key or oid as %s" % foid
			where = "%s='%s'" % (pk, a[pk])
		v = []
		k = 0
		fnames = self.get_attnames(qcl)
		for ff in fnames.keys():
			if ff != 'oid' and a.has_key(ff):
				v.append("%s=%s" % (ff, _quote(a[ff], fnames[ff])))
		if v == []:
			return None
		q = "UPDATE %s SET %s WHERE %s" % (qcl, ','.join(v), where)
		self._do_debug(q)
		self.db.query(q)
		# Reload the dictionary to catch things modified by engine:
		if a.has_key(foid):
			return self.get(qcl, a, 'oid')
		else:
			return self.get(qcl, a)

	def clear(self, cl, a = {}):
		"""

		This method clears all the attributes to values determined by the types.
		Numeric types are set to 0, dates are set to 'TODAY' and everything
		else is set to the empty string.  If the array argument is present,
		it is used as the array and any entries matching attribute names
		are cleared with everything else left unchanged.

		"""
		# At some point we will need a way to get defaults from a table.
		qcl = "%s.%s"  % self.split_schema(cl) # determine qualified name
		fnames = self.get_attnames(qcl)
		for ff in fnames.keys():
			if fnames[ff] in ['int', 'decimal', 'seq', 'money']:
				a[ff] = 0
			else:
				a[ff] = ""
		a['oid'] = 0
		return a

	def delete(self, cl, a):
		"""Delete an existing row in a database table.

		This method deletes the row from a table.
		It deletes based on the OID munged as described above.

		"""
		# Like update, delete works on the oid.
		# One day we will be testing that the record to be deleted
		# isn't referenced somewhere (or else PostgreSQL will).
		cl = self.split_schema(cl) # split into schema and cl
		qcl = '%s.%s' % cl # build qualified name
		foid = 'oid_%s_%s' % cl # build mangled oid
		q = "DELETE FROM %s WHERE oid=%s" % (qcl, a[foid])
		self._do_debug(q)
		self.db.query(q)
