/*
 * $Id$
 * PyGres, version 2.2 A Python interface for PostgreSQL database. Written by
 * D'Arcy J.M. Cain, (darcy@druid.net).  Based heavily on code written by
 * Pascal Andre, andre@chimay.via.ecp.fr. Copyright (c) 1995, Pascal Andre
 * (andre@via.ecp.fr).
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies or in
 * any new file that contains a substantial portion of this file.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
 * SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
 * ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE
 * AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE
 * AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 *
 * Further modifications copyright 1997, 1998, 1999 by D'Arcy J.M. Cain
 * (darcy@druid.net) subject to the same terms and conditions as above.
 *
 */

/* Note: This should be linked against the same C runtime lib as Python */
#include <Python.h>

#include <libpq-fe.h>

/* some definitions from <libpq/libpq-fs.h> */
#include "pgfs.h"
/* the type definitions from <catalog/pg_type.h> */
#include "pgtypes.h"

static PyObject *Error, *Warning, *InterfaceError,
	*DatabaseError, *InternalError, *OperationalError, *ProgrammingError,
	*IntegrityError, *DataError, *NotSupportedError;

#define _TOSTRING(x) #x
#define TOSTRING(x) _TOSTRING(x)
static const char *PyPgVersion = TOSTRING(PYGRESQL_VERSION);

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

#if PY_VERSION_HEX >= 0x02050000 && SIZEOF_SIZE_T != SIZEOF_INT
#define Py_InitModule4 Py_InitModule4_64
#endif

/* taken from fileobject.c */
#define BUF(v) PyString_AS_STRING((PyStringObject *)(v))

/* default values */
#define MODULE_NAME			"pgsql"
#define PG_ARRAYSIZE			1

/* flags for object validity checks */
#define CHECK_OPEN			1
#define CHECK_CLOSE			2
#define CHECK_CNX			4
#define CHECK_RESULT		8
#define CHECK_DQL			16

/* query result types */
#define RESULT_EMPTY		1
#define RESULT_DML			2
#define RESULT_DDL			3
#define RESULT_DQL			4

/* flags for move methods */
#define QUERY_MOVEFIRST		1
#define QUERY_MOVELAST		2
#define QUERY_MOVENEXT		3
#define QUERY_MOVEPREV		4

/* moves names for errors */
const char *__movename[5] =
{"", "movefirst", "movelast", "movenext", "moveprev"};

#define MAX_BUFFER_SIZE 8192	/* maximum transaction size */

#ifndef PG_VERSION_NUM
#ifdef PQnoPasswordSupplied
#define PG_VERSION_NUM 80000
#else
#define PG_VERSION_NUM 70400
#endif
#endif

/* Before 8.0, PQsetdbLogin was not thread-safe with kerberos. */
#if PG_VERSION_NUM >= 80000 || !(defined(KRB4) || defined(KRB5))
#define PQsetdbLoginIsThreadSafe 1
#endif

/* --------------------------------------------------------------------- */

/* MODULE GLOBAL VARIABLES */

#ifdef DEFAULT_VARS
static PyObject *pg_default_host;	/* default database host */
static PyObject *pg_default_base;	/* default database name */
static PyObject *pg_default_opt;	/* default connection options */
static PyObject *pg_default_tty;	/* default debug tty */
static PyObject *pg_default_port;	/* default connection port */
static PyObject *pg_default_user;	/* default username */
static PyObject *pg_default_passwd;	/* default password */
#endif	/* DEFAULT_VARS */

DL_EXPORT(void) init_pg(void);
int *get_type_array(PGresult *result, int nfields);

static PyObject *decimal = NULL, /* decimal type */
				*namedresult = NULL; /* function for getting named results */
static char decimal_point = '.'; /* decimal point used in money values */
static int use_bool = 0; /* whether or not bool objects shall be returned */

/* --------------------------------------------------------------------- */
/* OBJECTS DECLARATION */

/* pg connection object */

typedef struct
{
	PyObject_HEAD
	int			valid;				/* validity flag */
	PGconn		*cnx;				/* PostGres connection handle */
	PyObject	*notice_receiver;	/* current notice receiver */
}	pgobject;

staticforward PyTypeObject PgType;

#define is_pgobject(v) ((v)->ob_type == &PgType)

static PyObject *
pgobject_New(void)
{
	pgobject	*pgobj;

	if (!(pgobj = PyObject_NEW(pgobject, &PgType)))
		return NULL;

	pgobj->valid = 1;
	pgobj->cnx = NULL;
	pgobj->notice_receiver = NULL;

	return (PyObject *) pgobj;
}

/* pg notice result object */

typedef struct
{
	PyObject_HEAD
	pgobject	*pgcnx;		/* parent connection object */
	PGresult	const *res;	/* an error or warning */
}	pgnoticeobject;

staticforward PyTypeObject PgNoticeType;

#define is_pgnoticeobject(v) ((v)->ob_type == &PgNoticeType)

/* pg query object */

typedef struct
{
	PyObject_HEAD
	PGresult	*result;		/* result content */
	int			result_type;	/* type of previous result */
	long		current_pos;	/* current position in last result */
	long		num_rows;		/* number of (affected) rows */
}	pgqueryobject;

staticforward PyTypeObject PgQueryType;

#define is_pgqueryobject(v) ((v)->ob_type == &PgQueryType)

/* pg source object */

typedef struct
{
	PyObject_HEAD
	int			valid;			/* validity flag */
	pgobject	*pgcnx;			/* parent connection object */
	PGresult	*result;		/* result content */
	int			result_type;	/* result type (DDL/DML/DQL) */
	long		arraysize;		/* array size for fetch method */
	int			current_row;	/* current selected row */
	int			max_row;		/* number of rows in the result */
	int			num_fields;		/* number of fields in each row */
}	pgsourceobject;

staticforward PyTypeObject PgSourceType;

#define is_pgsourceobject(v) ((v)->ob_type == &PgSourceType)


#ifdef LARGE_OBJECTS
/* pg large object */

typedef struct
{
	PyObject_HEAD
	pgobject	*pgcnx;			/* parent connection object */
	Oid			lo_oid;			/* large object oid */
	int			lo_fd;			/* large object fd */
}	pglargeobject;

staticforward PyTypeObject PglargeType;

#define is_pglargeobject(v) ((v)->ob_type == &PglargeType)
#endif /* LARGE_OBJECTS */

/* --------------------------------------------------------------------- */
/* INTERNAL FUNCTIONS */

/* sets database error with sqlstate attribute */
/* This should be used when raising a subclass of DatabaseError */
static void
set_dberror(PyObject *type, const char *msg, PGresult *result)
{
	PyObject *err = NULL;
	PyObject *str;

	if (!(str = PyString_FromString(msg)))
		err = NULL;
	else
	{
		err = PyObject_CallFunctionObjArgs(type, str, NULL);
		Py_DECREF(str);
	}
	if (err)
	{
		if (result)
		{
			char *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			str = sqlstate ? PyString_FromStringAndSize(sqlstate, 5) : NULL;
		}
		else
			str = NULL;
		if (!str)
		{
			Py_INCREF(Py_None);
			str = Py_None;
		}
		PyObject_SetAttrString(err, "sqlstate", str);
		Py_DECREF(str);
		PyErr_SetObject(type, err);
		Py_DECREF(err);
	}
	else
		PyErr_SetString(type, msg);
}


/* checks connection validity */
static int
check_cnx_obj(pgobject *self)
{
	if (!self->valid)
	{
		set_dberror(OperationalError, "connection has been closed.", NULL);
		return 0;
	}
	return 1;
}

#ifdef LARGE_OBJECTS
/* checks large object validity */
static int
check_lo_obj(pglargeobject *self, int level)
{
	if (!check_cnx_obj(self->pgcnx))
		return 0;

	if (!self->lo_oid)
	{
		set_dberror(IntegrityError, "object is not valid (null oid).", NULL);
		return 0;
	}

	if (level & CHECK_OPEN)
	{
		if (self->lo_fd < 0)
		{
			PyErr_SetString(PyExc_IOError, "object is not opened.");
			return 0;
		}
	}

	if (level & CHECK_CLOSE)
	{
		if (self->lo_fd >= 0)
		{
			PyErr_SetString(PyExc_IOError, "object is already opened.");
			return 0;
		}
	}

	return 1;
}
#endif /* LARGE_OBJECTS */

/* checks source object validity */
static int
check_source_obj(pgsourceobject *self, int level)
{
	if (!self->valid)
	{
		set_dberror(OperationalError, "object has been closed", NULL);
		return 0;
	}

	if ((level & CHECK_RESULT) && !self->result)
	{
		set_dberror(DatabaseError, "no result.", NULL);
		return 0;
	}

	if ((level & CHECK_DQL) && self->result_type != RESULT_DQL)
	{
		set_dberror(DatabaseError,
			"last query did not return tuples.", self->result);
		return 0;
	}

	if ((level & CHECK_CNX) && !check_cnx_obj(self->pgcnx))
		return 0;

	return 1;
}

/* define internal types */

#define PYGRES_INT 1
#define PYGRES_LONG 2
#define PYGRES_FLOAT 3
#define PYGRES_DECIMAL 4
#define PYGRES_MONEY 5
#define PYGRES_BOOL 6
#define PYGRES_DEFAULT 7

/* shared functions for converting PG types to Python types */
int *
get_type_array(PGresult *result, int nfields)
{
	int *typ;
	int j;

	if (!(typ = malloc(sizeof(int) * nfields)))
	{
		PyErr_SetString(PyExc_MemoryError, "memory error in getresult().");
		return NULL;
	}

	for (j = 0; j < nfields; j++)
	{
		switch (PQftype(result, j))
		{
			case INT2OID:
			case INT4OID:
			case OIDOID:
				typ[j] = PYGRES_INT;
				break;

			case INT8OID:
				typ[j] = PYGRES_LONG;
				break;

			case FLOAT4OID:
			case FLOAT8OID:
				typ[j] = PYGRES_FLOAT;
				break;

			case NUMERICOID:
				typ[j] = PYGRES_DECIMAL;
				break;

			case CASHOID:
				typ[j] = PYGRES_MONEY;
				break;

			case BOOLOID:
				typ[j] = PYGRES_BOOL;
				break;

			default:
				typ[j] = PYGRES_DEFAULT;
				break;
		}
	}

	return typ;
}

/* format result (mostly useful for debugging) */
/* Note: This is similar to the Postgres function PQprint().
 * PQprint() is not used because handing over a stream from Python to
 * Postgres can be problematic if they use different libs for streams
 * and because using PQprint() and tp_print is not recommended any more.
 */

static PyObject *
format_result(const PGresult *res)
{
	const int n = PQnfields(res);

	if (n > 0)
	{
		char * const aligns = (char *) malloc(n * sizeof(char));
		int * const sizes = (int *) malloc(n * sizeof(int));

		if (aligns && sizes)
		{
			const int m = PQntuples(res);
			int i, j;
			size_t size;
			char *buffer;

			/* calculate sizes and alignments */
			for (j = 0; j < n; j++)
			{
				const char * const s = PQfname(res, j);
				const int format = PQfformat(res, j);

				sizes[j] = s ? (int)strlen(s) : 0;
				if (format)
				{
					aligns[j] = '\0';
					if (m && sizes[j] < 8)
						/* "<binary>" must fit */
						sizes[j] = 8;
				}
				else
				{
					const Oid ftype = PQftype(res, j);

					switch (ftype)
					{
						case INT2OID:
						case INT4OID:
						case INT8OID:
						case FLOAT4OID:
						case FLOAT8OID:
						case NUMERICOID:
						case OIDOID:
						case XIDOID:
						case CIDOID:
						case CASHOID:
							aligns[j] = 'r';
							break;
						default:
							aligns[j] = 'l';
							break;
					}
				}
			}
			for (i = 0; i < m; i++)
			{
				for (j = 0; j < n; j++)
				{
					if (aligns[j])
					{
						const int k = PQgetlength(res, i, j);

						if (sizes[j] < k)
							/* value must fit */
							sizes[j] = k;
					}
				}
			}
			size = 0;
			/* size of one row */
			for (j = 0; j < n; j++) size += sizes[j] + 1;
			/* times number of rows incl. heading */
			size *= (m + 2);
			/* plus size of footer */
			size += 40;
			/* is the buffer size that needs to be allocated */
			buffer = (char *) malloc(size);
			if (buffer)
			{
				char *p = buffer;
				PyObject *result;

				/* create the header */
				for (j = 0; j < n; j++)
				{
					const char * const s = PQfname(res, j);
					const int k = sizes[j];
					const int h = (k - (int)strlen(s)) / 2;

					sprintf(p, "%*s", h, "");
					sprintf(p + h, "%-*s", k - h, s);
					p += k;
					if (j + 1 < n)
						*p++ = '|';
				}
				*p++ = '\n';
				for (j = 0; j < n; j++)
				{
					int k = sizes[j];

					while (k--)
						*p++ = '-';
					if (j + 1 < n)
						*p++ = '+';
				}
				*p++ = '\n';
				/* create the body */
				for (i = 0; i < m; i++)
				{
					for (j = 0; j < n; j++)
					{
						const char align = aligns[j];
						const int k = sizes[j];

						if (align)
						{
							sprintf(p, align == 'r' ?
								"%*s" : "%-*s", k,
								PQgetvalue(res, i, j));
						}
						else
						{
							sprintf(p, "%-*s", k,
								PQgetisnull(res, i, j) ?
								"" : "<binary>");
						}
						p += k;
						if (j + 1 < n)
							*p++ = '|';
					}
					*p++ = '\n';
				}
				/* free memory */
				free(aligns);
				free(sizes);
				/* create the footer */
				sprintf(p, "(%d row%s)", m, m == 1 ? "" : "s");
				/* return the result */
				result = PyString_FromString(buffer);
				free(buffer);
				return result;
			}
			else
			{
				PyErr_SetString(PyExc_MemoryError,
					"Not enough memory for formatting the query result.");
				return NULL;
			}
		} else {
			if (aligns)
				free(aligns);
			if (sizes)
				free(aligns);
			PyErr_SetString(PyExc_MemoryError,
				"Not enough memory for formatting the query result.");
			return NULL;
		}
	}
	else
		return PyString_FromString("(nothing selected)");
}

/* prototypes for constructors */
static pgsourceobject *pgsource_new(pgobject *pgcnx);

/* --------------------------------------------------------------------- */
/* PG SOURCE OBJECT IMPLEMENTATION */

/* constructor (internal use only) */
static pgsourceobject *
pgsource_new(pgobject *pgcnx)
{
	pgsourceobject *npgobj;

	/* allocates new query object */
	if (!(npgobj = PyObject_NEW(pgsourceobject, &PgSourceType)))
		return NULL;

	/* initializes internal parameters */
	Py_XINCREF(pgcnx);
	npgobj->pgcnx = pgcnx;
	npgobj->result = NULL;
	npgobj->valid = 1;
	npgobj->arraysize = PG_ARRAYSIZE;

	return npgobj;
}

/* destructor */
static void
pgsource_dealloc(pgsourceobject *self)
{
	if (self->result)
		PQclear(self->result);

	Py_XDECREF(self->pgcnx);
	PyObject_Del(self);
}

/* closes object */
static char pgsource_close__doc__[] =
"close() -- close query object without deleting it. "
"All instances of the query object can no longer be used after this call.";

static PyObject *
pgsource_close(pgsourceobject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError, "method close() takes no parameter.");
		return NULL;
	}

	/* frees result if necessary and invalidates object */
	if (self->result)
	{
		PQclear(self->result);
		self->result_type = RESULT_EMPTY;
		self->result = NULL;
	}

	self->valid = 0;

	/* return None */
	Py_INCREF(Py_None);
	return Py_None;
}

/* database query */
static char pgsource_execute__doc__[] =
"execute(sql) -- execute a SQL statement (string).\n "
"On success, this call returns the number of affected rows, "
"or None for DQL (SELECT, ...) statements.\n"
"The fetch (fetch(), fetchone() and fetchall()) methods can be used "
"to get result rows.";

static PyObject *
pgsource_execute(pgsourceobject *self, PyObject *args)
{
	char		*query;

	/* checks validity */
	if (!check_source_obj(self, CHECK_CNX))
		return NULL;

	/* make sure that the connection object is valid */
	if (!self->pgcnx->cnx)
		return NULL;

	/* get query args */
	if (!PyArg_ParseTuple(args, "s", &query))
	{
		PyErr_SetString(PyExc_TypeError, "execute(sql), with sql (string).");
		return NULL;
	}

	/* frees previous result */
	if (self->result)
	{
		PQclear(self->result);
		self->result = NULL;
	}
	self->max_row = 0;
	self->current_row = 0;
	self->num_fields = 0;

	/* gets result */
	Py_BEGIN_ALLOW_THREADS
	self->result = PQexec(self->pgcnx->cnx, query);
	Py_END_ALLOW_THREADS

	/* checks result validity */
	if (!self->result)
	{
		PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->pgcnx->cnx));
		return NULL;
	}

	/* checks result status */
	switch (PQresultStatus(self->result))
	{
		long	num_rows;
		char   *temp;

		/* query succeeded */
		case PGRES_TUPLES_OK:	/* DQL: returns None (DB-SIG compliant) */
			self->result_type = RESULT_DQL;
			self->max_row = PQntuples(self->result);
			self->num_fields = PQnfields(self->result);
			Py_INCREF(Py_None);
			return Py_None;
		case PGRES_COMMAND_OK:	/* other requests */
		case PGRES_COPY_OUT:
		case PGRES_COPY_IN:
			self->result_type = RESULT_DDL;
			temp = PQcmdTuples(self->result);
			num_rows = -1;
			if (temp[0])
			{
				self->result_type = RESULT_DML;
				num_rows = atol(temp);
			}
			return PyInt_FromLong(num_rows);

		/* query failed */
		case PGRES_EMPTY_QUERY:
			PyErr_SetString(PyExc_ValueError, "empty query.");
			break;
		case PGRES_BAD_RESPONSE:
		case PGRES_FATAL_ERROR:
		case PGRES_NONFATAL_ERROR:
			set_dberror(ProgrammingError,
				PQerrorMessage(self->pgcnx->cnx), self->result);
			break;
		default:
			set_dberror(InternalError, "internal error: "
				"unknown result status.", self->result);
			break;
	}

	/* frees result and returns error */
	PQclear(self->result);
	self->result = NULL;
	self->result_type = RESULT_EMPTY;
	return NULL;
}

/* gets oid status for last query (valid for INSERTs, 0 for other) */
static char pgsource_oidstatus__doc__[] =
"oidstatus() -- return oid of last inserted row (if available).";

static PyObject *
pgsource_oidstatus(pgsourceobject *self, PyObject *args)
{
	Oid			oid;

	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT))
		return NULL;

	/* checks args */
	if (args && !PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method oidstatus() takes no parameters.");
		return NULL;
	}

	/* retrieves oid status */
	if ((oid = PQoidValue(self->result)) == InvalidOid)
	{
		Py_INCREF(Py_None);
		return Py_None;
	}

	return PyInt_FromLong(oid);
}

/* fetches rows from last result */
static char pgsource_fetch__doc__[] =
"fetch(num) -- return the next num rows from the last result in a list. "
"If num parameter is omitted arraysize attribute value is used. "
"If size equals -1, all rows are fetched.";

static PyObject *
pgsource_fetch(pgsourceobject *self, PyObject *args)
{
	PyObject   *rowtuple,
			   *reslist,
			   *str;
	int			i,
				j;
	long		size;

	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
		return NULL;

	/* checks args */
	size = self->arraysize;
	if (!PyArg_ParseTuple(args, "|l", &size))
	{
		PyErr_SetString(PyExc_TypeError,
			"fetch(num), with num (integer, optional).");
		return NULL;
	}

	/* seeks last line */
	/* limit size to be within the amount of data we actually have */
	if (size == -1 || (self->max_row - self->current_row) < size)
		size = self->max_row - self->current_row;

	/* allocate list for result */
	if (!(reslist = PyList_New(0)))
		return NULL;

	/* builds result */
	for (i = 0; i < size; i++)
	{
		if (!(rowtuple = PyTuple_New(self->num_fields)))
		{
			Py_DECREF(reslist);
			return NULL;
		}

		for (j = 0; j < self->num_fields; j++)
		{
			if (PQgetisnull(self->result, self->current_row, j))
			{
				Py_INCREF(Py_None);
				str = Py_None;
			}
			else
				str = PyString_FromString(PQgetvalue(self->result, self->current_row, j));

			PyTuple_SET_ITEM(rowtuple, j, str);
		}

		PyList_Append(reslist, rowtuple);
		Py_DECREF(rowtuple);
		self->current_row++;
	}

	return reslist;
}

/* changes current row (internal wrapper for all "move" methods) */
static PyObject *
pgsource_move(pgsourceobject *self, PyObject *args, int move)
{
	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
		return NULL;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		char		errbuf[256];
		PyOS_snprintf(errbuf, sizeof(errbuf),
			"method %s() takes no parameter.", __movename[move]);
		PyErr_SetString(PyExc_TypeError, errbuf);
		return NULL;
	}

	/* changes the current row */
	switch (move)
	{
		case QUERY_MOVEFIRST:
			self->current_row = 0;
			break;
		case QUERY_MOVELAST:
			self->current_row = self->max_row - 1;
			break;
		case QUERY_MOVENEXT:
			if (self->current_row != self->max_row)
				self->current_row++;
			break;
		case QUERY_MOVEPREV:
			if (self->current_row > 0)
				self->current_row--;
			break;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

/* move to first result row */
static char pgsource_movefirst__doc__[] =
"movefirst() -- move to first result row.";

static PyObject *
pgsource_movefirst(pgsourceobject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVEFIRST);
}

/* move to last result row */
static char pgsource_movelast__doc__[] =
"movelast() -- move to last valid result row.";

static PyObject *
pgsource_movelast(pgsourceobject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVELAST);
}

/* move to next result row */
static char pgsource_movenext__doc__[] =
"movenext() -- move to next result row.";

static PyObject *
pgsource_movenext(pgsourceobject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVENEXT);
}

/* move to previous result row */
static char pgsource_moveprev__doc__[] =
"moveprev() -- move to previous result row.";

static PyObject *
pgsource_moveprev(pgsourceobject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVEPREV);
}

/* finds field number from string/integer (internal use only) */
static int
pgsource_fieldindex(pgsourceobject *self, PyObject *param, const char *usage)
{
	int			num;

	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
		return -1;

	/* gets field number */
	if (PyString_Check(param))
		num = PQfnumber(self->result, PyString_AsString(param));
	else if (PyInt_Check(param))
		num = PyInt_AsLong(param);
	else
	{
		PyErr_SetString(PyExc_TypeError, usage);
		return -1;
	}

	/* checks field validity */
	if (num < 0 || num >= self->num_fields)
	{
		PyErr_SetString(PyExc_ValueError, "Unknown field.");
		return -1;
	}

	return num;
}

/* builds field information from position (internal use only) */
static PyObject *
pgsource_buildinfo(pgsourceobject *self, int num)
{
	PyObject *result;

	/* allocates tuple */
	result = PyTuple_New(3);
	if (!result)
		return NULL;

	/* affects field information */
	PyTuple_SET_ITEM(result, 0, PyInt_FromLong(num));
	PyTuple_SET_ITEM(result, 1,
		PyString_FromString(PQfname(self->result, num)));
	PyTuple_SET_ITEM(result, 2,
		PyInt_FromLong(PQftype(self->result, num)));

	return result;
}

/* lists fields info */
static char pgsource_listinfo__doc__[] =
"listinfo() -- return information for all fields "
"(position, name, type oid).";

static PyObject *
pgsource_listinfo(pgsourceobject *self, PyObject *args)
{
	int			i;
	PyObject   *result,
			   *info;

	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
		return NULL;

	/* gets args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method listinfo() takes no parameter.");
		return NULL;
	}

	/* builds result */
	if (!(result = PyTuple_New(self->num_fields)))
		return NULL;

	for (i = 0; i < self->num_fields; i++)
	{
		info = pgsource_buildinfo(self, i);
		if (!info)
		{
			Py_DECREF(result);
			return NULL;
		}
		PyTuple_SET_ITEM(result, i, info);
	}

	/* returns result */
	return result;
};

/* list fields information for last result */
static char pgsource_fieldinfo__doc__[] =
"fieldinfo(string|integer) -- return specified field information "
"(position, name, type oid).";

static PyObject *
pgsource_fieldinfo(pgsourceobject *self, PyObject *args)
{
	static const char short_usage[] =
	"fieldinfo(desc), with desc (string|integer).";
	int			num;
	PyObject   *param;

	/* gets args */
	if (!PyArg_ParseTuple(args, "O", &param))
	{
		PyErr_SetString(PyExc_TypeError, short_usage);
		return NULL;
	}

	/* checks args and validity */
	if ((num = pgsource_fieldindex(self, param, short_usage)) == -1)
		return NULL;

	/* returns result */
	return pgsource_buildinfo(self, num);
};

/* retrieve field value */
static char pgsource_field__doc__[] =
"field(string|integer) -- return specified field value.";

static PyObject *
pgsource_field(pgsourceobject *self, PyObject *args)
{
	static const char short_usage[] =
	"field(desc), with desc (string|integer).";
	int			num;
	PyObject   *param;

	/* gets args */
	if (!PyArg_ParseTuple(args, "O", &param))
	{
		PyErr_SetString(PyExc_TypeError, short_usage);
		return NULL;
	}

	/* checks args and validity */
	if ((num = pgsource_fieldindex(self, param, short_usage)) == -1)
		return NULL;

	return PyString_FromString(PQgetvalue(self->result,
									self->current_row, num));
}

/* query object methods */
static PyMethodDef pgsource_methods[] = {
	{"close", (PyCFunction) pgsource_close, METH_VARARGS,
			pgsource_close__doc__},
	{"execute", (PyCFunction) pgsource_execute, METH_VARARGS,
			pgsource_execute__doc__},
	{"oidstatus", (PyCFunction) pgsource_oidstatus, METH_VARARGS,
			pgsource_oidstatus__doc__},
	{"fetch", (PyCFunction) pgsource_fetch, METH_VARARGS,
			pgsource_fetch__doc__},
	{"movefirst", (PyCFunction) pgsource_movefirst, METH_VARARGS,
			pgsource_movefirst__doc__},
	{"movelast", (PyCFunction) pgsource_movelast, METH_VARARGS,
			pgsource_movelast__doc__},
	{"movenext", (PyCFunction) pgsource_movenext, METH_VARARGS,
			pgsource_movenext__doc__},
	{"moveprev", (PyCFunction) pgsource_moveprev, METH_VARARGS,
			pgsource_moveprev__doc__},
	{"field", (PyCFunction) pgsource_field, METH_VARARGS,
			pgsource_field__doc__},
	{"fieldinfo", (PyCFunction) pgsource_fieldinfo, METH_VARARGS,
			pgsource_fieldinfo__doc__},
	{"listinfo", (PyCFunction) pgsource_listinfo, METH_VARARGS,
			pgsource_listinfo__doc__},
	{NULL, NULL}
};

/* gets query object attributes */
static PyObject *
pgsource_getattr(pgsourceobject *self, char *name)
{
	/* pg connection object */
	if (!strcmp(name, "pgcnx"))
	{
		if (check_source_obj(self, 0))
		{
			Py_INCREF(self->pgcnx);
			return (PyObject *) (self->pgcnx);
		}
		Py_INCREF(Py_None);
		return Py_None;
	}

	/* arraysize */
	if (!strcmp(name, "arraysize"))
		return PyInt_FromLong(self->arraysize);

	/* resulttype */
	if (!strcmp(name, "resulttype"))
		return PyInt_FromLong(self->result_type);

	/* ntuples */
	if (!strcmp(name, "ntuples"))
		return PyInt_FromLong(self->max_row);

	/* nfields */
	if (!strcmp(name, "nfields"))
		return PyInt_FromLong(self->num_fields);

	/* attributes list */
	if (!strcmp(name, "__members__"))
	{
		PyObject *list = PyList_New(5);

		PyList_SET_ITEM(list, 0, PyString_FromString("pgcnx"));
		PyList_SET_ITEM(list, 1, PyString_FromString("arraysize"));
		PyList_SET_ITEM(list, 2, PyString_FromString("resulttype"));
		PyList_SET_ITEM(list, 3, PyString_FromString("ntuples"));
		PyList_SET_ITEM(list, 4, PyString_FromString("nfields"));

		return list;
	}

	/* module name */
	if (!strcmp(name, "__module__"))
		return PyString_FromString(MODULE_NAME);

	/* class name */
	if (!strcmp(name, "__class__"))
		return PyString_FromString("pgsource");

	/* seeks name in methods (fallback) */
	return Py_FindMethod(pgsource_methods, (PyObject *) self, name);
}

/* sets query object attributes */
static int
pgsource_setattr(pgsourceobject *self, char *name, PyObject *v)
{
	/* arraysize */
	if (!strcmp(name, "arraysize"))
	{
		if (!PyInt_Check(v))
		{
			PyErr_SetString(PyExc_TypeError, "arraysize must be integer.");
			return -1;
		}

		self->arraysize = PyInt_AsLong(v);
		return 0;
	}

	/* unknown attribute */
	PyErr_SetString(PyExc_TypeError, "not a writable attribute.");
	return -1;
}

static PyObject *
pgsource_repr(pgsourceobject *self)
{
	return PyString_FromString("<pg source object>");
}

/* returns source object as string in human readable format */

static PyObject *
pgsource_str(pgsourceobject *self)
{
	switch (self->result_type)
	{
		case RESULT_DQL:
			return format_result(self->result);
		case RESULT_DDL:
		case RESULT_DML:
			return PyString_FromString(PQcmdStatus(self->result));
		case RESULT_EMPTY:
		default:
			return PyString_FromString("(empty PostgreSQL source object)");
	}
}

/* query type definition */
staticforward PyTypeObject PgSourceType = {
	PyObject_HEAD_INIT(NULL)
	0,								/* ob_size */
	"pgsourceobject",				/* tp_name */
	sizeof(pgsourceobject),			/* tp_basicsize */
	0,								/* tp_itemsize */
	/* methods */
	(destructor) pgsource_dealloc,	/* tp_dealloc */
	0,								/* tp_print */
	(getattrfunc) pgsource_getattr,	/* tp_getattr */
	(setattrfunc) pgsource_setattr,	/* tp_setattr */
	0,								/* tp_compare */
	(reprfunc) pgsource_repr,		/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) pgsource_str,		/* tp_str */
};

/* --------------------------------------------------------------------- */
/* PG "LARGE" OBJECT IMPLEMENTATION */

#ifdef LARGE_OBJECTS

/* constructor (internal use only) */
static pglargeobject *
pglarge_new(pgobject *pgcnx, Oid oid)
{
	pglargeobject *npglo;

	if (!(npglo = PyObject_NEW(pglargeobject, &PglargeType)))
		return NULL;

	Py_XINCREF(pgcnx);
	npglo->pgcnx = pgcnx;
	npglo->lo_fd = -1;
	npglo->lo_oid = oid;

	return npglo;
}

/* destructor */
static void
pglarge_dealloc(pglargeobject *self)
{
	if (self->lo_fd >= 0 && check_cnx_obj(self->pgcnx))
		lo_close(self->pgcnx->cnx, self->lo_fd);

	Py_XDECREF(self->pgcnx);
	PyObject_Del(self);
}

/* opens large object */
static char pglarge_open__doc__[] =
"open(mode) -- open access to large object with specified mode "
"(INV_READ, INV_WRITE constants defined by module).";

static PyObject *
pglarge_open(pglargeobject *self, PyObject *args)
{
	int			mode,
				fd;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "i", &mode))
	{
		PyErr_SetString(PyExc_TypeError, "open(mode), with mode(integer).");
		return NULL;
	}

	/* check validity */
	if (!check_lo_obj(self, CHECK_CLOSE))
		return NULL;

	/* opens large object */
	if ((fd = lo_open(self->pgcnx->cnx, self->lo_oid, mode)) < 0)
	{
		PyErr_SetString(PyExc_IOError, "can't open large object.");
		return NULL;
	}
	self->lo_fd = fd;

	/* no error : returns Py_None */
	Py_INCREF(Py_None);
	return Py_None;
}

/* close large object */
static char pglarge_close__doc__[] =
"close() -- close access to large object data.";

static PyObject *
pglarge_close(pglargeobject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method close() takes no parameters.");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* closes large object */
	if (lo_close(self->pgcnx->cnx, self->lo_fd))
	{
		PyErr_SetString(PyExc_IOError, "error while closing large object fd.");
		return NULL;
	}
	self->lo_fd = -1;

	/* no error : returns Py_None */
	Py_INCREF(Py_None);
	return Py_None;
}

/* reads from large object */
static char pglarge_read__doc__[] =
"read(integer) -- read from large object to sized string. "
"Object must be opened in read mode before calling this method.";

static PyObject *
pglarge_read(pglargeobject *self, PyObject *args)
{
	int			size;
	PyObject   *buffer;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "i", &size))
	{
		PyErr_SetString(PyExc_TypeError, "read(size), with size (integer).");
		return NULL;
	}

	if (size <= 0)
	{
		PyErr_SetString(PyExc_ValueError, "size must be positive.");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* allocate buffer and runs read */
	buffer = PyString_FromStringAndSize((char *) NULL, size);

	if ((size = lo_read(self->pgcnx->cnx, self->lo_fd, BUF(buffer), size)) < 0)
	{
		PyErr_SetString(PyExc_IOError, "error while reading.");
		Py_XDECREF(buffer);
		return NULL;
	}

	/* resize buffer and returns it */
	_PyString_Resize(&buffer, size);
	return buffer;
}

/* write to large object */
static char pglarge_write__doc__[] =
"write(string) -- write sized string to large object. "
"Object must be opened in read mode before calling this method.";

static PyObject *
pglarge_write(pglargeobject *self, PyObject *args)
{
	char	   *buffer;
	int			size,
				bufsize;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "s#", &buffer, &bufsize))
	{
		PyErr_SetString(PyExc_TypeError,
			"write(buffer), with buffer (sized string).");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* sends query */
	if ((size = lo_write(self->pgcnx->cnx, self->lo_fd, buffer,
						 bufsize)) < bufsize)
	{
		PyErr_SetString(PyExc_IOError, "buffer truncated during write.");
		return NULL;
	}

	/* no error : returns Py_None */
	Py_INCREF(Py_None);
	return Py_None;
}

/* go to position in large object */
static char pglarge_seek__doc__[] =
"seek(off, whence) -- move to specified position. Object must be opened "
"before calling this method. whence can be SEEK_SET, SEEK_CUR or SEEK_END, "
"constants defined by module.";

static PyObject *
pglarge_lseek(pglargeobject *self, PyObject *args)
{
	/* offset and whence are initialized to keep compiler happy */
	int			ret,
				offset = 0,
				whence = 0;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "ii", &offset, &whence))
	{
		PyErr_SetString(PyExc_TypeError,
			"lseek(offset, whence), with offset and whence (integers).");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* sends query */
	if ((ret = lo_lseek(self->pgcnx->cnx, self->lo_fd, offset, whence)) == -1)
	{
		PyErr_SetString(PyExc_IOError, "error while moving cursor.");
		return NULL;
	}

	/* returns position */
	return PyInt_FromLong(ret);
}

/* gets large object size */
static char pglarge_size__doc__[] =
"size() -- return large object size. "
"Object must be opened before calling this method.";

static PyObject *
pglarge_size(pglargeobject *self, PyObject *args)
{
	int			start,
				end;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method size() takes no parameters.");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* gets current position */
	if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1)
	{
		PyErr_SetString(PyExc_IOError, "error while getting current position.");
		return NULL;
	}

	/* gets end position */
	if ((end = lo_lseek(self->pgcnx->cnx, self->lo_fd, 0, SEEK_END)) == -1)
	{
		PyErr_SetString(PyExc_IOError, "error while getting end position.");
		return NULL;
	}

	/* move back to start position */
	if ((start = lo_lseek(self->pgcnx->cnx, self->lo_fd, start, SEEK_SET)) == -1)
	{
		PyErr_SetString(PyExc_IOError,
			"error while moving back to first position.");
		return NULL;
	}

	/* returns size */
	return PyInt_FromLong(end);
}

/* gets large object cursor position */
static char pglarge_tell__doc__[] =
"tell() -- give current position in large object. "
"Object must be opened before calling this method.";

static PyObject *
pglarge_tell(pglargeobject *self, PyObject *args)
{
	int			start;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method tell() takes no parameters.");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_OPEN))
		return NULL;

	/* gets current position */
	if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1)
	{
		PyErr_SetString(PyExc_IOError, "error while getting position.");
		return NULL;
	}

	/* returns size */
	return PyInt_FromLong(start);
}

/* exports large object as unix file */
static char pglarge_export__doc__[] =
"export(string) -- export large object data to specified file. "
"Object must be closed when calling this method.";

static PyObject *
pglarge_export(pglargeobject *self, PyObject *args)
{
	char *name;

	/* checks validity */
	if (!check_lo_obj(self, CHECK_CLOSE))
		return NULL;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "s", &name))
	{
		PyErr_SetString(PyExc_TypeError,
			"export(filename), with filename (string).");
		return NULL;
	}

	/* runs command */
	if (!lo_export(self->pgcnx->cnx, self->lo_oid, name))
	{
		PyErr_SetString(PyExc_IOError, "error while exporting large object.");
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

/* deletes a large object */
static char pglarge_unlink__doc__[] =
"unlink() -- destroy large object. "
"Object must be closed when calling this method.";

static PyObject *
pglarge_unlink(pglargeobject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method unlink() takes no parameters.");
		return NULL;
	}

	/* checks validity */
	if (!check_lo_obj(self, CHECK_CLOSE))
		return NULL;

	/* deletes the object, invalidate it on success */
	if (!lo_unlink(self->pgcnx->cnx, self->lo_oid))
	{
		PyErr_SetString(PyExc_IOError, "error while unlinking large object");
		return NULL;
	}
	self->lo_oid = 0;

	Py_INCREF(Py_None);
	return Py_None;
}


/* large object methods */
static struct PyMethodDef pglarge_methods[] = {
	{"open", (PyCFunction) pglarge_open, METH_VARARGS, pglarge_open__doc__},
	{"close", (PyCFunction) pglarge_close, METH_VARARGS, pglarge_close__doc__},
	{"read", (PyCFunction) pglarge_read, METH_VARARGS, pglarge_read__doc__},
	{"write", (PyCFunction) pglarge_write, METH_VARARGS, pglarge_write__doc__},
	{"seek", (PyCFunction) pglarge_lseek, METH_VARARGS, pglarge_seek__doc__},
	{"size", (PyCFunction) pglarge_size, METH_VARARGS, pglarge_size__doc__},
	{"tell", (PyCFunction) pglarge_tell, METH_VARARGS, pglarge_tell__doc__},
	{"export",(PyCFunction) pglarge_export,METH_VARARGS,pglarge_export__doc__},
	{"unlink",(PyCFunction) pglarge_unlink,METH_VARARGS,pglarge_unlink__doc__},
	{NULL, NULL}
};

/* get attribute */
static PyObject *
pglarge_getattr(pglargeobject *self, char *name)
{
	/* list postgreSQL large object fields */

	/* associated pg connection object */
	if (!strcmp(name, "pgcnx"))
	{
		if (check_lo_obj(self, 0))
		{
			Py_INCREF(self->pgcnx);
			return (PyObject *) (self->pgcnx);
		}

		Py_INCREF(Py_None);
		return Py_None;
	}

	/* large object oid */
	if (!strcmp(name, "oid"))
	{
		if (check_lo_obj(self, 0))
			return PyInt_FromLong(self->lo_oid);

		Py_INCREF(Py_None);
		return Py_None;
	}

	/* error (status) message */
	if (!strcmp(name, "error"))
		return PyString_FromString(PQerrorMessage(self->pgcnx->cnx));

	/* attributes list */
	if (!strcmp(name, "__members__"))
	{
		PyObject *list = PyList_New(3);

		if (list)
		{
			PyList_SET_ITEM(list, 0, PyString_FromString("oid"));
			PyList_SET_ITEM(list, 1, PyString_FromString("pgcnx"));
			PyList_SET_ITEM(list, 2, PyString_FromString("error"));
		}

		return list;
	}

	/* module name */
	if (!strcmp(name, "__module__"))
		return PyString_FromString(MODULE_NAME);

	/* class name */
	if (!strcmp(name, "__class__"))
		return PyString_FromString("pglarge");

	/* seeks name in methods (fallback) */
	return Py_FindMethod(pglarge_methods, (PyObject *) self, name);
}

/* prints query object in human readable format */
static int
pglarge_print(pglargeobject *self, FILE *fp, int flags)
{
	char		print_buffer[128];
	PyOS_snprintf(print_buffer, sizeof(print_buffer),
		self->lo_fd >= 0 ?
			"Opened large object, oid %ld" :
			"Closed large object, oid %ld", (long) self->lo_oid);
	fputs(print_buffer, fp);
	return 0;
}

/* object type definition */
staticforward PyTypeObject PglargeType = {
	PyObject_HEAD_INIT(NULL)
	0,							/* ob_size */
	"pglarge",					/* tp_name */
	sizeof(pglargeobject),		/* tp_basicsize */
	0,							/* tp_itemsize */

	/* methods */
	(destructor) pglarge_dealloc,		/* tp_dealloc */
	(printfunc) pglarge_print,	/* tp_print */
	(getattrfunc) pglarge_getattr,		/* tp_getattr */
	0,							/* tp_setattr */
	0,							/* tp_compare */
	0,							/* tp_repr */
	0,							/* tp_as_number */
	0,							/* tp_as_sequence */
	0,							/* tp_as_mapping */
	0,							/* tp_hash */
};
#endif /* LARGE_OBJECTS */


/* --------------------------------------------------------------------- */
/* PG QUERY OBJECT IMPLEMENTATION */

/* connects to a database */
static char connect__doc__[] =
"connect(dbname, host, port, opt, tty) -- connect to a PostgreSQL database "
"using specified parameters (optionals, keywords aware).";

static PyObject *
pgconnect(pgobject *self, PyObject *args, PyObject *dict)
{
	static const char *kwlist[] = {"dbname", "host", "port", "opt",
	"tty", "user", "passwd", NULL};

	char	   *pghost,
			   *pgopt,
			   *pgtty,
			   *pgdbname,
			   *pguser,
			   *pgpasswd;
	int			pgport;
	char		port_buffer[20];
	pgobject   *npgobj;

	pghost = pgopt = pgtty = pgdbname = pguser = pgpasswd = NULL;
	pgport = -1;

	/*
	 * parses standard arguments With the right compiler warnings, this
	 * will issue a diagnostic. There is really no way around it.  If I
	 * don't declare kwlist as const char *kwlist[] then it complains when
	 * I try to assign all those constant strings to it.
	 */
	if (!PyArg_ParseTupleAndKeywords(args, dict, "|zzizzzz", (char **) kwlist,
		&pgdbname, &pghost, &pgport, &pgopt, &pgtty, &pguser, &pgpasswd))
		return NULL;

#ifdef DEFAULT_VARS
	/* handles defaults variables (for uninitialised vars) */
	if ((!pghost) && (pg_default_host != Py_None))
		pghost = PyString_AsString(pg_default_host);

	if ((pgport == -1) && (pg_default_port != Py_None))
		pgport = PyInt_AsLong(pg_default_port);

	if ((!pgopt) && (pg_default_opt != Py_None))
		pgopt = PyString_AsString(pg_default_opt);

	if ((!pgtty) && (pg_default_tty != Py_None))
		pgtty = PyString_AsString(pg_default_tty);

	if ((!pgdbname) && (pg_default_base != Py_None))
		pgdbname = PyString_AsString(pg_default_base);

	if ((!pguser) && (pg_default_user != Py_None))
		pguser = PyString_AsString(pg_default_user);

	if ((!pgpasswd) && (pg_default_passwd != Py_None))
		pgpasswd = PyString_AsString(pg_default_passwd);
#endif /* DEFAULT_VARS */

	if (!(npgobj = (pgobject *) pgobject_New()))
		return NULL;

	if (pgport != -1)
	{
		memset(port_buffer, 0, sizeof(port_buffer));
		sprintf(port_buffer, "%d", pgport);
	}

#ifdef PQsetdbLoginIsThreadSafe
	Py_BEGIN_ALLOW_THREADS
#endif
	npgobj->cnx = PQsetdbLogin(pghost, pgport == -1 ? NULL : port_buffer,
		pgopt, pgtty, pgdbname, pguser, pgpasswd);
#ifdef PQsetdbLoginIsThreadSafe
	Py_END_ALLOW_THREADS
#endif

	if (PQstatus(npgobj->cnx) == CONNECTION_BAD)
	{
		set_dberror(InternalError, PQerrorMessage(npgobj->cnx), NULL);
		Py_XDECREF(npgobj);
		return NULL;
	}

	return (PyObject *) npgobj;
}

/* internal wrapper for the notice receiver callback */
void notice_receiver(void *arg, const PGresult *res)
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	pgobject *self = (pgobject*) arg;
	PyObject *proc = self->notice_receiver;
	if (proc && PyCallable_Check(proc))
	{
		pgnoticeobject *notice = PyObject_NEW(pgnoticeobject, &PgNoticeType);
		PyObject *args, *ret;
		if (notice)
		{
			notice->pgcnx = arg;
			notice->res = res;
		}
		else
		{
			Py_INCREF(Py_None);
			notice = (pgnoticeobject *)(void *)Py_None;
		}
		args = Py_BuildValue("(O)", notice);
		ret = PyObject_CallObject(proc, args);
		Py_XDECREF(ret);
		Py_DECREF(args);
	}
	PyGILState_Release(gstate);
}

/* pgobject methods */

/* destructor */
static void
pg_dealloc(pgobject *self)
{
	if (self->cnx)
	{
		Py_BEGIN_ALLOW_THREADS
		PQfinish(self->cnx);
		Py_END_ALLOW_THREADS
	}
	if (self->notice_receiver)
	{
		Py_DECREF(self->notice_receiver);
	}
	PyObject_Del(self);
}

/* close without deleting */
static char pg_close__doc__[] =
"close() -- close connection. All instances of the connection object and "
"derived objects (queries and large objects) can no longer be used after "
"this call.";

static PyObject *
pg_close(pgobject *self, PyObject *args)
{
	/* gets args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError, "close().");
		return NULL;
	}

	/* connection object cannot already be closed */
	if (!self->cnx)
	{
		set_dberror(InternalError, "Connection already closed", NULL);
		return NULL;
	}

	Py_BEGIN_ALLOW_THREADS
	PQfinish(self->cnx);
	Py_END_ALLOW_THREADS

	self->cnx = NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

static void
pgquery_dealloc(pgqueryobject *self)
{
	if (self->result)
		PQclear(self->result);

	PyObject_Del(self);
}

/* resets connection */
static char pg_reset__doc__[] =
"reset() -- reset connection with current parameters. All derived queries "
"and large objects derived from this connection will not be usable after "
"this call.";

static PyObject *
pg_reset(pgobject *self, PyObject *args)
{
	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method reset() takes no parameters.");
		return NULL;
	}

	/* resets the connection */
	PQreset(self->cnx);
	Py_INCREF(Py_None);
	return Py_None;
}

/* cancels current command */
static char pg_cancel__doc__[] =
"cancel() -- abandon processing of the current command.";

static PyObject *
pg_cancel(pgobject *self, PyObject *args)
{
	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method cancel() takes no parameters.");
		return NULL;
	}

	/* request that the server abandon processing of the current command */
	return PyInt_FromLong((long) PQrequestCancel(self->cnx));
}

/* get connection socket */
static char pg_fileno__doc__[] =
"fileno() -- return database connection socket file handle.";

static PyObject *
pg_fileno(pgobject *self, PyObject *args)
{
	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method fileno() takes no parameters.");
		return NULL;
	}

#ifdef NO_PQSOCKET
	return PyInt_FromLong((long) self->cnx->sock);
#else
	return PyInt_FromLong((long) PQsocket(self->cnx));
#endif
}

/* set notice receiver callback function */
static char pg_set_notice_receiver__doc__[] =
"set_notice_receiver() -- set the current notice receiver.";

static PyObject *
pg_set_notice_receiver(pgobject * self, PyObject * args)
{
	PyObject *ret = NULL;
	PyObject *proc;

	if (PyArg_ParseTuple(args, "O", &proc))
	{
		if (PyCallable_Check(proc))
		{
			Py_XINCREF(proc);
			self->notice_receiver = proc;
			PQsetNoticeReceiver(self->cnx, notice_receiver, self);
			Py_INCREF(Py_None); ret = Py_None;
		}
		else
			PyErr_SetString(PyExc_TypeError, "notice receiver must be callable");
	}
	return ret;
}

/* get notice receiver callback function */
static char pg_get_notice_receiver__doc__[] =
"get_notice_receiver() -- get the current notice receiver.";

static PyObject *
pg_get_notice_receiver(pgobject * self, PyObject * args)
{
	PyObject *ret = NULL;

	if (PyArg_ParseTuple(args, ""))
	{
		ret = self->notice_receiver;
		if (!ret)
			ret = Py_None;
		Py_INCREF(ret);
	}
	else
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_notice_receiver() takes no parameters.");
	}
	return ret;
}

/* get number of rows */
static char pgquery_ntuples__doc__[] =
"ntuples() -- returns number of tuples returned by query.";

static PyObject *
pgquery_ntuples(pgqueryobject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method ntuples() takes no parameters.");
		return NULL;
	}

	return PyInt_FromLong((long) PQntuples(self->result));
}

/* list fields names from query result */
static char pgquery_listfields__doc__[] =
"listfields() -- Lists field names from result.";

static PyObject *
pgquery_listfields(pgqueryobject *self, PyObject *args)
{
	int			i,
				n;
	char	   *name;
	PyObject   *fieldstuple,
			   *str;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method listfields() takes no parameters.");
		return NULL;
	}

	/* builds tuple */
	n = PQnfields(self->result);
	fieldstuple = PyTuple_New(n);

	for (i = 0; i < n; i++)
	{
		name = PQfname(self->result, i);
		str = PyString_FromString(name);
		PyTuple_SET_ITEM(fieldstuple, i, str);
	}

	return fieldstuple;
}

/* get field name from last result */
static char pgquery_fieldname__doc__[] =
"fieldname() -- returns name of field from result from its position.";

static PyObject *
pgquery_fieldname(pgqueryobject *self, PyObject *args)
{
	int		i;
	char   *name;

	/* gets args */
	if (!PyArg_ParseTuple(args, "i", &i))
	{
		PyErr_SetString(PyExc_TypeError,
			"fieldname(number), with number(integer).");
		return NULL;
	}

	/* checks number validity */
	if (i >= PQnfields(self->result))
	{
		PyErr_SetString(PyExc_ValueError, "invalid field number.");
		return NULL;
	}

	/* gets fields name and builds object */
	name = PQfname(self->result, i);
	return PyString_FromString(name);
}

/* gets fields number from name in last result */
static char pgquery_fieldnum__doc__[] =
"fieldnum() -- returns position in query for field from its name.";

static PyObject *
pgquery_fieldnum(pgqueryobject *self, PyObject *args)
{
	int		num;
	char   *name;

	/* gets args */
	if (!PyArg_ParseTuple(args, "s", &name))
	{
		PyErr_SetString(PyExc_TypeError, "fieldnum(name), with name (string).");
		return NULL;
	}

	/* gets field number */
	if ((num = PQfnumber(self->result, name)) == -1)
	{
		PyErr_SetString(PyExc_ValueError, "Unknown field.");
		return NULL;
	}

	return PyInt_FromLong(num);
}

/* retrieves last result */
static char pgquery_getresult__doc__[] =
"getresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a tuple of fields in the order returned "
"by the server.";

static PyObject *
pgquery_getresult(pgqueryobject *self, PyObject *args)
{
	PyObject   *rowtuple,
			   *reslist,
			   *val;
	int			i,
				j,
				m,
				n,
			   *typ;

	/* checks args (args == NULL for an internal call) */
	if (args && !PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method getresult() takes no parameters.");
		return NULL;
	}

	/* stores result in tuple */
	m = PQntuples(self->result);
	n = PQnfields(self->result);
	reslist = PyList_New(m);

	typ = get_type_array(self->result, n);

	for (i = 0; i < m; i++)
	{
		if (!(rowtuple = PyTuple_New(n)))
		{
			Py_DECREF(reslist);
			reslist = NULL;
			goto exit;
		}

		for (j = 0; j < n; j++)
		{
			int			k;
			char	   *s = PQgetvalue(self->result, i, j);
			char		cashbuf[64];
			PyObject   *tmp_obj;

			if (PQgetisnull(self->result, i, j))
			{
				Py_INCREF(Py_None);
				val = Py_None;
			}
			else
				switch (typ[j])
				{
					case PYGRES_INT:
						val = PyInt_FromString(s, NULL, 10);
						break;

					case PYGRES_LONG:
						val = PyLong_FromString(s, NULL, 10);
						break;

					case PYGRES_FLOAT:
						tmp_obj = PyString_FromString(s);
						val = PyFloat_FromString(tmp_obj, NULL);
						Py_DECREF(tmp_obj);
						break;

					case PYGRES_MONEY:
						/* convert to decimal only if decimal point is set */
						if (!decimal_point) goto default_case;
						for (k = 0;
							 *s && k < sizeof(cashbuf) / sizeof(cashbuf[0]) - 1;
							 s++)
						{
							if (*s >= '0' && *s <= '9')
								cashbuf[k++] = *s;
							else if (*s == decimal_point)
								cashbuf[k++] = '.';
							else if (*s == '(' || *s == '-')
								cashbuf[k++] = '-';
						}
						cashbuf[k] = 0;
						s = cashbuf;
						/* FALLTHROUGH */ /* no break */

					case PYGRES_DECIMAL:
						if (decimal)
						{
							tmp_obj = Py_BuildValue("(s)", s);
							val = PyEval_CallObject(decimal, tmp_obj);
						}
						else
						{
							tmp_obj = PyString_FromString(s);
							val = PyFloat_FromString(tmp_obj, NULL);
						}
						Py_DECREF(tmp_obj);
						break;

					case PYGRES_BOOL:
						/* convert to bool only if bool_type is set */
						if (use_bool)
						{
							val = *s == 't' ? Py_True : Py_False;
							Py_INCREF(val);
							break;
						}
						/* FALLTHROUGH */ /* no break */

					default:
					default_case:
						val = PyString_FromString(s);
						break;
				}

			if (!val)
			{
				Py_DECREF(reslist);
				Py_DECREF(rowtuple);
				reslist = NULL;
				goto exit;
			}

			PyTuple_SET_ITEM(rowtuple, j, val);
		}

		PyList_SET_ITEM(reslist, i, rowtuple);
	}

exit:
	free(typ);

	/* returns list */
	return reslist;
}

/* retrieves last result as a list of dictionaries*/
static char pgquery_dictresult__doc__[] =
"dictresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a dictionary with the field names used "
"as the labels.";

static PyObject *
pgquery_dictresult(pgqueryobject *self, PyObject *args)
{
	PyObject   *dict,
			   *reslist,
			   *val;
	int			i,
				j,
				m,
				n,
			   *typ;

	/* checks args (args == NULL for an internal call) */
	if (args && !PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method dictresult() takes no parameters.");
		return NULL;
	}

	/* stores result in list */
	m = PQntuples(self->result);
	n = PQnfields(self->result);
	reslist = PyList_New(m);

	typ = get_type_array(self->result, n);

	for (i = 0; i < m; i++)
	{
		if (!(dict = PyDict_New()))
		{
			Py_DECREF(reslist);
			reslist = NULL;
			goto exit;
		}

		for (j = 0; j < n; j++)
		{
			int			k;
			char	   *s = PQgetvalue(self->result, i, j);
			char		cashbuf[64];
			PyObject   *tmp_obj;

			if (PQgetisnull(self->result, i, j))
			{
				Py_INCREF(Py_None);
				val = Py_None;
			}
			else
				switch (typ[j])
				{
					case PYGRES_INT:
						val = PyInt_FromString(s, NULL, 10);
						break;

					case PYGRES_LONG:
						val = PyLong_FromString(s, NULL, 10);
						break;

					case PYGRES_FLOAT:
						tmp_obj = PyString_FromString(s);
						val = PyFloat_FromString(tmp_obj, NULL);
						Py_DECREF(tmp_obj);
						break;

					case PYGRES_MONEY:
						/* convert to decimal only if decimal point is set */
						if (!decimal_point) goto default_case;

						for (k = 0;
							 *s && k < sizeof(cashbuf) / sizeof(cashbuf[0]) - 1;
							 s++)
						{
							if (*s >= '0' && *s <= '9')
								cashbuf[k++] = *s;
							else if (*s == decimal_point)
								cashbuf[k++] = '.';
							else if (*s == '(' || *s == '-')
								cashbuf[k++] = '-';
						}
						cashbuf[k] = 0;
						s = cashbuf;
						/* FALLTHROUGH */ /* no break */

					case PYGRES_DECIMAL:
						if (decimal)
						{
							tmp_obj = Py_BuildValue("(s)", s);
							val = PyEval_CallObject(decimal, tmp_obj);
						}
						else
						{
							tmp_obj = PyString_FromString(s);
							val = PyFloat_FromString(tmp_obj, NULL);
						}
						Py_DECREF(tmp_obj);
						break;

					case PYGRES_BOOL:
						/* convert to bool only if bool_type is set */
						if (use_bool)
						{
							val = *s == 't' ? Py_True : Py_False;
							Py_INCREF(val);
							break;
						}
						/* FALLTHROUGH */ /* no break */

					default:
					default_case:
						val = PyString_FromString(s);
						break;
				}

			if (!val)
			{
				Py_DECREF(dict);
				Py_DECREF(reslist);
				reslist = NULL;
				goto exit;
			}

			PyDict_SetItemString(dict, PQfname(self->result, j), val);
			Py_DECREF(val);
		}

		PyList_SET_ITEM(reslist, i, dict);
	}

exit:
	free(typ);

	/* returns list */
	return reslist;
}

/* retrieves last result as named tuples */
static char pgquery_namedresult__doc__[] =
"namedresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a tuple of fields in the order returned "
"by the server.";

static PyObject *
pgquery_namedresult(pgqueryobject *self, PyObject *args)
{
	PyObject   *arglist,
			   *ret;

	/* checks args (args == NULL for an internal call) */
	if (args && !PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method namedresult() takes no parameters.");
		return NULL;
	}

	if (!namedresult)
	{
		PyErr_SetString(PyExc_TypeError,
			"named tuples are not supported.");
		return NULL;
	}

	arglist = Py_BuildValue("(O)", self);
	ret = PyObject_CallObject(namedresult, arglist);
	Py_DECREF(arglist);

	if (ret == NULL)
	    return NULL;

	return ret;
}

/* gets asynchronous notify */
static char pg_getnotify__doc__[] =
"getnotify() -- get database notify for this connection.";

static PyObject *
pg_getnotify(pgobject *self, PyObject *args)
{
	PGnotify   *notify;

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method getnotify() takes no parameters.");
		return NULL;
	}

	/* checks for NOTIFY messages */
	PQconsumeInput(self->cnx);

	if (!(notify = PQnotifies(self->cnx)))
	{
		Py_INCREF(Py_None);
		return Py_None;
	}
	else
	{
		PyObject   *notify_result,
				   *temp;

		if (!(temp = PyString_FromString(notify->relname)))
			return NULL;

		if (!(notify_result = PyTuple_New(3)))
			return NULL;

		PyTuple_SET_ITEM(notify_result, 0, temp);

		if (!(temp = PyInt_FromLong(notify->be_pid)))
		{
			Py_DECREF(notify_result);
			return NULL;
		}

		PyTuple_SET_ITEM(notify_result, 1, temp);

		/* extra exists even in old versions that did not support it */
		if (!(temp = PyString_FromString(notify->extra)))
		{
			Py_DECREF(notify_result);
			return NULL;
		}

		PyTuple_SET_ITEM(notify_result, 2, temp);

		PQfreemem(notify);

		return notify_result;
	}
}

/* source creation */
static char pg_source__doc__[] =
"source() -- creates a new source object for this connection";

static PyObject *
pg_source(pgobject *self, PyObject *args)
{
	/* checks validity */
	if (!check_cnx_obj(self))
		return NULL;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError, "method source() takes no parameter.");
		return NULL;
	}

	/* allocate new pg query object */
	return (PyObject *) pgsource_new(self);
}

/* database query */
static char pg_query__doc__[] =
"query(sql, [args]) -- creates a new query object for this connection, using"
" sql (string) request and optionally a tuple with positional parameters.";

static PyObject *
pg_query(pgobject *self, PyObject *args)
{
	char		*query;
	PyObject	*oargs = NULL;
	PGresult	*result;
	pgqueryobject *npgobj;
	int			status,
				nparms = 0;

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* get query args */
	if (!PyArg_ParseTuple(args, "s|O", &query, &oargs))
	{
		PyErr_SetString(PyExc_TypeError, "query(sql, [args]), with sql (string).");
		return NULL;
	}

	/* If oargs is passed, ensure it's a non-empty tuple. We want to treat
	 * an empty tuple the same as no argument since we'll get that when the
	 * caller passes no arguments to db.query(), and historic behaviour was
	 * to call PQexec() in that case, which can execute multiple commands. */
	if (oargs)
	{
		if (!PyTuple_Check(oargs) && !PyList_Check(oargs))
		{
			PyErr_SetString(PyExc_TypeError, "query parameters must be a tuple or list.");
			return NULL;
		}

		nparms = (int)PySequence_Size(oargs);
	}

	/* gets result */
	if (nparms)
	{
		/* prepare arguments */
		PyObject	**str, **s, *obj = PySequence_GetItem(oargs, 0);
		char		**parms, **p, *enc=NULL;
		int			*lparms, *l;
		register int i;

		/* if there's a single argument and it's a list or tuple, it
		 * contains the positional aguments. */
		if (nparms == 1 && (PyList_Check(obj) || PyTuple_Check(obj)))
		{
			oargs = obj;
			nparms = (int)PySequence_Size(oargs);
		}
		str = (PyObject **)malloc(nparms * sizeof(*str));
		parms = (char **)malloc(nparms * sizeof(*parms));
		lparms = (int *)malloc(nparms * sizeof(*lparms));

		/* convert optional args to a list of strings -- this allows
		 * the caller to pass whatever they like, and prevents us
		 * from having to map types to OIDs */
		for (i = 0, s=str, p=parms, l=lparms; i < nparms; i++, s++, p++, l++)
		{
			obj = PySequence_GetItem(oargs, i);

			if (obj == Py_None)
			{
				*s = NULL;
				*p = NULL;
				*l = 0;
			}
			else if (PyUnicode_Check(obj))
			{
				if (!enc)
					enc = (char *)pg_encoding_to_char(
						PQclientEncoding(self->cnx));
				if (!strcmp(enc, "UTF8"))
					*s = PyUnicode_AsUTF8String(obj);
				else if (!strcmp(enc, "LATIN1"))
					*s = PyUnicode_AsLatin1String(obj);
				else if (!strcmp(enc, "SQL_ASCII"))
					*s = PyUnicode_AsASCIIString(obj);
				else
					*s = PyUnicode_AsEncodedString(obj, enc, "strict");
				if (*s == NULL)
				{
					free(lparms); free(parms); free(str);
					PyErr_SetString(PyExc_UnicodeError, "query parameter"
						" could not be decoded (bad client encoding)");
					while (i--)
					{
						if (*--s)
						{
							Py_DECREF(*s);
						}
					}
					return NULL;
				}
				*p = PyString_AsString(*s);
				*l = (int)PyString_Size(*s);
			}
			else
			{
				*s = PyObject_Str(obj);
				if (*s == NULL)
				{
					free(lparms); free(parms); free(str);
					PyErr_SetString(PyExc_TypeError,
						"query parameter has no string representation");
					while (i--)
					{
						if (*--s)
						{
							Py_DECREF(*s);
						}
					}
					return NULL;
				}
				*p = PyString_AsString(*s);
				*l = (int)PyString_Size(*s);
			}
		}

		Py_BEGIN_ALLOW_THREADS
		result = PQexecParams(self->cnx, query, nparms,
			NULL, (const char * const *)parms, lparms, NULL, 0);
		Py_END_ALLOW_THREADS

		free(lparms); free(parms);
		for (i = 0, s=str; i < nparms; i++, s++)
		{
			if (*s)
			{
				Py_DECREF(*s);
			}
		}
		free(str);
	}
	else
	{
		Py_BEGIN_ALLOW_THREADS
		result = PQexec(self->cnx, query);
		Py_END_ALLOW_THREADS
	}

	/* checks result validity */
	if (!result)
	{
		PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
		return NULL;
	}

	/* checks result status */
	if ((status = PQresultStatus(result)) != PGRES_TUPLES_OK)
	{
		switch (status)
		{
			case PGRES_EMPTY_QUERY:
				PyErr_SetString(PyExc_ValueError, "empty query.");
				break;
			case PGRES_BAD_RESPONSE:
			case PGRES_FATAL_ERROR:
			case PGRES_NONFATAL_ERROR:
				set_dberror(ProgrammingError,
					PQerrorMessage(self->cnx), result);
				break;
			case PGRES_COMMAND_OK:
				{						/* INSERT, UPDATE, DELETE */
					Oid		oid = PQoidValue(result);
					if (oid == InvalidOid)	/* not a single insert */
					{
						char	*ret = PQcmdTuples(result);

						PQclear(result);
						if (ret[0])		/* return number of rows affected */
						{
							return PyString_FromString(ret);
						}
						Py_INCREF(Py_None);
						return Py_None;
					}
					/* for a single insert, return the oid */
					PQclear(result);
					return PyInt_FromLong(oid);
				}
			case PGRES_COPY_OUT:		/* no data will be received */
			case PGRES_COPY_IN:
				PQclear(result);
				Py_INCREF(Py_None);
				return Py_None;
			default:
				set_dberror(InternalError,
					"internal error: unknown result status.", result);
				break;
		}

		PQclear(result);
		return NULL;			/* error detected on query */
	}

	if (!(npgobj = PyObject_NEW(pgqueryobject, &PgQueryType)))
		return NULL;

	/* stores result and returns object */
	npgobj->result = result;
	return (PyObject *) npgobj;
}

#ifdef DIRECT_ACCESS
static char pg_putline__doc__[] =
"putline() -- sends a line directly to the backend";

/* direct acces function : putline */
static PyObject *
pg_putline(pgobject *self, PyObject *args)
{
	char *line;

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* reads args */
	if (!PyArg_ParseTuple(args, "s", &line))
	{
		PyErr_SetString(PyExc_TypeError, "putline(line), with line (string).");
		return NULL;
	}

	/* sends line to backend */
	if (PQputline(self->cnx, line))
	{
		PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
		return NULL;
	}
	Py_INCREF(Py_None);
	return Py_None;
}

/* direct access function : getline */
static char pg_getline__doc__[] =
"getline() -- gets a line directly from the backend.";

static PyObject *
pg_getline(pgobject *self, PyObject *args)
{
	char		line[MAX_BUFFER_SIZE];
	PyObject   *str = NULL;		/* GCC */

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method getline() takes no parameters.");
		return NULL;
	}

	/* gets line */
	switch (PQgetline(self->cnx, line, MAX_BUFFER_SIZE))
	{
		case 0:
			str = PyString_FromString(line);
			break;
		case 1:
			PyErr_SetString(PyExc_MemoryError, "buffer overflow");
			str = NULL;
			break;
		case EOF:
			Py_INCREF(Py_None);
			str = Py_None;
			break;
	}

	return str;
}

/* direct access function : end copy */
static char pg_endcopy__doc__[] =
"endcopy() -- synchronizes client and server";

static PyObject *
pg_endcopy(pgobject *self, PyObject *args)
{
	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method endcopy() takes no parameters.");
		return NULL;
	}

	/* ends direct copy */
	if (PQendcopy(self->cnx))
	{
		PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
		return NULL;
	}
	Py_INCREF(Py_None);
	return Py_None;
}
#endif /* DIRECT_ACCESS */

static PyObject *
pgquery_repr(pgqueryobject *self)
{
	return PyString_FromString("<pg query result>");
}

static PyObject *
pgquery_str(pgqueryobject *self)
{
	return format_result(self->result);
}

/* insert table */
static char pg_inserttable__doc__[] =
"inserttable(string, list) -- insert list in table. The fields in the "
"list must be in the same order as in the table.";

static PyObject *
pg_inserttable(pgobject *self, PyObject *args)
{
	PGresult	*result;
	char		*table,
				*buffer,
				*bufpt;
	size_t		bufsiz;
	PyObject	*list,
				*sublist,
				*item;
	PyObject	*(*getitem) (PyObject *, Py_ssize_t);
	PyObject	*(*getsubitem) (PyObject *, Py_ssize_t);
	Py_ssize_t	i,
				j,
				m,
				n;

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "sO:filter", &table, &list))
	{
		PyErr_SetString(PyExc_TypeError,
			"inserttable(table, content), with table (string) "
			"and content (list).");
		return NULL;
	}

	/* checks list type */
	if (PyTuple_Check(list))
	{
		m = PyTuple_Size(list);
		getitem = PyTuple_GetItem;
	}
	else if (PyList_Check(list))
	{
		m = PyList_Size(list);
		getitem = PyList_GetItem;
	}
	else
	{
		PyErr_SetString(PyExc_TypeError,
			"second arg must be some kind of array.");
		return NULL;
	}

	/* allocate buffer */
	if (!(buffer = malloc(MAX_BUFFER_SIZE)))
	{
		PyErr_SetString(PyExc_MemoryError,
			"can't allocate insert buffer.");
		return NULL;
	}

	/* starts query */
	sprintf(buffer, "copy %s from stdin", table);

	Py_BEGIN_ALLOW_THREADS
	result = PQexec(self->cnx, buffer);
	Py_END_ALLOW_THREADS

	if (!result)
	{
		free(buffer);
		PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
		return NULL;
	}

	PQclear(result);

	n = 0; /* not strictly necessary but avoids warning */

	/* feed table */
	for (i = 0; i < m; i++)
	{
		sublist = getitem(list, i);
		if (PyTuple_Check(sublist))
		{
			j = PyTuple_Size(sublist);
			getsubitem = PyTuple_GetItem;
		}
		else if (PyList_Check(sublist))
		{
			j = PyList_Size(sublist);
			getsubitem = PyList_GetItem;
		}
		else
		{
			PyErr_SetString(PyExc_TypeError,
				"second arg must contain some kind of arrays.");
			return NULL;
		}
		if (i)
		{
			if (j != n)
			{
				free(buffer);
				PyErr_SetString(PyExc_TypeError,
					"arrays contained in second arg must have same size.");
				return NULL;
			}
		}
		else
		{
			n = j; /* never used before this assignment */
		}

		/* builds insert line */
		bufpt = buffer;
		bufsiz = MAX_BUFFER_SIZE - 1;

		for (j = 0; j < n; j++)
		{
			if (j)
			{
				*bufpt++ = '\t'; --bufsiz;
			}

			item = getsubitem(sublist, j);

			/* convert item to string and append to buffer */
			if (item == Py_None)
			{
				if (bufsiz > 2)
				{
					*bufpt++ = '\\'; *bufpt++ = 'N';
					bufsiz -= 2;
				}
				else
					bufsiz = 0;
			}
			else if (PyString_Check(item))
			{
				const char* t = PyString_AS_STRING(item);
				while (*t && bufsiz)
				{
					if (*t == '\\' || *t == '\t' || *t == '\n')
					{
						*bufpt++ = '\\'; --bufsiz;
						if (!bufsiz) break;
					}
					*bufpt++ = *t++; --bufsiz;
				}
			}
			else if (PyInt_Check(item) || PyLong_Check(item))
			{
				PyObject* s = PyObject_Str(item);
				const char* t = PyString_AsString(s);
				while (*t && bufsiz)
				{
					*bufpt++ = *t++; --bufsiz;
				}
				Py_DECREF(s);
			}
			else
			{
				PyObject* s = PyObject_Repr(item);
				const char* t = PyString_AsString(s);
				while (*t && bufsiz)
				{
					if (*t == '\\' || *t == '\t' || *t == '\n')
					{
						*bufpt++ = '\\'; --bufsiz;
						if (!bufsiz) break;
					}
					*bufpt++ = *t++; --bufsiz;
				}
				Py_DECREF(s);
			}

			if (bufsiz <= 0)
			{
				free(buffer);
				PyErr_SetString(PyExc_MemoryError,
					"insert buffer overflow.");
				return NULL;
			}

		}

		*bufpt++ = '\n'; *bufpt = '\0';

		/* sends data */
		if (PQputline(self->cnx, buffer))
		{
			PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
			PQendcopy(self->cnx);
			free(buffer);
			return NULL;
		}
	}

	/* ends query */
	if (PQputline(self->cnx, "\\.\n"))
	{
		PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
		PQendcopy(self->cnx);
		free(buffer);
		return NULL;
	}

	if (PQendcopy(self->cnx))
	{
		PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
		free(buffer);
		return NULL;
	}

	free(buffer);

	/* no error : returns nothing */
	Py_INCREF(Py_None);
	return Py_None;
}

/* get transaction state */
static char pg_transaction__doc__[] =
"Returns the current transaction status.";

static PyObject *
pg_transaction(pgobject *self, PyObject *args)
{
	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method transaction() takes no parameters.");
		return NULL;
	}

	return PyInt_FromLong(PQtransactionStatus(self->cnx));
}

/* get parameter setting */
static char pg_parameter__doc__[] =
"Looks up a current parameter setting.";

static PyObject *
pg_parameter(pgobject *self, PyObject *args)
{
	const char *name;

	if (!self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* get query args */
	if (!PyArg_ParseTuple(args, "s", &name))
	{
		PyErr_SetString(PyExc_TypeError, "parameter(name), with name (string).");
		return NULL;
	}

	name = PQparameterStatus(self->cnx, name);

	if (name)
		return PyString_FromString(name);

	/* unknown parameter, return None */
	Py_INCREF(Py_None);
	return Py_None;
}

#ifdef ESCAPING_FUNCS

/* escape literal */
static char pg_escape_literal__doc__[] =
"pg_escape_literal(str) -- escape a literal constant for use within SQL.";

static PyObject *
pg_escape_literal(pgobject *self, PyObject *args)
{
	char *str; /* our string argument */
	int str_length; /* length of string */
	char *esc; /* the escaped version of the string */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &str, &str_length))
		return NULL;
	esc = PQescapeLiteral(self->cnx, str, (size_t)str_length);
	ret = Py_BuildValue("s", esc);
	if (esc)
		PQfreemem(esc);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* escape identifier */
static char pg_escape_identifier__doc__[] =
"pg_escape_identifier(str) -- escape an identifier for use within SQL.";

static PyObject *
pg_escape_identifier(pgobject *self, PyObject *args)
{
	char *str; /* our string argument */
	int str_length; /* length of string */
	char *esc; /* the escaped version of the string */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &str, &str_length))
		return NULL;
	esc = PQescapeIdentifier(self->cnx, str, (size_t)str_length);
	ret = Py_BuildValue("s", esc);
	if (esc)
		PQfreemem(esc);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

#endif	/* ESCAPING_FUNCS */

/* escape string */
static char pg_escape_string__doc__[] =
"pg_escape_string(str) -- escape a string for use within SQL.";

static PyObject *
pg_escape_string(pgobject *self, PyObject *args)
{
	char *from; /* our string argument */
	char *to=NULL; /* the result */
	int from_length; /* length of string */
	int to_length; /* length of result */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &from, &from_length))
		return NULL;
	to_length = 2*from_length + 1;
	if (to_length < from_length) /* overflow */
	{
		to_length = from_length;
		from_length = (from_length - 1)/2;
	}
	to = (char *)malloc(to_length);
	to_length = (int)PQescapeStringConn(self->cnx,
		to, from, (size_t)from_length, NULL);
	ret = Py_BuildValue("s#", to, to_length);
	if (to)
		free(to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* escape bytea */
static char pg_escape_bytea__doc__[] =
"pg_escape_bytea(data) -- escape binary data for use within SQL as type bytea.";

static PyObject *
pg_escape_bytea(pgobject *self, PyObject *args)
{
	unsigned char *from; /* our string argument */
	unsigned char *to; /* the result */
	int from_length; /* length of string */
	size_t to_length; /* length of result */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &from, &from_length))
		return NULL;
	to = PQescapeByteaConn(self->cnx, from, (int)from_length, &to_length);
	ret = Py_BuildValue("s", to);
	if (to)
		PQfreemem((void *)to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

#ifdef LARGE_OBJECTS
/* creates large object */
static char pg_locreate__doc__[] =
"locreate() -- creates a new large object in the database.";

static PyObject *
pg_locreate(pgobject *self, PyObject *args)
{
	int			mode;
	Oid			lo_oid;

	/* checks validity */
	if (!check_cnx_obj(self))
		return NULL;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "i", &mode))
	{
		PyErr_SetString(PyExc_TypeError,
			"locreate(mode), with mode (integer).");
		return NULL;
	}

	/* creates large object */
	lo_oid = lo_creat(self->cnx, mode);
	if (lo_oid == 0)
	{
		set_dberror(OperationalError, "can't create large object.", NULL);
		return NULL;
	}

	return (PyObject *) pglarge_new(self, lo_oid);
}

/* init from already known oid */
static char pg_getlo__doc__[] =
"getlo(long) -- create a large object instance for the specified oid.";

static PyObject *
pg_getlo(pgobject *self, PyObject *args)
{
	int			lo_oid;

	/* checks validity */
	if (!check_cnx_obj(self))
		return NULL;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "i", &lo_oid))
	{
		PyErr_SetString(PyExc_TypeError, "getlo(oid), with oid (integer).");
		return NULL;
	}

	if (!lo_oid)
	{
		PyErr_SetString(PyExc_ValueError, "the object oid can't be null.");
		return NULL;
	}

	/* creates object */
	return (PyObject *) pglarge_new(self, lo_oid);
}

/* import unix file */
static char pg_loimport__doc__[] =
"loimport(string) -- create a new large object from specified file.";

static PyObject *
pg_loimport(pgobject *self, PyObject *args)
{
	char   *name;
	Oid		lo_oid;

	/* checks validity */
	if (!check_cnx_obj(self))
		return NULL;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "s", &name))
	{
		PyErr_SetString(PyExc_TypeError, "loimport(name), with name (string).");
		return NULL;
	}

	/* imports file and checks result */
	lo_oid = lo_import(self->cnx, name);
	if (lo_oid == 0)
	{
		set_dberror(OperationalError, "can't create large object.", NULL);
		return NULL;
	}

	return (PyObject *) pglarge_new(self, lo_oid);
}
#endif /* LARGE_OBJECTS */


/* connection object methods */
static struct PyMethodDef pgobj_methods[] = {
	{"source", (PyCFunction) pg_source, METH_VARARGS, pg_source__doc__},
	{"query", (PyCFunction) pg_query, METH_VARARGS, pg_query__doc__},
	{"reset", (PyCFunction) pg_reset, METH_VARARGS, pg_reset__doc__},
	{"cancel", (PyCFunction) pg_cancel, METH_VARARGS, pg_cancel__doc__},
	{"close", (PyCFunction) pg_close, METH_VARARGS, pg_close__doc__},
	{"fileno", (PyCFunction) pg_fileno, METH_VARARGS, pg_fileno__doc__},
	{"get_notice_receiver", (PyCFunction) pg_get_notice_receiver, METH_VARARGS,
			pg_get_notice_receiver__doc__},
	{"set_notice_receiver", (PyCFunction) pg_set_notice_receiver, METH_VARARGS,
			pg_set_notice_receiver__doc__},
	{"getnotify", (PyCFunction) pg_getnotify, METH_VARARGS,
			pg_getnotify__doc__},
	{"inserttable", (PyCFunction) pg_inserttable, METH_VARARGS,
			pg_inserttable__doc__},
	{"transaction", (PyCFunction) pg_transaction, METH_VARARGS,
			pg_transaction__doc__},
	{"parameter", (PyCFunction) pg_parameter, METH_VARARGS,
			pg_parameter__doc__},

#ifdef ESCAPING_FUNCS
	{"escape_literal", (PyCFunction) pg_escape_literal, METH_VARARGS,
			pg_escape_literal__doc__},
	{"escape_identifier", (PyCFunction) pg_escape_identifier, METH_VARARGS,
			pg_escape_identifier__doc__},
#endif	/* ESCAPING_FUNCS */
	{"escape_string", (PyCFunction) pg_escape_string, METH_VARARGS,
			pg_escape_string__doc__},
	{"escape_bytea", (PyCFunction) pg_escape_bytea, METH_VARARGS,
			pg_escape_bytea__doc__},

#ifdef DIRECT_ACCESS
	{"putline", (PyCFunction) pg_putline, 1, pg_putline__doc__},
	{"getline", (PyCFunction) pg_getline, 1, pg_getline__doc__},
	{"endcopy", (PyCFunction) pg_endcopy, 1, pg_endcopy__doc__},
#endif /* DIRECT_ACCESS */

#ifdef LARGE_OBJECTS
	{"locreate", (PyCFunction) pg_locreate, 1, pg_locreate__doc__},
	{"getlo", (PyCFunction) pg_getlo, 1, pg_getlo__doc__},
	{"loimport", (PyCFunction) pg_loimport, 1, pg_loimport__doc__},
#endif /* LARGE_OBJECTS */

	{NULL, NULL} /* sentinel */
};

/* get attribute */
static PyObject *
pg_getattr(pgobject *self, char *name)
{
	/*
	 * Although we could check individually, there are only a few
	 * attributes that don't require a live connection and unless someone
	 * has an urgent need, this will have to do
	 */

	/* first exception - close which returns a different error */
	if (strcmp(name, "close") && !self->cnx)
	{
		PyErr_SetString(PyExc_TypeError, "Connection is not valid.");
		return NULL;
	}

	/* list PostgreSQL connection fields */

	/* postmaster host */
	if (!strcmp(name, "host"))
	{
		char *r = PQhost(self->cnx);
		if (!r)
			r = "localhost";
		return PyString_FromString(r);
	}

	/* postmaster port */
	if (!strcmp(name, "port"))
		return PyInt_FromLong(atol(PQport(self->cnx)));

	/* selected database */
	if (!strcmp(name, "db"))
		return PyString_FromString(PQdb(self->cnx));

	/* selected options */
	if (!strcmp(name, "options"))
		return PyString_FromString(PQoptions(self->cnx));

	/* selected postgres tty */
	if (!strcmp(name, "tty"))
		return PyString_FromString(PQtty(self->cnx));

	/* error (status) message */
	if (!strcmp(name, "error"))
		return PyString_FromString(PQerrorMessage(self->cnx));

	/* connection status : 1 - OK, 0 - BAD */
	if (!strcmp(name, "status"))
		return PyInt_FromLong(PQstatus(self->cnx) == CONNECTION_OK ? 1 : 0);

	/* provided user name */
	if (!strcmp(name, "user"))
		return PyString_FromString(PQuser(self->cnx));

	/* protocol version */
	if (!strcmp(name, "protocol_version"))
		return PyInt_FromLong(PQprotocolVersion(self->cnx));

	/* backend version */
	if (!strcmp(name, "server_version"))
#if PG_VERSION_NUM < 80000
		return PyInt_FromLong(PG_VERSION_NUM);
#else
		return PyInt_FromLong(PQserverVersion(self->cnx));
#endif

	/* attributes list */
	if (!strcmp(name, "__members__"))
	{
		PyObject *list = PyList_New(10);

		if (list)
		{
			PyList_SET_ITEM(list, 0, PyString_FromString("host"));
			PyList_SET_ITEM(list, 1, PyString_FromString("port"));
			PyList_SET_ITEM(list, 2, PyString_FromString("db"));
			PyList_SET_ITEM(list, 3, PyString_FromString("options"));
			PyList_SET_ITEM(list, 4, PyString_FromString("tty"));
			PyList_SET_ITEM(list, 5, PyString_FromString("error"));
			PyList_SET_ITEM(list, 6, PyString_FromString("status"));
			PyList_SET_ITEM(list, 7, PyString_FromString("user"));
			PyList_SET_ITEM(list, 8, PyString_FromString("protocol_version"));
			PyList_SET_ITEM(list, 9, PyString_FromString("server_version"));
		}

		return list;
	}

	return Py_FindMethod(pgobj_methods, (PyObject *) self, name);
}

/* object type definition */
staticforward PyTypeObject PgType = {
	PyObject_HEAD_INIT(NULL)
	0,							/* ob_size */
	"pgobject",					/* tp_name */
	sizeof(pgobject),			/* tp_basicsize */
	0,							/* tp_itemsize */
	/* methods */
	(destructor) pg_dealloc,	/* tp_dealloc */
	0,							/* tp_print */
	(getattrfunc) pg_getattr,	/* tp_getattr */
	0,							/* tp_setattr */
	0,							/* tp_compare */
	0,							/* tp_repr */
	0,							/* tp_as_number */
	0,							/* tp_as_sequence */
	0,							/* tp_as_mapping */
	0,							/* tp_hash */
};


/* get attribute */
static PyObject *
pgnotice_getattr(pgnoticeobject *self, char *name)
{
	PGresult const *res = self->res;
	int fieldcode;

	if (!res)
	{
		PyErr_SetString(PyExc_TypeError, "Cannot get current notice.");
		return NULL;
	}

	/* pg connection object */
	if (!strcmp(name, "pgcnx"))
	{
		if (self->pgcnx && check_cnx_obj(self->pgcnx))
		{
			Py_INCREF(self->pgcnx);
			return (PyObject *) self->pgcnx;
		}
		else
		{
			Py_INCREF(Py_None);
			return Py_None;
		}
	}

	/* full message */
	if (!strcmp(name, "message"))
		return PyString_FromString(PQresultErrorMessage(res));

	/* other possible fields */
	fieldcode = 0;
	if (!strcmp(name, "severity"))
		fieldcode = PG_DIAG_SEVERITY;
	else if (!strcmp(name, "primary"))
		fieldcode = PG_DIAG_MESSAGE_PRIMARY;
	else if (!strcmp(name, "detail"))
		fieldcode = PG_DIAG_MESSAGE_DETAIL;
	else if (!strcmp(name, "hint"))
		fieldcode = PG_DIAG_MESSAGE_HINT;
	if (fieldcode)
	{
		char *s = PQresultErrorField(res, fieldcode);
		if (s)
			return PyString_FromString(s);
		else
		{
			Py_INCREF(Py_None); return Py_None;
		}
	}

	/* attributes list */
	if (!strcmp(name, "__members__"))
	{
		PyObject *list = PyList_New(6);
		if (list)
		{
			PyList_SET_ITEM(list, 0, PyString_FromString("pgcnx"));
			PyList_SET_ITEM(list, 1, PyString_FromString("severity"));
			PyList_SET_ITEM(list, 2, PyString_FromString("message"));
			PyList_SET_ITEM(list, 3, PyString_FromString("primary"));
			PyList_SET_ITEM(list, 4, PyString_FromString("detail"));
			PyList_SET_ITEM(list, 5, PyString_FromString("hint"));
		}
		return list;
	}

	PyErr_Format(PyExc_AttributeError,
		"'pgnoticeobject' has no attribute %s", name);
	return NULL;
}

static PyObject *
pgnotice_str(pgnoticeobject *self)
{
	return pgnotice_getattr(self, "message");
}

/* object type definition */
staticforward PyTypeObject PgNoticeType = {
	PyObject_HEAD_INIT(NULL)
	0,							/* ob_size */
	"pgnoticeobject",			/* tp_name */
	sizeof(pgnoticeobject),		/* tp_basicsize */
	0,							/* tp_itemsize */
	/* methods */
	0,								/* tp_dealloc */
	0,								/* tp_print */
	(getattrfunc) pgnotice_getattr,	/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	0,								/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) pgnotice_str			/* tp_str */
};


/* query object methods */
static struct PyMethodDef pgquery_methods[] = {
	{"getresult", (PyCFunction) pgquery_getresult, METH_VARARGS,
			pgquery_getresult__doc__},
	{"dictresult", (PyCFunction) pgquery_dictresult, METH_VARARGS,
			pgquery_dictresult__doc__},
	{"namedresult", (PyCFunction) pgquery_namedresult, METH_VARARGS,
			pgquery_namedresult__doc__},
	{"fieldname", (PyCFunction) pgquery_fieldname, METH_VARARGS,
			 pgquery_fieldname__doc__},
	{"fieldnum", (PyCFunction) pgquery_fieldnum, METH_VARARGS,
			pgquery_fieldnum__doc__},
	{"listfields", (PyCFunction) pgquery_listfields, METH_VARARGS,
			pgquery_listfields__doc__},
	{"ntuples", (PyCFunction) pgquery_ntuples, METH_VARARGS,
			pgquery_ntuples__doc__},
	{NULL, NULL}
};

/* gets query object attributes */
static PyObject *
pgquery_getattr(pgqueryobject *self, char *name)
{
	/* list postgreSQL connection fields */
	return Py_FindMethod(pgquery_methods, (PyObject *) self, name);
}

/* query type definition */
staticforward PyTypeObject PgQueryType = {
	PyObject_HEAD_INIT(NULL)
	0,								/* ob_size */
	"pgqueryobject",				/* tp_name */
	sizeof(pgqueryobject),			/* tp_basicsize */
	0,								/* tp_itemsize */
	/* methods */
	(destructor) pgquery_dealloc,	/* tp_dealloc */
	0,								/* tp_print */
	(getattrfunc) pgquery_getattr,	/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	(reprfunc) pgquery_repr,		/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) pgquery_str			/* tp_str */
};


/* --------------------------------------------------------------------- */

/* MODULE FUNCTIONS */

/* escape string */
static char escape_string__doc__[] =
"escape_string(str) -- escape a string for use within SQL.";

static PyObject *
escape_string(PyObject *self, PyObject *args)
{
	char *from; /* our string argument */
	char *to=NULL; /* the result */
	int from_length; /* length of string */
	int to_length; /* length of result */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &from, &from_length))
		return NULL;
	to_length = 2*from_length + 1;
	if (to_length < from_length) /* overflow */
	{
		to_length = from_length;
		from_length = (from_length - 1)/2;
	}
	to = (char *)malloc(to_length);
	to_length = (int)PQescapeString(to, from, (size_t)from_length);
	ret = Py_BuildValue("s#", to, to_length);
	if (to)
		free(to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* escape bytea */
static char escape_bytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea.";

static PyObject *
escape_bytea(PyObject *self, PyObject *args)
{
	unsigned char *from; /* our string argument */
	unsigned char *to; /* the result */
	int from_length; /* length of string */
	size_t to_length; /* length of result */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &from, &from_length))
		return NULL;
	to = PQescapeBytea(from, (int)from_length, &to_length);
	ret = Py_BuildValue("s", to);
	if (to)
		PQfreemem((void *)to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* unescape bytea */
static char unescape_bytea__doc__[] =
"unescape_bytea(str) -- unescape bytea data that has been retrieved as text.";

static PyObject
*unescape_bytea(PyObject *self, PyObject *args)
{
	unsigned char *from; /* our string argument */
	unsigned char *to; /* the result */
	size_t to_length; /* length of result string */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s", &from))
		return NULL;
	to = PQunescapeBytea(from, &to_length);
	ret = Py_BuildValue("s#", to, (int)to_length);
	if (to)
		PQfreemem((void *)to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* get decimal point */
static char get_decimal_point__doc__[] =
"get_decimal_point() -- get decimal point to be used for money values.";

static PyObject *
get_decimal_point(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;
	char s[2];

	if (PyArg_ParseTuple(args, ""))
	{
		if (decimal_point)
		{
			s[0] = decimal_point; s[1] = '\0';
			ret = PyString_FromString(s);
		} else {
			Py_INCREF(Py_None); ret = Py_None;
		}
	}
	else
	{
		PyErr_SetString(PyExc_TypeError,
			"get_decimal_point() takes no parameter");
	}

	return ret;
}

/* set decimal point */
static char set_decimal_point__doc__[] =
"set_decimal_point(char) -- set decimal point to be used for money values.";

static PyObject *
set_decimal_point(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;
	char *s = NULL;

	/* gets arguments */
	if (PyArg_ParseTuple(args, "z", &s)) {
		if (!s)
			s = "\0";
		else if (*s && (*(s+1) || !strchr(".,;: '*/_`|", *s)))
		 	s = NULL;
	}

	if (s) {
		decimal_point = *s;
		Py_INCREF(Py_None); ret = Py_None;
	} else {
		PyErr_SetString(PyExc_TypeError,
			"set_decimal_point() expects a decimal mark character");
	}

	return ret;
}

/* get decimal type */
static char get_decimal__doc__[] =
"get_decimal() -- set a decimal type to be used for numeric values.";

static PyObject *
get_decimal(PyObject *self, PyObject *args)
{
	PyObject *ret = NULL;

	if (PyArg_ParseTuple(args, ""))
	{
		ret = decimal ? decimal : Py_None;
		Py_INCREF(ret);
	}

	return ret;
}

/* set decimal type */
static char set_decimal__doc__[] =
"set_decimal(cls) -- set a decimal type to be used for numeric values.";

static PyObject *
set_decimal(PyObject *self, PyObject *args)
{
	PyObject *ret = NULL;
	PyObject *cls;

	if (PyArg_ParseTuple(args, "O", &cls))
	{
		if (cls == Py_None)
		{
			Py_XDECREF(decimal); decimal = NULL;
			Py_INCREF(Py_None); ret = Py_None;
		}
		else if (PyCallable_Check(cls))
		{
			Py_XINCREF(cls); Py_XDECREF(decimal); decimal = cls;
			Py_INCREF(Py_None); ret = Py_None;
		}
		else
			PyErr_SetString(PyExc_TypeError,
				"decimal type must be None or callable");
	}

	return ret;
}

/* get usage of bool values */
static char get_bool__doc__[] =
"get_bool() -- check whether boolean values are converted to bool.";

static PyObject *
get_bool(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;

	if (PyArg_ParseTuple(args, ""))
	{
		ret = use_bool ? Py_True : Py_False;
		Py_INCREF(ret);
	}

	return ret;
}

/* set usage of bool values */
static char set_bool__doc__[] =
"set_bool(bool) -- set whether boolean values should be converted to bool.";

static PyObject *
set_bool(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;
	int			i;

	/* gets arguments */
	if (PyArg_ParseTuple(args, "i", &i))
	{
		use_bool = i ? 1 : 0;
		Py_INCREF(Py_None); ret = Py_None;
	}

	return ret;
}

/* get named result factory */
static char get_namedresult__doc__[] =
"get_namedresult(cls) -- get the function used for getting named results.";

static PyObject *
get_namedresult(PyObject *self, PyObject *args)
{
	PyObject *ret = NULL;

	if (PyArg_ParseTuple(args, ""))
	{
		ret = namedresult ? namedresult : Py_None;
		Py_INCREF(ret);
	}

	return ret;
}

/* set named result factory */
static char set_namedresult__doc__[] =
"set_namedresult(cls) -- set a function to be used for getting named results.";

static PyObject *
set_namedresult(PyObject *self, PyObject *args)
{
	PyObject *ret = NULL;
	PyObject *func;

	if (PyArg_ParseTuple(args, "O", &func))
	{
		if (PyCallable_Check(func))
		{
			Py_XINCREF(func); Py_XDECREF(namedresult); namedresult = func;
			Py_INCREF(Py_None); ret = Py_None;
		}
		else
			PyErr_SetString(PyExc_TypeError, "parameter must be callable");
	}

	return ret;
}

#ifdef DEFAULT_VARS

/* gets default host */
static char getdefhost__doc__[] =
"get_defhost() -- return default database host.";

static PyObject *
pggetdefhost(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_defhost() takes no parameter.");
		return NULL;
	}

	Py_XINCREF(pg_default_host);
	return pg_default_host;
}

/* sets default host */
static char setdefhost__doc__[] =
"set_defhost(string) -- set default database host. Return previous value.";

static PyObject *
pgsetdefhost(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;
	PyObject   *old;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_defhost(name), with name (string/None).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_host;

	if (temp)
		pg_default_host = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_host = Py_None;
	}

	return old;
}

/* gets default base */
static char getdefbase__doc__[] =
"get_defbase() -- return default database name.";

static PyObject *
pggetdefbase(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_defbase() takes no parameter.");
		return NULL;
	}

	Py_XINCREF(pg_default_base);
	return pg_default_base;
}

/* sets default base */
static char setdefbase__doc__[] =
"set_defbase(string) -- set default database name. Return previous value";

static PyObject *
pgsetdefbase(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;
	PyObject   *old;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_defbase(name), with name (string/None).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_base;

	if (temp)
		pg_default_base = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_base = Py_None;
	}

	return old;
}

/* gets default options */
static char getdefopt__doc__[] =
"get_defopt() -- return default database options.";

static PyObject *
pggetdefopt(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_defopt() takes no parameter.");
		return NULL;
	}

	Py_XINCREF(pg_default_opt);
	return pg_default_opt;
}

/* sets default opt */
static char setdefopt__doc__[] =
"set_defopt(string) -- set default database options. Return previous value.";

static PyObject *
pgsetdefopt(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;
	PyObject   *old;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_defopt(name), with name (string/None).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_opt;

	if (temp)
		pg_default_opt = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_opt = Py_None;
	}

	return old;
}

/* gets default tty */
static char getdeftty__doc__[] =
"get_deftty() -- return default database debug terminal.";

static PyObject *
pggetdeftty(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_deftty() takes no parameter.");
		return NULL;
	}

	Py_XINCREF(pg_default_tty);
	return pg_default_tty;
}

/* sets default tty */
static char setdeftty__doc__[] =
"set_deftty(string) -- set default database debug terminal. "
"Return previous value.";

static PyObject *
pgsetdeftty(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;
	PyObject   *old;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_deftty(name), with name (string/None).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_tty;

	if (temp)
		pg_default_tty = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_tty = Py_None;
	}

	return old;
}

/* gets default username */
static char getdefuser__doc__[] =
"get_defuser() -- return default database username.";

static PyObject *
pggetdefuser(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_defuser() takes no parameter.");

		return NULL;
	}

	Py_XINCREF(pg_default_user);
	return pg_default_user;
}

/* sets default username */
static char setdefuser__doc__[] =
"set_defuser() -- set default database username. Return previous value.";

static PyObject *
pgsetdefuser(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;
	PyObject   *old;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_defuser(name), with name (string/None).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_user;

	if (temp)
		pg_default_user = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_user = Py_None;
	}

	return old;
}

/* sets default password */
static char setdefpasswd__doc__[] =
"set_defpasswd() -- set default database password.";

static PyObject *
pgsetdefpasswd(PyObject *self, PyObject *args)
{
	char	   *temp = NULL;

	/* gets arguments */
	if (!PyArg_ParseTuple(args, "z", &temp))
	{
		PyErr_SetString(PyExc_TypeError,
			"set_defpasswd(password), with password (string/None).");
		return NULL;
	}

	if (temp)
		pg_default_passwd = PyString_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_passwd = Py_None;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

/* gets default port */
static char getdefport__doc__[] =
"get_defport() -- return default database port.";

static PyObject *
pggetdefport(PyObject *self, PyObject *args)
{
	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError,
			"method get_defport() takes no parameter.");
		return NULL;
	}

	Py_XINCREF(pg_default_port);
	return pg_default_port;
}

/* sets default port */
static char setdefport__doc__[] =
"set_defport(integer) -- set default database port. Return previous value.";

static PyObject *
pgsetdefport(PyObject *self, PyObject *args)
{
	long int	port = -2;
	PyObject   *old;

	/* gets arguments */
	if ((!PyArg_ParseTuple(args, "l", &port)) || (port < -1))
	{
		PyErr_SetString(PyExc_TypeError, "set_defport(port), with port "
			"(positive integer/-1).");
		return NULL;
	}

	/* adjusts value */
	old = pg_default_port;

	if (port != -1)
		pg_default_port = PyInt_FromLong(port);
	else
	{
		Py_INCREF(Py_None);
		pg_default_port = Py_None;
	}

	return old;
}
#endif /* DEFAULT_VARS */

/* List of functions defined in the module */

static struct PyMethodDef pg_methods[] = {
	{"connect", (PyCFunction) pgconnect, METH_VARARGS|METH_KEYWORDS,
			connect__doc__},
	{"escape_string", (PyCFunction) escape_string, METH_VARARGS,
			escape_string__doc__},
	{"escape_bytea", (PyCFunction) escape_bytea, METH_VARARGS,
			escape_bytea__doc__},
	{"unescape_bytea", (PyCFunction) unescape_bytea, METH_VARARGS,
			unescape_bytea__doc__},
	{"get_decimal_point", (PyCFunction) get_decimal_point, METH_VARARGS,
			get_decimal_point__doc__},
	{"set_decimal_point", (PyCFunction) set_decimal_point, METH_VARARGS,
			set_decimal_point__doc__},
	{"get_decimal", (PyCFunction) get_decimal, METH_VARARGS,
			get_decimal__doc__},
	{"set_decimal", (PyCFunction) set_decimal, METH_VARARGS,
			set_decimal__doc__},
	{"get_bool", (PyCFunction) get_bool, METH_VARARGS, get_bool__doc__},
	{"set_bool", (PyCFunction) set_bool, METH_VARARGS, set_bool__doc__},
	{"get_namedresult", (PyCFunction) get_namedresult, METH_VARARGS,
			get_namedresult__doc__},
	{"set_namedresult", (PyCFunction) set_namedresult, METH_VARARGS,
			set_namedresult__doc__},

#ifdef DEFAULT_VARS
	{"get_defhost", pggetdefhost, METH_VARARGS, getdefhost__doc__},
	{"set_defhost", pgsetdefhost, METH_VARARGS, setdefhost__doc__},
	{"get_defbase", pggetdefbase, METH_VARARGS, getdefbase__doc__},
	{"set_defbase", pgsetdefbase, METH_VARARGS, setdefbase__doc__},
	{"get_defopt", pggetdefopt, METH_VARARGS, getdefopt__doc__},
	{"set_defopt", pgsetdefopt, METH_VARARGS, setdefopt__doc__},
	{"get_deftty", pggetdeftty, METH_VARARGS, getdeftty__doc__},
	{"set_deftty", pgsetdeftty, METH_VARARGS, setdeftty__doc__},
	{"get_defport", pggetdefport, METH_VARARGS, getdefport__doc__},
	{"set_defport", pgsetdefport, METH_VARARGS, setdefport__doc__},
	{"get_defuser", pggetdefuser, METH_VARARGS, getdefuser__doc__},
	{"set_defuser", pgsetdefuser, METH_VARARGS, setdefuser__doc__},
	{"set_defpasswd", pgsetdefpasswd, METH_VARARGS, setdefpasswd__doc__},
#endif /* DEFAULT_VARS */
	{NULL, NULL} /* sentinel */
};

static char pg__doc__[] = "Python interface to PostgreSQL DB";

/* Initialization function for the module */
DL_EXPORT(void)
init_pg(void)
{
	PyObject   *mod,
			   *dict,
			   *v;

	/* Initialize here because some WIN platforms get confused otherwise */
	PgType.ob_type = PgNoticeType.ob_type =
		PgQueryType.ob_type = PgSourceType.ob_type = &PyType_Type;
#ifdef LARGE_OBJECTS
	PglargeType.ob_type = &PyType_Type;
#endif

	/* Create the module and add the functions */
	mod = Py_InitModule4("_pg", pg_methods, pg__doc__, NULL, PYTHON_API_VERSION);
	dict = PyModule_GetDict(mod);

	/* Exceptions as defined by DB-API 2.0 */
	Error = PyErr_NewException("pg.Error", PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Error", Error);

	Warning = PyErr_NewException("pg.Warning", PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Warning", Warning);

	InterfaceError = PyErr_NewException("pg.InterfaceError", Error, NULL);
	PyDict_SetItemString(dict, "InterfaceError", InterfaceError);

	DatabaseError = PyErr_NewException("pg.DatabaseError", Error, NULL);
	PyDict_SetItemString(dict, "DatabaseError", DatabaseError);

	InternalError = PyErr_NewException("pg.InternalError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "InternalError", InternalError);

	OperationalError =
		PyErr_NewException("pg.OperationalError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "OperationalError", OperationalError);

	ProgrammingError =
		PyErr_NewException("pg.ProgrammingError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "ProgrammingError", ProgrammingError);

	IntegrityError =
		PyErr_NewException("pg.IntegrityError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "IntegrityError", IntegrityError);

	DataError = PyErr_NewException("pg.DataError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "DataError", DataError);

	NotSupportedError =
		PyErr_NewException("pg.NotSupportedError", DatabaseError, NULL);
	PyDict_SetItemString(dict, "NotSupportedError", NotSupportedError);

	/* Make the version available */
	v = PyString_FromString(PyPgVersion);
	PyDict_SetItemString(dict, "version", v);
	PyDict_SetItemString(dict, "__version__", v);
	Py_DECREF(v);

	/* results type for queries */
	PyDict_SetItemString(dict, "RESULT_EMPTY", PyInt_FromLong(RESULT_EMPTY));
	PyDict_SetItemString(dict, "RESULT_DML", PyInt_FromLong(RESULT_DML));
	PyDict_SetItemString(dict, "RESULT_DDL", PyInt_FromLong(RESULT_DDL));
	PyDict_SetItemString(dict, "RESULT_DQL", PyInt_FromLong(RESULT_DQL));

	/* transaction states */
	PyDict_SetItemString(dict,"TRANS_IDLE",PyInt_FromLong(PQTRANS_IDLE));
	PyDict_SetItemString(dict,"TRANS_ACTIVE",PyInt_FromLong(PQTRANS_ACTIVE));
	PyDict_SetItemString(dict,"TRANS_INTRANS",PyInt_FromLong(PQTRANS_INTRANS));
	PyDict_SetItemString(dict,"TRANS_INERROR",PyInt_FromLong(PQTRANS_INERROR));
	PyDict_SetItemString(dict,"TRANS_UNKNOWN",PyInt_FromLong(PQTRANS_UNKNOWN));

#ifdef LARGE_OBJECTS
	/* create mode for large objects */
	PyDict_SetItemString(dict, "INV_READ", PyInt_FromLong(INV_READ));
	PyDict_SetItemString(dict, "INV_WRITE", PyInt_FromLong(INV_WRITE));

	/* position flags for lo_lseek */
	PyDict_SetItemString(dict, "SEEK_SET", PyInt_FromLong(SEEK_SET));
	PyDict_SetItemString(dict, "SEEK_CUR", PyInt_FromLong(SEEK_CUR));
	PyDict_SetItemString(dict, "SEEK_END", PyInt_FromLong(SEEK_END));
#endif /* LARGE_OBJECTS */

#ifdef DEFAULT_VARS
	/* prepares default values */
	Py_INCREF(Py_None);
	pg_default_host = Py_None;
	Py_INCREF(Py_None);
	pg_default_base = Py_None;
	Py_INCREF(Py_None);
	pg_default_opt = Py_None;
	Py_INCREF(Py_None);
	pg_default_port = Py_None;
	Py_INCREF(Py_None);
	pg_default_tty = Py_None;
	Py_INCREF(Py_None);
	pg_default_user = Py_None;
	Py_INCREF(Py_None);
	pg_default_passwd = Py_None;
#endif /* DEFAULT_VARS */

	/* Check for errors */
	if (PyErr_Occurred())
		Py_FatalError("can't initialize module _pg");
}
