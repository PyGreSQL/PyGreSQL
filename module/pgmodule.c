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
 * Further modifications copyright 1997 to 2015 by D'Arcy J.M. Cain
 * (darcy@PyGreSQL.org) subject to the same terms and conditions as above.
 *
 */

/* Note: This should be linked against the same C runtime lib as Python */

#include <Python.h>

#include <libpq-fe.h>

/* some definitions from <libpq/libpq-fs.h> */
#include "pgfs.h"
/* the type definitions from <catalog/pg_type.h> */
#include "pgtypes.h"

/* macros for single-source Python 2/3 compatibility */
#include "py3c.h"

static PyObject *Error, *Warning, *InterfaceError,
	*DatabaseError, *InternalError, *OperationalError, *ProgrammingError,
	*IntegrityError, *DataError, *NotSupportedError;

#define _TOSTRING(x) #x
#define TOSTRING(x) _TOSTRING(x)
static const char *PyPgVersion = TOSTRING(PYGRESQL_VERSION);

#if SIZEOF_SIZE_T != SIZEOF_INT
#define Py_InitModule4 Py_InitModule4_64
#endif

/* default values */
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

static PyObject *decimal = NULL, /* decimal type */
				*namedresult = NULL; /* function for getting named results */
static char *decimal_point = "."; /* decimal point used in money values */

/*
OBJECTS
=======

  Each object has a number of elements.  The naming scheme will be based on
  the object type.  Here are the elements using example object type "foo".
   - fooObject: A structure to hold local object information.
   - fooXxx: Object methods such as Delete and Getattr.
   - fooMethods: Methods declaration.
   - fooType: Type definition for object.

  This is followed by the object methods.

  The objects that we need to create:
   - pg: The module itself.
   - conn: Connection object returned from pg.connect().
   - notice: Notice object returned from pg.notice().
   - large: Large object returned by pg.conn.locreate() and Pg.Conn.loimport().
   - query: Query object returned by pg.conn.Conn.query().
   - source: Source object returned by pg.conn.source().
*/

/* forward declarations for types */
static PyTypeObject noticeType;
static PyTypeObject queryType;
static PyTypeObject sourceType;
static PyTypeObject largeType;
static PyTypeObject connType;

/* forward static declarations */
static void notice_receiver(void *, const PGresult *);

/* --------------------------------------------------------------------- */
/* Object declarations */
/* --------------------------------------------------------------------- */
typedef struct
{
	PyObject_HEAD
	int			valid;				/* validity flag */
	PGconn		*cnx;				/* PostGres connection handle */
	PyObject	*notice_receiver;	/* current notice receiver */
}	connObject;
#define is_connObject(v) (PyType(v) == &connType)

typedef struct
{
	PyObject_HEAD
	int			valid;			/* validity flag */
	connObject	*pgcnx;			/* parent connection object */
	PGresult	*result;		/* result content */
	int			result_type;	/* result type (DDL/DML/DQL) */
	long		arraysize;		/* array size for fetch method */
	int			current_row;	/* current selected row */
	int			max_row;		/* number of rows in the result */
	int			num_fields;		/* number of fields in each row */
}	sourceObject;
#define is_sourceObject(v) (PyType(v) == &sourceType)

typedef struct
{
	PyObject_HEAD
	connObject	*pgcnx;		/* parent connection object */
	PGresult	const *res;	/* an error or warning */
}	noticeObject;
#define is_noticeObject(v) (PyType(v) == &noticeType)

typedef struct
{
	PyObject_HEAD
	PGresult	*result;		/* result content */
	int			result_type;	/* type of previous result */
	long		current_pos;	/* current position in last result */
	long		num_rows;		/* number of (affected) rows */
}	queryObject;
#define is_queryObject(v) (PyType(v) == &queryType)

#ifdef LARGE_OBJECTS
typedef struct
{
	PyObject_HEAD
	connObject	*pgcnx;			/* parent connection object */
	Oid			lo_oid;			/* large object oid */
	int			lo_fd;			/* large object fd */
}	largeObject;
#define is_largeObject(v) (PyType(v) == &largeType)
#endif /* LARGE_OBJECTS */


/* --------------------------------------------------------------------- */
/* Internal Functions */
/* --------------------------------------------------------------------- */
/* shared functions for converting PG types to Python types */
static int *
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
				typ[j] = 1;
				break;

			case INT8OID:
				typ[j] = 2;
				break;

			case FLOAT4OID:
			case FLOAT8OID:
				typ[j] = 3;
				break;

			case NUMERICOID:
				typ[j] = 4;
				break;

			case CASHOID:
				typ[j] = 5;
				break;

			default:
				typ[j] = 6;
				break;
		}
	}

	return typ;
}

/* internal wrapper for the notice receiver callback */
static void notice_receiver(void *arg, const PGresult *res)
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	connObject *self = (connObject*) arg;
	PyObject *proc = self->notice_receiver;
	if (proc && PyCallable_Check(proc))
	{
		noticeObject *notice = PyObject_NEW(noticeObject, &noticeType);
		PyObject *args, *ret;
		if (notice)
		{
			notice->pgcnx = arg;
			notice->res = res;
		}
		else
		{
			Py_INCREF(Py_None);
			notice = (noticeObject *)(void *)Py_None;
		}
		args = Py_BuildValue("(O)", notice);
		ret = PyObject_CallObject(proc, args);
		Py_XDECREF(ret);
		Py_DECREF(args);
	}
	PyGILState_Release(gstate);
}

/* sets database error with sqlstate attribute */
/* This should be used when raising a subclass of DatabaseError */
static void
set_dberror(PyObject *type, const char *msg, PGresult *result)
{
	PyObject *err = NULL;
	PyObject *str;

	if (!(str = PyStr_FromString(msg)))
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
			str = sqlstate ? PyStr_FromStringAndSize(sqlstate, 5) : NULL;
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
check_cnx_obj(connObject *self)
{
	if (!self->valid)
	{
		set_dberror(OperationalError, "connection has been closed.", NULL);
		return 0;
	}
	return 1;
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
				result = PyStr_FromString(buffer);
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
		return PyStr_FromString("(nothing selected)");
}

/* --------------------------------------------------------------------- */
/* large objects                                                         */
/* --------------------------------------------------------------------- */
#ifdef LARGE_OBJECTS

/* checks large object validity */
static int
check_lo_obj(largeObject *self, int level)
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

/* constructor (internal use only) */
static largeObject *
largeNew(connObject *pgcnx, Oid oid)
{
	largeObject *npglo;

	if (!(npglo = PyObject_NEW(largeObject, &largeType)))
		return NULL;

	Py_XINCREF(pgcnx);
	npglo->pgcnx = pgcnx;
	npglo->lo_fd = -1;
	npglo->lo_oid = oid;

	return npglo;
}

/* destructor */
static void
largeDealloc(largeObject *self)
{
	if (self->lo_fd >= 0 && check_cnx_obj(self->pgcnx))
		lo_close(self->pgcnx->cnx, self->lo_fd);

	Py_XDECREF(self->pgcnx);
	PyObject_Del(self);
}

/* opens large object */
static char largeOpen__doc__[] =
"open(mode) -- open access to large object with specified mode "
"(INV_READ, INV_WRITE constants defined by module).";

static PyObject *
largeOpen(largeObject *self, PyObject *args)
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
static char largeClose__doc__[] =
"close() -- close access to large object data.";

static PyObject *
largeClose(largeObject *self, PyObject *args)
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
static char largeRead__doc__[] =
"read(integer) -- read from large object to sized string. "
"Object must be opened in read mode before calling this method.";

static PyObject *
largeRead(largeObject *self, PyObject *args)
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
	buffer = PyBytes_FromStringAndSize((char *) NULL, size);

	if ((size = lo_read(self->pgcnx->cnx, self->lo_fd,
	    PyBytes_AS_STRING((PyBytesObject *)(buffer)), size)) < 0)
	{
		PyErr_SetString(PyExc_IOError, "error while reading.");
		Py_XDECREF(buffer);
		return NULL;
	}

	/* resize buffer and returns it */
	_PyBytes_Resize(&buffer, size);
	return buffer;
}

/* write to large object */
static char largeWrite__doc__[] =
"write(string) -- write sized string to large object. "
"Object must be opened in read mode before calling this method.";

static PyObject *
largeWrite(largeObject *self, PyObject *args)
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
static char largeSeek__doc__[] =
"seek(off, whence) -- move to specified position. Object must be opened "
"before calling this method. whence can be SEEK_SET, SEEK_CUR or SEEK_END, "
"constants defined by module.";

static PyObject *
largeSeek(largeObject *self, PyObject *args)
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
static char largeSize__doc__[] =
"size() -- return large object size. "
"Object must be opened before calling this method.";

static PyObject *
largeSize(largeObject *self, PyObject *args)
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
static char largeTell__doc__[] =
"tell() -- give current position in large object. "
"Object must be opened before calling this method.";

static PyObject *
largeTell(largeObject *self, PyObject *args)
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
static char largeExport__doc__[] =
"export(string) -- export large object data to specified file. "
"Object must be closed when calling this method.";

static PyObject *
largeExport(largeObject *self, PyObject *args)
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
static char largeUnlink__doc__[] =
"unlink() -- destroy large object. "
"Object must be closed when calling this method.";

static PyObject *
largeUnlink(largeObject *self, PyObject *args)
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

/* get the list of large object attributes */
static PyObject *
largeDir(largeObject *self) {
	PyObject *attrs;

	attrs = PyObject_Dir(PyObject_Type((PyObject *)self));
	PyObject_CallMethod(attrs, "extend", "[sss]",
		"oid", "pgcnx", "error");

	return attrs;
}

/* large object methods */
static struct PyMethodDef largeMethods[] = {
	{"__dir__", (PyCFunction) largeDir,  METH_NOARGS, NULL},
	{"open", (PyCFunction) largeOpen, METH_VARARGS, largeOpen__doc__},
	{"close", (PyCFunction) largeClose, METH_VARARGS, largeClose__doc__},
	{"read", (PyCFunction) largeRead, METH_VARARGS, largeRead__doc__},
	{"write", (PyCFunction) largeWrite, METH_VARARGS, largeWrite__doc__},
	{"seek", (PyCFunction) largeSeek, METH_VARARGS, largeSeek__doc__},
	{"size", (PyCFunction) largeSize, METH_VARARGS, largeSize__doc__},
	{"tell", (PyCFunction) largeTell, METH_VARARGS, largeTell__doc__},
	{"export",(PyCFunction) largeExport,METH_VARARGS,largeExport__doc__},
	{"unlink",(PyCFunction) largeUnlink,METH_VARARGS,largeUnlink__doc__},
	{NULL, NULL}
};

/* gets large object attributes */
static PyObject *
largeGetAttr(largeObject *self, PyObject *nameobj)
{
	const char *name = PyStr_AsString(nameobj);

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
		return PyStr_FromString(PQerrorMessage(self->pgcnx->cnx));

	/* seeks name in methods (fallback) */
	return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* prints query object in human readable format */
static int
largePrint(largeObject *self, FILE *fp, int flags)
{
	char		print_buffer[128];
	PyOS_snprintf(print_buffer, sizeof(print_buffer),
		self->lo_fd >= 0 ?
			"Opened large object, oid %ld" :
			"Closed large object, oid %ld", (long) self->lo_oid);
	fputs(print_buffer, fp);
	return 0;
}

static char large__doc__[] = "PostgreSQL large object";

/* large object type definition */
static PyTypeObject largeType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"pglarge",						/* tp_name */
	sizeof(largeObject),			/* tp_basicsize */
	0,								/* tp_itemsize */

	/* methods */
	(destructor) largeDealloc,		/* tp_dealloc */
	(printfunc) largePrint,			/* tp_print */
	0,								/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	0,								/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,                              /* tp_call */
	0,								/* tp_str */
	(getattrofunc) largeGetAttr,	/* tp_getattro */
	0,                              /* tp_setattro */
	0,                              /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,             /* tp_flags */
	large__doc__,					/* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	largeMethods,					/* tp_methods */
};
#endif /* LARGE_OBJECTS */

/* --------------------------------------------------------------------- */
/* connection object */
/* --------------------------------------------------------------------- */
static void
connDelete(connObject *self)
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

/* source creation */
static char connSource__doc__[] =
"source() -- creates a new source object for this connection";

static PyObject *
connSource(connObject *self, PyObject *args)
{
	sourceObject *npgobj;

	/* checks validity */
	if (!check_cnx_obj(self))
		return NULL;

	/* checks args */
	if (!PyArg_ParseTuple(args, ""))
	{
		PyErr_SetString(PyExc_TypeError, "method source() takes no parameter.");
		return NULL;
	}

	/* allocates new query object */
	if (!(npgobj = PyObject_NEW(sourceObject, &sourceType)))
		return NULL;

	/* initializes internal parameters */
	Py_XINCREF(self);
	npgobj->pgcnx = self;
	npgobj->result = NULL;
	npgobj->valid = 1;
	npgobj->arraysize = PG_ARRAYSIZE;

	return (PyObject *) npgobj;
}

/* database query */
static char connQuery__doc__[] =
"query(sql, [args]) -- creates a new query object for this connection, using"
" sql (string) request and optionally a tuple with positional parameters.";

static PyObject *
connQuery(connObject *self, PyObject *args)
{
	char		*query;
	PyObject	*oargs = NULL;
	PGresult	*result;
	queryObject *npgobj;
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
			else if (PyBytes_Check(obj))
			{
				*s = obj;
				*p = PyBytes_AsString(*s);
				*l = (int)PyBytes_Size(*s);
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
				*p = PyBytes_AsString(*s);
				*l = (int)PyBytes_Size(*s);
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
				*p = PyStr_AsString(*s);
				*l = (int)strlen(*p);
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
							return PyStr_FromString(ret);
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

	if (!(npgobj = PyObject_NEW(queryObject, &queryType)))
	{
		PyErr_SetString(PyExc_MemoryError, "Can't create query object");
		return NULL;
	}

	/* stores result and returns object */
	npgobj->result = result;
	return (PyObject *) npgobj;
}

#ifdef DIRECT_ACCESS
static char connPutLine__doc__[] =
"putline() -- sends a line directly to the backend";

/* direct acces function : putline */
static PyObject *
connPutLine(connObject *self, PyObject *args)
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
static char connGetLine__doc__[] =
"getline() -- gets a line directly from the backend.";

static PyObject *
connGetLine(connObject *self, PyObject *args)
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
			str = PyStr_FromString(line);
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
static char connEndCopy__doc__[] =
"endcopy() -- synchronizes client and server";

static PyObject *
connEndCopy(connObject *self, PyObject *args)
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
queryRepr(queryObject *self)
{
	return PyStr_FromString("<pg query result>");
}

static PyObject *
queryStr(queryObject *self)
{
	return format_result(self->result);
}

/* insert table */
static char connInsertTable__doc__[] =
"inserttable(string, list) -- insert list in table. The fields in the "
"list must be in the same order as in the table.";

static PyObject *
connInsertTable(connObject *self, PyObject *args)
{
	PGresult	*result;
	char		*table,
				*buffer,
				*bufpt;
	char		*enc=NULL;
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
			else if (PyBytes_Check(item))
			{
				const char* t = PyBytes_AsString(item);
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
			else if (PyUnicode_Check(item))
			{
				PyObject *s;
				if (!enc)
					enc = (char *)pg_encoding_to_char(
						PQclientEncoding(self->cnx));
				if (!strcmp(enc, "UTF8"))
					s = PyUnicode_AsUTF8String(item);
				else if (!strcmp(enc, "LATIN1"))
					s = PyUnicode_AsLatin1String(item);
				else if (!strcmp(enc, "SQL_ASCII"))
					s = PyUnicode_AsASCIIString(item);
				else
					s = PyUnicode_AsEncodedString(item, enc, "strict");
				const char* t = PyBytes_AsString(s);
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
			else if (PyInt_Check(item) || PyLong_Check(item))
			{
				PyObject* s = PyObject_Str(item);
				const char* t = PyStr_AsString(s);
				while (*t && bufsiz)
				{
					*bufpt++ = *t++; --bufsiz;
				}
				Py_DECREF(s);
			}
			else
			{
				PyObject* s = PyObject_Repr(item);
				const char* t = PyStr_AsString(s);
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
static char connTransaction__doc__[] =
"Returns the current transaction status.";

static PyObject *
connTransaction(connObject *self, PyObject *args)
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
static char connParameter__doc__[] =
"Looks up a current parameter setting.";

static PyObject *
connParameter(connObject *self, PyObject *args)
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
		return PyStr_FromString(name);

	/* unknown parameter, return None */
	Py_INCREF(Py_None);
	return Py_None;
}

#ifdef ESCAPING_FUNCS

/* escape literal */
static char connEscapeLiteral__doc__[] =
"escape_literal(str) -- escape a literal constant for use within SQL.";

static PyObject *
connEscapeLiteral(connObject *self, PyObject *args)
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
static char connEscapeIdentifier__doc__[] =
"escape_identifier(str) -- escape an identifier for use within SQL.";

static PyObject *
connEscapeIdentifier(connObject *self, PyObject *args)
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
static char connEscapeString__doc__[] =
"escape_string(str) -- escape a string for use within SQL.";

static PyObject *
connEscapeString(connObject *self, PyObject *args)
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
static char connEscapeBytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea.";

static PyObject *
connEscapeBytea(connObject *self, PyObject *args)
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
static char connCreateLO__doc__[] =
"locreate() -- creates a new large object in the database.";

static PyObject *
connCreateLO(connObject *self, PyObject *args)
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

	return (PyObject *) largeNew(self, lo_oid);
}

/* init from already known oid */
static char connGetLO__doc__[] =
"getlo(long) -- create a large object instance for the specified oid.";

static PyObject *
connGetLO(connObject *self, PyObject *args)
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
	return (PyObject *) largeNew(self, lo_oid);
}

/* import unix file */
static char connImportLO__doc__[] =
"loimport(string) -- create a new large object from specified file.";

static PyObject *
connImportLO(connObject *self, PyObject *args)
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

	return (PyObject *) largeNew(self, lo_oid);
}
#endif /* LARGE_OBJECTS */

/* resets connection */
static char connReset__doc__[] =
"reset() -- reset connection with current parameters. All derived queries "
"and large objects derived from this connection will not be usable after "
"this call.";

static PyObject *
connReset(connObject *self, PyObject *args)
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
static char connCancel__doc__[] =
"cancel() -- abandon processing of the current command.";

static PyObject *
connCancel(connObject *self, PyObject *args)
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
static char connFileno__doc__[] =
"fileno() -- return database connection socket file handle.";

static PyObject *
connFileno(connObject *self, PyObject *args)
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
static char connSetNoticeReceiver__doc__[] =
"set_notice_receiver() -- set the current notice receiver.";

static PyObject *
connSetNoticeReceiver(connObject * self, PyObject * args)
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
static char connGetNoticeReceiver__doc__[] =
"get_notice_receiver() -- get the current notice receiver.";

static PyObject *
connGetNoticeReceiver(connObject * self, PyObject * args)
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

/* close without deleting */
static char connClose__doc__[] =
"close() -- close connection. All instances of the connection object and "
"derived objects (queries and large objects) can no longer be used after "
"this call.";

static PyObject *
connClose(connObject *self, PyObject *args)
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

/* gets asynchronous notify */
static char connGetNotify__doc__[] =
"getnotify() -- get database notify for this connection.";

static PyObject *
connGetNotify(connObject *self, PyObject *args)
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

		if (!(temp = PyStr_FromString(notify->relname)))
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
		if (!(temp = PyStr_FromString(notify->extra)))
		{
			Py_DECREF(notify_result);
			return NULL;
		}

		PyTuple_SET_ITEM(notify_result, 2, temp);

		PQfreemem(notify);

		return notify_result;
	}
}

/* get the list of connection attributes */
static PyObject *
connDir(connObject *self) {
	PyObject *attrs;

	attrs = PyObject_Dir(PyObject_Type((PyObject *)self));
	PyObject_CallMethod(attrs, "extend", "[ssssssssss]",
		"host", "port", "db", "options", "tty", "error", "status", "user",
		"protocol_version", "server_version");

	return attrs;
}

/* connection object methods */
static struct PyMethodDef connMethods[] = {
	{"__dir__", (PyCFunction) connDir,  METH_NOARGS, NULL},

	{"source", (PyCFunction) connSource, METH_VARARGS, connSource__doc__},
	{"query", (PyCFunction) connQuery, METH_VARARGS, connQuery__doc__},
	{"reset", (PyCFunction) connReset, METH_VARARGS, connReset__doc__},
	{"cancel", (PyCFunction) connCancel, METH_VARARGS, connCancel__doc__},
	{"close", (PyCFunction) connClose, METH_VARARGS, connClose__doc__},
	{"fileno", (PyCFunction) connFileno, METH_VARARGS, connFileno__doc__},
	{"get_notice_receiver", (PyCFunction) connGetNoticeReceiver, METH_VARARGS,
			connGetNoticeReceiver__doc__},
	{"set_notice_receiver", (PyCFunction) connSetNoticeReceiver, METH_VARARGS,
			connSetNoticeReceiver__doc__},
	{"getnotify", (PyCFunction) connGetNotify, METH_VARARGS,
			connGetNotify__doc__},
	{"inserttable", (PyCFunction) connInsertTable, METH_VARARGS,
			connInsertTable__doc__},
	{"transaction", (PyCFunction) connTransaction, METH_VARARGS,
			connTransaction__doc__},
	{"parameter", (PyCFunction) connParameter, METH_VARARGS,
			connParameter__doc__},

#ifdef ESCAPING_FUNCS
	{"escape_literal", (PyCFunction) connEscapeLiteral, METH_VARARGS,
			connEscapeLiteral__doc__},
	{"escape_identifier", (PyCFunction) connEscapeIdentifier, METH_VARARGS,
			connEscapeIdentifier__doc__},
#endif	/* ESCAPING_FUNCS */
	{"escape_string", (PyCFunction) connEscapeString, METH_VARARGS,
			connEscapeString__doc__},
	{"escape_bytea", (PyCFunction) connEscapeBytea, METH_VARARGS,
			connEscapeBytea__doc__},

#ifdef DIRECT_ACCESS
	{"putline", (PyCFunction) connPutLine, METH_VARARGS, connPutLine__doc__},
	{"getline", (PyCFunction) connGetLine, METH_VARARGS, connGetLine__doc__},
	{"endcopy", (PyCFunction) connEndCopy, METH_VARARGS, connEndCopy__doc__},
#endif /* DIRECT_ACCESS */

#ifdef LARGE_OBJECTS
	{"locreate", (PyCFunction) connCreateLO, METH_VARARGS, connCreateLO__doc__},
	{"getlo", (PyCFunction) connGetLO, METH_VARARGS, connGetLO__doc__},
	{"loimport", (PyCFunction) connImportLO, METH_VARARGS, connImportLO__doc__},
#endif /* LARGE_OBJECTS */

	{NULL, NULL} /* sentinel */
};

/* gets connection attributes */
static PyObject *
connGetAttr(connObject *self, PyObject *nameobj)
{
	const char *name = PyStr_AsString(nameobj);

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
		return PyStr_FromString(r);
	}

	/* postmaster port */
	if (!strcmp(name, "port"))
		return PyInt_FromLong(atol(PQport(self->cnx)));

	/* selected database */
	if (!strcmp(name, "db"))
		return PyStr_FromString(PQdb(self->cnx));

	/* selected options */
	if (!strcmp(name, "options"))
		return PyStr_FromString(PQoptions(self->cnx));

	/* selected postgres tty */
	if (!strcmp(name, "tty"))
		return PyStr_FromString(PQtty(self->cnx));

	/* error (status) message */
	if (!strcmp(name, "error"))
		return PyStr_FromString(PQerrorMessage(self->cnx));

	/* connection status : 1 - OK, 0 - BAD */
	if (!strcmp(name, "status"))
		return PyInt_FromLong(PQstatus(self->cnx) == CONNECTION_OK ? 1 : 0);

	/* provided user name */
	if (!strcmp(name, "user"))
		return PyStr_FromString(PQuser(self->cnx));

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

	return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* connection type definition */
static PyTypeObject connType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"pgconnobject",				/* tp_name */
	sizeof(connObject),			/* tp_basicsize */
	0,							/* tp_itemsize */
	(destructor) connDelete,	/* tp_dealloc */
	0,							/* tp_print */
	0,							/* tp_getattr */
	0,							/* tp_setattr */
	0,							/* tp_reserved */
	0,							/* tp_repr */
	0,							/* tp_as_number */
	0,							/* tp_as_sequence */
	0,							/* tp_as_mapping */
	0,							/* tp_hash */
	0,							/* tp_call */
	0,							/* tp_str */
	(getattrofunc) connGetAttr,	/* tp_getattro */
	0,							/* tp_setattro */
	0,							/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,         /* tp_flags */
	0,							/* tp_doc */
	0,							/* tp_traverse */
	0,							/* tp_clear */
	0,							/* tp_richcompare */
	0,							/* tp_weaklistoffset */
	0,							/* tp_iter */
	0,							/* tp_iternext */
	connMethods,				/* tp_methods */
};

/* --------------------------------------------------------------------- */
/* source object */
/* --------------------------------------------------------------------- */
/* checks source object validity */
static int
check_source_obj(sourceObject *self, int level)
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

/* destructor */
static void
sourceDealloc(sourceObject *self)
{
	if (self->result)
		PQclear(self->result);

	Py_XDECREF(self->pgcnx);
	PyObject_Del(self);
}

/* closes object */
static char sourceClose__doc__[] =
"close() -- close query object without deleting it. "
"All instances of the query object can no longer be used after this call.";

static PyObject *
sourceClose(sourceObject *self, PyObject *args)
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
static char sourceExecute__doc__[] =
"execute(sql) -- execute a SQL statement (string).\n "
"On success, this call returns the number of affected rows, "
"or None for DQL (SELECT, ...) statements.\n"
"The fetch (fetch(), fetchone() and fetchall()) methods can be used "
"to get result rows.";

static PyObject *
sourceExecute(sourceObject *self, PyObject *args)
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
static char sourceStatusOID__doc__[] =
"oidstatus() -- return oid of last inserted row (if available).";

static PyObject *
sourceStatusOID(sourceObject *self, PyObject *args)
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
static char sourceFetch__doc__[] =
"fetch(num) -- return the next num rows from the last result in a list. "
"If num parameter is omitted arraysize attribute value is used. "
"If size equals -1, all rows are fetched.";

static PyObject *
sourceFetch(sourceObject *self, PyObject *args)
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
				str = PyStr_FromString(PQgetvalue(self->result, self->current_row, j));

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
pgsource_move(sourceObject *self, PyObject *args, int move)
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
static char sourceMoveFirst__doc__[] =
"movefirst() -- move to first result row.";

static PyObject *
sourceMoveFirst(sourceObject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVEFIRST);
}

/* move to last result row */
static char sourceMoveLast__doc__[] =
"movelast() -- move to last valid result row.";

static PyObject *
sourceMoveLast(sourceObject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVELAST);
}

/* move to next result row */
static char sourceMoveNext__doc__[] =
"movenext() -- move to next result row.";

static PyObject *
sourceMoveNext(sourceObject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVENEXT);
}

/* move to previous result row */
static char sourceMovePrev__doc__[] =
"moveprev() -- move to previous result row.";

static PyObject *
sourceMovePrev(sourceObject *self, PyObject *args)
{
	return pgsource_move(self, args, QUERY_MOVEPREV);
}

/* finds field number from string/integer (internal use only) */
static int
sourceFieldindex(sourceObject *self, PyObject *param, const char *usage)
{
	int			num;

	/* checks validity */
	if (!check_source_obj(self, CHECK_RESULT | CHECK_DQL))
		return -1;

	/* gets field number */
	if (PyStr_Check(param))
		num = PQfnumber(self->result, PyBytes_AsString(param));
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
pgsource_buildinfo(sourceObject *self, int num)
{
	PyObject *result;

	/* allocates tuple */
	result = PyTuple_New(3);
	if (!result)
		return NULL;

	/* affects field information */
	PyTuple_SET_ITEM(result, 0, PyInt_FromLong(num));
	PyTuple_SET_ITEM(result, 1,
		PyStr_FromString(PQfname(self->result, num)));
	PyTuple_SET_ITEM(result, 2,
		PyInt_FromLong(PQftype(self->result, num)));

	return result;
}

/* lists fields info */
static char sourceListInfo__doc__[] =
"listinfo() -- return information for all fields "
"(position, name, type oid).";

static PyObject *
sourceListInfo(sourceObject *self, PyObject *args)
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
static char sourceFieldInfo__doc__[] =
"fieldinfo(string|integer) -- return specified field information "
"(position, name, type oid).";

static PyObject *
sourceFieldInfo(sourceObject *self, PyObject *args)
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
	if ((num = sourceFieldindex(self, param, short_usage)) == -1)
		return NULL;

	/* returns result */
	return pgsource_buildinfo(self, num);
};

/* retrieve field value */
static char sourceField__doc__[] =
"field(string|integer) -- return specified field value.";

static PyObject *
sourceField(sourceObject *self, PyObject *args)
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
	if ((num = sourceFieldindex(self, param, short_usage)) == -1)
		return NULL;

	return PyStr_FromString(PQgetvalue(self->result,
									self->current_row, num));
}

/* get the list of source object attributes */
static PyObject *
sourceDir(connObject *self) {
	PyObject *attrs;

	attrs = PyObject_Dir(PyObject_Type((PyObject *)self));
	PyObject_CallMethod(attrs, "extend", "[sssss]",
		"pgcnx", "arraysize", "resulttype", "ntuples", "nfields");

	return attrs;
}

/* source object methods */
static PyMethodDef sourceMethods[] = {
	{"__dir__", (PyCFunction) sourceDir,  METH_NOARGS, NULL},
	{"close", (PyCFunction) sourceClose, METH_VARARGS,
			sourceClose__doc__},
	{"execute", (PyCFunction) sourceExecute, METH_VARARGS,
			sourceExecute__doc__},
	{"oidstatus", (PyCFunction) sourceStatusOID, METH_VARARGS,
			sourceStatusOID__doc__},
	{"fetch", (PyCFunction) sourceFetch, METH_VARARGS,
			sourceFetch__doc__},
	{"movefirst", (PyCFunction) sourceMoveFirst, METH_VARARGS,
			sourceMoveFirst__doc__},
	{"movelast", (PyCFunction) sourceMoveLast, METH_VARARGS,
			sourceMoveLast__doc__},
	{"movenext", (PyCFunction) sourceMoveNext, METH_VARARGS,
			sourceMoveNext__doc__},
	{"moveprev", (PyCFunction) sourceMovePrev, METH_VARARGS,
			sourceMovePrev__doc__},
	{"field", (PyCFunction) sourceField, METH_VARARGS,
			sourceField__doc__},
	{"fieldinfo", (PyCFunction) sourceFieldInfo, METH_VARARGS,
			sourceFieldInfo__doc__},
	{"listinfo", (PyCFunction) sourceListInfo, METH_VARARGS,
			sourceListInfo__doc__},
	{NULL, NULL}
};

/* gets source object attributes */
static PyObject *
sourceGetAttr(sourceObject *self, PyObject *nameobj)
{
	const char *name = PyStr_AsString(nameobj);

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

	/* seeks name in methods (fallback) */
	return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* sets query object attributes */
static int
sourceSetAttr(sourceObject *self, char *name, PyObject *v)
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
sourceRepr(sourceObject *self)
{
	return PyStr_FromString("<pg source object>");
}

/* returns source object as string in human readable format */

static PyObject *
sourceStr(sourceObject *self)
{
	switch (self->result_type)
	{
		case RESULT_DQL:
			return format_result(self->result);
		case RESULT_DDL:
		case RESULT_DML:
			return PyStr_FromString(PQcmdStatus(self->result));
		case RESULT_EMPTY:
		default:
			return PyStr_FromString("(empty PostgreSQL source object)");
	}
}

static char source__doc__[] = "PyGreSQL source object";

/* source type definition */
static PyTypeObject sourceType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"pgsourceobject",				/* tp_name */
	sizeof(sourceObject),			/* tp_basicsize */
	0,								/* tp_itemsize */
	/* methods */
	(destructor) sourceDealloc,		/* tp_dealloc */
	0,								/* tp_print */
	0,								/* tp_getattr */
	(setattrfunc) sourceSetAttr,	/* tp_setattr */
	0,								/* tp_compare */
	(reprfunc) sourceRepr,			/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) sourceStr,			/* tp_str */
	(getattrofunc) sourceGetAttr,	/* tp_getattro */
	0,								/* tp_setattro */
	0,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,				/* tp_flags */
	source__doc__,					/* tp_doc */
	0,								/* tp_traverse */
	0,								/* tp_clear */
	0,								/* tp_richcompare */
	0,								/* tp_weaklistoffset */
	0,								/* tp_iter */
	0,								/* tp_iternext */
	sourceMethods,					/* tp_methods */
};

/* connects to a database */
static char pgConnect__doc__[] =
"connect(dbname, host, port, opt, tty) -- connect to a PostgreSQL database "
"using specified parameters (optionals, keywords aware).";

static PyObject *
pgConnect(PyObject *self, PyObject *args, PyObject *dict)
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
	connObject   *npgobj;

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
		pghost = PyBytes_AsString(pg_default_host);

	if ((pgport == -1) && (pg_default_port != Py_None))
		pgport = PyInt_AsLong(pg_default_port);

	if ((!pgopt) && (pg_default_opt != Py_None))
		pgopt = PyBytes_AsString(pg_default_opt);

	if ((!pgtty) && (pg_default_tty != Py_None))
		pgtty = PyBytes_AsString(pg_default_tty);

	if ((!pgdbname) && (pg_default_base != Py_None))
		pgdbname = PyBytes_AsString(pg_default_base);

	if ((!pguser) && (pg_default_user != Py_None))
		pguser = PyBytes_AsString(pg_default_user);

	if ((!pgpasswd) && (pg_default_passwd != Py_None))
		pgpasswd = PyBytes_AsString(pg_default_passwd);
#endif /* DEFAULT_VARS */

	if (!(npgobj = PyObject_NEW(connObject, &connType)))
	{
		set_dberror(InternalError, "Can't create new connection object", NULL);
		return NULL;
	}

	npgobj->valid = 1;
	npgobj->cnx = NULL;
	npgobj->notice_receiver = NULL;

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

static void
queryDealloc(queryObject *self)
{
	if (self->result)
		PQclear(self->result);

	PyObject_Del(self);
}

/* get number of rows */
static char queryNTuples__doc__[] =
"ntuples() -- returns number of tuples returned by query.";

static PyObject *
queryNTuples(queryObject *self, PyObject *args)
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
static char queryListFields__doc__[] =
"listfields() -- Lists field names from result.";

static PyObject *
queryListFields(queryObject *self, PyObject *args)
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
		str = PyStr_FromString(name);
		PyTuple_SET_ITEM(fieldstuple, i, str);
	}

	return fieldstuple;
}

/* get field name from last result */
static char queryFieldName__doc__[] =
"fieldname() -- returns name of field from result from its position.";

static PyObject *
queryFieldName(queryObject *self, PyObject *args)
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
	return PyStr_FromString(name);
}

/* gets fields number from name in last result */
static char queryFieldNumber__doc__[] =
"fieldnum() -- returns position in query for field from its name.";

static PyObject *
queryFieldNumber(queryObject *self, PyObject *args)
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
static char queryGetResult__doc__[] =
"getresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a tuple of fields in the order returned "
"by the server.";

static PyObject *
queryGetResult(queryObject *self, PyObject *args)
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
					case 1:  /* int2/4 */
						val = PyInt_FromString(s, NULL, 10);
						break;

					case 2:  /* int8 */
						val = PyLong_FromString(s, NULL, 10);
						break;

					case 3:  /* float/double */
						tmp_obj = PyBytes_FromString(s);
#if IS_PY3
						val = PyFloat_FromString(tmp_obj);
#else
						val = PyFloat_FromString(tmp_obj, NULL);
#endif
						Py_DECREF(tmp_obj);
						break;

					case 5:  /* money */
						for (k = 0;
							 *s && k < sizeof(cashbuf) / sizeof(cashbuf[0]) - 1;
							 s++)
						{
							if (isdigit((int)*s))
								cashbuf[k++] = *s;
							else if (*s == *decimal_point)
								cashbuf[k++] = '.';
							else if (*s == '(' || *s == '-')
								cashbuf[k++] = '-';
						}
						cashbuf[k] = 0;
						s = cashbuf;

					/* FALLTHROUGH */ /* no break */
					case 4:  /* numeric */
						if (decimal)
						{
							tmp_obj = Py_BuildValue("(s)", s);
							val = PyEval_CallObject(decimal, tmp_obj);
						}
						else
						{
							tmp_obj = PyBytes_FromString(s);
#if IS_PY3
							val = PyFloat_FromString(tmp_obj);
#else
							val = PyFloat_FromString(tmp_obj, NULL);
#endif
						}
						Py_DECREF(tmp_obj);
						break;

					default:
						val = PyStr_FromString(s);
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
static char queryDictResult__doc__[] =
"dictresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a dictionary with the field names used "
"as the labels.";

static PyObject *
queryDictResult(queryObject *self, PyObject *args)
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
					case 1:  /* int2/4 */
						val = PyInt_FromString(s, NULL, 10);
						break;

					case 2:  /* int8 */
						val = PyLong_FromString(s, NULL, 10);
						break;

					case 3:  /* float/double */
						tmp_obj = PyBytes_FromString(s);
#if IS_PY3
						val = PyFloat_FromString(tmp_obj);
#else
						val = PyFloat_FromString(tmp_obj, NULL);
#endif
						Py_DECREF(tmp_obj);
						break;

					case 5:  /* money */
						for (k = 0;
							 *s && k < sizeof(cashbuf) / sizeof(cashbuf[0]) - 1;
							 s++)
						{
							if (isdigit((int)*s))
								cashbuf[k++] = *s;
							else if (*s == *decimal_point)
								cashbuf[k++] = '.';
							else if (*s == '(' || *s == '-')
								cashbuf[k++] = '-';
						}
						cashbuf[k] = 0;
						s = cashbuf;

					/* FALLTHROUGH */ /* no break */
					case 4:  /* numeric */
						if (decimal)
						{
							tmp_obj = Py_BuildValue("(s)", s);
							val = PyEval_CallObject(decimal, tmp_obj);
						}
						else
						{
							tmp_obj = PyBytes_FromString(s);
#if IS_PY3
							val = PyFloat_FromString(tmp_obj);
#else
							val = PyFloat_FromString(tmp_obj, NULL);
#endif
						}
						Py_DECREF(tmp_obj);
						break;

					default:
						val = PyStr_FromString(s);
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
static char queryNamedResult__doc__[] =
"namedresult() -- Gets the result of a query.  The result is returned "
"as a list of rows, each one a tuple of fields in the order returned "
"by the server.";

static PyObject *
queryNamedResult(queryObject *self, PyObject *args)
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

/* gets notice object attributes */
static PyObject *
noticeGetAttr(noticeObject *self, PyObject *nameobj)
{
	PGresult const *res = self->res;
	const char *name = PyStr_AsString(nameobj);
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
		return PyStr_FromString(PQresultErrorMessage(res));

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
			return PyStr_FromString(s);
		else
		{
			Py_INCREF(Py_None); return Py_None;
		}
	}

	return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

static PyObject *
noticeStr(noticeObject *self)
{
	return noticeGetAttr(self, PyBytes_FromString("message"));
}

/* get the list of notice attributes */
static PyObject *
noticeDir(noticeObject *self) {
	PyObject *attrs;

	attrs = PyObject_Dir(PyObject_Type((PyObject *)self));
	PyObject_CallMethod(attrs, "extend", "[ssssss]",
		"pgcnx", "severity", "message", "primary", "detail", "hint");

	return attrs;
}

/* notice object methods */
static struct PyMethodDef noticeMethods[] = {
	{"__dir__", (PyCFunction) noticeDir,  METH_NOARGS, NULL},
	{NULL, NULL}
};

/* notice type definition */
static PyTypeObject noticeType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"pgnoticeobject",				/* tp_name */
	sizeof(noticeObject),			/* tp_basicsize */
	0,								/* tp_itemsize */
	/* methods */
	0,								/* tp_dealloc */
	0,								/* tp_print */
	0,								/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	0,								/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) noticeStr,			/* tp_str */
	(getattrofunc) noticeGetAttr,	/* tp_getattro */
	PyObject_GenericSetAttr,		/* tp_setattro */
	0,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,				/* tp_flags */
	0,								/* tp_doc */
	0,								/* tp_traverse */
	0,								/* tp_clear */
	0,								/* tp_richcompare */
	0,								/* tp_weaklistoffset */
	0,								/* tp_iter */
	0,								/* tp_iternext */
	noticeMethods,					/* tp_methods */
};

/* query object methods */
static struct PyMethodDef queryMethods[] = {
	{"getresult", (PyCFunction) queryGetResult, METH_VARARGS,
			queryGetResult__doc__},
	{"dictresult", (PyCFunction) queryDictResult, METH_VARARGS,
			queryDictResult__doc__},
	{"namedresult", (PyCFunction) queryNamedResult, METH_VARARGS,
			queryNamedResult__doc__},
	{"fieldname", (PyCFunction) queryFieldName, METH_VARARGS,
			 queryFieldName__doc__},
	{"fieldnum", (PyCFunction) queryFieldNumber, METH_VARARGS,
			queryFieldNumber__doc__},
	{"listfields", (PyCFunction) queryListFields, METH_VARARGS,
			queryListFields__doc__},
	{"ntuples", (PyCFunction) queryNTuples, METH_VARARGS,
			queryNTuples__doc__},
	{NULL, NULL}
};

/* query type definition */
static PyTypeObject queryType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"pgqueryobject",				/* tp_name */
	sizeof(queryObject),			/* tp_basicsize */
	0,								/* tp_itemsize */
	/* methods */
	(destructor) queryDealloc,		/* tp_dealloc */
	0,								/* tp_print */
	0,								/* tp_getattr */
	0,								/* tp_setattr */
	0,								/* tp_compare */
	(reprfunc) queryRepr,			/* tp_repr */
	0,								/* tp_as_number */
	0,								/* tp_as_sequence */
	0,								/* tp_as_mapping */
	0,								/* tp_hash */
	0,								/* tp_call */
	(reprfunc) queryStr,			/* tp_str */
	PyObject_GenericGetAttr,		/* tp_getattro */
	0,								/* tp_setattro */
	0,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,				/* tp_flags */
	0,								/* tp_doc */
	0,								/* tp_traverse */
	0,								/* tp_clear */
	0,								/* tp_richcompare */
	0,								/* tp_weaklistoffset */
	0,								/* tp_iter */
	0,								/* tp_iternext */
	queryMethods,					/* tp_methods */
};

/* --------------------------------------------------------------------- */

/* MODULE FUNCTIONS */

/* escape string */
static char pgEscapeString__doc__[] =
"escape_string(str) -- escape a string for use within SQL.";

static PyObject *
pgEscapeString(PyObject *self, PyObject *args)
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
static char pgEscapeBytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea.";

static PyObject *
pgEscapeBytea(PyObject *self, PyObject *args)
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
static char pgUnescapeBytea__doc__[] =
"unescape_bytea(str) -- unescape bytea data that has been retrieved as text.";

static PyObject
*pgUnescapeBytea(PyObject *self, PyObject *args)
{
	unsigned char *from; /* our string argument */
	unsigned char *to; /* the result */
	int from_length; /* length of string */
	size_t to_length; /* length of result string */
	PyObject *ret; /* string object to return */

	if (!PyArg_ParseTuple(args, "s#", &from, &from_length))
		return NULL;
	to = PQunescapeBytea(from, &to_length);
	if (!to)
	    return NULL;
	ret = Py_BuildValue("s#", to, (int)to_length);
	if (to)
	    PQfreemem((void *)to);
	if (!ret) /* pass on exception */
		return NULL;
	return ret;
}

/* set decimal point */
static char pgSetDecimalPoint__doc__[] =
"set_decimal_point() -- set decimal point to be used for money values.";

static PyObject *
pgSetDecimalPoint(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;
	char *s;

	if (PyArg_ParseTuple(args, "s", &s))
	{
		decimal_point = s;
		Py_INCREF(Py_None); ret = Py_None;
	}
	return ret;
}

/* get decimal point */
static char pgGetDecimalPoint__doc__[] =
"get_decimal_point() -- get decimal point to be used for money values.";

static PyObject *
pgGetDecimalPoint(PyObject *self, PyObject * args)
{
	PyObject *ret = NULL;

	if (PyArg_ParseTuple(args, ""))
	{
		ret = PyStr_FromString(decimal_point);
	}
	else
	{
		PyErr_SetString(PyExc_TypeError,
			" get_decimal_point() takes no parameter");
	}


	return ret;
}

/* set decimal */
static char pgSetDecimal__doc__[] =
"set_decimal(cls) -- set a decimal type to be used for numeric values.";

static PyObject *
pgSetDecimal(PyObject *self, PyObject *args)
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
			PyErr_SetString(PyExc_TypeError, "decimal type must be None or callable");
	}
	return ret;
}

/* set named result */
static char pgSetNamedresult__doc__[] =
"set_namedresult(cls) -- set a function to be used for getting named results.";

static PyObject *
pgSetNamedresult(PyObject *self, PyObject *args)
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
static char pgGetDefHost__doc__[] =
"get_defhost() -- return default database host.";

static PyObject *
pgGetDefHost(PyObject *self, PyObject *args)
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
static char pgSetDefHost__doc__[] =
"set_defhost(string) -- set default database host. Return previous value.";

static PyObject *
pgSetDefHost(PyObject *self, PyObject *args)
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
		pg_default_host = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_host = Py_None;
	}

	return old;
}

/* gets default base */
static char pgGetDefBase__doc__[] =
"get_defbase() -- return default database name.";

static PyObject *
pgGetDefBase(PyObject *self, PyObject *args)
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
static char pgSetDefBase__doc__[] =
"set_defbase(string) -- set default database name. Return previous value";

static PyObject *
pgSetDefBase(PyObject *self, PyObject *args)
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
		pg_default_base = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_base = Py_None;
	}

	return old;
}

/* gets default options */
static char pgGetDefOpt__doc__[] =
"get_defopt() -- return default database options.";

static PyObject *
pgGetDefOpt(PyObject *self, PyObject *args)
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
static char pgSetDefOpt__doc__[] =
"set_defopt(string) -- set default database options. Return previous value.";

static PyObject *
pgSetDefOpt(PyObject *self, PyObject *args)
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
		pg_default_opt = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_opt = Py_None;
	}

	return old;
}

/* gets default tty */
static char pgGetDefTTY__doc__[] =
"get_deftty() -- return default database debug terminal.";

static PyObject *
pgGetDefTTY(PyObject *self, PyObject *args)
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
static char pgSetDefTTY__doc__[] =
"set_deftty(string) -- set default database debug terminal. "
"Return previous value.";

static PyObject *
pgSetDefTTY(PyObject *self, PyObject *args)
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
		pg_default_tty = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_tty = Py_None;
	}

	return old;
}

/* gets default username */
static char pgGetDefUser__doc__[] =
"get_defuser() -- return default database username.";

static PyObject *
pgGetDefUser(PyObject *self, PyObject *args)
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

static char pgSetDefUser__doc__[] =
"set_defuser() -- set default database username. Return previous value.";

static PyObject *
pgSetDefUser(PyObject *self, PyObject *args)
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
		pg_default_user = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_user = Py_None;
	}

	return old;
}

/* sets default password */
static char pgSetDefPassword__doc__[] =
"set_defpasswd() -- set default database password.";

static PyObject *
pgSetDefPassword(PyObject *self, PyObject *args)
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
		pg_default_passwd = PyStr_FromString(temp);
	else
	{
		Py_INCREF(Py_None);
		pg_default_passwd = Py_None;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

/* gets default port */
static char pgGetDefPort__doc__[] =
"get_defport() -- return default database port.";

static PyObject *
pgGetDefPort(PyObject *self, PyObject *args)
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
static char pgSetDefPort__doc__[] =
"set_defport(integer) -- set default database port. Return previous value.";

static PyObject *
pgSetDefPort(PyObject *self, PyObject *args)
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

static struct PyMethodDef pgMethods[] = {
	{"connect", (PyCFunction) pgConnect, METH_VARARGS|METH_KEYWORDS,
			pgConnect__doc__},
	{"escape_string", (PyCFunction) pgEscapeString, METH_VARARGS,
			pgEscapeString__doc__},
	{"escape_bytea", (PyCFunction) pgEscapeBytea, METH_VARARGS,
			pgEscapeBytea__doc__},
	{"unescape_bytea", (PyCFunction) pgUnescapeBytea, METH_VARARGS,
			pgUnescapeBytea__doc__},
	{"set_decimal_point", (PyCFunction) pgSetDecimalPoint, METH_VARARGS,
			pgSetDecimalPoint__doc__},
	{"get_decimal_point", (PyCFunction) pgGetDecimalPoint, METH_VARARGS,
			pgGetDecimalPoint__doc__},
	{"set_decimal", (PyCFunction) pgSetDecimal, METH_VARARGS,
			pgSetDecimal__doc__},
	{"set_namedresult", (PyCFunction) pgSetNamedresult, METH_VARARGS,
			pgSetNamedresult__doc__},

#ifdef DEFAULT_VARS
	{"get_defhost", pgGetDefHost, METH_VARARGS, pgGetDefHost__doc__},
	{"set_defhost", pgSetDefHost, METH_VARARGS, pgSetDefHost__doc__},
	{"get_defbase", pgGetDefBase, METH_VARARGS, pgGetDefBase__doc__},
	{"set_defbase", pgSetDefBase, METH_VARARGS, pgSetDefBase__doc__},
	{"get_defopt", pgGetDefOpt, METH_VARARGS, pgGetDefOpt__doc__},
	{"set_defopt", pgSetDefOpt, METH_VARARGS, pgSetDefOpt__doc__},
	{"get_deftty", pgGetDefTTY, METH_VARARGS, pgGetDefTTY__doc__},
	{"set_deftty", pgSetDefTTY, METH_VARARGS, pgSetDefTTY__doc__},
	{"get_defport", pgGetDefPort, METH_VARARGS, pgGetDefPort__doc__},
	{"set_defport", pgSetDefPort, METH_VARARGS, pgSetDefPort__doc__},
	{"get_defuser", pgGetDefUser, METH_VARARGS, pgGetDefUser__doc__},
	{"set_defuser", pgSetDefUser, METH_VARARGS, pgSetDefUser__doc__},
	{"set_defpasswd", pgSetDefPassword, METH_VARARGS, pgSetDefPassword__doc__},
#endif /* DEFAULT_VARS */
	{NULL, NULL} /* sentinel */
};

static char pg__doc__[] = "Python interface to PostgreSQL DB";

static struct PyModuleDef moduleDef = {
	PyModuleDef_HEAD_INIT,
	"_pg",		/* m_name */
	pg__doc__,	/* m_doc */
	-1,			/* m_size */
	pgMethods	/* m_methods */
};

/* Initialization function for the module */
MODULE_INIT_FUNC(_pg)
{
	PyObject   *mod, *dict, *s;

	/* Create the module and add the functions */

	mod = PyModule_Create(&moduleDef);

	/* Initialize here because some Windows platforms get confused otherwise */
#if IS_PY3
	connType.tp_base = noticeType.tp_base =
		queryType.tp_base = sourceType.tp_base = &PyBaseObject_Type;
#ifdef LARGE_OBJECTS
	largeType.tp_base = &PyBaseObject_Type;
#endif
#else
	connType.ob_type = noticeType.ob_type =
		queryType.ob_type = sourceType.ob_type = &PyType_Type;
#ifdef LARGE_OBJECTS
	largeType.ob_type = &PyType_Type;
#endif
#endif

	if (PyType_Ready(&connType)
		|| PyType_Ready(&noticeType)
		|| PyType_Ready(&queryType)
		|| PyType_Ready(&sourceType)
#ifdef LARGE_OBJECTS
		|| PyType_Ready(&largeType)
#endif
		) return NULL;

	/* make the module names available */
	s = PyStr_FromString("pg");
	PyDict_SetItemString(connType.tp_dict, "__module__", s);
	PyDict_SetItemString(noticeType.tp_dict, "__module__", s);
	PyDict_SetItemString(queryType.tp_dict, "__module__", s);
#ifdef LARGE_OBJECTS
	PyDict_SetItemString(largeType.tp_dict, "__module__", s);
#endif
	Py_DECREF(s);
	s = PyStr_FromString("pgdb");
	PyDict_SetItemString(sourceType.tp_dict, "__module__", s);
	Py_DECREF(s);

	dict = PyModule_GetDict(mod);

	/* Exceptions as defined by DB-API 2.0 */
	Error = PyErr_NewException("pg.Error", PyExc_Exception, NULL);
	PyDict_SetItemString(dict, "Error", Error);

	Warning = PyErr_NewException("pg.Warning", PyExc_Exception, NULL);
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
	s = PyStr_FromString(PyPgVersion);
	PyDict_SetItemString(dict, "version", s);
	PyDict_SetItemString(dict, "__version__", s);
	Py_DECREF(s);

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
		return NULL;

	return mod;
}
