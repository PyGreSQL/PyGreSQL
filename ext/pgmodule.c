/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * This is the main file for the C extension module.
 *
 * Copyright (c) 2026 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Note: This should be linked against the same C runtime lib as Python */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

/* Shared headers */
#include "pginternal.h"
#include "pgmodule.h"

PyObject *Error, *Warning, *InterfaceError, *DatabaseError, *InternalError,
    *OperationalError, *ProgrammingError, *IntegrityError, *DataError,
    *NotSupportedError, *InvalidResultError, *NoResultError,
    *MultipleResultsError;

#define _TOSTRING(x) #x
#define TOSTRING(x) _TOSTRING(x)
static const char *PyPgVersion = TOSTRING(PYGRESQL_VERSION);

#if SIZEOF_SIZE_T != SIZEOF_INT
#define Py_InitModule4 Py_InitModule4_64
#endif

/* MODULE GLOBAL VARIABLES */

PyObject *pg_default_host;   /* default database host */
PyObject *pg_default_base;   /* default database name */
PyObject *pg_default_opt;    /* default connection options */
PyObject *pg_default_port;   /* default connection port */
PyObject *pg_default_user;   /* default username */
PyObject *pg_default_passwd; /* default password */

PyObject *decimal = NULL;       /* decimal type */
PyObject *dictiter = NULL;      /* function for getting dict results */
PyObject *namediter = NULL;     /* function for getting named results */
PyObject *namednext = NULL;     /* function for getting one named result */
PyObject *scalariter = NULL;    /* function for getting scalar results */
PyObject *bytea_type = NULL;    /* type for marking bytea values */
PyObject *jsondecode = NULL;    /* function for decoding json strings */
const char *date_format = NULL; /* date format that is always assumed */
char decimal_point = '.';       /* decimal point used in money values */
int bool_as_text = 0;           /* whether bool shall be returned as text */
int array_as_text = 0;          /* whether arrays shall be returned as text */
int bytea_escaped = 0;          /* whether bytea shall be returned escaped */

int pg_encoding_utf8 = 0;
int pg_encoding_latin1 = 0;
int pg_encoding_ascii = 0;

/*
OBJECTS
=======

  Each object has a number of elements.  The naming scheme will be based on
  the object type.  Here are the elements using example object type "foo".
   - fooType: Type definition for object.
   - fooObject: A structure to hold local object information.
   - foo_methods: Methods declaration.
   - foo_method_name: Object methods.

  The objects that we need to create:
   - pg: The module itself.
   - conn: Connection object returned from pg.connect().
   - notice: Notice object returned from pg.notice().
   - large: Large object returned by pg.conn.locreate() and pg.conn.loimport().
   - query: Query object returned by pg.conn.query().
   - source: Source object returned by pg.conn.source().
*/

/* Type objects are defined in their respective .c files */

/* Object declarations are provided by shared headers */

/* Object implementation files are compiled separately now */

/* MODULE FUNCTIONS */

/* Get a connection parameter given as str or bytes as a C string
   (internal use only). A str is encoded as UTF-8, since there is no
   client encoding before connecting, bytes are passed unchanged.
   The string is valid as long as the object exists. Returns NULL with
   an exception if the object has a wrong type or contains NUL. */
static const char *
_param_string(PyObject *obj)
{
    const char *s;
    Py_ssize_t size;

    if (PyBytes_Check(obj)) {
        char *b;

        /* this raises ValueError if the bytes contain NUL */
        return PyBytes_AsStringAndSize(obj, &b, NULL) ? NULL : b;
    }
    if (!PyUnicode_Check(obj)) {
        PyErr_Format(PyExc_TypeError,
                     "Connection parameters must be str, bytes or None,"
                     " not %.100s",
                     Py_TYPE(obj)->tp_name);
        return NULL;
    }
    if (!(s = PyUnicode_AsUTF8AndSize(obj, &size)))
        return NULL; /* pass the UnicodeEncodeError */
    if (strlen(s) != (size_t)size) {
        PyErr_SetString(PyExc_ValueError, "embedded null character");
        return NULL;
    }
    return s;
}

/* Converter for connection parameters given as str, bytes or None
   (internal use only). Stores a borrowed reference to the object or
   NULL for None in addr, after checking that it is a valid parameter. */
static int
_param_converter(PyObject *obj, void *addr)
{
    if (obj == Py_None) {
        obj = NULL;
    }
    else if (!_param_string(obj)) {
        return 0;
    }
    *(PyObject **)addr = obj;
    return 1;
}

/* Converter for port numbers given as int or None (internal use only).
   Stores the port in addr as an int, or -1 for None or -1, which both
   stand for the default port. */
static int
_port_converter(PyObject *obj, void *addr)
{
    long port = -1;

    if (obj != Py_None) {
        int overflow;

        port = PyLong_AsLongAndOverflow(obj, &overflow);
        if (port == -1 && PyErr_Occurred())
            return 0; /* pass the TypeError */
        if (overflow || port == 0 || port < -1 || port > 65535) {
            PyErr_SetString(PyExc_ValueError,
                            "The port must be between 1 and 65535,"
                            " or -1 or None for the default port");
            return 0;
        }
    }
    *(int *)addr = (int)port;
    return 1;
}

/* Get the value of a connection parameter as a C string (internal use
   only). If the parameter has not been passed, the default value is
   used and a reference to it is stored in defaults, since the default
   could be changed while the GIL is released. Returns 0 on error. */
static int
_param_value(PyObject *param, PyObject *dflt, const char **value,
             PyObject **defaults, size_t *ndef)
{
    if (!param) {
        if (dflt == Py_None)
            return 1;
        param = defaults[(*ndef)++] = Py_NewRef(dflt);
    }
    return (*value = _param_string(param)) != NULL;
}

/* Connect to a database. */
static char pg_connect__doc__[] =
    "connect(dbname, host, port, opt, user, passwd, nowait) -- connect to a "
    "PostgreSQL database\n\n"
    "The connection uses the specified parameters (optional, keywords "
    "aware).\n";

static PyObject *
pg_connect(PyObject *Py_UNUSED(self), PyObject *args, PyObject *dict)
{
    static const char *kwlist[] = {"dbname", "host",   "port",   "opt",
                                   "user",   "passwd", "nowait", NULL};

    PyObject *host_obj = NULL, *opt_obj = NULL, *dbname_obj = NULL,
             *user_obj = NULL, *passwd_obj = NULL;
    const char *pghost, *pgopt, *pgdbname, *pguser, *pgpasswd;
    int pgport = -1, nowait = 0, nkw = 0;
    char port_buffer[20];
    const char *keywords[sizeof(kwlist) / sizeof(*kwlist) + 1],
        *values[sizeof(kwlist) / sizeof(*kwlist) + 1];
    connObject *conn_obj;
    PyObject *defaults[5] = {NULL}; /* references to the defaults used */
    size_t ndef = 0;

    pghost = pgopt = pgdbname = pguser = pgpasswd = NULL;

    /* the string parameters can be str, bytes or None (use the default),
       the port can be an int or None (use the default) */
    if (!PyArg_ParseTupleAndKeywords(
            args, dict, "|O&O&O&O&O&O&i", (char **)kwlist, _param_converter,
            &dbname_obj, _param_converter, &host_obj, _port_converter, &pgport,
            _param_converter, &opt_obj, _param_converter, &user_obj,
            _param_converter, &passwd_obj, &nowait)) {
        return NULL;
    }

    /* handles defaults variables (for uninitialised vars) */
    if ((pgport == -1) && (pg_default_port != Py_None))
        pgport = (int)PyLong_AsLong(pg_default_port);

    if (!_param_value(host_obj, pg_default_host, &pghost, defaults, &ndef) ||
        !_param_value(opt_obj, pg_default_opt, &pgopt, defaults, &ndef) ||
        !_param_value(dbname_obj, pg_default_base, &pgdbname, defaults,
                      &ndef) ||
        !_param_value(user_obj, pg_default_user, &pguser, defaults, &ndef) ||
        !_param_value(passwd_obj, pg_default_passwd, &pgpasswd, defaults,
                      &ndef)) {
        while (ndef) Py_DECREF(defaults[--ndef]);
        return NULL;
    }

    if (!(conn_obj = PyObject_GC_New(connObject, &connType))) {
        while (ndef) Py_DECREF(defaults[--ndef]);
        set_error_msg(InternalError, "Can't create new connection object");
        return NULL;
    }

    conn_obj->valid = 1;
    conn_obj->cnx = NULL;
    conn_obj->date_format = date_format;
    conn_obj->cast_hook = NULL;
    conn_obj->notice_receiver = NULL;
    conn_obj->default_notice_receiver = NULL;
    conn_obj->encoding = -1;
    conn_obj->async_query = NULL;
    conn_obj->busy = 0;
    PyObject_GC_Track(conn_obj);

    if (pghost) {
        keywords[nkw] = "host";
        values[nkw++] = pghost;
    }
    if (pgopt) {
        keywords[nkw] = "options";
        values[nkw++] = pgopt;
    }
    if (pgdbname) {
        keywords[nkw] = "dbname";
        values[nkw++] = pgdbname;
    }
    if (pguser) {
        keywords[nkw] = "user";
        values[nkw++] = pguser;
    }
    if (pgpasswd) {
        keywords[nkw] = "password";
        values[nkw++] = pgpasswd;
    }
    if (pgport != -1) {
        memset(port_buffer, 0, sizeof(port_buffer));
        snprintf(port_buffer, sizeof(port_buffer), "%d", pgport);

        keywords[nkw] = "port";
        values[nkw++] = port_buffer;
    }
    keywords[nkw] = values[nkw] = NULL;

    Py_BEGIN_ALLOW_THREADS
    conn_obj->cnx = nowait ? PQconnectStartParams(keywords, values, 1)
                           : PQconnectdbParams(keywords, values, 1);
    Py_END_ALLOW_THREADS
    while (ndef) Py_DECREF(defaults[--ndef]);

    if (PQstatus(conn_obj->cnx) == CONNECTION_BAD) {
        set_error(InternalError, "Cannot connect", conn_obj->cnx, NULL);
        Py_XDECREF(conn_obj);
        return NULL;
    }

    return (PyObject *)conn_obj;
}

/* Get version of libpq that is being used */
static char pg_get_pqlib_version__doc__[] =
    "get_pqlib_version() -- get the version of libpq that is being used";

static PyObject *
pg_get_pqlib_version(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return PyLong_FromLong(PQlibVersion());
}

/* Escape string */
static char pg_escape_string__doc__[] =
    "escape_string(string) -- escape a string for use within SQL";

static PyObject *
pg_escape_string(PyObject *Py_UNUSED(self), PyObject *string)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        encoding = pg_encoding_ascii;
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method escape_string() expects a string as argument");
        return NULL;
    }

    /* libpq stops at NUL characters, so they would be truncated */
    if (memchr(from, '\0', (size_t)from_length)) {
        Py_XDECREF(tmp_obj);
        PyErr_SetString(PyExc_ValueError, "embedded null character");
        return NULL;
    }

    to_length = 2 * (size_t)from_length + 1;
    if ((Py_ssize_t)to_length < from_length) { /* overflow */
        to_length = (size_t)from_length;
        from_length = (from_length - 1) / 2;
    }
    to = (char *)PyMem_Malloc(to_length);
    if (!to) {
        Py_XDECREF(tmp_obj);
        return PyErr_NoMemory();
    }
    to_length = (size_t)PQescapeString(to, from, (size_t)from_length);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length, encoding);
    PyMem_Free(to);
    return to_obj;
}

/* Escape bytea */
static char pg_escape_bytea__doc__[] =
    "escape_bytea(data) -- escape binary data for use within SQL as type "
    "bytea";

static PyObject *
pg_escape_bytea(PyObject *Py_UNUSED(self), PyObject *data)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        encoding = pg_encoding_ascii;
        tmp_obj = get_encoded_string(data, encoding);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method escape_bytea() expects a string as argument");
        return NULL;
    }

    to = (char *)PQescapeBytea((unsigned char *)from, (size_t)from_length,
                               &to_length);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length - 1);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length - 1, encoding);
    if (to)
        PQfreemem(to);
    return to_obj;
}

/* Unescape bytea */
static char pg_unescape_bytea__doc__[] =
    "unescape_bytea(string) -- unescape bytea data retrieved as text";

static PyObject *
pg_unescape_bytea(PyObject *Py_UNUSED(self), PyObject *data)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        tmp_obj = get_encoded_string(data, pg_encoding_ascii);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method unescape_bytea() expects a string as argument");
        return NULL;
    }

    to = (char *)PQunescapeBytea((unsigned char *)from, &to_length);

    Py_XDECREF(tmp_obj);

    if (!to)
        return PyErr_NoMemory();

    to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length);
    PQfreemem(to);

    return to_obj;
}

/* Set fixed datestyle. */
static char pg_set_datestyle__doc__[] =
    "set_datestyle(style) -- set which style is assumed";

static PyObject *
pg_set_datestyle(PyObject *Py_UNUSED(self), PyObject *args)
{
    const char *datestyle = NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &datestyle)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_datestyle() expects a string or None as argument");
        return NULL;
    }

    date_format = datestyle ? date_style_to_format(datestyle) : NULL;

    Py_RETURN_NONE;
}

/* Get fixed datestyle. */
static char pg_get_datestyle__doc__[] =
    "get_datestyle() -- get which date style is assumed";

static PyObject *
pg_get_datestyle(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    if (date_format) {
        return PyUnicode_FromString(date_format_to_style(date_format));
    }
    else {
        Py_RETURN_NONE;
    }
}

/* Get decimal point. */
static char pg_get_decimal_point__doc__[] =
    "get_decimal_point() -- get decimal point to be used for money values";

static PyObject *
pg_get_decimal_point(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    PyObject *ret;
    char s[2];

    if (decimal_point) {
        s[0] = decimal_point;
        s[1] = '\0';
        ret = PyUnicode_FromString(s);
    }
    else {
        ret = Py_NewRef(Py_None);
    }

    return ret;
}

/* Set decimal point. */
static char pg_set_decimal_point__doc__[] =
    "set_decimal_point(char) -- set decimal point to be used for money values";

static PyObject *
pg_set_decimal_point(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *ret = NULL;
    const char *s = NULL;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "z", &s)) {
        if (!s)
            s = "\0";
        else if (*s && (*(s + 1) || !strchr(".,;: '*/_`|", *s)))
            s = NULL;
    }

    if (s) {
        decimal_point = *s;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_decimal_point() expects"
                        " a decimal mark character as argument");
    }
    return ret;
}

/* Get decimal type. */
static char pg_get_decimal__doc__[] =
    "get_decimal() -- get the decimal type to be used for numeric values";

static PyObject *
pg_get_decimal(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(decimal ? decimal : Py_None);
}

/* Set decimal type. */
static char pg_set_decimal__doc__[] =
    "set_decimal(cls) -- set a decimal type to be used for numeric values";

static PyObject *
pg_set_decimal(PyObject *Py_UNUSED(self), PyObject *cls)
{
    PyObject *ret = NULL;

    if (cls == Py_None) {
        Py_XDECREF(decimal);
        decimal = NULL;
        ret = Py_NewRef(Py_None);
    }
    else if (PyCallable_Check(cls)) {
        Py_XINCREF(cls);
        Py_XDECREF(decimal);
        decimal = cls;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_decimal() expects"
                        " a callable or None as argument");
    }

    return ret;
}

/* Get usage of bool values. */
static char pg_get_bool__doc__[] =
    "get_bool() -- check whether boolean values are converted to bool";

static PyObject *
pg_get_bool(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return PyBool_FromLong(!bool_as_text);
}

/* Set usage of bool values. */
static char pg_set_bool__doc__[] =
    "set_bool(on) -- set whether boolean values should be converted to bool";

static PyObject *
pg_set_bool(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        bool_as_text = i ? 0 : 1;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_bool() expects a boolean value as argument");
    }

    return ret;
}

/* Get conversion of arrays to lists. */
static char pg_get_array__doc__[] =
    "get_array() -- check whether arrays are converted as lists";

static PyObject *
pg_get_array(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return PyBool_FromLong(!array_as_text);
}

/* Set conversion of arrays to lists. */
static char pg_set_array__doc__[] =
    "set_array(on) -- set whether arrays should be converted to lists";

static PyObject *
pg_set_array(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        array_as_text = i ? 0 : 1;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_array() expects a boolean value as argument");
    }

    return ret;
}

/* Check whether bytea values are unescaped. */
static char pg_get_bytea_escaped__doc__[] =
    "get_bytea_escaped() -- check whether bytea will be returned escaped";

static PyObject *
pg_get_bytea_escaped(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return PyBool_FromLong(bytea_escaped);
}

/* Set usage of bool values. */
static char pg_set_bytea_escaped__doc__[] =
    "set_bytea_escaped(on) -- set whether bytea will be returned escaped";

static PyObject *
pg_set_bytea_escaped(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        bytea_escaped = i ? 1 : 0;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_bytea_escaped() expects"
                        " a boolean value as argument");
    }

    return ret;
}

/* set query helper functions (not part of public API) */

static char pg_set_query_helpers__doc__[] =
    "set_query_helpers(*helpers) -- set internal query helper functions\n\n"
    "The helpers are the functions dictiter, namediter, namednext and\n"
    "scalariter, and optionally the type used for marking bytea values.\n";

static PyObject *
pg_set_query_helpers(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *bytea = NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "O!O!O!O!|O!", &PyFunction_Type, &dictiter,
                          &PyFunction_Type, &namediter, &PyFunction_Type,
                          &namednext, &PyFunction_Type, &scalariter,
                          &PyType_Type, &bytea)) {
        return NULL;
    }
    if (bytea)
        Py_XSETREF(bytea_type, Py_NewRef(bytea));

    Py_RETURN_NONE;
}

/* Get json decode function. */
static char pg_get_jsondecode__doc__[] =
    "get_jsondecode() -- get the function used for decoding json results";

static PyObject *
pg_get_jsondecode(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(jsondecode ? jsondecode : Py_None);
}

/* Set json decode function. */
static char pg_set_jsondecode__doc__[] =
    "set_jsondecode(func) -- set a function to be used for decoding json "
    "results";

static PyObject *
pg_set_jsondecode(PyObject *Py_UNUSED(self), PyObject *func)
{
    PyObject *ret = NULL;

    if (func == Py_None) {
        Py_XDECREF(jsondecode);
        jsondecode = NULL;
        ret = Py_NewRef(Py_None);
    }
    else if (PyCallable_Check(func)) {
        Py_XINCREF(func);
        Py_XDECREF(jsondecode);
        jsondecode = func;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function jsondecode() expects"
                        " a callable or None as argument");
    }

    return ret;
}

/* Set a default connection parameter given as str, bytes or None
   (internal use only). Returns the previous value. */
static PyObject *
_set_default(PyObject **dflt, PyObject *args, const char *format)
{
    PyObject *value, *old;

    if (!PyArg_ParseTuple(args, format, _param_converter, &value)) {
        return NULL;
    }

    old = *dflt;
    *dflt = Py_NewRef(value ? value : Py_None);
    return old;
}

/* Get default database host. */
static char pg_get_defhost__doc__[] =
    "get_defhost() -- return default database host";

static PyObject *
pg_get_defhost(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(pg_default_host);
}

/* Set default database host. */
static char pg_set_defhost__doc__[] =
    "set_defhost(host) -- set default database host and return previous value";

static PyObject *
pg_set_defhost(PyObject *Py_UNUSED(self), PyObject *args)
{
    return _set_default(&pg_default_host, args, "O&:set_defhost");
}

/* Get default database name. */
static char pg_get_defbase__doc__[] =
    "get_defbase() -- return default database name";

static PyObject *
pg_get_defbase(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(pg_default_base);
}

/* Set default database name. */
static char pg_set_defbase__doc__[] =
    "set_defbase(name) -- set default database name and return previous value";

static PyObject *
pg_set_defbase(PyObject *Py_UNUSED(self), PyObject *args)
{
    return _set_default(&pg_default_base, args, "O&:set_defbase");
}

/* Get default connection options. */
static char pg_get_defopt__doc__[] =
    "get_defopt() -- return default connection options";

static PyObject *
pg_get_defopt(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(pg_default_opt);
}

/* Set default connection options. */
static char pg_set_defopt__doc__[] =
    "set_defopt(options) -- set default connection options and return "
    "previous value";

static PyObject *
pg_set_defopt(PyObject *Py_UNUSED(self), PyObject *args)
{
    return _set_default(&pg_default_opt, args, "O&:set_defopt");
}

/* Get default database username. */
static char pg_get_defuser__doc__[] =
    "get_defuser() -- return default database username";

static PyObject *
pg_get_defuser(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(pg_default_user);
}

/* Set default database username. */
static char pg_set_defuser__doc__[] =
    "set_defuser(name) -- set default database username and return previous "
    "value";

static PyObject *
pg_set_defuser(PyObject *Py_UNUSED(self), PyObject *args)
{
    return _set_default(&pg_default_user, args, "O&:set_defuser");
}

/* Set default password. */
static char pg_set_defpasswd__doc__[] =
    "set_defpasswd(password) -- set default database password";

static PyObject *
pg_set_defpasswd(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *old;

    if (!(old = _set_default(&pg_default_passwd, args, "O&:set_defpasswd")))
        return NULL;

    /* the previous password is not returned */
    Py_DECREF(old);
    Py_RETURN_NONE;
}

/* Get default port. */
static char pg_get_defport__doc__[] =
    "get_defport() -- return default database port";

static PyObject *
pg_get_defport(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(noargs))
{
    return Py_NewRef(pg_default_port);
}

/* Set default port. */
static char pg_set_defport__doc__[] =
    "set_defport(port) -- set default port and return previous value";

static PyObject *
pg_set_defport(PyObject *Py_UNUSED(self), PyObject *args)
{
    PyObject *port_obj, *old;
    int port;

    if (!PyArg_ParseTuple(args, "O&:set_defport", _port_converter, &port)) {
        return NULL;
    }

    if (port == -1) {
        port_obj = Py_NewRef(Py_None);
    }
    else if (!(port_obj = PyLong_FromLong(port))) {
        return NULL;
    }

    old = pg_default_port;
    pg_default_port = port_obj;
    return old;
}

/* Cast a string with a text representation of an array to a list. */
static char pg_cast_array__doc__[] =
    "cast_array(string, cast=None, delim=',') -- cast a string as an array";

static PyObject *
pg_cast_array(PyObject *Py_UNUSED(self), PyObject *args, PyObject *dict)
{
    static const char *kwlist[] = {"string", "cast", "delim", NULL};
    PyObject *string_obj, *cast_obj = NULL, *ret;
    char *string, delim = ',';
    Py_ssize_t size;
    int encoding;

    if (!PyArg_ParseTupleAndKeywords(args, dict, "O|Oc", (char **)kwlist,
                                     &string_obj, &cast_obj, &delim)) {
        return NULL;
    }

    if (PyBytes_Check(string_obj)) {
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        string_obj = NULL;
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string_obj)) {
        string_obj = PyUnicode_AsUTF8String(string_obj);
        if (!string_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_array() expects a string as first argument");
        return NULL;
    }

    if (cast_obj == Py_None) {
        cast_obj = NULL;
    }
    else if (cast_obj && !PyCallable_Check(cast_obj)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_array() expects a callable as second argument");
        return NULL;
    }

    ret = cast_array(string, size, encoding, 0, cast_obj, delim);

    Py_XDECREF(string_obj);

    return ret;
}

/* Cast a string with a text representation of a record to a tuple. */
static char pg_cast_record__doc__[] =
    "cast_record(string, cast=None, delim=',') -- cast a string as a record";

static PyObject *
pg_cast_record(PyObject *Py_UNUSED(self), PyObject *args, PyObject *dict)
{
    static const char *kwlist[] = {"string", "cast", "delim", NULL};
    PyObject *string_obj, *cast_obj = NULL, *ret;
    char *string, delim = ',';
    Py_ssize_t size, len;
    int encoding;

    if (!PyArg_ParseTupleAndKeywords(args, dict, "O|Oc", (char **)kwlist,
                                     &string_obj, &cast_obj, &delim)) {
        return NULL;
    }

    if (PyBytes_Check(string_obj)) {
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        string_obj = NULL;
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string_obj)) {
        string_obj = PyUnicode_AsUTF8String(string_obj);
        if (!string_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_record() expects a string as first argument");
        return NULL;
    }

    if (!cast_obj || PyCallable_Check(cast_obj)) {
        len = 0;
    }
    else if (cast_obj == Py_None) {
        cast_obj = NULL;
        len = 0;
    }
    else if (PyTuple_Check(cast_obj) || PyList_Check(cast_obj)) {
        len = PySequence_Size(cast_obj);
        if (!len) {
            cast_obj = NULL;
        }
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function cast_record() expects a callable"
                        " or tuple or list of callables as second argument");
        return NULL;
    }

    ret = cast_record(string, size, encoding, 0, cast_obj, len, delim);

    Py_XDECREF(string_obj);

    return ret;
}

/* Cast a string with a text representation of an hstore to a dict. */
static char pg_cast_hstore__doc__[] =
    "cast_hstore(string) -- cast a string as an hstore";

static PyObject *
pg_cast_hstore(PyObject *Py_UNUSED(self), PyObject *string)
{
    PyObject *tmp_obj = NULL, *ret;
    char *s;
    Py_ssize_t size;
    int encoding;

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &s, &size);
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string)) {
        tmp_obj = PyUnicode_AsUTF8String(string);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &s, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_hstore() expects a string as first argument");
        return NULL;
    }

    ret = cast_hstore(s, size, encoding);

    Py_XDECREF(tmp_obj);

    return ret;
}

/* The list of functions defined in the module */

static struct PyMethodDef pg_methods[] = {
    {"connect", (PyCFunction)(void (*)(void))pg_connect,
     METH_VARARGS | METH_KEYWORDS, pg_connect__doc__},
    {"escape_string", (PyCFunction)pg_escape_string, METH_O,
     pg_escape_string__doc__},
    {"escape_bytea", (PyCFunction)pg_escape_bytea, METH_O,
     pg_escape_bytea__doc__},
    {"unescape_bytea", (PyCFunction)pg_unescape_bytea, METH_O,
     pg_unescape_bytea__doc__},
    {"get_datestyle", (PyCFunction)pg_get_datestyle, METH_NOARGS,
     pg_get_datestyle__doc__},
    {"set_datestyle", (PyCFunction)pg_set_datestyle, METH_VARARGS,
     pg_set_datestyle__doc__},
    {"get_decimal_point", (PyCFunction)pg_get_decimal_point, METH_NOARGS,
     pg_get_decimal_point__doc__},
    {"set_decimal_point", (PyCFunction)pg_set_decimal_point, METH_VARARGS,
     pg_set_decimal_point__doc__},
    {"get_decimal", (PyCFunction)pg_get_decimal, METH_NOARGS,
     pg_get_decimal__doc__},
    {"set_decimal", (PyCFunction)pg_set_decimal, METH_O,
     pg_set_decimal__doc__},
    {"get_bool", (PyCFunction)pg_get_bool, METH_NOARGS, pg_get_bool__doc__},
    {"set_bool", (PyCFunction)pg_set_bool, METH_VARARGS, pg_set_bool__doc__},
    {"get_array", (PyCFunction)pg_get_array, METH_NOARGS, pg_get_array__doc__},
    {"set_array", (PyCFunction)pg_set_array, METH_VARARGS,
     pg_set_array__doc__},
    {"set_query_helpers", (PyCFunction)pg_set_query_helpers, METH_VARARGS,
     pg_set_query_helpers__doc__},
    {"get_bytea_escaped", (PyCFunction)pg_get_bytea_escaped, METH_NOARGS,
     pg_get_bytea_escaped__doc__},
    {"set_bytea_escaped", (PyCFunction)pg_set_bytea_escaped, METH_VARARGS,
     pg_set_bytea_escaped__doc__},
    {"get_jsondecode", (PyCFunction)pg_get_jsondecode, METH_NOARGS,
     pg_get_jsondecode__doc__},
    {"set_jsondecode", (PyCFunction)pg_set_jsondecode, METH_O,
     pg_set_jsondecode__doc__},
    {"cast_array", (PyCFunction)(void (*)(void))pg_cast_array,
     METH_VARARGS | METH_KEYWORDS, pg_cast_array__doc__},
    {"cast_record", (PyCFunction)(void (*)(void))pg_cast_record,
     METH_VARARGS | METH_KEYWORDS, pg_cast_record__doc__},
    {"cast_hstore", (PyCFunction)pg_cast_hstore, METH_O,
     pg_cast_hstore__doc__},
    {"get_defhost", pg_get_defhost, METH_NOARGS, pg_get_defhost__doc__},
    {"set_defhost", pg_set_defhost, METH_VARARGS, pg_set_defhost__doc__},
    {"get_defbase", pg_get_defbase, METH_NOARGS, pg_get_defbase__doc__},
    {"set_defbase", pg_set_defbase, METH_VARARGS, pg_set_defbase__doc__},
    {"get_defopt", pg_get_defopt, METH_NOARGS, pg_get_defopt__doc__},
    {"set_defopt", pg_set_defopt, METH_VARARGS, pg_set_defopt__doc__},
    {"get_defport", pg_get_defport, METH_NOARGS, pg_get_defport__doc__},
    {"set_defport", pg_set_defport, METH_VARARGS, pg_set_defport__doc__},
    {"get_defuser", pg_get_defuser, METH_NOARGS, pg_get_defuser__doc__},
    {"set_defuser", pg_set_defuser, METH_VARARGS, pg_set_defuser__doc__},
    {"set_defpasswd", pg_set_defpasswd, METH_VARARGS, pg_set_defpasswd__doc__},
    {"get_pqlib_version", (PyCFunction)pg_get_pqlib_version, METH_NOARGS,
     pg_get_pqlib_version__doc__},
    {NULL, NULL, 0, NULL} /* sentinel */
};

static char pg__doc__[] = "Python interface to PostgreSQL DB";

static struct PyModuleDef moduleDef = {
    PyModuleDef_HEAD_INIT, .m_name = "_pg",         .m_doc = pg__doc__,
    .m_size = -1,          .m_methods = pg_methods,
};

/* Create an exception type and add it to the module.
   The name must be fully qualified, the module gets the last part.
   Returns a new reference, or NULL with an exception set on failure. */
static PyObject *
add_exception(PyObject *mod, const char *name, PyObject *base)
{
    PyObject *exc = PyErr_NewException(name, base, NULL);

    if (exc && PyModule_AddObjectRef(mod, strrchr(name, '.') + 1, exc) < 0)
        Py_CLEAR(exc);
    return exc;
}

/* Integer constants exported by the module */
static const struct {
    const char *name;
    long value;
} int_constants[] = {
    /* Result types for queries */
    {"RESULT_EMPTY", RESULT_EMPTY},
    {"RESULT_DML", RESULT_DML},
    {"RESULT_DDL", RESULT_DDL},
    {"RESULT_DQL", RESULT_DQL},
    /* Transaction states */
    {"TRANS_IDLE", PQTRANS_IDLE},
    {"TRANS_ACTIVE", PQTRANS_ACTIVE},
    {"TRANS_INTRANS", PQTRANS_INTRANS},
    {"TRANS_INERROR", PQTRANS_INERROR},
    {"TRANS_UNKNOWN", PQTRANS_UNKNOWN},
    /* Polling results */
    {"POLLING_OK", PGRES_POLLING_OK},
    {"POLLING_FAILED", PGRES_POLLING_FAILED},
    {"POLLING_READING", PGRES_POLLING_READING},
    {"POLLING_WRITING", PGRES_POLLING_WRITING},
    /* Create mode for large objects */
    {"INV_READ", INV_READ},
    {"INV_WRITE", INV_WRITE},
    /* Position flags for lo_lseek */
    {"SEEK_SET", SEEK_SET},
    {"SEEK_CUR", SEEK_CUR},
    {"SEEK_END", SEEK_END},
    {NULL, 0} /* sentinel */
};

/* Initialization function for the module */
PyMODINIT_FUNC
PyInit__pg(void);

PyMODINIT_FUNC
PyInit__pg(void)
{
    PyObject *mod;
    int i;

    /* Create the module and add the functions */
    mod = PyModule_Create(&moduleDef);
    if (!mod)
        return NULL;

    /* Exceptions as defined by DB-API 2.0 */
    if (!(Error = add_exception(mod, "pg.Error", PyExc_Exception)) ||
        !(Warning = add_exception(mod, "pg.Warning", PyExc_Exception)) ||
        !(InterfaceError = add_exception(mod, "pg.InterfaceError", Error)) ||
        !(DatabaseError = add_exception(mod, "pg.DatabaseError", Error)) ||
        !(InternalError =
              add_exception(mod, "pg.InternalError", DatabaseError)) ||
        !(OperationalError =
              add_exception(mod, "pg.OperationalError", DatabaseError)) ||
        !(ProgrammingError =
              add_exception(mod, "pg.ProgrammingError", DatabaseError)) ||
        !(IntegrityError =
              add_exception(mod, "pg.IntegrityError", DatabaseError)) ||
        !(DataError = add_exception(mod, "pg.DataError", DatabaseError)) ||
        !(NotSupportedError =
              add_exception(mod, "pg.NotSupportedError", DatabaseError)) ||
        !(InvalidResultError =
              add_exception(mod, "pg.InvalidResultError", DataError)) ||
        !(NoResultError =
              add_exception(mod, "pg.NoResultError", InvalidResultError)) ||
        !(MultipleResultsError = add_exception(mod, "pg.MultipleResultsError",
                                               InvalidResultError))) {
        goto error;
    }

    /* Types (only some of them are exposed in the module) */
    if (PyModule_AddType(mod, &connType) < 0 ||
        PyModule_AddType(mod, &queryType) < 0 ||
        PyModule_AddType(mod, &largeType) < 0 ||
        PyType_Ready(&noticeType) < 0 || PyType_Ready(&sourceType) < 0) {
        goto error;
    }

    /* Make the version available */
    if (PyModule_AddStringConstant(mod, "version", PyPgVersion) < 0 ||
        PyModule_AddStringConstant(mod, "__version__", PyPgVersion) < 0) {
        goto error;
    }

    /* Integer constants */
    for (i = 0; int_constants[i].name; ++i) {
        if (PyModule_AddIntConstant(mod, int_constants[i].name,
                                    int_constants[i].value) < 0)
            goto error;
    }

    /* Prepare default values */
    pg_default_host = Py_NewRef(Py_None);
    pg_default_base = Py_NewRef(Py_None);
    pg_default_opt = Py_NewRef(Py_None);
    pg_default_port = Py_NewRef(Py_None);
    pg_default_user = Py_NewRef(Py_None);
    pg_default_passwd = Py_NewRef(Py_None);

    /* Store common pg encoding ids */
    pg_encoding_utf8 = pg_char_to_encoding("UTF8");
    pg_encoding_latin1 = pg_char_to_encoding("LATIN1");
    pg_encoding_ascii = pg_char_to_encoding("SQL_ASCII");
    init_codecs();

    return mod;

error:
    Py_DECREF(mod);
    return NULL;
}
