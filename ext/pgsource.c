/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The source object - this file is part a of the C extension module.
 *
 * Copyright (c) 2026 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Shared headers */
#include "pginternal.h"
#include "pgmodule.h"

/* Deallocate source object. */
static void
source_dealloc(sourceObject *self)
{
    if (self->result)
        PQclear(self->result);

    Py_XDECREF(self->pgcnx);
    PyObject_Del(self);
}

/* Return source object as string in human readable form. */
static PyObject *
source_str(sourceObject *self)
{
    switch (self->result_type) {
        case RESULT_DQL:
            return format_result(self->result, self->encoding);
        case RESULT_DDL:
        case RESULT_DML:
            return PyUnicode_FromString(PQcmdStatus(self->result));
        case RESULT_EMPTY:
        default:
            return PyUnicode_FromString("(empty PostgreSQL source object)");
    }
}

/* Check source object validity. */
static int
_check_source_obj(sourceObject *self, int level)
{
    if (!self->valid) {
        set_error_msg(OperationalError, "Object has been closed");
        return 0;
    }

    if ((level & CHECK_RESULT) && !self->result) {
        set_error_msg(DatabaseError, "No result");
        return 0;
    }

    if ((level & CHECK_DQL) && self->result_type != RESULT_DQL) {
        set_error_msg(DatabaseError, "Last query did not return tuples");
        return 0;
    }

    if ((level & CHECK_CNX) && !_check_cnx_obj(self->pgcnx)) {
        return 0;
    }

    return 1;
}

/* Get source object attributes. */
static PyObject *
source_getattr(sourceObject *self, PyObject *nameobj)
{
    const char *name = PyUnicode_AsUTF8(nameobj);
    if (!name)
        return NULL;

    /* pg connection object */
    if (!strcmp(name, "pgcnx")) {
        if (_check_source_obj(self, 0)) {
            return Py_NewRef(self->pgcnx);
        }
        Py_RETURN_NONE;
    }

    /* arraysize */
    if (!strcmp(name, "arraysize"))
        return PyLong_FromLong(self->arraysize);

    /* resulttype */
    if (!strcmp(name, "resulttype"))
        return PyLong_FromLong(self->result_type);

    /* ntuples */
    if (!strcmp(name, "ntuples"))
        return PyLong_FromLong(self->max_row);

    /* nfields */
    if (!strcmp(name, "nfields"))
        return PyLong_FromLong(self->num_fields);

    /* seeks name in methods (fallback) */
    return PyObject_GenericGetAttr((PyObject *)self, nameobj);
}

/* Set source object attributes. */
static int
source_setattr(sourceObject *self, char *name, PyObject *v)
{
    /* arraysize */
    if (!strcmp(name, "arraysize")) {
        if (!PyLong_Check(v)) {
            PyErr_SetString(PyExc_TypeError, "arraysize must be integer");
            return -1;
        }

        self->arraysize = PyLong_AsLong(v);
        return 0;
    }

    /* unknown attribute */
    PyErr_SetString(PyExc_TypeError, "Not a writable attribute");
    return -1;
}

/* Close object. */
static char source_close__doc__[] =
    "close() -- close source object without deleting it\n\n"
    "All instances of the source object can no longer be used after this "
    "call.\n";

static PyObject *
source_close(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    /* frees result if necessary and invalidates object */
    if (self->result) {
        PQclear(self->result);
        self->result_type = RESULT_EMPTY;
        self->result = NULL;
    }

    self->valid = 0;

    /* return None */
    Py_RETURN_NONE;
}

/* Database query. */
static char source_execute__doc__[] =
    "execute(sql) -- execute a SQL statement (string)\n\n"
    "On success, this call returns the number of affected rows, or None\n"
    "for DQL (SELECT, ...) statements.  The fetch (fetch(), fetchone()\n"
    "and fetchall()) methods can be used to get result rows.\n";

static PyObject *
source_execute(sourceObject *self, PyObject *sql)
{
    PyObject *tmp_obj; /* auxiliary string object */
    char *query;
    int encoding;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    if ((encoding = get_client_encoding(self->pgcnx)) < 0)
        return NULL;

    if (!(tmp_obj = get_encoded_arg(
              sql, encoding, "Method execute() expects a string as argument")))
        return NULL;
    query = PyBytes_AS_STRING(tmp_obj);

    /* frees previous result */
    if (self->result) {
        PQclear(self->result);
        self->result = NULL;
    }
    self->max_row = 0;
    self->current_row = 0;
    self->num_fields = 0;
    self->encoding = encoding;

    /* gets result */
    Py_BEGIN_ALLOW_THREADS
    self->result = PQexec(self->pgcnx->cnx, query);
    Py_END_ALLOW_THREADS

    /* we don't need the auxiliary string any more */
    Py_DECREF(tmp_obj);

    /* checks result validity */
    if (!self->result) {
        set_error_text(PyExc_ValueError, NULL, self->pgcnx->cnx);
        return NULL;
    }

    /* this may have changed the datestyle, so we reset the date format
       in order to force fetching it newly when next time requested */
    self->pgcnx->date_format = date_format; /* this is normally NULL */

    /* checks result status */
    switch (PQresultStatus(self->result)) {
        /* query succeeded */
        case PGRES_TUPLES_OK: /* DQL: returns None (DB-SIG compliant) */
            self->result_type = RESULT_DQL;
            self->max_row = PQntuples(self->result);
            self->num_fields = PQnfields(self->result);
            Py_RETURN_NONE;
        case PGRES_COMMAND_OK: /* other requests */
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN: {
            long num_rows;
            char *tmp;

            tmp = PQcmdTuples(self->result);
            if (tmp[0]) {
                self->result_type = RESULT_DML;
                num_rows = atol(tmp);
            }
            else {
                self->result_type = RESULT_DDL;
                num_rows = -1;
            }
            return PyLong_FromLong(num_rows);
        }

        /* query failed */
        case PGRES_EMPTY_QUERY:
            PyErr_SetString(PyExc_ValueError, "Empty query");
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
        case PGRES_NONFATAL_ERROR:
            set_error(ProgrammingError, "Cannot execute command",
                      self->pgcnx->cnx, self->result);
            break;
        default:
            set_error_msg(InternalError,
                          "Internal error: unknown result status");
    }

    /* frees result and returns error */
    PQclear(self->result);
    self->result = NULL;
    self->result_type = RESULT_EMPTY;
    return NULL;
}

/* Get oid status for last query (valid for INSERTs, 0 for other). */
static char source_oidstatus__doc__[] =
    "oidstatus() -- return oid of last inserted row (if available)";

static PyObject *
source_oidstatus(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    Oid oid;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT)) {
        return NULL;
    }

    /* retrieves oid status */
    if ((oid = PQoidValue(self->result)) == InvalidOid) {
        Py_RETURN_NONE;
    }

    return PyLong_FromLong((long)oid);
}

/* Fetch rows from last result. */
static char source_fetch__doc__[] =
    "fetch(num) -- return the next num rows from the last result in a list\n\n"
    "If num parameter is omitted arraysize attribute value is used.\n"
    "If size equals -1, all rows are fetched.\n";

static PyObject *
source_fetch(sourceObject *self, PyObject *args)
{
    PyObject *res_list;
    int i, k;
    long size;
    int encoding;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL | CHECK_CNX)) {
        return NULL;
    }

    /* checks args */
    size = self->arraysize;
    if (!PyArg_ParseTuple(args, "|l", &size)) {
        PyErr_SetString(PyExc_TypeError,
                        "fetch(num), with num (integer, optional)");
        return NULL;
    }

    /* seeks last line */
    /* limit size to be within the amount of data we actually have */
    if (size == -1 || (self->max_row - self->current_row) < size) {
        size = self->max_row - self->current_row;
    }

    /* allocate list for result */
    if (!(res_list = PyList_New(0)))
        return NULL;

    encoding = self->encoding;

    /* builds result */
    for (i = 0, k = self->current_row; i < size; ++i, ++k) {
        PyObject *rowtuple;
        int j;

        if (!(rowtuple = PyTuple_New(self->num_fields))) {
            Py_DECREF(res_list);
            return NULL;
        }

        for (j = 0; j < self->num_fields; ++j) {
            PyObject *str;

            if (PQgetisnull(self->result, k, j)) {
                str = Py_NewRef(Py_None);
            }
            else {
                char *s = PQgetvalue(self->result, k, j);
                Py_ssize_t length = PQgetlength(self->result, k, j);
                if (PQfformat(self->result, j) == 0) { /* textual format */
                    str = get_decoded_string_or_bytes(s, length, encoding);
                }
                else {
                    str = PyBytes_FromStringAndSize(s, length);
                }
                if (!str) {
                    Py_DECREF(rowtuple);
                    Py_DECREF(res_list);
                    return NULL;
                }
            }
            PyTuple_SET_ITEM(rowtuple, j, str);
        }

        if (PyList_Append(res_list, rowtuple)) {
            Py_DECREF(rowtuple);
            Py_DECREF(res_list);
            return NULL;
        }
        Py_DECREF(rowtuple);
    }

    self->current_row = k;
    return res_list;
}

/* Change current row (internal wrapper for all "move" methods). */
static PyObject *
_source_move(sourceObject *self, int move)
{
    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL)) {
        return NULL;
    }

    /* changes the current row */
    switch (move) {
        case QUERY_MOVEFIRST:
            self->current_row = 0;
            break;
        case QUERY_MOVELAST:
            self->current_row = self->max_row - 1;
            break;
        case QUERY_MOVENEXT:
            if (self->current_row != self->max_row)
                ++self->current_row;
            break;
        case QUERY_MOVEPREV:
            if (self->current_row > 0)
                self->current_row--;
            break;
    }

    Py_RETURN_NONE;
}

/* Move to first result row. */
static char source_movefirst__doc__[] =
    "movefirst() -- move to first result row";

static PyObject *
source_movefirst(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    return _source_move(self, QUERY_MOVEFIRST);
}

/* Move to last result row. */
static char source_movelast__doc__[] =
    "movelast() -- move to last valid result row";

static PyObject *
source_movelast(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    return _source_move(self, QUERY_MOVELAST);
}

/* Move to next result row. */
static char source_movenext__doc__[] = "movenext() -- move to next result row";

static PyObject *
source_movenext(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    return _source_move(self, QUERY_MOVENEXT);
}

/* Move to previous result row. */
static char source_moveprev__doc__[] =
    "moveprev() -- move to previous result row";

static PyObject *
source_moveprev(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    return _source_move(self, QUERY_MOVEPREV);
}

/* Get the final result of a copy operation (internal use only).
   Returns the number of rows or NULL with an IOError, using errormsg
   as the error message if it is not NULL. Also consumes the results of
   the remaining statements of the query, so that the connection becomes
   idle again. If the query continues with another copy operation, this
   becomes the current result, so that it can be run with putdata() or
   getdata() as well. */
static PyObject *
_source_end_copy(sourceObject *self, const char *errormsg)
{
    PGconn *cnx = self->pgcnx->cnx;
    PGresult *result;
    PGresult *error = NULL;     /* first error of a remaining statement */
    PGresult *next_copy = NULL; /* the next copy operation, if any */
    PyObject *ret;

    Py_BEGIN_ALLOW_THREADS;
    result = PQgetResult(cnx);
    Py_END_ALLOW_THREADS;

    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
        const char *tmp = PQcmdTuples(result);

        ret = PyLong_FromLong(tmp[0] ? atol(tmp) : -1);
    }
    else {
        /* the error message has been encoded with the client encoding */
        set_error_text(PyExc_IOError, errormsg, cnx);
        ret = NULL;
    }
    PQclear(result);

    /* consume the results of the remaining statements, up to the next
       copy operation, since PQgetResult() would return this repeatedly */
    Py_BEGIN_ALLOW_THREADS;
    while (!next_copy && (result = PQgetResult(cnx))) {
        ExecStatusType status = PQresultStatus(result);

        if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT ||
            status == PGRES_COPY_BOTH)
            next_copy = result;
        else if (!error &&
                 (status == PGRES_FATAL_ERROR || status == PGRES_BAD_RESPONSE))
            error = result;
        else
            PQclear(result);
    }
    Py_END_ALLOW_THREADS;

    if (error) {
        /* a remaining statement failed, which usually also rolled back
           the copy, so report the error instead of the row count */
        if (ret) {
            Py_DECREF(ret);
            set_error_text(PyExc_IOError, PQresultErrorMessage(error), cnx);
            ret = NULL;
        }
        PQclear(error);
    }

    PQclear(self->result);
    self->result = next_copy;
    self->result_type = next_copy ? RESULT_DDL : RESULT_EMPTY;

    return ret;
}

/* Put copy data. */
static char source_putdata__doc__[] =
    "putdata(buffer) -- send data to server during copy from stdin";

static PyObject *
source_putdata(sourceObject *self, PyObject *buffer)
{
    PyObject *tmp_obj = NULL; /* an auxiliary object */
    char *buf;                /* the buffer as encoded string */
    Py_ssize_t nbytes;        /* length of string */
    char *errormsg = NULL;    /* error message */
    int res;                  /* direct result of the operation */
    PyObject *ret;            /* return value */

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    if (buffer == Py_None) {
        /* pass None for terminating the operation */
        buf = errormsg = NULL;
    }
    else if (PyBytes_Check(buffer)) {
        /* or pass a byte string */
        PyBytes_AsStringAndSize(buffer, &buf, &nbytes);
    }
    else if (PyUnicode_Check(buffer)) {
        /* or pass a unicode string */
        int encoding = get_client_encoding(self->pgcnx);

        if (encoding < 0)
            return NULL;
        tmp_obj = get_encoded_string(buffer, encoding);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &buf, &nbytes);
    }
    else if (PyErr_GivenExceptionMatches(buffer, PyExc_BaseException)) {
        /* or pass a Python exception for sending an error message */
        int encoding = get_client_encoding(self->pgcnx);
        PyObject *str_obj;

        if (encoding < 0)
            return NULL;
        if (!(str_obj = PyObject_Str(buffer)))
            return NULL; /* pass the error raised by __str__() */
        /* the message must not prevent aborting the copy, so characters
           that cannot be encoded and NUL characters are escaped */
        tmp_obj = get_encoded_text(str_obj, encoding);
        Py_DECREF(str_obj);
        if (!tmp_obj)
            return NULL;
        /* errormsg points into tmp_obj, which must be kept until the end */
        errormsg = PyBytes_AsString(tmp_obj);
        buf = NULL;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method putdata() expects a buffer, None"
                        " or an exception as argument");
        return NULL;
    }

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX | CHECK_RESULT) ||
        PQresultStatus(self->result) != PGRES_COPY_IN) {
        PyErr_SetString(PyExc_IOError,
                        "Connection is invalid or not in copy_in state");
        Py_XDECREF(tmp_obj);
        return NULL;
    }

    if (buf) {
        res = nbytes ? PQputCopyData(self->pgcnx->cnx, buf, (int)nbytes) : 1;
    }
    else {
        res = PQputCopyEnd(self->pgcnx->cnx, errormsg);
    }

    if (res != 1) {
        set_error_text(PyExc_IOError, NULL, self->pgcnx->cnx);
        ret = NULL;
    }
    else if (buf) { /* buffer has been sent */
        ret = Py_NewRef(Py_None);
    }
    else { /* copy is done */
        ret = _source_end_copy(self, errormsg);
    }

    Py_XDECREF(tmp_obj); /* only now, since errormsg may point into it */

    return ret; /* None or number of rows */
}

/* Get copy data. */
static char source_getdata__doc__[] =
    "getdata(decode) -- receive data to server during copy to stdout";

static PyObject *
source_getdata(sourceObject *self, PyObject *args)
{
    int decode = 0;    /* decode flag */
    char *buffer;      /* the copied buffer as encoded byte string */
    Py_ssize_t nbytes; /* length of the byte string */
    PyObject *ret;     /* return value */

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    if (!PyArg_ParseTuple(args, "|i", &decode)) {
        return NULL;
    }

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX | CHECK_RESULT) ||
        PQresultStatus(self->result) != PGRES_COPY_OUT) {
        PyErr_SetString(PyExc_IOError,
                        "Connection is invalid or not in copy_out state");
        return NULL;
    }

    nbytes = PQgetCopyData(self->pgcnx->cnx, &buffer, 0);

    if (!nbytes || nbytes < -1) { /* an error occurred */
        set_error_text(PyExc_IOError, NULL, self->pgcnx->cnx);
        return NULL;
    }

    if (nbytes == -1) { /* copy is done */
        ret = _source_end_copy(self, NULL);
    }
    else if (!decode) { /* a row has been returned */
        ret = PyBytes_FromStringAndSize(buffer, nbytes);
        PQfreemem(buffer);
    }
    else { /* a row has been returned and shall be decoded */
        int encoding = get_client_encoding(self->pgcnx);

        ret = encoding < 0
                  ? NULL
                  : get_decoded_string_or_bytes(buffer, nbytes, encoding);
        PQfreemem(buffer);
    }

    return ret; /* buffer or number of rows */
}

/* Find field number from string/integer (internal use only). */
static int
_source_fieldindex(sourceObject *self, PyObject *param, const char *usage)
{
    int num;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL))
        return -1;

    /* gets field number */
    if (PyLong_Check(param)) {
        num = (int)PyLong_AsLong(param);
    }
    else {
        num = get_field_number(self->result, param, self->encoding, usage);
        if (num == -2)
            return -1;
    }

    /* checks field validity */
    if (num < 0 || num >= self->num_fields) {
        PyErr_SetString(PyExc_ValueError, "Unknown field");
        return -1;
    }

    return num;
}

/* Build field information from position (internal use only). */
static PyObject *
_source_buildinfo(sourceObject *self, int num)
{
    PyObject *result, *name;

    if (!(name =
              get_decoded_text(PQfname(self->result, num), self->encoding))) {
        return NULL;
    }

    /* allocates tuple */
    result = PyTuple_New(5);
    if (!result) {
        Py_DECREF(name);
        return NULL;
    }

    /* affects field information */
    PyTuple_SET_ITEM(result, 0, PyLong_FromLong(num));
    PyTuple_SET_ITEM(result, 1, name);
    PyTuple_SET_ITEM(result, 2,
                     PyLong_FromLong((long)PQftype(self->result, num)));
    PyTuple_SET_ITEM(result, 3, PyLong_FromLong(PQfsize(self->result, num)));
    PyTuple_SET_ITEM(result, 4, PyLong_FromLong(PQfmod(self->result, num)));

    return result;
}

/* Lists fields info. */
static char source_listinfo__doc__[] =
    "listinfo() -- get information for all fields"
    " (position, name, type oid, size, type modifier)";

static PyObject *
source_listInfo(sourceObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *result, *info;
    int i;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL)) {
        return NULL;
    }

    /* builds result */
    if (!(result = PyTuple_New(self->num_fields))) {
        return NULL;
    }

    for (i = 0; i < self->num_fields; ++i) {
        info = _source_buildinfo(self, i);
        if (!info) {
            Py_DECREF(result);
            return NULL;
        }
        PyTuple_SET_ITEM(result, i, info);
    }

    /* returns result */
    return result;
}

/* List fields information for last result. */
static char source_fieldinfo__doc__[] =
    "fieldinfo(desc) -- get specified field info"
    " (position, name, type oid, size, type modifier)";

static PyObject *
source_fieldinfo(sourceObject *self, PyObject *desc)
{
    int num;

    /* checks args and validity */
    if ((num = _source_fieldindex(
             self, desc,
             "Method fieldinfo() needs a string or integer as argument")) ==
        -1) {
        return NULL;
    }

    /* returns result */
    return _source_buildinfo(self, num);
}

/* Retrieve field value. */
static char source_field__doc__[] =
    "field(desc) -- return specified field value";

static PyObject *
source_field(sourceObject *self, PyObject *desc)
{
    int num;

    /* checks args and validity */
    if ((num = _source_fieldindex(
             self, desc,
             "Method field() needs a string or integer as argument")) == -1) {
        return NULL;
    }

    if (self->current_row < 0 || self->current_row >= self->max_row) {
        PyErr_SetString(PyExc_IndexError, "No current row");
        return NULL;
    }
    if (PQgetisnull(self->result, self->current_row, num)) {
        Py_RETURN_NONE;
    }
    return get_decoded_string_or_bytes(
        PQgetvalue(self->result, self->current_row, num),
        PQgetlength(self->result, self->current_row, num), self->encoding);
}

/* Get the list of source object attributes. */
static PyObject *
source_dir(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *attrs, *ret;

    /* the type is a borrowed reference, the result of extend() a new one */
    if (!(attrs = PyObject_Dir((PyObject *)Py_TYPE(self))))
        return NULL;
    ret = PyObject_CallMethod(attrs, "extend", "[sssss]", "pgcnx", "arraysize",
                              "resulttype", "ntuples", "nfields");
    if (!ret) {
        Py_DECREF(attrs);
        return NULL;
    }
    Py_DECREF(ret);

    return attrs;
}

/* Source object methods */
static PyMethodDef source_methods[] = {
    {"__dir__", (PyCFunction)source_dir, METH_NOARGS, NULL},

    {"close", (PyCFunction)source_close, METH_NOARGS, source_close__doc__},
    {"execute", (PyCFunction)source_execute, METH_O, source_execute__doc__},
    {"oidstatus", (PyCFunction)source_oidstatus, METH_NOARGS,
     source_oidstatus__doc__},
    {"fetch", (PyCFunction)source_fetch, METH_VARARGS, source_fetch__doc__},
    {"movefirst", (PyCFunction)source_movefirst, METH_NOARGS,
     source_movefirst__doc__},
    {"movelast", (PyCFunction)source_movelast, METH_NOARGS,
     source_movelast__doc__},
    {"movenext", (PyCFunction)source_movenext, METH_NOARGS,
     source_movenext__doc__},
    {"moveprev", (PyCFunction)source_moveprev, METH_NOARGS,
     source_moveprev__doc__},
    {"putdata", (PyCFunction)source_putdata, METH_O, source_putdata__doc__},
    {"getdata", (PyCFunction)source_getdata, METH_VARARGS,
     source_getdata__doc__},
    {"field", (PyCFunction)source_field, METH_O, source_field__doc__},
    {"fieldinfo", (PyCFunction)source_fieldinfo, METH_O,
     source_fieldinfo__doc__},
    {"listinfo", (PyCFunction)source_listInfo, METH_NOARGS,
     source_listinfo__doc__},
    {NULL, NULL, 0, NULL}};

static char source__doc__[] = "PyGreSQL source object";

/* Source type definition */
PyTypeObject sourceType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pgdb.Source",
    .tp_basicsize = sizeof(sourceObject),
    .tp_dealloc = (destructor)source_dealloc,
    .tp_setattr = (setattrfunc)source_setattr,
    .tp_str = (reprfunc)source_str,
    .tp_getattro = (getattrofunc)source_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = source__doc__,
    .tp_methods = source_methods,
};
