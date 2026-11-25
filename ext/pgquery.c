/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The query object - this file is part a of the C extension module.
 *
 * Copyright (c) 2026 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.txt file for specific restrictions.
 */

/* Shared headers */
#include "pginternal.h"
#include "pgmodule.h"

/* Asynchronous queries (created by send_query()):

   The result methods returning the whole result (getresult(), dictresult(),
   namedresult(), scalarresult() and the corresponding iterator methods)
   move to the next result of the query and return it, or None after the
   last result. All other methods (the row accessors, len(), iteration and
   the field methods) access the current result without moving on. If no
   result has been fetched yet, they fetch the first one, which will then
   also be returned by the next result method.

   While the query is the pending query of the connection, the results are
   fetched from the connection. Before another command is executed on the
   connection, the remaining results are moved into the query object, so
   that they are not discarded and can still be returned (detached).

   While results are fetched from the connection, the connection is busy,
   so that it cannot be used otherwise meanwhile. */

/* Clear the current result of the query. */
static void
_query_clear_result(queryObject *self)
{
    if (self->result) {
        PQclear(self->result);
        self->result = NULL;
    }
    if (self->col_types) {
        PyMem_Free(self->col_types);
        self->col_types = NULL;
    }
    Py_CLEAR(self->value);
    self->current_row = self->max_row = self->num_fields = 0;
    self->copy = 0;
}

/* Clear the results that have been moved from the connection. */
static void
_query_clear_pending(queryObject *self)
{
    if (self->pending) {
        while (self->next_pending < self->num_pending)
            PQclear(self->pending[self->next_pending++]);
        PyMem_Free(self->pending);
        self->pending = NULL;
    }
    self->num_pending = self->next_pending = 0;
}

/* Deallocate the query object. */
static void
query_dealloc(queryObject *self)
{
    if (self->pgcnx && self->pgcnx->async_query == self)
        self->pgcnx->async_query = NULL;
    Py_XDECREF(self->pgcnx);
    _query_clear_result(self);
    _query_clear_pending(self);

    PyObject_Del(self);
}

/* Check whether a result status is a COPY state, which libpq returns
   repeatedly until the COPY has been completed. */
static int
_is_copy_status(ExecStatusType status)
{
    return status == PGRES_COPY_IN || status == PGRES_COPY_OUT ||
           status == PGRES_COPY_BOTH;
}

/* Move the results of an asynchronous query that are still pending on the
   connection into the query object, so that the connection can be used for
   another command. Return 0 on success or -1 with an exception set.
   A COPY can only be completed using the connection, so when a COPY result
   is reached, the query stays the pending query of the connection, and an
   error is raised, since the other command would abort the COPY.
   Must be called while the connection is busy. */
int
_query_detach(queryObject *self)
{
    connObject *cnx_obj = self->pgcnx;
    PGconn *cnx = cnx_obj->cnx;
    PGresult *result;
    int copy = 0;

    while (cnx) {
        Py_BEGIN_ALLOW_THREADS
        result = PQgetResult(cnx);
        Py_END_ALLOW_THREADS
        if (!result)
            break;
        if (_is_copy_status(PQresultStatus(result))) {
            copy = 1;
            if (self->next_pending == self->num_pending
                    ? self->copy
                    : _is_copy_status(PQresultStatus(
                          self->pending[self->num_pending - 1]))) {
                /* this is the current or the last moved result, which is
                   returned again, since the COPY has not been completed
                   (a completed COPY is followed by its command status) */
                PQclear(result);
                break;
            }
        }
        if (self->num_pending % 4 == 0) {
            /* grow the array of pending results */
            PGresult **pending =
                PyMem_Realloc(self->pending, (size_t)(self->num_pending + 4) *
                                                 sizeof(*pending));
            if (!pending) {
                /* GCOVR_EXCL_START: out of memory */
                PQclear(result);
                PyErr_NoMemory();
                return -1;
                /* GCOVR_EXCL_STOP */
            }
            self->pending = pending;
        }
        self->pending[self->num_pending++] = result;
        if (copy)
            break;
    }
    /* the results may have changed the datestyle */
    cnx_obj->date_format = date_format;

    if (copy) {
        set_error_msg(InterfaceError,
                      "The COPY of the pending asynchronous query"
                      " must be completed first");
        return -1;
    }

    cnx_obj->async_query = NULL;
    self->detached = 1;
    return 0;
}

/* Mark an asynchronous query as done. If it is still the pending query of
   the connection (e.g. after an error), consume the remaining results on
   the connection, except for a COPY result. This is only called by
   _query_fetch() after checking that the connection is not busy; the check
   here is only defensive (the results would then be discarded before the
   next command, like for a deleted query). */
static void
_query_finish(queryObject *self)
{
    connObject *cnx_obj = self->pgcnx;

    if (cnx_obj->async_query == self) {
        PGconn *cnx = cnx_obj->cnx;
        PGresult *result;

        if (cnx && !cnx_obj->busy) {
            /* stay the pending query until the results have been consumed
               (like _conn_begin(), but without raising an error) */
            cnx_obj->busy = 1;
            Py_BEGIN_ALLOW_THREADS
            while ((result = PQgetResult(cnx))) {
                ExecStatusType status = PQresultStatus(result);

                PQclear(result);
                /* this is only called after the last result or an error,
                   and the server skips the statements after an error */
                if (_is_copy_status(status))
                    break; /* GCOVR_EXCL_LINE: see above */
            }
            Py_END_ALLOW_THREADS
            _conn_end(cnx_obj);
        }
        cnx_obj->async_query = NULL;
    }
    _query_clear_pending(self);
    self->async = ASYNC_DONE;
}

/* Fetch the next result of an asynchronous query as the current result.
   Return the query itself (borrowed) as sentinel for a result with rows,
   the value of other results as a new reference (an empty string where
   query() would return None), None after the last result, or NULL with
   an exception set. */
static PyObject *
_query_fetch(queryObject *self)
{
    connObject *cnx_obj = self->pgcnx;
    PGresult *result;
    PyObject *value;
    ExecStatusType status;
    int moved = 0; /* whether the result has been moved from the connection */

    if (cnx_obj->async_query == self && cnx_obj->busy) {
        set_error_msg(InterfaceError, "The connection is busy");
        return NULL;
    }
    if (self->in_use) {
        set_error_msg(InterfaceError,
                      "The current result of this query is being converted");
        return NULL;
    }
    if (self->async == ASYNC_DONE)
        Py_RETURN_NONE; /* GCOVR_EXCL_LINE: the callers check this */

    if (self->next_pending < self->num_pending) {
        /* results that have been moved from the connection come first */
        result = self->pending[self->next_pending];
        self->pending[self->next_pending++] = NULL;
        moved = 1;
    }
    else if (self->detached) {
        result = NULL;
    }
    else if (cnx_obj->async_query == self) {
        PGconn *cnx = cnx_obj->cnx;

        /* like _conn_begin(), the busy flag has already been checked above,
           also for the moved results, which may still be added to */
        cnx_obj->busy = 1;
        Py_BEGIN_ALLOW_THREADS
        result = PQgetResult(cnx);
        Py_END_ALLOW_THREADS
        _conn_end(cnx_obj);
        /* the result may have changed the datestyle */
        cnx_obj->date_format = date_format;
    }
    else {
        /* the connection has been closed or reset */
        if (cnx_obj->cnx)
            set_error_msg(OperationalError,
                          "The results of this query have been discarded"
                          " by a reset of the connection");
        else
            PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* forget the previous result */
    _query_clear_result(self);

    if (!result) { /* no more results */
        _query_finish(self);
        Py_RETURN_NONE;
    }

    status = PQresultStatus(result);
    if (status == PGRES_TUPLES_OK) {
        self->result = result;
        self->max_row = PQntuples(result);
        self->num_fields = PQnfields(result);
        self->col_types = get_col_types(result, self->num_fields);
        if (!self->col_types) {
            /* GCOVR_EXCL_START: out of memory */
            _query_clear_result(self);
            return NULL;
            /* GCOVR_EXCL_STOP */
        }
        return (PyObject *)self;
    }

    if (_is_copy_status(status))
        self->copy = 1; /* this result is returned again by libpq */
    if (moved &&
        (status == PGRES_BAD_RESPONSE || status == PGRES_FATAL_ERROR ||
         status == PGRES_NONFATAL_ERROR)) {
        /* the result has been moved from the connection, so the last
           error of the connection may belong to another command */
        set_result_error(result, self->encoding);
        PQclear(result);
        value = NULL;
    }
    else {
        value = _conn_non_query_result(status, result, cnx_obj->cnx);
    }
    if (!value) {
        /* the results after an error cannot be fetched any more */
        _query_finish(self);
    }
    else if (value == Py_None) {
        /* None is returned after the last result, so return an empty
           string where query() would return None */
        Py_DECREF(value);
        value = PyUnicode_FromString("");
    }
    return value;
}

/* Move to the next result of an asynchronous query for a result method.
   Return the query itself (borrowed) as sentinel if the rows of the current
   result shall be returned, otherwise the value that shall be returned
   instead as a new reference, or NULL with an exception set. Normal queries
   always return the sentinel. */
static PyObject *
_query_next_result(queryObject *self)
{
    PyObject *res;

    switch (self->async) {
        case 0:
            return (PyObject *)self;
        case ASYNC_UNDELIVERED:
            /* return the result fetched by an accessor */
            self->async = ASYNC_DELIVERED;
            if (self->value) {
                res = self->value;
                self->value = NULL;
                return res;
            }
            return (PyObject *)self;
        case ASYNC_DONE:
            Py_RETURN_NONE;
        default:
            break;
    }

    res = _query_fetch(self);
    if (res && self->async != ASYNC_DONE)
        self->async = ASYNC_DELIVERED;
    return res;
}

/* Make sure that an asynchronous query has a current result for the methods
   that access the current result without moving to the next one. If no
   result has been fetched yet, fetch the first one, which will then also be
   returned by the next result method. Return 0 or -1 with an exception. */
static int
_query_current_result(queryObject *self)
{
    PyObject *res;

    if (self->async != ASYNC_INITIAL)
        return 0;

    if (!(res = _query_fetch(self)))
        return -1;
    if (self->async == ASYNC_DONE) {
        /* GCOVR_EXCL_START: an asynchronous query has at least one result */
        if (res != (PyObject *)self)
            Py_DECREF(res);
        /* GCOVR_EXCL_STOP */
    }
    else {
        self->async = ASYNC_UNDELIVERED;
        if (res != (PyObject *)self)
            self->value = res; /* keep the value for the result method */
    }
    return 0;
}

/* Return query as string in human readable form. */
static PyObject *
query_str(queryObject *self)
{
    if (_query_current_result(self) < 0)
        return NULL;
    return format_result(self->result, self->encoding);
}

/* Return length of a query object. */
static Py_ssize_t
query_len(PyObject *self)
{
    queryObject *q = (queryObject *)self;

    if (_query_current_result(q) < 0)
        return -1;
    return (Py_ssize_t)q->max_row;
}

/* Return the value in the given row and column. */
static PyObject *
_query_value_in_column(queryObject *self, int row, int column)
{
    char *s;
    int type;

    if (PQgetisnull(self->result, row, column)) {
        Py_RETURN_NONE;
    }

    /* get the string representation of the value */
    /* note: this is always null-terminated text format */
    s = PQgetvalue(self->result, row, column);
    /* get the PyGreSQL type of the column */
    type = self->col_types[column];

    /* cast the string representation into a Python object */
    if (type & PYGRES_ARRAY)
        return cast_array(s, PQgetlength(self->result, row, column),
                          self->encoding, type, NULL, 0);
    if (type == PYGRES_BYTEA)
        return cast_bytea_text(s);
    if (type == PYGRES_OTHER)
        return cast_other(s, PQgetlength(self->result, row, column),
                          self->encoding, PQftype(self->result, column),
                          self->pgcnx->cast_hook);
    if (type & PYGRES_TEXT)
        return cast_sized_text(s, PQgetlength(self->result, row, column),
                               self->encoding, type);
    return cast_unsized_simple(s, type);
}

/* Return the given row as a tuple.
   The cast functions may run Python code which may use the query again.
   The result must not change while the row is converted, therefore it is
   marked as being in use, and the row is passed instead of using the
   current row, which may change. */
static PyObject *
_query_row_as_tuple(queryObject *self, int row)
{
    PyObject *row_tuple = NULL;
    int j;

    if (!(row_tuple = PyTuple_New(self->num_fields))) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }

    ++self->in_use;
    for (j = 0; j < self->num_fields; ++j) {
        PyObject *val = _query_value_in_column(self, row, j);
        if (!val) {
            Py_DECREF(row_tuple);
            row_tuple = NULL;
            break;
        }
        PyTuple_SET_ITEM(row_tuple, j, val);
    }
    --self->in_use;

    return row_tuple;
}

/* Return given item from a query object. */
static PyObject *
query_getitem(PyObject *self, Py_ssize_t i)
{
    queryObject *q = (queryObject *)self;

    if (_query_current_result(q) < 0)
        return NULL;

    if (i < 0 || i >= q->max_row) {
        PyErr_SetNone(PyExc_IndexError);
        return NULL;
    }

    q->current_row = (int)i;
    return _query_row_as_tuple(q, (int)i);
}

/* __iter__() method of the queryObject:
   Returns the default iterator yielding rows as tuples. */
static PyObject *
query_iter(queryObject *self)
{
    if (_query_current_result(self) < 0)
        return NULL;

    self->current_row = 0;
    return Py_NewRef(self);
}

/* __next__() method of the queryObject:
  Returns the current row as a tuple and moves to the next one. */
static PyObject *
query_next(queryObject *self)
{
    PyObject *row_tuple = NULL;

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->current_row >= self->max_row) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    {
        int row = self->current_row;

        row_tuple = _query_row_as_tuple(self, row);
        if (row_tuple)
            self->current_row = row + 1;
    }
    return row_tuple;
}

/* Get number of bytes allocated for PGresult object */
static char query_memsize__doc__[] =
    "memsize() -- return number of bytes allocated by query result";
static PyObject *
query_memsize(queryObject *self, PyObject *Py_UNUSED(noargs))
{
#ifdef MEMORY_SIZE
    if (_query_current_result(self) < 0)
        return NULL;
    return PyLong_FromSize_t(PQresultMemorySize(self->result));
#else
    set_error_msg(NotSupportedError, "Memory size functions not supported");
    return NULL;
#endif /* MEMORY_SIZE */
}

/* List field names from query result. */
static char query_listfields__doc__[] =
    "listfields() -- List field names from result";

static PyObject *
query_listfields(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    int i;
    PyObject *fieldstuple, *str;

    if (_query_current_result(self) < 0)
        return NULL;

    /* builds tuple */
    fieldstuple = PyTuple_New(self->num_fields);
    if (fieldstuple) {
        for (i = 0; i < self->num_fields; ++i) {
            str = get_decoded_text(PQfname(self->result, i), self->encoding);
            if (!str) {
                Py_DECREF(fieldstuple);
                return NULL; /* GCOVR_EXCL_LINE: out of memory */
            }
            PyTuple_SET_ITEM(fieldstuple, i, str);
        }
    }
    return fieldstuple;
}

/* Get field name from number in last result. */
static char query_fieldname__doc__[] =
    "fieldname(num) -- return name of field from result from its position";

static PyObject *
query_fieldname(queryObject *self, PyObject *args)
{
    int i;

    /* gets args */
    if (!PyArg_ParseTuple(args, "i", &i)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method fieldname() takes an integer as argument");
        return NULL;
    }

    if (_query_current_result(self) < 0)
        return NULL;

    /* checks number validity */
    if (i < 0 || i >= self->num_fields) {
        PyErr_SetString(PyExc_ValueError, "Invalid field number");
        return NULL;
    }

    /* gets fields name and builds object */
    return get_decoded_text(PQfname(self->result, i), self->encoding);
}

/* Get field number from name in last result. */
static char query_fieldnum__doc__[] =
    "fieldnum(name) -- return position in query for field from its name";

static PyObject *
query_fieldnum(queryObject *self, PyObject *args)
{
    int num;
    PyObject *name_obj;

    /* gets args */
    if (!PyArg_ParseTuple(args, "O", &name_obj)) {
        return NULL;
    }

    if (_query_current_result(self) < 0)
        return NULL;

    /* gets field number */
    num = get_field_number(self->result, name_obj, self->encoding,
                           "Method fieldnum() takes a string as argument");
    if (num == -2)
        return NULL;
    if (num == -1) {
        PyErr_SetString(PyExc_ValueError, "Unknown field");
        return NULL;
    }

    return PyLong_FromLong(num);
}

/* Build a tuple with info for query field with given number. */
static PyObject *
_query_build_field_info(PGresult *res, int col_num, int encoding)
{
    PyObject *info, *name;

    if (!(name = get_decoded_text(PQfname(res, col_num), encoding)))
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    info = PyTuple_New(4);
    if (!info) {
        Py_DECREF(name);
    }
    else {
        PyTuple_SET_ITEM(info, 0, name);
        PyTuple_SET_ITEM(
            info, 1,
            PyLong_FromUnsignedLong((unsigned long)PQftype(res, col_num)));
        PyTuple_SET_ITEM(info, 2, PyLong_FromLong(PQfsize(res, col_num)));
        PyTuple_SET_ITEM(info, 3, PyLong_FromLong(PQfmod(res, col_num)));
    }
    return info;
}

/* Get information on one or all fields of the query result. */
static char query_fieldinfo__doc__[] =
    "fieldinfo([name]) -- return information about field(s) in query result";

static PyObject *
query_fieldinfo(queryObject *self, PyObject *args)
{
    PyObject *result, *field = NULL;
    int num;

    /* gets args */
    if (!PyArg_ParseTuple(args, "|O", &field)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method fieldinfo() takes one optional argument only");
        return NULL;
    }

    if (_query_current_result(self) < 0)
        return NULL;

    /* check optional field arg */
    if (field) {
        /* gets field number */
        if (PyLong_Check(field)) {
            /* numbers that do not fit into an int are unknown fields */
            long n = PyLong_AsLong(field);

            if (n == -1 && PyErr_Occurred())
                PyErr_Clear();
            num = n < 0 || n >= self->num_fields ? -1 : (int)n;
        }
        else {
            num = get_field_number(
                self->result, field, self->encoding,
                "Field should be given as column number or name");
            if (num == -2)
                return NULL;
        }
        if (num < 0 || num >= self->num_fields) {
            PyErr_SetString(PyExc_IndexError, "Unknown field");
            return NULL;
        }
        return _query_build_field_info(self->result, num, self->encoding);
    }

    if (!(result = PyTuple_New(self->num_fields))) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }
    for (num = 0; num < self->num_fields; ++num) {
        PyObject *info =
            _query_build_field_info(self->result, num, self->encoding);
        if (!info) {
            Py_DECREF(result);
            return NULL; /* GCOVR_EXCL_LINE: out of memory */
        }
        PyTuple_SET_ITEM(result, num, info);
    }
    return result;
}

/* Retrieve one row from the result as a tuple. */
static char query_one__doc__[] =
    "one() -- Get one row from the result of a query\n\n"
    "Only one row from the result is returned as a tuple of fields.\n"
    "This method can be called multiple times to return more rows.\n"
    "It returns None if the result does not contain one more row.\n";

static PyObject *
query_one(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *row_tuple;

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->current_row >= self->max_row) {
        Py_RETURN_NONE;
    }

    {
        int row = self->current_row;

        row_tuple = _query_row_as_tuple(self, row);
        if (row_tuple)
            self->current_row = row + 1;
    }
    return row_tuple;
}

/* Retrieve the single row from the result as a tuple. */
static char query_single__doc__[] =
    "single() -- Get the result of a query as single row\n\n"
    "The single row from the query result is returned as a tuple of fields.\n"
    "This method returns the same single row when called multiple times.\n"
    "It raises an InvalidResultError if the result doesn't have exactly one "
    "row,\n"
    "which will be of type NoResultError or MultipleResultsError "
    "specifically.\n";

static PyObject *
query_single(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *row_tuple;

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    {
        int row = self->current_row;

        row_tuple = _query_row_as_tuple(self, row);
        if (row_tuple)
            self->current_row = row + 1;
    }
    return row_tuple;
}

/* Get the current result as a list of tuples. */
static PyObject *
_query_tuple_list(queryObject *self)
{
    PyObject *result_list;
    int i;

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }

    ++self->in_use;
    for (i = 0; i < self->max_row; ++i) {
        PyObject *row_tuple = _query_row_as_tuple(self, i);

        if (!row_tuple) {
            Py_CLEAR(result_list);
            break;
        }
        PyList_SET_ITEM(result_list, i, row_tuple);
    }
    --self->in_use;
    self->current_row = self->max_row;

    return result_list;
}

/* Get an iterator over the given list, releasing the list. */
static PyObject *
_list_iter(PyObject *list)
{
    PyObject *iter;

    if (!list)
        return NULL;
    iter = PyObject_GetIter(list);
    Py_DECREF(list);
    return iter;
}

/* Retrieve the last query result as a list of tuples. */
static char query_getresult__doc__[] =
    "getresult() -- Get the result of a query\n\n"
    "The result is returned as a list of rows, each one a tuple of fields\n"
    "in the order returned by the server.\n";

static PyObject *
query_getresult(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *res;

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    return _query_tuple_list(self);
}

/* Return the given row as a dict (see _query_row_as_tuple()). */
static PyObject *
_query_row_as_dict(queryObject *self, int row)
{
    PyObject *row_dict = NULL;
    int j;

    if (!(row_dict = PyDict_New())) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }

    ++self->in_use;
    for (j = 0; j < self->num_fields; ++j) {
        PyObject *key, *val;
        int ret;

        if (!(val = _query_value_in_column(self, row, j))) {
            Py_CLEAR(row_dict);
            break;
        }
        if (!(key = get_decoded_text(PQfname(self->result, j),
                                     self->encoding))) {
            Py_DECREF(val);
            /* GCOVR_EXCL_START: out of memory */
            Py_CLEAR(row_dict);
            break;
            /* GCOVR_EXCL_STOP */
        }
        ret = PyDict_SetItem(row_dict, key, val);
        Py_DECREF(key);
        Py_DECREF(val);
        if (ret) {
            /* GCOVR_EXCL_START: out of memory */
            Py_CLEAR(row_dict);
            break;
            /* GCOVR_EXCL_STOP */
        }
    }
    --self->in_use;

    return row_dict;
}

/* Return the value in the first column of the given row
   (see _query_row_as_tuple()). */
static PyObject *
_query_row_as_scalar(queryObject *self, int row)
{
    PyObject *value;

    ++self->in_use;
    value = _query_value_in_column(self, row, 0);
    --self->in_use;
    return value;
}

/* Retrieve one row from the result as a dictionary. */
static char query_onedict__doc__[] =
    "onedict() -- Get one row from the result of a query\n\n"
    "Only one row from the result is returned as a dictionary with\n"
    "the field names used as the keys.\n"
    "This method can be called multiple times to return more rows.\n"
    "It returns None if the result does not contain one more row.\n";

static PyObject *
query_onedict(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *row_dict;

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->current_row >= self->max_row) {
        Py_RETURN_NONE;
    }

    {
        int row = self->current_row;

        row_dict = _query_row_as_dict(self, row);
        if (row_dict)
            self->current_row = row + 1;
    }
    return row_dict;
}

/* Retrieve the single row from the result as a dictionary. */
static char query_singledict__doc__[] =
    "singledict() -- Get the result of a query as single row\n\n"
    "The single row from the query result is returned as a dictionary with\n"
    "the field names used as the keys.\n"
    "This method returns the same single row when called multiple times.\n"
    "It raises an InvalidResultError if the result doesn't have exactly one "
    "row,\n"
    "which will be of type NoResultError or MultipleResultsError "
    "specifically.\n";

static PyObject *
query_singledict(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *row_dict;

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    {
        int row = self->current_row;

        row_dict = _query_row_as_dict(self, row);
        if (row_dict)
            self->current_row = row + 1;
    }
    return row_dict;
}

/* Get the current result as a list of dictionaries. */
static PyObject *
_query_dict_list(queryObject *self)
{
    PyObject *result_list;
    int i;

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }

    ++self->in_use;
    for (i = 0; i < self->max_row; ++i) {
        PyObject *row_dict = _query_row_as_dict(self, i);

        if (!row_dict) {
            Py_CLEAR(result_list);
            break;
        }
        PyList_SET_ITEM(result_list, i, row_dict);
    }
    --self->in_use;
    self->current_row = self->max_row;

    return result_list;
}

/* Retrieve the last query result as a list of dictionaries. */
static char query_dictresult__doc__[] =
    "dictresult() -- Get the result of a query\n\n"
    "The result is returned as a list of rows, each one a dictionary with\n"
    "the field names used as the keys.\n";

static PyObject *
query_dictresult(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *res;

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    return _query_dict_list(self);
}

/* Retrieve last result as iterator of dictionaries. */
static char query_dictiter__doc__[] =
    "dictiter() -- Get the result of a query\n\n"
    "The result is returned as an iterator of rows, each one a dictionary\n"
    "with the field names used as the keys.\n";

static PyObject *
query_dictiter(queryObject *self, PyObject *noargs)
{
    PyObject *res;

    if (!dictiter) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return query_dictresult(self, noargs);
        /* GCOVR_EXCL_STOP */
    }

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    /* the iterator of an asynchronous query must not be affected by
       moving to the next result, so its rows are fetched immediately */
    if (self->async)
        return _list_iter(_query_dict_list(self));

    return PyObject_CallFunction(dictiter, "(O)", self);
}

/* Retrieve one row from the result as a named tuple. */
static char query_onenamed__doc__[] =
    "onenamed() -- Get one row from the result of a query\n\n"
    "Only one row from the result is returned as a named tuple of fields.\n"
    "This method can be called multiple times to return more rows.\n"
    "It returns None if the result does not contain one more row.\n";

static PyObject *
query_onenamed(queryObject *self, PyObject *noargs)
{
    PyObject *res;

    if (!namednext) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return query_one(self, noargs);
        /* GCOVR_EXCL_STOP */
    }

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->current_row >= self->max_row) {
        Py_RETURN_NONE;
    }

    ++self->in_use;
    res = PyObject_CallFunction(namednext, "(O)", self);
    --self->in_use;
    return res;
}

/* Retrieve the single row from the result as a tuple. */
static char query_singlenamed__doc__[] =
    "singlenamed() -- Get the result of a query as single row\n\n"
    "The single row from the query result is returned as named tuple of "
    "fields.\n"
    "This method returns the same single row when called multiple times.\n"
    "It raises an InvalidResultError if the result doesn't have exactly one "
    "row,\n"
    "which will be of type NoResultError or MultipleResultsError "
    "specifically.\n";

static PyObject *
query_singlenamed(queryObject *self, PyObject *noargs)
{
    PyObject *res;

    if (!namednext) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return query_single(self, noargs);
        /* GCOVR_EXCL_STOP */
    }

    if (_query_current_result(self) < 0)
        return NULL;

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    ++self->in_use;
    res = PyObject_CallFunction(namednext, "(O)", self);
    --self->in_use;
    return res;
}

/* Get the current result as a list of named tuples. */
static PyObject *
_query_named_list(queryObject *self)
{
    PyObject *res, *res_list;

    if (!namediter) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return _query_tuple_list(self);
        /* GCOVR_EXCL_STOP */
    }

    ++self->in_use;
    res = PyObject_CallFunction(namediter, "(O)", self);
    if (!res || PyList_Check(res)) {
        res_list = res;
    }
    else {
        res_list = PySequence_List(res);
        Py_DECREF(res);
    }
    --self->in_use;
    return res_list;
}

/* Retrieve last result as list of named tuples. */
static char query_namedresult__doc__[] =
    "namedresult() -- Get the result of a query\n\n"
    "The result is returned as a list of rows, each one a named tuple of "
    "fields\n"
    "in the order returned by the server.\n";

static PyObject *
query_namedresult(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *res;

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    return _query_named_list(self);
}

/* Retrieve last result as iterator of named tuples. */
static char query_namediter__doc__[] =
    "namediter() -- Get the result of a query\n\n"
    "The result is returned as an iterator of rows, each one a named tuple\n"
    "of fields in the order returned by the server.\n";

static PyObject *
query_namediter(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *res;

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    if (!namediter) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return _list_iter(_query_tuple_list(self));
        /* GCOVR_EXCL_STOP */
    }

    /* see query_dictiter() */
    if (self->async)
        return _list_iter(_query_named_list(self));

    res = PyObject_CallFunction(namediter, "(O)", self);
    if (res && PyList_Check(res))
        return _list_iter(res);
    return res;
}

/* Get the current result as a list of scalar values. */
static PyObject *
_query_scalar_list(queryObject *self)
{
    PyObject *result_list;
    int i;

    if (!self->num_fields) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL; /* GCOVR_EXCL_LINE: out of memory */
    }

    ++self->in_use;
    for (i = 0; i < self->max_row; ++i) {
        PyObject *value = _query_row_as_scalar(self, i);

        if (!value) {
            Py_CLEAR(result_list);
            break;
        }
        PyList_SET_ITEM(result_list, i, value);
    }
    --self->in_use;
    self->current_row = self->max_row;

    return result_list;
}

/* Retrieve the last query result as a list of scalar values. */
static char query_scalarresult__doc__[] =
    "scalarresult() -- Get query result as scalars\n\n"
    "The result is returned as a list of scalar values where the values\n"
    "are the first fields of the rows in the order returned by the server.\n";

static PyObject *
query_scalarresult(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *res;

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    return _query_scalar_list(self);
}

/* Retrieve the last query result as iterator of scalar values. */
static char query_scalariter__doc__[] =
    "scalariter() -- Get query result as scalars\n\n"
    "The result is returned as an iterator of scalar values where the values\n"
    "are the first fields of the rows in the order returned by the server.\n";

static PyObject *
query_scalariter(queryObject *self, PyObject *noargs)
{
    PyObject *res;

    if (!scalariter) {
        /* GCOVR_EXCL_START: unreachable, set when pg is imported */
        return query_scalarresult(self, noargs);
        /* GCOVR_EXCL_STOP */
    }

    if ((res = _query_next_result(self)) != (PyObject *)self)
        return res;

    /* see query_dictiter() */
    if (self->async)
        return _list_iter(_query_scalar_list(self));

    if (!self->num_fields) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    return PyObject_CallFunction(scalariter, "(O)", self);
}

/* Retrieve one result as scalar value. */
static char query_onescalar__doc__[] =
    "onescalar() -- Get one scalar value from the result of a query\n\n"
    "Returns the first field of the next row from the result as a scalar "
    "value.\n"
    "This method can be called multiple times to return more rows as "
    "scalars.\n"
    "It returns None if the result does not contain one more row.\n";

static PyObject *
query_onescalar(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *value;

    if (_query_current_result(self) < 0)
        return NULL;

    /* no result (e.g. after the last one) is handled like no rows */
    if (!self->num_fields && self->result) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    if (self->current_row >= self->max_row) {
        Py_RETURN_NONE;
    }

    {
        int row = self->current_row;

        value = _query_row_as_scalar(self, row);
        if (value)
            self->current_row = row + 1;
    }
    return value;
}

/* Retrieves the single row from the result as a tuple. */
static char query_singlescalar__doc__[] =
    "singlescalar() -- Get scalar value from single result of a query\n\n"
    "Returns the first field of the single row from the result as a scalar "
    "value.\n"
    "This method returns the same single row when called multiple times.\n"
    "It raises an InvalidResultError if the result doesn't have exactly one "
    "row,\n"
    "which will be of type NoResultError or MultipleResultsError "
    "specifically.\n";

static PyObject *
query_singlescalar(queryObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *value;

    if (_query_current_result(self) < 0)
        return NULL;

    /* no result (e.g. after the last one) is handled like no rows */
    if (!self->num_fields && self->result) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    {
        int row = self->current_row;

        value = _query_row_as_scalar(self, row);
        if (value)
            self->current_row = row + 1;
    }
    return value;
}

/* Query sequence protocol methods */
static PySequenceMethods query_sequence_methods = {
    .sq_length = (lenfunc)query_len,
    .sq_item = (ssizeargfunc)query_getitem,
};

/* Query object methods */
static struct PyMethodDef query_methods[] = {
    {"getresult", (PyCFunction)query_getresult, METH_NOARGS,
     query_getresult__doc__},
    {"dictresult", (PyCFunction)query_dictresult, METH_NOARGS,
     query_dictresult__doc__},
    {"dictiter", (PyCFunction)query_dictiter, METH_NOARGS,
     query_dictiter__doc__},
    {"namedresult", (PyCFunction)query_namedresult, METH_NOARGS,
     query_namedresult__doc__},
    {"namediter", (PyCFunction)query_namediter, METH_NOARGS,
     query_namediter__doc__},
    {"one", (PyCFunction)query_one, METH_NOARGS, query_one__doc__},
    {"single", (PyCFunction)query_single, METH_NOARGS, query_single__doc__},
    {"onedict", (PyCFunction)query_onedict, METH_NOARGS, query_onedict__doc__},
    {"singledict", (PyCFunction)query_singledict, METH_NOARGS,
     query_singledict__doc__},
    {"onenamed", (PyCFunction)query_onenamed, METH_NOARGS,
     query_onenamed__doc__},
    {"singlenamed", (PyCFunction)query_singlenamed, METH_NOARGS,
     query_singlenamed__doc__},
    {"scalarresult", (PyCFunction)query_scalarresult, METH_NOARGS,
     query_scalarresult__doc__},
    {"scalariter", (PyCFunction)query_scalariter, METH_NOARGS,
     query_scalariter__doc__},
    {"onescalar", (PyCFunction)query_onescalar, METH_NOARGS,
     query_onescalar__doc__},
    {"singlescalar", (PyCFunction)query_singlescalar, METH_NOARGS,
     query_singlescalar__doc__},
    {"fieldname", (PyCFunction)query_fieldname, METH_VARARGS,
     query_fieldname__doc__},
    {"fieldnum", (PyCFunction)query_fieldnum, METH_VARARGS,
     query_fieldnum__doc__},
    {"listfields", (PyCFunction)query_listfields, METH_NOARGS,
     query_listfields__doc__},
    {"fieldinfo", (PyCFunction)query_fieldinfo, METH_VARARGS,
     query_fieldinfo__doc__},
    {"memsize", (PyCFunction)query_memsize, METH_NOARGS, query_memsize__doc__},
    {NULL, NULL, 0, NULL}};

static char query__doc__[] = "PyGreSQL query object";

/* Query type definition */
PyTypeObject queryType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.Query",
    .tp_basicsize = sizeof(queryObject),
    .tp_dealloc = (destructor)query_dealloc,
    .tp_as_sequence = &query_sequence_methods,
    .tp_str = (reprfunc)query_str,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = query__doc__,
    .tp_iter = (getiterfunc)query_iter,
    .tp_iternext = (iternextfunc)query_next,
    .tp_methods = query_methods,
};
