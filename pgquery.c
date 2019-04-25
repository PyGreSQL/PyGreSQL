/*
 * $Id: pgquery.c 985 2019-04-22 22:07:43Z cito $
 *
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The query object - this file is part a of the C extension module.
 *
 * Copyright (c) 2019 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 *
 */

/* Deallocate the query object. */
static void
query_dealloc(queryObject *self)
{
    Py_XDECREF(self->pgcnx);
    if (self->col_types) {
        PyMem_Free(self->col_types);
    }
    if (self->result) {
        PQclear(self->result);
    }

    PyObject_Del(self);
}

/* Return query as string in human readable form. */
static PyObject *
query_str(queryObject *self)
{
    return format_result(self->result);
}

/* Return length of a query object. */
static Py_ssize_t
query_len(PyObject *self)
{
    PyObject *tmp;
    Py_ssize_t len;

    tmp = PyLong_FromLong(((queryObject*) self)->max_row);
    len = PyLong_AsSsize_t(tmp);
    Py_DECREF(tmp);
    return len;
}

/* Return the value in the given column of the current row. */
static PyObject *
_query_value_in_column(queryObject *self, int column)
{
    char *s;
    int type;

    if (PQgetisnull(self->result, self->current_row, column)) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    /* get the string representation of the value */
    /* note: this is always null-terminated text format */
    s = PQgetvalue(self->result, self->current_row, column);
    /* get the PyGreSQL type of the column */
    type = self->col_types[column];

    /* cast the string representation into a Python object */
    if (type & PYGRES_ARRAY)
        return cast_array(s,
            PQgetlength(self->result, self->current_row, column),
            self->encoding, type, NULL, 0);
    if (type == PYGRES_BYTEA)
        return cast_bytea_text(s);
    if (type == PYGRES_OTHER)
        return cast_other(s,
            PQgetlength(self->result, self->current_row, column),
            self->encoding,
            PQftype(self->result, column), self->pgcnx->cast_hook);
    if (type & PYGRES_TEXT)
        return cast_sized_text(s,
            PQgetlength(self->result, self->current_row, column),
            self->encoding, type);
    return cast_unsized_simple(s, type);
}

/* Return the current row as a tuple. */
static PyObject *
_query_row_as_tuple(queryObject *self)
{
    PyObject *row_tuple = NULL;
    int j;

    if (!(row_tuple = PyTuple_New(self->num_fields))) {
        return NULL;
    }

    for (j = 0; j < self->num_fields; ++j) {
        PyObject *val = _query_value_in_column(self, j);
        if (!val) {
            Py_DECREF(row_tuple); return NULL;
        }
        PyTuple_SET_ITEM(row_tuple, j, val);
    }

    return row_tuple;
}

/* Return given item from a query object. */
static PyObject *
query_getitem(PyObject *self, Py_ssize_t i)
{
    queryObject *q = (queryObject *) self;
    PyObject *tmp;
    long row;

    tmp = PyLong_FromSize_t(i);
    row = PyLong_AsLong(tmp);
    Py_DECREF(tmp);

    if (row < 0 || row >= q->max_row) {
        PyErr_SetNone(PyExc_IndexError);
        return NULL;
    }

    q->current_row = row;
    return _query_row_as_tuple(q);
}

/* __iter__() method of the queryObject:
   Returns the default iterator yielding rows as tuples. */
static PyObject* query_iter(queryObject *self)
{
    self->current_row = 0;
    Py_INCREF(self);
    return (PyObject*) self;
}

/* __next__() method of the queryObject:
   Returns the current current row as a tuple and moves to the next one. */
static PyObject *
query_next(queryObject *self, PyObject *noargs)
{
    PyObject *row_tuple = NULL;

    if (self->current_row >= self->max_row) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    row_tuple = _query_row_as_tuple(self);
    if (row_tuple) ++self->current_row;
    return row_tuple;
}

/* Get number of rows. */
static char query_ntuples__doc__[] =
"ntuples() -- return number of tuples returned by query";

static PyObject *
query_ntuples(queryObject *self, PyObject *noargs)
{
    return PyInt_FromLong(self->max_row);
}

/* List field names from query result. */
static char query_listfields__doc__[] =
"listfields() -- List field names from result";

static PyObject *
query_listfields(queryObject *self, PyObject *noargs)
{
    int i;
    char *name;
    PyObject *fieldstuple, *str;

    /* builds tuple */
    fieldstuple = PyTuple_New(self->num_fields);
    if (fieldstuple) {
        for (i = 0; i < self->num_fields; ++i) {
            name = PQfname(self->result, i);
            str = PyStr_FromString(name);
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
    char *name;

    /* gets args */
    if (!PyArg_ParseTuple(args, "i", &i)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method fieldname() takes an integer as argument");
        return NULL;
    }

    /* checks number validity */
    if (i >= self->num_fields) {
        PyErr_SetString(PyExc_ValueError, "Invalid field number");
        return NULL;
    }

    /* gets fields name and builds object */
    name = PQfname(self->result, i);
    return PyStr_FromString(name);
}

/* Get field number from name in last result. */
static char query_fieldnum__doc__[] =
"fieldnum(name) -- return position in query for field from its name";

static PyObject *
query_fieldnum(queryObject *self, PyObject *args)
{
    int num;
    char *name;

    /* gets args */
    if (!PyArg_ParseTuple(args, "s", &name)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method fieldnum() takes a string as argument");
        return NULL;
    }

    /* gets field number */
    if ((num = PQfnumber(self->result, name)) == -1) {
        PyErr_SetString(PyExc_ValueError, "Unknown field");
        return NULL;
    }

    return PyInt_FromLong(num);
}

/* Retrieve one row from the result as a tuple. */
static char query_one__doc__[] =
"one() -- Get one row from the result of a query\n\n"
"Only one row from the result is returned as a tuple of fields.\n"
"This method can be called multiple times to return more rows.\n"
"It returns None if the result does not contain one more row.\n";

static PyObject *
query_one(queryObject *self, PyObject *noargs)
{
    PyObject *row_tuple;

    if (self->current_row >= self->max_row) {
        Py_INCREF(Py_None); return Py_None;
    }

    row_tuple = _query_row_as_tuple(self);
    if (row_tuple) ++self->current_row;
    return row_tuple;
}

/* Retrieve the single row from the result as a tuple. */
static char query_single__doc__[] =
"single() -- Get the result of a query as single row\n\n"
"The single row from the query result is returned as a tuple of fields.\n"
"This method returns the same single row when called multiple times.\n"
"It raises an InvalidResultError if the result doesn't have exactly one row,\n"
"which will be of type NoResultError or MultipleResultsError specifically.\n";

static PyObject *
query_single(queryObject *self, PyObject *noargs)
{
    PyObject *row_tuple;

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    row_tuple = _query_row_as_tuple(self);
    if (row_tuple) ++self->current_row;
    return row_tuple;
}

/* Retrieve the last query result as a list of tuples. */
static char query_getresult__doc__[] =
"getresult() -- Get the result of a query\n\n"
"The result is returned as a list of rows, each one a tuple of fields\n"
"in the order returned by the server.\n";

static PyObject *
query_getresult(queryObject *self, PyObject *noargs)
{
    PyObject *result_list;
    int i;

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL;
    }

    for (i = self->current_row = 0; i < self->max_row; ++i) {
        PyObject *row_tuple = query_next(self, noargs);

        if (!row_tuple) {
            Py_DECREF(result_list); return NULL;
        }
        PyList_SET_ITEM(result_list, i, row_tuple);
    }

    return result_list;
}

/* Return the current row as a dict. */
static PyObject *
_query_row_as_dict(queryObject *self)
{
    PyObject *row_dict = NULL;
    int j;

    if (!(row_dict = PyDict_New())) {
        return NULL;
    }

    for (j = 0; j < self->num_fields; ++j) {
        PyObject *val = _query_value_in_column(self, j);

        if (!val) {
            Py_DECREF(row_dict); return NULL;
        }
        PyDict_SetItemString(row_dict, PQfname(self->result, j), val);
        Py_DECREF(val);
    }

    return row_dict;
}

/* Return the current current row as a dict and move to the next one. */
static PyObject *
query_next_dict(queryObject *self, PyObject *noargs)
{
    PyObject *row_dict = NULL;

    if (self->current_row >= self->max_row) {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    row_dict = _query_row_as_dict(self);
    if (row_dict) ++self->current_row;
    return row_dict;
}

/* Retrieve one row from the result as a dictionary. */
static char query_onedict__doc__[] =
"onedict() -- Get one row from the result of a query\n\n"
"Only one row from the result is returned as a dictionary with\n"
"the field names used as the keys.\n"
"This method can be called multiple times to return more rows.\n"
"It returns None if the result does not contain one more row.\n";

static PyObject *
query_onedict(queryObject *self, PyObject *noargs)
{
    PyObject *row_dict;

    if (self->current_row >= self->max_row) {
        Py_INCREF(Py_None); return Py_None;
    }

    row_dict = _query_row_as_dict(self);
    if (row_dict) ++self->current_row;
    return row_dict;
}

/* Retrieve the single row from the result as a dictionary. */
static char query_singledict__doc__[] =
"singledict() -- Get the result of a query as single row\n\n"
"The single row from the query result is returned as a dictionary with\n"
"the field names used as the keys.\n"
"This method returns the same single row when called multiple times.\n"
"It raises an InvalidResultError if the result doesn't have exactly one row,\n"
"which will be of type NoResultError or MultipleResultsError specifically.\n";

static PyObject *
query_singledict(queryObject *self, PyObject *noargs)
{
    PyObject *row_dict;

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    row_dict = _query_row_as_dict(self);
    if (row_dict) ++self->current_row;
    return row_dict;
}

/* Retrieve the last query result as a list of dictionaries. */
static char query_dictresult__doc__[] =
"dictresult() -- Get the result of a query\n\n"
"The result is returned as a list of rows, each one a dictionary with\n"
"the field names used as the keys.\n";

static PyObject *
query_dictresult(queryObject *self, PyObject *noargs)
{
    PyObject *result_list;
    int i;

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL;
    }

    for (i = self->current_row = 0; i < self->max_row; ++i) {
        PyObject *row_dict = query_next_dict(self, noargs);

        if (!row_dict) {
            Py_DECREF(result_list); return NULL;
        }
        PyList_SET_ITEM(result_list, i, row_dict);
    }

    return result_list;
}

/* Retrieve last result as iterator of dictionaries. */
static char query_dictiter__doc__[] =
"dictiter() -- Get the result of a query\n\n"
"The result is returned as an iterator of rows, each one a a dictionary\n"
"with the field names used as the keys.\n";

static PyObject *
query_dictiter(queryObject *self, PyObject *noargs)
{
    if (!dictiter) {
        return query_dictresult(self, noargs);
    }

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
    if (!namednext) {
        return query_one(self, noargs);
    }

    if (self->current_row >= self->max_row) {
        Py_INCREF(Py_None); return Py_None;
    }

    return PyObject_CallFunction(namednext, "(O)", self);
}

/* Retrieve the single row from the result as a tuple. */
static char query_singlenamed__doc__[] =
"singlenamed() -- Get the result of a query as single row\n\n"
"The single row from the query result is returned as named tuple of fields.\n"
"This method returns the same single row when called multiple times.\n"
"It raises an InvalidResultError if the result doesn't have exactly one row,\n"
"which will be of type NoResultError or MultipleResultsError specifically.\n";

static PyObject *
query_singlenamed(queryObject *self, PyObject *noargs)
{
    if (!namednext) {
        return query_single(self, noargs);
    }

    if (self->max_row != 1) {
        if (self->max_row)
            set_error_msg(MultipleResultsError, "Multiple results found");
        else
            set_error_msg(NoResultError, "No result found");
        return NULL;
    }

    self->current_row = 0;
    return PyObject_CallFunction(namednext, "(O)", self);
}

/* Retrieve last result as list of named tuples. */
static char query_namedresult__doc__[] =
"namedresult() -- Get the result of a query\n\n"
"The result is returned as a list of rows, each one a named tuple of fields\n"
"in the order returned by the server.\n";

static PyObject *
query_namedresult(queryObject *self, PyObject *noargs)
{
    PyObject *res, *res_list;

    if (!namediter) {
        return query_getresult(self, noargs);
    }

    res = PyObject_CallFunction(namediter, "(O)", self);
    if (!res) return NULL;
    if (PyList_Check(res)) return res;
    res_list = PySequence_List(res);
    Py_DECREF(res);
    return res_list;
}

/* Retrieve last result as iterator of named tuples. */
static char query_namediter__doc__[] =
"namediter() -- Get the result of a query\n\n"
"The result is returned as an iterator of rows, each one a named tuple\n"
"of fields in the order returned by the server.\n";

static PyObject *
query_namediter(queryObject *self, PyObject *noargs)
{
    PyObject *res, *res_iter;

    if (!namediter) {
        return query_iter(self);
    }

    res = PyObject_CallFunction(namediter, "(O)", self);
    if (!res) return NULL;
    if (!PyList_Check(res)) return res;
    res_iter = (Py_TYPE(res)->tp_iter)((PyObject *) self);
    Py_DECREF(res);
    return res_iter;
}

/* Retrieve the last query result as a list of scalar values. */
static char query_scalarresult__doc__[] =
"scalarresult() -- Get query result as scalars\n\n"
"The result is returned as a list of scalar values where the values\n"
"are the first fields of the rows in the order returned by the server.\n";

static PyObject *
query_scalarresult(queryObject *self, PyObject *noargs)
{
    PyObject *result_list;

    if (!self->num_fields) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    if (!(result_list = PyList_New(self->max_row))) {
        return NULL;
    }

    for (self->current_row = 0;
         self->current_row < self->max_row;
         ++self->current_row)
    {
        PyObject *value = _query_value_in_column(self, 0);

        if (!value) {
            Py_DECREF(result_list); return NULL;
        }
        PyList_SET_ITEM(result_list, self->current_row, value);
    }

    return result_list;
}

/* Retrieve the last query result as iterator of scalar values. */
static char query_scalariter__doc__[] =
"scalariter() -- Get query result as scalars\n\n"
"The result is returned as an iterator of scalar values where the values\n"
"are the first fields of the rows in the order returned by the server.\n";

static PyObject *
query_scalariter(queryObject *self, PyObject *noargs)
{
    if (!scalariter) {
        return query_scalarresult(self, noargs);
    }

    if (!self->num_fields) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    return PyObject_CallFunction(scalariter, "(O)", self);
}

/* Retrieve one result as scalar value. */
static char query_onescalar__doc__[] =
"onescalar() -- Get one scalar value from the result of a query\n\n"
"Returns the first field of the next row from the result as a scalar value.\n"
"This method can be called multiple times to return more rows as scalars.\n"
"It returns None if the result does not contain one more row.\n";

static PyObject *
query_onescalar(queryObject *self, PyObject *noargs)
{
    PyObject *value;

    if (!self->num_fields) {
        set_error_msg(ProgrammingError, "No fields in result");
        return NULL;
    }

    if (self->current_row >= self->max_row) {
        Py_INCREF(Py_None); return Py_None;
    }

    value = _query_value_in_column(self, 0);
    if (value) ++self->current_row;
    return value;
}

/* Retrieves the single row from the result as a tuple. */
static char query_singlescalar__doc__[] =
"singlescalar() -- Get scalar value from single result of a query\n\n"
"Returns the first field of the next row from the result as a scalar value.\n"
"This method returns the same single row when called multiple times.\n"
"It raises an InvalidResultError if the result doesn't have exactly one row,\n"
"which will be of type NoResultError or MultipleResultsError specifically.\n";

static PyObject *
query_singlescalar(queryObject *self, PyObject *noargs)
{
    PyObject *value;

    if (!self->num_fields) {
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
    value = _query_value_in_column(self, 0);
    if (value) ++self->current_row;
    return value;
}

/* Query sequence protocol methods */
static PySequenceMethods query_sequence_methods = {
    (lenfunc) query_len,           /* sq_length */
    0,                             /* sq_concat */
    0,                             /* sq_repeat */
    (ssizeargfunc) query_getitem,  /* sq_item */
    0,                             /* sq_ass_item */
    0,                             /* sq_contains */
    0,                             /* sq_inplace_concat */
    0,                             /* sq_inplace_repeat */
};

/* Query object methods */
static struct PyMethodDef query_methods[] = {
    {"getresult", (PyCFunction) query_getresult,
        METH_NOARGS, query_getresult__doc__},
    {"dictresult", (PyCFunction) query_dictresult,
        METH_NOARGS, query_dictresult__doc__},
    {"dictiter", (PyCFunction) query_dictiter,
        METH_NOARGS, query_dictiter__doc__},
    {"namedresult", (PyCFunction) query_namedresult,
        METH_NOARGS, query_namedresult__doc__},
    {"namediter", (PyCFunction) query_namediter,
        METH_NOARGS, query_namediter__doc__},
    {"one", (PyCFunction) query_one,
        METH_NOARGS, query_one__doc__},
    {"single", (PyCFunction) query_single,
        METH_NOARGS, query_single__doc__},
    {"onedict", (PyCFunction) query_onedict,
        METH_NOARGS, query_onedict__doc__},
    {"singledict", (PyCFunction) query_singledict,
        METH_NOARGS, query_singledict__doc__},
    {"onenamed", (PyCFunction) query_onenamed,
        METH_NOARGS, query_onenamed__doc__},
    {"singlenamed", (PyCFunction) query_singlenamed,
        METH_NOARGS, query_singlenamed__doc__},
    {"scalarresult", (PyCFunction) query_scalarresult,
        METH_NOARGS, query_scalarresult__doc__},
    {"scalariter", (PyCFunction) query_scalariter,
        METH_NOARGS, query_scalariter__doc__},
    {"onescalar", (PyCFunction) query_onescalar,
        METH_NOARGS, query_onescalar__doc__},
    {"singlescalar", (PyCFunction) query_singlescalar,
        METH_NOARGS, query_singlescalar__doc__},
    {"fieldname", (PyCFunction) query_fieldname,
        METH_VARARGS, query_fieldname__doc__},
    {"fieldnum", (PyCFunction) query_fieldnum,
        METH_VARARGS, query_fieldnum__doc__},
    {"listfields", (PyCFunction) query_listfields,
        METH_NOARGS, query_listfields__doc__},
    {"ntuples", (PyCFunction) query_ntuples,
        METH_NOARGS, query_ntuples__doc__},
    {NULL, NULL}
};

static char query__doc__[] = "PyGreSQL query object";

/* Query type definition */
static PyTypeObject queryType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pg.Query",                  /* tp_name */
    sizeof(queryObject),         /* tp_basicsize */
    0,                           /* tp_itemsize */
    /* methods */
    (destructor) query_dealloc,  /* tp_dealloc */
    0,                           /* tp_print */
    0,                           /* tp_getattr */
    0,                           /* tp_setattr */
    0,                           /* tp_compare */
    0,                           /* tp_repr */
    0,                           /* tp_as_number */
    &query_sequence_methods,     /* tp_as_sequence */
    0,                           /* tp_as_mapping */
    0,                           /* tp_hash */
    0,                           /* tp_call */
    (reprfunc) query_str,        /* tp_str */
    PyObject_GenericGetAttr,     /* tp_getattro */
    0,                           /* tp_setattro */
    0,                           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT
        |Py_TPFLAGS_HAVE_ITER,   /* tp_flags */
    query__doc__,               /* tp_doc */
    0,                           /* tp_traverse */
    0,                           /* tp_clear */
    0,                           /* tp_richcompare */
    0,                           /* tp_weaklistoffset */
    (getiterfunc) query_iter,    /* tp_iter */
    (iternextfunc) query_next,   /* tp_iternext */
    query_methods,               /* tp_methods */
};
