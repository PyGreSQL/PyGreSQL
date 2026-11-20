/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The notice object - this file is part a of the C extension module.
 *
 * Copyright (c) 2026 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Shared headers */
#include "pginternal.h"
#include "pgmodule.h"

/* Get notice object attributes. */
static PyObject *
notice_getattr(noticeObject *self, PyObject *nameobj)
{
    PGresult const *res = self->res;
    const char *name = PyUnicode_AsUTF8(nameobj);
    int fieldcode;

    if (!name)
        return NULL;

    if (!res) {
        PyErr_SetString(PyExc_TypeError, "Cannot get current notice");
        return NULL;
    }

    /* pg connection object */
    if (!strcmp(name, "pgcnx")) {
        if (self->pgcnx && _check_cnx_obj(self->pgcnx)) {
            return Py_NewRef(self->pgcnx);
        }
        else {
            Py_RETURN_NONE;
        }
    }

    /* full message */
    if (!strcmp(name, "message")) {
        return PyUnicode_FromString(PQresultErrorMessage(res));
    }

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
    if (fieldcode) {
        char *s = PQresultErrorField(res, fieldcode);
        if (s) {
            return PyUnicode_FromString(s);
        }
        else {
            Py_RETURN_NONE;
        }
    }

    return PyObject_GenericGetAttr((PyObject *)self, nameobj);
}

/* Get the list of notice attributes. */
static PyObject *
notice_dir(noticeObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *attrs;

    attrs = PyObject_Dir(PyObject_Type((PyObject *)self));
    PyObject_CallMethod(attrs, "extend", "[ssssss]", "pgcnx", "severity",
                        "message", "primary", "detail", "hint");

    return attrs;
}

/* Return notice as string in human readable form. */
static PyObject *
notice_str(noticeObject *self)
{
    return notice_getattr(self, PyBytes_FromString("message"));
}

/* Notice object methods */
static struct PyMethodDef notice_methods[] = {
    {"__dir__", (PyCFunction)notice_dir, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL}};

static char notice__doc__[] = "PostgreSQL notice object";

/* Notice type definition */
PyTypeObject noticeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.Notice",
    .tp_basicsize = sizeof(noticeObject),
    .tp_str = (reprfunc)notice_str,
    .tp_getattro = (getattrofunc)notice_getattr,
    .tp_setattro = PyObject_GenericSetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = notice__doc__,
    .tp_methods = notice_methods,
};
