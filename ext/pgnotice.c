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

/* Needs to be after Python.h */
#include <structmember.h>

/* Deallocate notice object. */
static void
notice_dealloc(noticeObject *self)
{
    /* the connection is a borrowed reference */
    Py_XDECREF(self->message);
    Py_XDECREF(self->severity);
    Py_XDECREF(self->primary);
    Py_XDECREF(self->detail);
    Py_XDECREF(self->hint);
    PyObject_Del(self);
}

/* Return notice as string in human readable form. */
static PyObject *
notice_str(noticeObject *self)
{
    return Py_NewRef(self->message);
}

/* Notice object members (all read-only, NULL is returned as None) */
static struct PyMemberDef notice_members[] = {
    {"pgcnx", T_OBJECT, offsetof(noticeObject, pgcnx), READONLY,
     "the connection (only available inside the notice receiver)"},
    {"message", T_OBJECT, offsetof(noticeObject, message), READONLY,
     "the full message with a trailing newline"},
    {"severity", T_OBJECT, offsetof(noticeObject, severity), READONLY,
     "the level of the message, e.g. 'NOTICE' or 'WARNING'"},
    {"primary", T_OBJECT, offsetof(noticeObject, primary), READONLY,
     "the primary human-readable message"},
    {"detail", T_OBJECT, offsetof(noticeObject, detail), READONLY,
     "an optional secondary message"},
    {"hint", T_OBJECT, offsetof(noticeObject, hint), READONLY,
     "an optional suggestion what to do about the problem"},
    {NULL, 0, 0, 0, NULL}};

static char notice__doc__[] = "PostgreSQL notice object";

/* Notice type definition */
PyTypeObject noticeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.Notice",
    .tp_basicsize = sizeof(noticeObject),
    .tp_dealloc = (destructor)notice_dealloc,
    .tp_str = (reprfunc)notice_str,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = notice__doc__,
    .tp_members = notice_members,
};
