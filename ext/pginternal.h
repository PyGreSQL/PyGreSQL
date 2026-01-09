/*
 * Internal functions header for the PyGreSQL C extension.
 * Provides prototypes for helpers implemented in pginternal.c
 * and extern declarations for module globals used therein.
 */

#ifndef PYGRE_SQL_PGINTERNAL_H
#define PYGRE_SQL_PGINTERNAL_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>

#include "pgmodule.h"

/* Encoding helpers */
/* PyGreSQL internal types */
#define PYGRES_INT 1
#define PYGRES_LONG 2
#define PYGRES_FLOAT 3
#define PYGRES_DECIMAL 4
#define PYGRES_MONEY 5
#define PYGRES_BOOL 6
/* Text based types */
#define PYGRES_TEXT 8
#define PYGRES_BYTEA 9
#define PYGRES_JSON 10
#define PYGRES_OTHER 11
/* Array types */
#define PYGRES_ARRAY 16
PyObject *
get_decoded_string(const char *str, Py_ssize_t size, int encoding);
PyObject *
get_encoded_string(PyObject *unicode_obj, int encoding);

/* Result/Type helpers */
int *
get_col_types(PGresult *result, int nfields);
PyObject *
format_result(const PGresult *res);

/* Casting helpers */
PyObject *
cast_bytea_text(char *s);
PyObject *
cast_sized_text(char *s, Py_ssize_t size, int encoding, int type);
PyObject *
cast_other(char *s, Py_ssize_t size, int encoding, Oid pgtype,
           PyObject *cast_hook);
PyObject *
cast_sized_simple(char *s, Py_ssize_t size, int type);
PyObject *
cast_unsized_simple(char *s, int type);
PyObject *
cast_array(char *s, Py_ssize_t size, int encoding, int type, PyObject *cast,
           char delim);
PyObject *
cast_record(char *s, Py_ssize_t size, int encoding, int *type, PyObject *cast,
            Py_ssize_t len, char delim);
PyObject *
cast_hstore(char *s, Py_ssize_t size, int encoding);

/* Error helpers */
void
set_error_msg(PyObject *type, const char *msg);
void
set_error(PyObject *type, const char *msg, PGconn *cnx, PGresult *result);

/* SSL attributes helper */
PyObject *
get_ssl_attributes(PGconn *cnx);

/* Date style helpers */
const char *
date_style_to_format(const char *s);
const char *
date_format_to_style(const char *s);

/* Notice receiver */
void
notice_receiver(void *arg, const PGresult *res);

/* Char buffer helpers */
int
init_char_buffer(struct CharBuffer *buf, size_t initial_size);
void
ext_char_buffer_s(struct CharBuffer *buf, const char *s);
void
ext_char_buffer_c(struct CharBuffer *buf, char c);

#endif /* PYGRE_SQL_PGINTERNAL_H */
