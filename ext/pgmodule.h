/*
 * Shared header for the PyGreSQL C extension module.
 * Declares common types, macros, and extern symbols used across files.
 */

#ifndef PYGRE_SQL_PGMODULE_H
#define PYGRE_SQL_PGMODULE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#include "pgtypes.h"

/* Default values */
#define PG_ARRAYSIZE 1

/* Flags for object validity checks */
#define CHECK_OPEN 1
#define CHECK_CLOSE 2
#define CHECK_CNX 4
#define CHECK_RESULT 8
#define CHECK_DQL 16

/* Query result types */
#define RESULT_EMPTY 1
#define RESULT_DML 2
#define RESULT_DDL 3
#define RESULT_DQL 4

/* Flags for move methods */
#define QUERY_MOVEFIRST 1
#define QUERY_MOVELAST 2
#define QUERY_MOVENEXT 3
#define QUERY_MOVEPREV 4

#define MAX_ARRAY_DEPTH 16 /* maximum allowed depth of an array */

/* Character buffer used by COPY and formatting helpers */
struct CharBuffer {
    char *data;      /* actual string data */
    size_t size;     /* current size of data */
    size_t max_size; /* allocated size */
    int error;       /* error flag (invalid data) */
};

/* Forward declarations for type objects (defined in their respective .c files)
 */
extern PyTypeObject connType;
extern PyTypeObject sourceType;
extern PyTypeObject noticeType;
extern PyTypeObject queryType;
extern PyTypeObject largeType;

/* Exception types (created in pgmodule.c) */
extern PyObject *Error, *Warning, *InterfaceError, *DatabaseError,
    *InternalError, *OperationalError, *ProgrammingError, *IntegrityError,
    *DataError, *NotSupportedError, *InvalidResultError, *NoResultError,
    *MultipleResultsError;

/* Module global configuration/state (defined in pgmodule.c) */
extern PyObject *pg_default_host;   /* default database host */
extern PyObject *pg_default_base;   /* default database name */
extern PyObject *pg_default_opt;    /* default connection options */
extern PyObject *pg_default_port;   /* default connection port */
extern PyObject *pg_default_user;   /* default username */
extern PyObject *pg_default_passwd; /* default password */

extern PyObject *decimal;       /* decimal type */
extern PyObject *dictiter;      /* function for getting dict results */
extern PyObject *namediter;     /* function for getting named results */
extern PyObject *namednext;     /* function for getting one named result */
extern PyObject *scalariter;    /* function for getting scalar results */
extern PyObject *jsondecode;    /* function for decoding json strings */
extern const char *date_format; /* date format that is always assumed */
extern char decimal_point;      /* decimal point used in money values */
extern int bool_as_text;        /* whether bool shall be returned as text */
extern int array_as_text;       /* whether arrays shall be returned as text */
extern int bytea_escaped;       /* whether bytea shall be returned escaped */

extern int pg_encoding_utf8;
extern int pg_encoding_latin1;
extern int pg_encoding_ascii;

/* Object declarations */
typedef struct {
    PyObject_HEAD int valid;   /* validity flag */
    PGconn *cnx;               /* Postgres connection handle */
    const char *date_format;   /* date format derived from datestyle */
    PyObject *cast_hook;       /* external typecast method */
    PyObject *notice_receiver; /* current notice receiver */
} connObject;

#define is_connObject(v) (PyType(v) == &connType)

typedef struct {
    PyObject_HEAD int valid; /* validity flag */
    connObject *pgcnx;       /* parent connection object */
    PGresult *result;        /* result content */
    int encoding;            /* client encoding */
    int result_type;         /* result type (DDL/DML/DQL) */
    long arraysize;          /* array size for fetch method */
    int current_row;         /* currently selected row */
    int max_row;             /* number of rows in the result */
    int num_fields;          /* number of fields in each row */
} sourceObject;

#define is_sourceObject(v) (PyType(v) == &sourceType)

typedef struct {
    PyObject_HEAD connObject *pgcnx; /* parent connection object */
    PGresult const *res;             /* an error or warning */
} noticeObject;

#define is_noticeObject(v) (PyType(v) == &noticeType)

typedef struct {
    PyObject_HEAD connObject *pgcnx; /* parent connection object */
    PGresult *result;                /* result content */
    int async;                       /* flag for asynchronous queries */
    int encoding;                    /* client encoding */
    int current_row;                 /* currently selected row */
    int max_row;                     /* number of rows in the result */
    int num_fields;                  /* number of fields in each row */
    int *col_types;                  /* PyGreSQL column types */
} queryObject;

#define is_queryObject(v) (PyType(v) == &queryType)

typedef struct {
    PyObject_HEAD connObject *pgcnx; /* parent connection object */
    Oid lo_oid;                      /* large object oid */
    int lo_fd;                       /* large object fd */
} largeObject;

#define is_largeObject(v) (PyType(v) == &largeType)

/* Cross-object helpers exported by pgconn.c */
int
_check_cnx_obj(connObject *self);
PyObject *
_conn_non_query_result(int status, PGresult *result, PGconn *cnx);

#endif /* PYGRE_SQL_PGMODULE_H */
