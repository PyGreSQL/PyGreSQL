/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The connection object - this file is part a of the C extension module.
 *
 * Copyright (c) 2026 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.txt file for specific restrictions.
 */

/* Shared headers */
#include "pginternal.h"
#include "pgmodule.h"

/* Deallocate connection object. */
static void
conn_dealloc(connObject *self)
{
    PyObject_GC_UnTrack(self);
    /* do not call the notice receiver on a deallocated connection */
    Py_CLEAR(self->notice_receiver);
    if (self->cnx) {
        Py_BEGIN_ALLOW_THREADS
        PQfinish(self->cnx);
        Py_END_ALLOW_THREADS
    }
    Py_CLEAR(self->cast_hook);
    PyObject_GC_Del(self);
}

/* Visit the objects referenced by the connection (for the GC). */
static int
conn_traverse(connObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->cast_hook);
    Py_VISIT(self->notice_receiver);
    return 0;
}

/* Clear the objects referenced by the connection (for the GC).
   This breaks reference cycles through the hooks, e.g. when the
   notice receiver is a bound method of an object that holds the
   connection. */
static int
conn_clear(connObject *self)
{
    Py_CLEAR(self->cast_hook);
    Py_CLEAR(self->notice_receiver);
    return 0;
}

/* Get connection attributes. */
static PyObject *
conn_getattr(connObject *self, PyObject *nameobj)
{
    const char *name = PyUnicode_AsUTF8(nameobj);
    if (!name)
        return NULL;

    /*
     * Although we could check individually, there are only a few
     * attributes that don't require a live connection and unless someone
     * has an urgent need, this will have to do.
     */

    /* first exception - close which returns a different error */
    if (strcmp(name, "close") && !self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* list PostgreSQL connection fields */

    /* postmaster host */
    if (!strcmp(name, "host")) {
        const char *r = PQhost(self->cnx);
        if (!r || r[0] == '/') /* this can return a Unix socket path */
            r = "localhost";
        return get_decoded_text(r, pg_encoding_utf8);
    }

    /* server IP address, empty for Unix-domain sockets */
    if (!strcmp(name, "hostaddr")) {
        const char *r = PQhostaddr(self->cnx);
        return get_decoded_text(r ? r : "", pg_encoding_utf8);
    }

    /* postmaster port */
    if (!strcmp(name, "port"))
        return PyLong_FromLong(atol(PQport(self->cnx)));

    /* the connection parameters have been passed as UTF-8 by connect(),
       unless they have been passed as bytes */

    /* selected database */
    if (!strcmp(name, "db"))
        return get_decoded_text(PQdb(self->cnx), pg_encoding_utf8);

    /* selected options */
    if (!strcmp(name, "options"))
        return get_decoded_text(PQoptions(self->cnx), pg_encoding_utf8);

    /* error (status) message */
    if (!strcmp(name, "error"))
        return get_decoded_message(PQerrorMessage(self->cnx),
                                   PQclientEncoding(self->cnx));

    /* connection status : 1 - OK, 0 - BAD */
    if (!strcmp(name, "status"))
        return PyLong_FromLong(PQstatus(self->cnx) == CONNECTION_OK ? 1 : 0);

    /* provided user name */
    if (!strcmp(name, "user"))
        return get_decoded_text(PQuser(self->cnx), pg_encoding_utf8);

    /* protocol version */
    if (!strcmp(name, "protocol_version"))
        return PyLong_FromLong(PQprotocolVersion(self->cnx));

    /* full protocol version (e.g. 30002 for 3.2) */
    if (!strcmp(name, "full_protocol_version")) {
#ifdef LIBPQ_HAS_FULL_PROTOCOL_VERSION
        return PyLong_FromLong(PQfullProtocolVersion(self->cnx));
#else
        return PyLong_FromLong(PQprotocolVersion(self->cnx) * 10000L);
#endif
    }

    /* Python codec name of the client encoding */
    if (!strcmp(name, "encoding")) {
        int encoding = get_client_encoding(self);

        if (encoding < 0)
            return NULL;
        return PyUnicode_FromString(get_codec_name(encoding));
    }

    /* backend version */
    if (!strcmp(name, "server_version"))
        return PyLong_FromLong(PQserverVersion(self->cnx));

    /* descriptor number of connection socket */
    if (!strcmp(name, "socket")) {
        return PyLong_FromLong(PQsocket(self->cnx));
    }

    /* PID of backend process */
    if (!strcmp(name, "backend_pid")) {
        return PyLong_FromLong(PQbackendPID(self->cnx));
    }

    /* whether the connection uses SSL */
    if (!strcmp(name, "ssl_in_use")) {
        if (PQsslInUse(self->cnx)) {
            Py_RETURN_TRUE;
        }
        else {
            Py_RETURN_FALSE;
        }
    }

    /* SSL attributes */
    if (!strcmp(name, "ssl_attributes")) {
        return get_ssl_attributes(self->cnx);
    }

    return PyObject_GenericGetAttr((PyObject *)self, nameobj);
}

/* Check connection validity. */
int
_check_cnx_obj(connObject *self)
{
    if (!self || !self->valid || !self->cnx) {
        set_error_msg(OperationalError, "Connection has been closed");
        return 0;
    }
    return 1;
}

/* Mark the connection as busy before libpq functions that communicate with
   the server are called. While the connection is busy, it must not be used
   for other commands, neither by other threads (libpq is called with the
   GIL released), nor by Python code called back by libpq (the notice
   receiver), since libpq is not re-entrant. Return 1 on success, or 0 with
   an exception set if the connection is already busy or has been closed.
   Every successful call must be followed by a call to _conn_end(). */
int
_conn_begin(connObject *self)
{
    if (self->busy) {
        set_error_msg(InterfaceError, "The connection is busy");
        return 0;
    }
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return 0;
    }
    self->busy = 1;
    return 1;
}

/* Mark the connection as not busy any more. */
void
_conn_end(connObject *self)
{
    self->busy = 0;
}

/* Create source object. */
static char conn_source__doc__[] =
    "source() -- create a new source object for this connection";

static PyObject *
conn_source(connObject *self, PyObject *Py_UNUSED(noargs))
{
    sourceObject *source_obj;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* allocates new query object */
    if (!(source_obj = PyObject_New(sourceObject, &sourceType))) {
        return NULL;
    }

    /* initializes internal parameters */
    Py_XINCREF(self);
    source_obj->pgcnx = self;
    source_obj->result = NULL;
    source_obj->valid = 1;
    source_obj->arraysize = PG_ARRAYSIZE;

    return (PyObject *)source_obj;
}

/* For a non-query result, set the appropriate error status,
   return the appropriate value, and free the result set. */
PyObject *
_conn_non_query_result(int status, PGresult *result, PGconn *cnx)
{
    switch (status) {
        case PGRES_EMPTY_QUERY:
            PyErr_SetString(PyExc_ValueError, "Empty query");
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
        case PGRES_NONFATAL_ERROR:
            set_error(ProgrammingError, "Cannot execute query", cnx, result);
            break;
        case PGRES_COMMAND_OK: { /* INSERT, UPDATE, DELETE */
            Oid oid = PQoidValue(result);

            if (oid == InvalidOid) { /* not a single insert */
                char *ret = PQcmdTuples(result);

                if (ret[0]) { /* return number of rows affected */
                    PyObject *obj = PyUnicode_FromString(ret);
                    PQclear(result);
                    return obj;
                }
                PQclear(result);
                Py_RETURN_NONE;
            }
            /* for a single insert, return the oid */
            PQclear(result);
            return PyLong_FromUnsignedLong((unsigned long)oid);
        }
        case PGRES_COPY_OUT: /* no data will be received */
        case PGRES_COPY_IN:
            PQclear(result);
            Py_RETURN_NONE;
        default:
            set_error_msg(InternalError, "Unknown result status");
    }

    PQclear(result);
    return NULL; /* error detected on query */
}

/* Prepare the connection for a new command. If an asynchronous query still
   has pending results, they are moved into the query object, so that they
   are not discarded by libpq. Must be called while the connection is busy.
   Return 0 on success or -1 with an exception. */
int
_conn_prepare_command(connObject *self)
{
    if (self->async_query) {
        /* the query must not be deallocated while its results are moved
           with the GIL released, it is only a borrowed reference */
        PyObject *query = Py_NewRef((PyObject *)self->async_query);
        int rc = _query_detach(self->async_query);

        Py_DECREF(query);
        return rc;
    }

    if (self->cnx && PQtransactionStatus(self->cnx) == PQTRANS_ACTIVE) {
        /* The asynchronous query has been deleted before all results have
           been fetched. Nobody can get these results any more, so discard
           them like libpq does for synchronous commands, except for a COPY
           result, since a COPY can only be completed using the connection. */
        PGconn *cnx = self->cnx;
        PGresult *result;

        Py_BEGIN_ALLOW_THREADS
        while ((result = PQgetResult(cnx))) {
            ExecStatusType status = PQresultStatus(result);

            PQclear(result);
            if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT ||
                status == PGRES_COPY_BOTH)
                break;
        }
        Py_END_ALLOW_THREADS
    }
    return 0;
}

/* Create a new query object for the given result, which will be owned by
   the query object (also in case of an error). If async is set, the query
   becomes the pending asynchronous query of the connection. */
PyObject *
_conn_new_query(connObject *self, PGresult *result, int encoding, int async)
{
    queryObject *query_obj = PyObject_New(queryObject, &queryType);

    if (!query_obj) {
        if (result)
            PQclear(result);
        return NULL;
    }

    query_obj->pgcnx = (connObject *)Py_NewRef(self);
    query_obj->result = result;
    query_obj->async = async ? ASYNC_INITIAL : 0;
    query_obj->encoding = encoding;
    query_obj->current_row = 0;
    query_obj->max_row = 0;
    query_obj->num_fields = 0;
    query_obj->col_types = NULL;
    query_obj->value = NULL;
    query_obj->pending = NULL;
    query_obj->num_pending = 0;
    query_obj->next_pending = 0;
    query_obj->detached = 0;
    query_obj->in_use = 0;
    query_obj->copy = 0;
    if (result) {
        query_obj->max_row = PQntuples(result);
        query_obj->num_fields = PQnfields(result);
        query_obj->col_types = get_col_types(result, query_obj->num_fields);
        if (!query_obj->col_types) {
            /* this also clears the result and releases the connection */
            Py_DECREF(query_obj);
            return NULL;
        }
    }
    if (async)
        self->async_query = query_obj;

    return (PyObject *)query_obj;
}

/* Base method for execution of all different kinds of queries */
static PyObject *
_conn_query(connObject *self, PyObject *args, int prepared, int async)
{
    PyObject *query_str_obj, *param_obj = NULL;
    PGresult *result;
    char *query;
    int encoding, status, nparms = 0;
    const char *usage_str, *usage_seq;

    if (prepared) {
        usage_str =
            "Method query_prepared() expects a string as first argument";
        usage_seq =
            "Method query_prepared() expects a sequence as second argument";
    }
    else if (async) {
        usage_str = "Method send_query() expects a string as first argument";
        usage_seq =
            "Method send_query() expects a sequence as second argument";
    }
    else {
        usage_str = "Method query() expects a string as first argument";
        usage_seq = "Method query() expects a sequence as second argument";
    }

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* get query args */
    if (!PyArg_ParseTuple(args, "O|O", &query_str_obj, &param_obj)) {
        return NULL;
    }

    if ((encoding = get_client_encoding(self)) < 0)
        return NULL;

    if (!(query_str_obj = get_encoded_arg(query_str_obj, encoding, usage_str)))
        return NULL;
    query = PyBytes_AS_STRING(query_str_obj);

    /* If param_obj is passed, ensure it's a non-empty tuple. We want to treat
     * an empty tuple the same as no argument since we'll get that when the
     * caller passes no arguments to db.query(), and historic behaviour was
     * to call PQexec() in that case, which can execute multiple commands. */
    if (param_obj) {
        PyObject *seq_obj = PySequence_Fast(param_obj, usage_seq);

        /* use a tuple with own references to the parameters, since a list
         * could be changed while the parameters are converted to strings */
        param_obj = seq_obj ? PySequence_Tuple(seq_obj) : NULL;
        Py_XDECREF(seq_obj);
        if (!param_obj) {
            Py_DECREF(query_str_obj);
            return NULL;
        }
        nparms = (int)PyTuple_GET_SIZE(param_obj);

        /* if there's a single argument and it's a list or tuple, it
         * contains the positional arguments. */
        if (nparms == 1) {
            PyObject *first_obj = PyTuple_GET_ITEM(param_obj, 0);
            if (PyList_Check(first_obj) || PyTuple_Check(first_obj)) {
                /* get the tuple before the one holding the list is
                   released, since that may be its only reference */
                seq_obj = PySequence_Tuple(first_obj);
                Py_DECREF(param_obj);
                if (!(param_obj = seq_obj)) {
                    Py_DECREF(query_str_obj);
                    return NULL;
                }
                nparms = (int)PyTuple_GET_SIZE(param_obj);
            }
        }
    }

    /* gets result */
    if (nparms) {
        /* prepare arguments */
        PyObject **str, **s;
        const char **parms;
        Oid *types = NULL;
        int *lengths = NULL, *formats = NULL;
        int binary = 0;
        register int i;

        /* binary data marked as bytea is sent in binary format, so the
           lengths, formats and types are only needed if there is any */
        if (bytea_type) {
            for (i = 0; i < nparms; ++i) {
                if (PyObject_TypeCheck(PyTuple_GET_ITEM(param_obj, i),
                                       (PyTypeObject *)bytea_type)) {
                    binary = 1;
                    break;
                }
            }
        }

        str = (PyObject **)PyMem_Malloc((size_t)nparms * sizeof(*str));
        parms = (const char **)PyMem_Malloc((size_t)nparms * sizeof(*parms));
        if (binary) {
            types = (Oid *)PyMem_Calloc((size_t)nparms, sizeof(*types));
            lengths = (int *)PyMem_Calloc((size_t)nparms, sizeof(*lengths));
            formats = (int *)PyMem_Calloc((size_t)nparms, sizeof(*formats));
        }
        s = str;
        if (!str || !parms || (binary && (!types || !lengths || !formats))) {
            PyErr_NoMemory();
            status = -1;
            result = NULL;
            goto cleanup;
        }

        /* convert optional args to a list of strings -- this allows
         * the caller to pass whatever they like, and prevents us
         * from having to map types to OIDs */
        for (i = 0; i < nparms; ++i) {
            PyObject *obj = PyTuple_GET_ITEM(param_obj, i);
            PyObject *bytes_obj;

            if (obj == Py_None) {
                parms[i] = NULL;
                continue;
            }
            if (binary &&
                PyObject_TypeCheck(obj, (PyTypeObject *)bytea_type)) {
                char *data;
                Py_ssize_t size;

                /* binary data is sent unchanged, NUL bytes are allowed */
                if (PyBytes_AsStringAndSize(obj, &data, &size) < 0) {
                    status = -1;
                    result = NULL;
                    goto cleanup;
                }
                if (size > INT_MAX) {
                    PyErr_SetString(PyExc_ValueError,
                                    "Bytea parameter is too large");
                    status = -1;
                    result = NULL;
                    goto cleanup;
                }
                *s++ = Py_NewRef(obj);
                parms[i] = data;
                lengths[i] = (int)size;
                formats[i] = 1;
                types[i] = BYTEAOID;
                continue;
            }
            /* like the query, encode with the client encoding and check for
               NUL characters, since parameters are passed as C strings */
            obj = get_encoded_value(obj, encoding);
            bytes_obj = obj ? get_encoded_arg(obj, encoding, NULL) : NULL;
            Py_XDECREF(obj);
            if (!bytes_obj) {
                status = -1;
                result = NULL;
                goto cleanup;
            }
            *s++ = bytes_obj;
            parms[i] = PyBytes_AS_STRING(bytes_obj);
        }

        /* prepare the connection after the parameters have been converted,
           since this may have run Python code using the connection */
        if (!_conn_begin(self)) {
            status = -1;
            result = NULL;
        }
        else if (_conn_prepare_command(self) < 0) {
            _conn_end(self);
            status = -1;
            result = NULL;
        }
        else {
            Py_BEGIN_ALLOW_THREADS
            if (async) {
                status = PQsendQueryParams(self->cnx, query, nparms, types,
                                           (const char *const *)parms, lengths,
                                           formats, 0);
                result = NULL;
            }
            else {
                result = prepared
                             ? PQexecPrepared(self->cnx, query, nparms, parms,
                                              lengths, formats, 0)
                             : PQexecParams(self->cnx, query, nparms, types,
                                            parms, lengths, formats, 0);
                status = result != NULL;
            }
            Py_END_ALLOW_THREADS
            _conn_end(self);
        }

    cleanup:
        PyMem_Free(formats);
        PyMem_Free(lengths);
        PyMem_Free(types);
        PyMem_Free((void *)parms);
        if (str) {
            while (s != str) {
                s--;
                Py_DECREF(*s);
            }
            PyMem_Free(str);
        }
    }
    else if (!_conn_begin(self)) {
        status = -1;
        result = NULL;
    }
    else if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        status = -1;
        result = NULL;
    }
    else {
        Py_BEGIN_ALLOW_THREADS
        if (async) {
            status = PQsendQuery(self->cnx, query);
            result = NULL;
        }
        else {
            result = prepared ? PQexecPrepared(self->cnx, query, 0, NULL, NULL,
                                               NULL, 0)
                              : PQexec(self->cnx, query);
            status = result != NULL;
        }
        Py_END_ALLOW_THREADS
        _conn_end(self);
    }

    /* we don't need the query and its params any more */
    Py_DECREF(query_str_obj);
    Py_XDECREF(param_obj);

    if (status < 0) /* the connection is busy or could not be prepared */
        return NULL;

    /* checks result validity */
    if (!status) {
        set_error_text(PyExc_ValueError, NULL, self->cnx);
        return NULL;
    }

    /* this may have changed the datestyle, so we reset the date format
       in order to force fetching it newly when next time requested */
    self->date_format = date_format; /* this is normally NULL */

    /* checks result status */
    if (result && (status = PQresultStatus(result)) != PGRES_TUPLES_OK)
        return _conn_non_query_result(status, result, self->cnx);

    return _conn_new_query(self, result, encoding, async);
}

/* Database query */
static char conn_query__doc__[] =
    "query(sql, [arg]) -- create a new query object for this connection\n\n"
    "You must pass the SQL (string) request and you can optionally pass\n"
    "a tuple with positional parameters.\n";

static PyObject *
conn_query(connObject *self, PyObject *args)
{
    return _conn_query(self, args, 0, 0);
}

/* Asynchronous database query */
static char conn_send_query__doc__[] =
    "send_query(sql, [arg]) -- create a new asynchronous query for this "
    "connection\n\n"
    "You must pass the SQL (string) request and you can optionally pass\n"
    "a tuple with positional parameters.\n";

static PyObject *
conn_send_query(connObject *self, PyObject *args)
{
    return _conn_query(self, args, 0, 1);
}

/* Execute prepared statement. */
static char conn_query_prepared__doc__[] =
    "query_prepared(name, [arg]) -- execute a prepared statement\n\n"
    "You must pass the name (string) of the prepared statement and you can\n"
    "optionally pass a tuple with positional parameters.\n";

static PyObject *
conn_query_prepared(connObject *self, PyObject *args)
{
    return _conn_query(self, args, 1, 0);
}

/* Get the parameter types for prepare() from a sequence of type OIDs, where
   None or 0 means the type shall be inferred by the server. On success, set
   *types to a new array (NULL if no types are given) that must be freed with
   PyMem_Free() and *ntypes to its size, and return 0. Otherwise, return -1
   with an exception set. */
static int
_conn_get_param_types(PyObject *types_obj, Oid **types, int *ntypes)
{
    PyObject *seq_obj;
    Py_ssize_t i, n;
    Oid *oids;

    *types = NULL;
    *ntypes = 0;
    if (!types_obj || types_obj == Py_None)
        return 0;
    if (PyUnicode_Check(types_obj) || PyBytes_Check(types_obj) ||
        PyByteArray_Check(types_obj) || PyMemoryView_Check(types_obj) ||
        !PySequence_Check(types_obj)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method prepare() expects the types as a sequence"
                        " of OIDs");
        return -1;
    }
    /* use a tuple, since a list could be changed by __index__ */
    if (!(seq_obj = PySequence_Tuple(types_obj)))
        return -1;
    n = PyTuple_GET_SIZE(seq_obj);
    if (!n) {
        Py_DECREF(seq_obj);
        return 0;
    }
    if (n > INT_MAX) {
        Py_DECREF(seq_obj);
        PyErr_SetString(PyExc_ValueError, "Too many parameter types");
        return -1;
    }
    if (!(oids = (Oid *)PyMem_Malloc((size_t)n * sizeof(*oids)))) {
        Py_DECREF(seq_obj);
        PyErr_NoMemory();
        return -1;
    }
    for (i = 0; i < n; ++i) {
        PyObject *obj = PyTuple_GET_ITEM(seq_obj, i);
        Oid oid = 0;

        if (obj != Py_None) {
            PyObject *int_obj;
            int overflow;
            long long value;

            if (!PyIndex_Check(obj) || PyBool_Check(obj)) {
                PyErr_Format(PyExc_TypeError,
                             "Parameter type must be given as an int OID,"
                             " not %.200s",
                             Py_TYPE(obj)->tp_name);
                goto error;
            }
            if (!(int_obj = PyNumber_Index(obj)))
                goto error;
            value = PyLong_AsLongLongAndOverflow(int_obj, &overflow);
            Py_DECREF(int_obj);
            if (value == -1 && PyErr_Occurred())
                goto error;
            if (overflow || value < 0 || value > (long long)UINT_MAX) {
                PyErr_Format(PyExc_ValueError,
                             "Invalid parameter type OID: %R", obj);
                goto error;
            }
            oid = (Oid)value;
        }
        oids[i] = oid;
    }
    Py_DECREF(seq_obj);
    *types = oids;
    *ntypes = (int)n;
    return 0;

error:
    Py_DECREF(seq_obj);
    PyMem_Free(oids);
    return -1;
}

/* Create prepared statement. */
static char conn_prepare__doc__[] =
    "prepare(name, sql, [types]) -- create a prepared statement\n\n"
    "You must pass the name (string) of the prepared statement and the\n"
    "SQL (string) request for later execution. You can also pass a sequence\n"
    "with the type OIDs of the parameters, where None or 0 means that the\n"
    "type shall be inferred by the server.\n";

static PyObject *
conn_prepare(connObject *self, PyObject *args, PyObject *kwds)
{
    static const char usage[] =
        "Method prepare() expects the name and the command as strings";
    static char *kwlist[] = {"", "", "types", NULL};
    PyObject *name_obj, *query_obj, *types_obj = NULL;
    PGresult *result;
    Oid *types;
    int encoding, ntypes;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:prepare", kwlist,
                                     &name_obj, &query_obj, &types_obj)) {
        return NULL;
    }
    if ((encoding = get_client_encoding(self)) < 0)
        return NULL;
    if (_conn_get_param_types(types_obj, &types, &ntypes) < 0)
        return NULL;
    /* encode like query() and query_prepared() */
    if (!(name_obj = get_encoded_arg(name_obj, encoding, usage))) {
        PyMem_Free(types);
        return NULL;
    }
    if (!(query_obj = get_encoded_arg(query_obj, encoding, usage))) {
        Py_DECREF(name_obj);
        PyMem_Free(types);
        return NULL;
    }

    if (!_conn_begin(self)) {
        Py_DECREF(name_obj);
        Py_DECREF(query_obj);
        PyMem_Free(types);
        return NULL;
    }
    if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        Py_DECREF(name_obj);
        Py_DECREF(query_obj);
        PyMem_Free(types);
        return NULL;
    }

    /* create prepared statement */
    Py_BEGIN_ALLOW_THREADS
    result = PQprepare(self->cnx, PyBytes_AS_STRING(name_obj),
                       PyBytes_AS_STRING(query_obj), ntypes, types);
    Py_END_ALLOW_THREADS
    _conn_end(self);
    Py_DECREF(name_obj);
    Py_DECREF(query_obj);
    PyMem_Free(types);
    if (result && PQresultStatus(result) == PGRES_COMMAND_OK) {
        PQclear(result);
        Py_RETURN_NONE; /* success */
    }
    set_error(ProgrammingError, "Cannot create prepared statement", self->cnx,
              result);
    if (result)
        PQclear(result);
    return NULL; /* error */
}

/* Describe prepared statement. */
static char conn_describe_prepared__doc__[] =
    "describe_prepared(name) -- describe a prepared statement\n\n"
    "You must pass the name (string) of the prepared statement.\n";

static PyObject *
conn_describe_prepared(connObject *self, PyObject *args)
{
    static const char usage[] =
        "Method describe_prepared() takes a string argument";
    PyObject *name_obj;
    PGresult *result;
    int encoding;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTuple(args, "O", &name_obj)) {
        PyErr_SetString(PyExc_TypeError, usage);
        return NULL;
    }
    if ((encoding = get_client_encoding(self)) < 0)
        return NULL;
    if (!(name_obj = get_encoded_arg(name_obj, encoding, usage)))
        return NULL;

    if (!_conn_begin(self)) {
        Py_DECREF(name_obj);
        return NULL;
    }
    if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        Py_DECREF(name_obj);
        return NULL;
    }

    /* describe prepared statement */
    Py_BEGIN_ALLOW_THREADS
    result = PQdescribePrepared(self->cnx, PyBytes_AS_STRING(name_obj));
    Py_END_ALLOW_THREADS
    _conn_end(self);
    Py_DECREF(name_obj);
    if (result && PQresultStatus(result) == PGRES_COMMAND_OK)
        return _conn_new_query(self, result, encoding, 0);
    set_error(ProgrammingError, "Cannot describe prepared statement",
              self->cnx, result);
    if (result)
        PQclear(result);
    return NULL; /* error */
}

/* Consume the results at the end of a COPY made with getline() or with
   putline() and endcopy(), so that the connection becomes idle, and raise
   an error if the COPY failed. If the COPY belongs to the pending
   asynchronous query, the results are left to the query. Return 0 on
   success or -1 with an exception set. */
static int
_conn_finish_copy(connObject *self)
{
    PGconn *cnx = self->cnx;
    PGresult *result;
    int error = 0;

    if (self->async_query)
        return 0;

    for (;;) {
        ExecStatusType status;

        Py_BEGIN_ALLOW_THREADS
        result = PQgetResult(cnx);
        Py_END_ALLOW_THREADS
        if (!result)
            break;
        status = PQresultStatus(result);
        if (!error &&
            (status == PGRES_BAD_RESPONSE || status == PGRES_FATAL_ERROR ||
             status == PGRES_NONFATAL_ERROR)) {
            set_error(ProgrammingError, "Cannot copy", cnx, result);
            error = 1;
        }
        PQclear(result);
        if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT ||
            status == PGRES_COPY_BOTH)
            break;
    }
    return error ? -1 : 0;
}

static char conn_putline__doc__[] =
    "putline(line) -- send a line directly to the backend";

/* Direct access function: putline. */
static PyObject *
conn_putline(connObject *self, PyObject *args)
{
    PyObject *line_obj;
    int encoding, ret;
    size_t offset; /* length of the error message before sending */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* read args */
    if (!PyArg_ParseTuple(args, "O", &line_obj)) {
        return NULL;
    }
    /* like query(), check the connection also when bytes are passed */
    if ((encoding = get_client_encoding(self)) < 0)
        return NULL;
    if (PyBytes_Check(line_obj)) {
        Py_INCREF(line_obj);
    }
    else if (PyUnicode_Check(line_obj)) {
        if (!(line_obj = get_encoded_string(line_obj, encoding)))
            return NULL; /* pass the UnicodeEncodeError */
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method putline() takes a string argument");
        return NULL;
    }

    if (!_conn_begin(self)) {
        Py_DECREF(line_obj);
        return NULL;
    }
    /* send line to backend (the size is passed, so NUL bytes are sent) */
    offset = strlen(PQerrorMessage(self->cnx));
    ret = PQputCopyData(self->cnx, PyBytes_AS_STRING(line_obj),
                        (int)PyBytes_GET_SIZE(line_obj));
    _conn_end(self);
    Py_DECREF(line_obj);
    if (ret != 1) {
        set_error_text(
            PyExc_IOError,
            ret == -1
                ? get_new_error_message(self->cnx, offset)
                : "Line cannot be queued, wait for write-ready and try again",
            self->cnx);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Direct access function: getline. */
static char conn_getline__doc__[] =
    "getline() -- get a line directly from the backend";

static PyObject *
conn_getline(connObject *self, PyObject *Py_UNUSED(noargs))
{
    char *line = NULL;
    PyObject *str = NULL;
    int ret, finished = 0;
    size_t offset; /* length of the error message before receiving */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    /* get line synchronously */
    offset = strlen(PQerrorMessage(self->cnx));
    ret = PQgetCopyData(self->cnx, &line, 0);
    if (ret == -1) /* end of copy, consume the final results */
        finished = _conn_finish_copy(self);
    _conn_end(self);

    /* check result */
    if (ret <= 0) {
        if (line != NULL)
            PQfreemem(line);
        if (ret == -1) {
            if (finished < 0)
                return NULL;
            Py_RETURN_NONE;
        }
        set_error_text(
            PyExc_IOError,
            ret == -2 ? get_new_error_message(self->cnx, offset)
                      : "No line available, wait for read-ready and try again",
            self->cnx);
        return NULL;
    }
    if (line == NULL) {
        Py_RETURN_NONE;
    }
    /* for backward compatibility, strip the terminating newline */
    if (line[ret - 1] == '\n')
        --ret;
    /* decode like text values, i.e. return bytes if this is not possible */
    str = get_decoded_string_or_bytes(line, ret, PQclientEncoding(self->cnx));
    PQfreemem(line);
    return str;
}

/* Direct access function: end copy. */
static char conn_endcopy__doc__[] =
    "endcopy() -- synchronize client and server";

static PyObject *
conn_endcopy(connObject *self, PyObject *Py_UNUSED(noargs))
{
    int ret;
    size_t offset; /* length of the error message before ending */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    /* end direct copy */
    offset = strlen(PQerrorMessage(self->cnx));
    ret = PQputCopyEnd(self->cnx, NULL);
    if (ret != 1) {
        _conn_end(self);
        set_error_text(PyExc_IOError,
                       ret == -1 ? get_new_error_message(self->cnx, offset)
                                 : "Termination message cannot be queued,"
                                   " wait for write-ready and try again",
                       self->cnx);
        return NULL;
    }
    /* consume the final results, raising an error if the COPY failed */
    ret = _conn_finish_copy(self);
    _conn_end(self);
    if (ret < 0)
        return NULL;
    Py_RETURN_NONE;
}

/* Direct access function: set blocking status. */
static char conn_set_non_blocking__doc__[] =
    "set_non_blocking(nb) -- set the non-blocking status of the connection";

static PyObject *
conn_set_non_blocking(connObject *self, PyObject *args)
{
    int non_blocking, ret;
    size_t offset; /* length of the error message before the change */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!PyArg_ParseTuple(args, "i", &non_blocking)) {
        PyErr_SetString(
            PyExc_TypeError,
            "set_non_blocking() expects a boolean value as argument");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    /* this may flush the output buffer when switching to blocking mode */
    offset = strlen(PQerrorMessage(self->cnx));
    ret = PQsetnonblocking(self->cnx, non_blocking);
    _conn_end(self);
    if (ret < 0) {
        set_error_text(PyExc_IOError, get_new_error_message(self->cnx, offset),
                       self->cnx);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Direct access function: get blocking status. */
static char conn_is_non_blocking__doc__[] =
    "is_non_blocking() -- report the blocking status of the connection";

static PyObject *
conn_is_non_blocking(connObject *self, PyObject *Py_UNUSED(noargs))
{
    int rc;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    rc = PQisnonblocking(self->cnx);
    if (rc < 0) {
        set_error_text(PyExc_IOError, NULL, self->cnx);
        return NULL;
    }

    return PyBool_FromLong((long)rc);
}

/* Append an escaped identifier (str or bytes) to the buffer.
   If qualified is set, the name is split into parts at dots, and the parts
   are escaped separately. A part can be enclosed in double quotes, with a
   double quote inside written twice; it can then contain dots. This must
   work like split_qualified_name() in pg/helpers.py.
   Return 0 on success or -1 with an exception set. */
static int
_inserttable_append_identifier(connObject *self, struct CharBuffer *buf,
                               PyObject *name, int encoding, int qualified)
{
    PyObject *bytes_obj;
    char *s, *end, *part, *part_end, *scratch = NULL, *escaped;
    const char *error = NULL;
    Py_ssize_t len;
    int ret = -1;

    if (PyBytes_Check(name)) {
        bytes_obj = Py_NewRef(name);
    }
    else if (PyUnicode_Check(name)) {
        bytes_obj = get_encoded_string(name, encoding);
        if (!bytes_obj)
            return -1; /* pass the UnicodeEncodeError */
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "The third argument must contain only strings");
        return -1;
    }

    if (PyBytes_AsStringAndSize(bytes_obj, &s, &len) < 0)
        goto done;
    if (memchr(s, '\0', (size_t)len)) {
        PyErr_SetString(PyExc_ValueError,
                        "Table and column names must not contain NUL");
        goto done;
    }
    if (qualified) {
        /* buffer for the contents of quoted parts */
        scratch = PyMem_Malloc((size_t)len + 1);
        if (!scratch) {
            PyErr_NoMemory();
            goto done;
        }
    }

    /* neither the dot nor the double quote can be part of a multibyte
       character in any of the client encodings supported by PostgreSQL,
       so we can split the bytes */
    for (end = s + len;; ++s) {
        if (qualified && s < end && *s == '"') {
            for (part = part_end = scratch, ++s;; ++s) {
                if (s == end) {
                    error = "Unterminated quoted part in name %R";
                    goto done;
                }
                if (*s == '"') {
                    if (s + 1 == end || s[1] != '"')
                        break;
                    ++s; /* doubled quote */
                }
                *part_end++ = *s;
            }
            if (++s < end && *s != '.') {
                error = "Unexpected text after quoted part in name %R";
                goto done;
            }
        }
        else {
            part = s;
            part_end = qualified ? memchr(s, '.', (size_t)(end - s)) : NULL;
            if (!part_end)
                part_end = end;
            s = part_end;
        }
        if (qualified && part_end == part) {
            error = "Empty part in name %R";
            goto done;
        }
        escaped =
            PQescapeIdentifier(self->cnx, part, (size_t)(part_end - part));
        if (!escaped) {
            set_error(PyExc_ValueError, "Cannot escape identifier", self->cnx,
                      NULL);
            goto done;
        }
        ext_char_buffer_s(buf, escaped);
        PQfreemem(escaped);
        if (s == end)
            break;
        ext_char_buffer_c(buf, '.');
    }
    ret = 0;

done:
    if (error)
        PyErr_Format(PyExc_ValueError, error, name);
    PyMem_Free(scratch);
    Py_DECREF(bytes_obj);
    return ret;
}

/* Append binary data to the buffer as bytea in hex format, escaped for COPY.
   Return 0 on success or -1 with an exception set. */
static int
_inserttable_append_bytea(struct CharBuffer *buf, PyObject *item)
{
    static const char hex_digits[] = "0123456789abcdef";
    char *s, *end;
    Py_ssize_t len;

    if (PyBytes_AsStringAndSize(item, &s, &len) < 0)
        return -1;
    /* the backslash of the hex format must be escaped in COPY text */
    ext_char_buffer_s(buf, "\\\\x");
    for (end = s + len; s < end; ++s) {
        unsigned char c = (unsigned char)*s;

        ext_char_buffer_c(buf, hex_digits[c >> 4]);
        ext_char_buffer_c(buf, hex_digits[c & 0xf]);
    }
    return 0;
}

/* Append a value to the buffer in COPY text format.
   Return 0 on success or -1 with an exception set. */
static int
_inserttable_append_value(struct CharBuffer *buf, PyObject *item, int encoding)
{
    PyObject *bytes_obj;
    char *s, *end;
    Py_ssize_t len;
    int ret = 0;

    if (item == Py_None) {
        ext_char_buffer_c(buf, '\\');
        ext_char_buffer_c(buf, 'N');
        return 0;
    }

    /* binary data marked as bytea (plain bytes are raw text) */
    if (bytea_type && PyObject_TypeCheck(item, (PyTypeObject *)bytea_type))
        return _inserttable_append_bytea(buf, item);

    /* other values are converted like query parameters */
    if (!(bytes_obj = get_encoded_value(item, encoding)))
        return -1; /* pass the error */

    if (PyBytes_AsStringAndSize(bytes_obj, &s, &len) < 0) {
        Py_DECREF(bytes_obj);
        return -1;
    }
    for (end = s + len; s < end; ++s) {
        char c = *s;

        if (!c) {
            PyErr_SetString(PyExc_ValueError,
                            "Values must not contain NUL characters");
            ret = -1;
            break;
        }
        if (c == '\\' || c == '\t' || c == '\r' || c == '\n')
            ext_char_buffer_c(buf, '\\');
        ext_char_buffer_c(buf, c);
    }
    Py_DECREF(bytes_obj);
    return ret;
}

/* Send the buffered data to the server and empty the buffer.
   Return 0 on success or -1 with an exception set. */
static int
_inserttable_put_data(connObject *self, struct CharBuffer *buf)
{
    int rc = PQputCopyData(self->cnx, buf->data, (int)buf->size);

    buf->size = 0;
    if (rc == 1)
        return 0;
    if (rc == -1)
        set_error(PyExc_IOError, "Data cannot be sent", self->cnx, NULL);
    else
        PyErr_SetString(PyExc_IOError, "Data cannot be queued");
    return -1;
}

/* Consume the remaining results after the end of a copy operation, so that
   the connection is ready for the next command. */
static void
_inserttable_drain(connObject *self)
{
    PGresult *result;

    Py_BEGIN_ALLOW_THREADS
    while ((result = PQgetResult(self->cnx))) {
        ExecStatusType status = PQresultStatus(result);

        PQclear(result);
        /* still in copy state (should not happen), avoid an endless loop */
        if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT ||
            status == PGRES_COPY_BOTH)
            break;
    }
    Py_END_ALLOW_THREADS
}

/* Insert table */
static char conn_inserttable__doc__[] =
    "inserttable(table, values, [columns], *, freeze=False) -- insert "
    "iterable into table\n\n"
    "The fields in the iterable must be in the same order as in the table\n"
    "or in the list or tuple of columns if one is specified.\n\n"
    "If the optional argument 'freeze' is set to True, the inserted rows\n"
    "will be immediately frozen (can be useful for initial bulk loads).\n";

static PyObject *
conn_inserttable(connObject *self, PyObject *args, PyObject *kwds)
{
    PGresult *result;
    struct CharBuffer buffer = {0};
    const char *errormsg = NULL; /* reason for aborting the copy */
    int freeze = 0, encoding, busy = 0;
    PyObject *table, *rows, *columns = NULL;
    PyObject *iter_row = NULL, *cols = NULL, *row = NULL;
    PyObject *ret = NULL;
    Py_ssize_t i, j, m, n;

    static char *kwlist[] = {"table", "values", "columns", "freeze", NULL};

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|O$p", kwlist, &table,
                                     &rows, &columns, &freeze) ||
        !(PyUnicode_Check(table) || PyBytes_Check(table))) {
        PyErr_SetString(
            PyExc_TypeError,
            "Method inserttable() expects a string, an iterable, an optional "
            "list/tuple and an optional boolean 'freeze' as arguments");
        return NULL;
    }

    /* checks columns type (None means all columns) */
    if (columns == Py_None)
        columns = NULL;
    if (columns && !(PyTuple_Check(columns) || PyList_Check(columns))) {
        PyErr_SetString(PyExc_TypeError,
                        "Method inserttable() expects a tuple or a list"
                        " as third argument");
        return NULL;
    }

    /* checks list type */
    if (!(iter_row = PyObject_GetIter(rows))) {
        PyErr_SetString(
            PyExc_TypeError,
            "Method inserttable() expects an iterable as second argument");
        return NULL;
    }
    m = PySequence_Check(rows) ? PySequence_Size(rows) : -1;
    if (m < 0 && PyErr_Occurred()) {
        /* the number of rows is not known */
        if (!PyErr_ExceptionMatches(PyExc_TypeError))
            goto cleanup;
        PyErr_Clear();
    }
    else if (!m) {
        /* no rows specified, nothing to do */
        ret = PyLong_FromLong(0);
        goto cleanup;
    }

    if (columns) {
        /* use a tuple that cannot be changed while we are using it
           (do not use __iter__ here, which may be overridden) */
        cols = PyList_Check(columns) ? PyList_AsTuple(columns)
                                     : Py_NewRef(columns);
        if (!cols)
            goto cleanup;
        n = PyTuple_GET_SIZE(cols);
        if (!n) {
            /* no columns specified, nothing to do */
            ret = PyLong_FromLong(0);
            goto cleanup;
        }
    }
    else {
        n = -1; /* number of columns not yet known */
    }

    if ((encoding = get_client_encoding(self)) < 0)
        goto cleanup;

    /* pre-allocate some memory for the query buffer */
    if (!init_char_buffer(&buffer, 4096)) {
        PyErr_NoMemory();
        goto cleanup;
    }

    /* starts query */
    ext_char_buffer_s(&buffer, "copy ");

    /* adds the escaped (possibly qualified) table name */
    if (_inserttable_append_identifier(self, &buffer, table, encoding, 1) < 0)
        goto cleanup;

    if (cols) {
        /* adds a string like f" ({','.join(columns)})" */
        ext_char_buffer_s(&buffer, " (");
        for (j = 0; j < n; ++j) {
            if (j)
                ext_char_buffer_c(&buffer, ',');
            if (_inserttable_append_identifier(
                    self, &buffer, PyTuple_GET_ITEM(cols, j), encoding, 0) < 0)
                goto cleanup;
        }
        ext_char_buffer_c(&buffer, ')');
    }
    ext_char_buffer_s(&buffer, " from stdin");
    if (freeze)
        ext_char_buffer_s(&buffer, " freeze");
    ext_char_buffer_c(&buffer, '\0');

    if (buffer.error) {
        PyErr_NoMemory();
        goto cleanup;
    }

    /* the connection stays busy until the end of the copy operation,
       since converting the rows may run Python code */
    if (!(busy = _conn_begin(self)))
        goto cleanup;
    if (_conn_prepare_command(self) < 0)
        goto cleanup;

    Py_BEGIN_ALLOW_THREADS
    result = PQexec(self->cnx, buffer.data);
    Py_END_ALLOW_THREADS

    if (!result || PQresultStatus(result) != PGRES_COPY_IN) {
        PQclear(result);
        set_error(PyExc_ValueError, "Cannot copy", self->cnx, NULL);
        goto cleanup;
    }

    PQclear(result);

    /* from now on, errors must abort the copy operation */
    errormsg = "Invalid arguments";

    /* empty buffer while keeping allocated memory */
    buffer.size = 0;

    /* feed table */
    for (i = 0; m < 0 || i < m; ++i) {
        if (!(row = PyIter_Next(iter_row))) {
            if (PyErr_Occurred()) {
                errormsg = "Iteration error";
                goto cleanup;
            }
            break;
        }

        if (PyList_Check(row)) {
            /* use a copy that cannot be changed while we are using it,
               since converting the items may run arbitrary Python code */
            Py_SETREF(row, PyList_AsTuple(row));
            if (!row)
                goto cleanup;
        }
        else if (!PyTuple_Check(row)) {
            PyErr_SetString(
                PyExc_TypeError,
                "The second argument must contain tuples or lists");
            goto cleanup;
        }

        j = PyTuple_GET_SIZE(row);
        if (n < 0) {
            n = j;
        }
        else if (j != n) {
            PyErr_SetString(
                PyExc_TypeError,
                "The second arg must contain sequences of the same size");
            goto cleanup;
        }

        /* build insert line */
        for (j = 0; j < n; ++j) {
            if (j)
                ext_char_buffer_c(&buffer, '\t');
            if (_inserttable_append_value(&buffer, PyTuple_GET_ITEM(row, j),
                                          encoding) < 0) {
                errormsg = "Invalid data";
                goto cleanup;
            }
        }

        Py_CLEAR(row);

        /* terminate line */
        ext_char_buffer_c(&buffer, '\n');
        if (buffer.error) {
            PyErr_NoMemory();
            errormsg = "Memory error";
            goto cleanup;
        }

        if (buffer.size > 128 * 1024) {
            /* send buffered data after reaching 128KB */
            if (_inserttable_put_data(self, &buffer) < 0) {
                errormsg = "Data cannot be sent";
                goto cleanup;
            }
        }
    }

    /* flush any remaining data */
    if (buffer.size && _inserttable_put_data(self, &buffer) < 0) {
        errormsg = "Data cannot be sent";
        goto cleanup;
    }

    errormsg = NULL; /* the copy operation will be ended below */
    if (PQputCopyEnd(self->cnx, NULL) != 1) {
        set_error(PyExc_IOError, "Cannot end copy", self->cnx, NULL);
        _inserttable_drain(self);
        goto cleanup;
    }

    Py_BEGIN_ALLOW_THREADS
    result = PQgetResult(self->cnx);
    Py_END_ALLOW_THREADS
    if (PQresultStatus(result) == PGRES_COMMAND_OK)
        ret = PyLong_FromLong(atol(PQcmdTuples(result)));
    else
        set_error(PyExc_ValueError, "Cannot copy", self->cnx, NULL);
    PQclear(result);
    _inserttable_drain(self);

cleanup:
    if (errormsg) {
        /* abort the copy operation, the error is already set */
        PQputCopyEnd(self->cnx, errormsg);
        _inserttable_drain(self);
    }
    if (busy)
        _conn_end(self);
    PyMem_Free(buffer.data);
    Py_XDECREF(row);
    Py_XDECREF(cols);
    Py_XDECREF(iter_row);
    return ret;
}

/* Get transaction state. */
static char conn_transaction__doc__[] =
    "transaction() -- return the current transaction status";

static PyObject *
conn_transaction(connObject *self, PyObject *Py_UNUSED(noargs))
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    return PyLong_FromLong(PQtransactionStatus(self->cnx));
}

/* Get parameter setting. */
static char conn_parameter__doc__[] =
    "parameter(name) -- look up a current parameter setting";

static PyObject *
conn_parameter(connObject *self, PyObject *args)
{
    static const char usage[] =
        "Method parameter() takes a string as argument";
    PyObject *name_obj;
    const char *value;
    int encoding;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* get query args */
    if (!PyArg_ParseTuple(args, "O", &name_obj)) {
        PyErr_SetString(PyExc_TypeError, usage);
        return NULL;
    }

    /* this is only a lookup, so the name is encoded like field names */
    encoding = PQclientEncoding(self->cnx);
    if (!(name_obj = get_encoded_name(name_obj, encoding, usage)))
        return NULL;
    value = PQparameterStatus(self->cnx, PyBytes_AS_STRING(name_obj));
    Py_DECREF(name_obj);

    if (value)
        return get_decoded_text(value, encoding);

    /* unknown parameter, return None */
    Py_RETURN_NONE;
}

/* Get current date format. */
static char conn_date_format__doc__[] =
    "date_format() -- return the current date format";

static PyObject *
conn_date_format(connObject *self, PyObject *Py_UNUSED(noargs))
{
    const char *fmt;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* check if the date format is cached in the connection */
    fmt = self->date_format;
    if (!fmt) {
        fmt = date_style_to_format(PQparameterStatus(self->cnx, "DateStyle"));
        self->date_format = fmt; /* cache the result */
    }

    return PyUnicode_FromString(fmt);
}

/* Escape literal */
static char conn_escape_literal__doc__[] =
    "escape_literal(str) -- escape a literal constant for use within SQL";

static PyObject *
conn_escape_literal(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        if ((encoding = get_client_encoding(self)) < 0)
            return NULL;
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_literal() expects a string as argument");
        return NULL;
    }

    /* libpq stops at NUL characters, so they would be truncated */
    if (memchr(from, '\0', (size_t)from_length)) {
        Py_XDECREF(tmp_obj);
        PyErr_SetString(PyExc_ValueError, "embedded null character");
        return NULL;
    }

    to = PQescapeLiteral(self->cnx, from, (size_t)from_length);

    Py_XDECREF(tmp_obj);

    if (!to) {
        set_error(DataError, "Cannot escape literal", self->cnx, NULL);
        return NULL;
    }
    to_length = strlen(to);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length, encoding);
    PQfreemem(to);
    return to_obj;
}

/* Escape identifier */
static char conn_escape_identifier__doc__[] =
    "escape_identifier(str) -- escape an identifier for use within SQL";

static PyObject *
conn_escape_identifier(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        if ((encoding = get_client_encoding(self)) < 0)
            return NULL;
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj)
            return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_identifier() expects a string as argument");
        return NULL;
    }

    /* libpq stops at NUL characters, so they would be truncated */
    if (memchr(from, '\0', (size_t)from_length)) {
        Py_XDECREF(tmp_obj);
        PyErr_SetString(PyExc_ValueError, "embedded null character");
        return NULL;
    }

    to = PQescapeIdentifier(self->cnx, from, (size_t)from_length);

    Py_XDECREF(tmp_obj);

    if (!to) {
        set_error(DataError, "Cannot escape identifier", self->cnx, NULL);
        return NULL;
    }
    to_length = strlen(to);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length, encoding);
    PQfreemem(to);
    return to_obj;
}

/* Escape string */
static char conn_escape_string__doc__[] =
    "escape_string(str) -- escape a string for use within SQL";

static PyObject *
conn_escape_string(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */
    int error = 0;            /* error flag of libpq */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        if ((encoding = get_client_encoding(self)) < 0)
            return NULL;
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

    /* the result can be twice as long, do not truncate on overflow */
    if (from_length > (PY_SSIZE_T_MAX - 1) / 2) {
        Py_XDECREF(tmp_obj);
        return PyErr_NoMemory();
    }
    to_length = 2 * (size_t)from_length + 1;
    to = (char *)PyMem_Malloc(to_length);
    if (!to) {
        Py_XDECREF(tmp_obj);
        return PyErr_NoMemory();
    }
    to_length =
        PQescapeStringConn(self->cnx, to, from, (size_t)from_length, &error);

    Py_XDECREF(tmp_obj);

    /* invalid multibyte characters would be replaced silently */
    if (error) {
        PyMem_Free(to);
        set_error(DataError, "Cannot escape string", self->cnx, NULL);
        return NULL;
    }

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length, encoding);
    PyMem_Free(to);
    return to_obj;
}

/* Escape bytea */
static char conn_escape_bytea__doc__[] =
    "escape_bytea(data) -- escape binary data for use within SQL as type "
    "bytea";

static PyObject *
conn_escape_bytea(connObject *self, PyObject *data)
{
    PyObject *tmp_obj = NULL, /* auxiliary string object */
        *to_obj;              /* string object to return */
    char *from,               /* our string argument as encoded string */
        *to;                  /* the result as encoded string */
    Py_ssize_t from_length;   /* length of string */
    size_t to_length;         /* length of result */
    int encoding = -1;        /* client encoding */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        if ((encoding = get_client_encoding(self)) < 0)
            return NULL;
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

    to = (char *)PQescapeByteaConn(self->cnx, (unsigned char *)from,
                                   (size_t)from_length, &to_length);

    Py_XDECREF(tmp_obj);

    if (!to)
        return PyErr_NoMemory();

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t)to_length - 1);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t)to_length - 1, encoding);
    PQfreemem(to);
    return to_obj;
}

/* Get the password and the user name for encrypt_password() and
   change_password() as new references to bytes objects. The user name is
   an identifier written to the server, so a str is encoded strictly with
   the client encoding. A str password is always encoded as UTF-8, since
   connect() sends passwords as UTF-8 and SCRAM expects UTF-8, so that the
   password can be used to log in with connect() whatever the client
   encoding is. Bytes are passed unchanged. Return 0 on success or -1 with
   an exception set. */
static int
_conn_get_password_args(connObject *self, PyObject *passwd_obj,
                        PyObject *user_obj, const char *method,
                        PyObject **passwd, PyObject **user)
{
    char usage[100];
    int encoding;

    if ((encoding = get_client_encoding(self)) < 0)
        return -1;
    snprintf(usage, sizeof(usage),
             "Method %s() expects the password as str or bytes", method);
    if (!(*passwd = get_encoded_arg(passwd_obj, pg_encoding_utf8, usage)))
        return -1;
    snprintf(usage, sizeof(usage),
             "Method %s() expects the user name as str or bytes", method);
    if (!(*user = get_encoded_arg(user_obj, encoding, usage))) {
        Py_CLEAR(*passwd);
        return -1;
    }
    return 0;
}

/* Encrypt a password. */
static char conn_encrypt_password__doc__[] =
    "encrypt_password(password, user, algorithm=None) -- encrypt a password\n"
    "\n"
    "Return the password for the given user in encrypted form, as it can be\n"
    "used in a CREATE ROLE or ALTER ROLE command. The algorithm can be\n"
    "'scram-sha-256' or 'md5'. If it is None, the server setting\n"
    "password_encryption is used.\n";

static PyObject *
conn_encrypt_password(connObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"password", "user", "algorithm", NULL};
    PyObject *passwd_obj, *user_obj, *alg_obj = Py_None, *ret;
    char *encrypted, *alg = NULL;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:encrypt_password",
                                     kwlist, &passwd_obj, &user_obj,
                                     &alg_obj)) {
        return NULL;
    }
    if (_conn_get_password_args(self, passwd_obj, user_obj, "encrypt_password",
                                &passwd_obj, &user_obj) < 0)
        return NULL;
    if (alg_obj == Py_None) {
        alg_obj = NULL;
    }
    else {
        /* algorithm names are ASCII, other names are rejected by libpq */
        if (!(alg_obj = get_encoded_arg(
                  alg_obj, pg_encoding_utf8,
                  "Method encrypt_password() expects the algorithm"
                  " as str, bytes or None"))) {
            Py_DECREF(passwd_obj);
            Py_DECREF(user_obj);
            return NULL;
        }
        alg = PyBytes_AS_STRING(alg_obj);
    }

    /* the connection is used for error messages, and without an algorithm
       also for querying the server setting, so it must not be busy */
    if (!_conn_begin(self)) {
        Py_DECREF(passwd_obj);
        Py_DECREF(user_obj);
        Py_XDECREF(alg_obj);
        return NULL;
    }
    if (!alg && _conn_prepare_command(self) < 0) {
        _conn_end(self);
        Py_DECREF(passwd_obj);
        Py_DECREF(user_obj);
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    encrypted = PQencryptPasswordConn(self->cnx, PyBytes_AS_STRING(passwd_obj),
                                      PyBytes_AS_STRING(user_obj), alg);
    Py_END_ALLOW_THREADS
    _conn_end(self);
    Py_DECREF(passwd_obj);
    Py_DECREF(user_obj);
    Py_XDECREF(alg_obj);

    if (!encrypted) {
        /* with a given algorithm libpq does not contact the server, so the
           algorithm is invalid (or not supported by this libpq version) */
        if (alg)
            set_error_text(PyExc_ValueError, NULL, self->cnx);
        else
            set_error(ProgrammingError, "Cannot encrypt password", self->cnx,
                      NULL);
        return NULL;
    }
    /* the encrypted password is ASCII */
    ret = PyUnicode_DecodeASCII(encrypted, (Py_ssize_t)strlen(encrypted),
                                "strict");
    PQfreemem(encrypted);
    return ret;
}

#ifndef LIBPQ_HAS_CHANGE_PASSWORD
/* Replacement for PQchangePassword() of libpq 17 and newer, doing the same:
   encrypt the password with the algorithm from the server setting
   password_encryption and send it with ALTER USER, so that the password is
   never sent in clear text. Must be called with the GIL held while the
   connection is busy. Return the result, or NULL with the error message set
   in the connection, or NULL with a MemoryError set. */
static PGresult *
_conn_change_password(PGconn *cnx, const char *user, const char *passwd)
{
    static const char fmt[] = "ALTER USER %s PASSWORD %s";
    char *encrypted, *escaped_user = NULL, *escaped_passwd, *cmd = NULL;
    PGresult *result;
    size_t size;

    Py_BEGIN_ALLOW_THREADS
    encrypted = PQencryptPasswordConn(cnx, passwd, user, NULL);
    Py_END_ALLOW_THREADS
    if (!encrypted)
        return NULL;
    escaped_passwd = PQescapeLiteral(cnx, encrypted, strlen(encrypted));
    PQfreemem(encrypted);
    if (escaped_passwd)
        escaped_user = PQescapeIdentifier(cnx, user, strlen(user));
    if (escaped_user) {
        size = sizeof(fmt) + strlen(escaped_user) + strlen(escaped_passwd);
        if ((cmd = PyMem_Malloc(size)))
            snprintf(cmd, size, fmt, escaped_user, escaped_passwd);
        else
            PyErr_NoMemory();
        PQfreemem(escaped_user);
    }
    if (escaped_passwd)
        PQfreemem(escaped_passwd);
    if (!cmd)
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    result = PQexec(cnx, cmd);
    Py_END_ALLOW_THREADS
    PyMem_Free(cmd);
    return result;
}
#endif

/* Change a password. */
static char conn_change_password__doc__[] =
    "change_password(user, password) -- change the password of a user\n\n"
    "The password is encrypted with the algorithm set in the server setting\n"
    "password_encryption before it is sent to the server with ALTER USER.\n";

static PyObject *
conn_change_password(connObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"user", "password", NULL};
    PyObject *passwd_obj, *user_obj;
    PGresult *result;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO:change_password", kwlist,
                                     &user_obj, &passwd_obj)) {
        return NULL;
    }
    if (_conn_get_password_args(self, passwd_obj, user_obj, "change_password",
                                &passwd_obj, &user_obj) < 0)
        return NULL;

    if (!_conn_begin(self)) {
        Py_DECREF(passwd_obj);
        Py_DECREF(user_obj);
        return NULL;
    }
    if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        Py_DECREF(passwd_obj);
        Py_DECREF(user_obj);
        return NULL;
    }

#ifdef LIBPQ_HAS_CHANGE_PASSWORD
    Py_BEGIN_ALLOW_THREADS
    result = PQchangePassword(self->cnx, PyBytes_AS_STRING(user_obj),
                              PyBytes_AS_STRING(passwd_obj));
    Py_END_ALLOW_THREADS
#else
    result = _conn_change_password(self->cnx, PyBytes_AS_STRING(user_obj),
                                   PyBytes_AS_STRING(passwd_obj));
#endif
    _conn_end(self);
    Py_DECREF(passwd_obj);
    Py_DECREF(user_obj);
    if (result && PQresultStatus(result) == PGRES_COMMAND_OK) {
        PQclear(result);
        Py_RETURN_NONE; /* success */
    }
    if (result || !PyErr_Occurred())
        set_error(ProgrammingError, "Cannot change password", self->cnx,
                  result);
    if (result)
        PQclear(result);
    return NULL; /* error */
}

/* Constructor for large objects (internal use only) */
static largeObject *
large_new(connObject *pgcnx, Oid oid)
{
    largeObject *large_obj;

    if (!(large_obj = PyObject_New(largeObject, &largeType))) {
        return NULL;
    }

    Py_XINCREF(pgcnx);
    large_obj->pgcnx = pgcnx;
    large_obj->lo_fd = -1;
    large_obj->lo_oid = oid;

    return large_obj;
}

/* Create large object. */
static char conn_locreate__doc__[] =
    "locreate(mode) -- create a new large object in the database";

static PyObject *
conn_locreate(connObject *self, PyObject *args)
{
    int mode;
    Oid lo_oid;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i", &mode)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method locreate() takes an integer argument");
        return NULL;
    }

    /* creates large object */
    if (!_conn_begin(self))
        return NULL;
    if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        return NULL;
    }
    lo_oid = lo_creat(self->cnx, mode);
    _conn_end(self);
    if (lo_oid == 0) {
        set_error_msg(OperationalError, "Can't create large object");
        return NULL;
    }

    return (PyObject *)large_new(self, lo_oid);
}

/* Init from already known oid. */
static char conn_getlo__doc__[] =
    "getlo(oid) -- create a large object instance for the specified oid";

static PyObject *
conn_getlo(connObject *self, PyObject *args)
{
    PyObject *oid_obj, *int_obj;
    long long value;
    int overflow;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* gets arguments (OIDs are unsigned 32-bit integers) */
    if (!PyArg_ParseTuple(args, "O", &oid_obj)) {
        return NULL;
    }
    if (!PyIndex_Check(oid_obj) || PyBool_Check(oid_obj)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method getlo() takes an integer argument");
        return NULL;
    }
    if (!(int_obj = PyNumber_Index(oid_obj)))
        return NULL;
    value = PyLong_AsLongLongAndOverflow(int_obj, &overflow);
    Py_DECREF(int_obj);
    if (value == -1 && PyErr_Occurred())
        return NULL;
    if (overflow || value < 0 || value > (long long)UINT_MAX) {
        PyErr_Format(PyExc_ValueError, "Invalid large object OID: %R",
                     oid_obj);
        return NULL;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "The object oid can't be null");
        return NULL;
    }

    /* creates object */
    return (PyObject *)large_new(self, (Oid)value);
}

/* Import unix file. */
static char conn_loimport__doc__[] =
    "loimport(name) -- create a new large object from specified file";

static PyObject *
conn_loimport(connObject *self, PyObject *args)
{
    PyObject *name_obj;
    Oid lo_oid;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* gets arguments (a local file name, encoded like by os.fsencode) */
    if (!PyArg_ParseTuple(args, "O&", PyUnicode_FSConverter, &name_obj)) {
        return NULL;
    }

    if (!_conn_begin(self)) {
        Py_DECREF(name_obj);
        return NULL;
    }
    if (_conn_prepare_command(self) < 0) {
        _conn_end(self);
        Py_DECREF(name_obj);
        return NULL;
    }

    /* imports file and checks result */
    lo_oid = lo_import(self->cnx, PyBytes_AS_STRING(name_obj));
    _conn_end(self);
    Py_DECREF(name_obj);
    if (lo_oid == 0) {
        set_error_msg(OperationalError, "Can't create large object");
        return NULL;
    }

    return (PyObject *)large_new(self, lo_oid);
}

/* Reset connection. */
static char conn_reset__doc__[] =
    "reset() -- reset connection with current parameters\n\n"
    "Large objects and pending results of asynchronous queries derived\n"
    "from this connection will not be usable after this call.\n";

static PyObject *
conn_reset(connObject *self, PyObject *Py_UNUSED(noargs))
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    /* the results of a pending asynchronous query are discarded */
    self->async_query = NULL;

    /* resets the connection (with the GIL held, since other threads could
       otherwise access data of the connection that is freed by the reset,
       e.g. with parameter(), which does not check the busy flag) */
    PQreset(self->cnx);
    /* the new session uses the default date style of the server */
    self->date_format = NULL;
    _conn_end(self);
    /* like connect(), raise an error if the connection cannot be
       established again, instead of failing silently */
    if (PQstatus(self->cnx) != CONNECTION_OK) {
        set_error(ConnectError, "Cannot reset connection", self->cnx, NULL);
        return NULL;
    }
    Py_RETURN_NONE;
}

/* Cancel current command. */
static char conn_cancel__doc__[] =
    "cancel() -- abandon processing of the current command";

static PyObject *
conn_cancel(connObject *self, PyObject *Py_UNUSED(noargs))
{
    int ret, encoding;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }
    /* while another thread runs poll(), libpq may change the data that is
       needed for the cancel request, such as the cancel key */
    if (self->busy == 2) {
        set_error_msg(InterfaceError, "The connection is being established");
        return NULL;
    }

    /* request that the server abandon processing of the current command;
       the request is sent over a separate connection with its own error
       message, created while the GIL is held, so that the connection
       cannot be closed meanwhile, but sent without the GIL, since this
       method may be called while another thread runs a command, and
       PQrequestCancel() is not thread-safe; the connection must not be
       used any more after the GIL has been released */
    encoding = PQclientEncoding(self->cnx);
#ifdef LIBPQ_HAS_ASYNC_CANCEL
    {
        /* this uses the same encryption settings as the connection */
        PGcancelConn *cancel_conn = PQcancelCreate(self->cnx);

        if (!cancel_conn)
            return PyErr_NoMemory();
        Py_BEGIN_ALLOW_THREADS
        ret = PQcancelBlocking(cancel_conn);
        Py_END_ALLOW_THREADS
        if (!ret)
            set_error_msg_encoded(OperationalError,
                                  PQcancelErrorMessage(cancel_conn), encoding,
                                  "Cancel request failed");
        PQcancelFinish(cancel_conn);
    }
#else
    {
        char errbuf[256];
        PGcancel *cancel;

        /* older libpq versions do not check whether a cancel key has
           been received, then the request would be sent without a key */
        if (PQstatus(self->cnx) != CONNECTION_OK) {
            set_error_msg(OperationalError, "Connection is not open");
            return NULL;
        }
        /* this returns NULL if the connection is not open (or out of
           memory), and the request is not encrypted */
        cancel = PQgetCancel(self->cnx);
        if (!cancel) {
            set_error_msg(OperationalError, "Connection is not open");
            return NULL;
        }
        errbuf[0] = '\0';
        Py_BEGIN_ALLOW_THREADS
        ret = PQcancel(cancel, errbuf, (int)sizeof(errbuf));
        PQfreeCancel(cancel);
        Py_END_ALLOW_THREADS
        if (!ret)
            set_error_msg_encoded(OperationalError, errbuf, encoding,
                                  "Cancel request failed");
    }
#endif
    if (!ret)
        return NULL;
    /* the request was sent, but the command may still have completed */
    return PyLong_FromLong(1L);
}

/* Get connection socket. */
static char conn_fileno__doc__[] =
    "fileno() -- return database connection socket file handle";

static PyObject *
conn_fileno(connObject *self, PyObject *Py_UNUSED(noargs))
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    return PyLong_FromLong((long)PQsocket(self->cnx));
}

/* Set external typecast callback function. */
static char conn_set_cast_hook__doc__[] =
    "set_cast_hook(func) -- set a fallback typecast function";

static PyObject *
conn_set_cast_hook(connObject *self, PyObject *func)
{
    PyObject *ret = NULL;

    if (func == Py_None) {
        Py_XDECREF(self->cast_hook);
        self->cast_hook = NULL;
        ret = Py_NewRef(Py_None);
    }
    else if (PyCallable_Check(func)) {
        Py_XINCREF(func);
        Py_XDECREF(self->cast_hook);
        self->cast_hook = func;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method set_cast_hook() expects"
                        " a callable or None as argument");
    }

    return ret;
}

/* Get notice receiver callback function. */
static char conn_get_cast_hook__doc__[] =
    "get_cast_hook() -- get the fallback typecast function";

static PyObject *
conn_get_cast_hook(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *ret = self->cast_hook;

    return Py_NewRef(ret ? ret : Py_None);
}

/* Get asynchronous connection state. */
static char conn_poll__doc__[] =
    "poll() -- complete an asynchronous connection";

static PyObject *
conn_poll(connObject *self, PyObject *Py_UNUSED(noargs))
{
    int rc;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    self->busy = 2; /* the connection data may change, see cancel() */
    Py_BEGIN_ALLOW_THREADS
    rc = PQconnectPoll(self->cnx);
    Py_END_ALLOW_THREADS
    _conn_end(self);

    if (rc == PGRES_POLLING_FAILED) {
        set_error(ConnectError, "Polling failed", self->cnx, NULL);
        return NULL;
    }

    return PyLong_FromLong(rc);
}

/* Set notice receiver callback function. */
static char conn_set_notice_receiver__doc__[] =
    "set_notice_receiver(func) -- set the current notice receiver";

static PyObject *
conn_set_notice_receiver(connObject *self, PyObject *func)
{
    PyObject *ret = NULL;

    if (func == Py_None) {
        Py_CLEAR(self->notice_receiver);
        /* restore the default receiver of libpq (prints to stderr) */
        if (self->cnx && self->default_notice_receiver)
            PQsetNoticeReceiver(self->cnx, self->default_notice_receiver,
                                NULL);
        ret = Py_NewRef(Py_None);
    }
    else if (PyCallable_Check(func)) {
        PQnoticeReceiver old;

        Py_XSETREF(self->notice_receiver, Py_NewRef(func));
        old = PQsetNoticeReceiver(self->cnx, notice_receiver, self);
        if (old && old != notice_receiver)
            self->default_notice_receiver = old;
        ret = Py_NewRef(Py_None);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method set_notice_receiver() expects"
                        " a callable or None as argument");
    }

    return ret;
}

/* Get notice receiver callback function. */
static char conn_get_notice_receiver__doc__[] =
    "get_notice_receiver() -- get the current notice receiver";

static PyObject *
conn_get_notice_receiver(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *ret = self->notice_receiver;

    return Py_NewRef(ret ? ret : Py_None);
}

/* Close without deleting. */
static char conn_close__doc__[] =
    "close() -- close connection\n\n"
    "The connection, its large objects and pending results of asynchronous\n"
    "queries can no longer be used after this call.\n";

static PyObject *
conn_close(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PGconn *cnx;

    /* connection object cannot already be closed */
    if (!self->cnx) {
        set_error_msg(InternalError, "Connection already closed");
        return NULL;
    }

    if (self->busy) {
        set_error_msg(InterfaceError, "The connection is busy");
        return NULL;
    }
    /* the results of a pending asynchronous query are discarded */
    self->async_query = NULL;

    /* the connection is marked as closed before the GIL is released,
       so that it cannot be used by other threads while it is closed */
    cnx = self->cnx;
    self->cnx = NULL;
    Py_BEGIN_ALLOW_THREADS
    PQfinish(cnx);
    Py_END_ALLOW_THREADS

    /* the notice receiver will not be called any more, but the cast hook
       may still be needed to get the results of queries */
    Py_CLEAR(self->notice_receiver);
    Py_RETURN_NONE;
}

/* Get asynchronous notify. */
static char conn_get_notify__doc__[] =
    "getnotify() -- get database notify for this connection";

static PyObject *
conn_get_notify(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PGnotify *notify;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    if (!_conn_begin(self))
        return NULL;
    /* checks for NOTIFY messages (this may also call the notice receiver) */
    PQconsumeInput(self->cnx);
    notify = PQnotifies(self->cnx);
    _conn_end(self);

    if (!notify) {
        Py_RETURN_NONE;
    }
    else {
        int encoding = PQclientEncoding(self->cnx);
        PyObject *notify_result = NULL, *channel, *pid, *payload;

        /* the channel is a name, the payload is decoded like text values */
        channel = get_decoded_text(notify->relname, encoding);
        pid = channel ? PyLong_FromLong(notify->be_pid) : NULL;
        payload = pid ? get_decoded_string_or_bytes(
                            notify->extra, (Py_ssize_t)strlen(notify->extra),
                            encoding)
                      : NULL;
        PQfreemem(notify);

        if (payload)
            notify_result = PyTuple_Pack(3, channel, pid, payload);
        Py_XDECREF(channel);
        Py_XDECREF(pid);
        Py_XDECREF(payload);
        return notify_result;
    }
}

/* Get the list of connection attributes. */
static PyObject *
conn_dir(connObject *self, PyObject *Py_UNUSED(noargs))
{
    PyObject *attrs, *ret;

    /* the type is a borrowed reference, the result of extend() a new one */
    if (!(attrs = PyObject_Dir((PyObject *)Py_TYPE(self))))
        return NULL;
    ret = PyObject_CallMethod(
        attrs, "extend", "[ssssssssssssssss]", "host", "hostaddr", "port",
        "db", "options", "error", "status", "user", "protocol_version",
        "full_protocol_version", "server_version", "encoding", "socket",
        "backend_pid", "ssl_in_use", "ssl_attributes");
    if (!ret) {
        Py_DECREF(attrs);
        return NULL;
    }
    Py_DECREF(ret);

    return attrs;
}

/* Connection object methods */
static struct PyMethodDef conn_methods[] = {
    {"__dir__", (PyCFunction)conn_dir, METH_NOARGS, NULL},

    {"source", (PyCFunction)conn_source, METH_NOARGS, conn_source__doc__},
    {"query", (PyCFunction)conn_query, METH_VARARGS, conn_query__doc__},
    {"send_query", (PyCFunction)conn_send_query, METH_VARARGS,
     conn_send_query__doc__},
    {"query_prepared", (PyCFunction)conn_query_prepared, METH_VARARGS,
     conn_query_prepared__doc__},
    {"prepare", (PyCFunction)(void (*)(void))conn_prepare,
     METH_VARARGS | METH_KEYWORDS, conn_prepare__doc__},
    {"describe_prepared", (PyCFunction)conn_describe_prepared, METH_VARARGS,
     conn_describe_prepared__doc__},
    {"poll", (PyCFunction)conn_poll, METH_NOARGS, conn_poll__doc__},
    {"reset", (PyCFunction)conn_reset, METH_NOARGS, conn_reset__doc__},
    {"cancel", (PyCFunction)conn_cancel, METH_NOARGS, conn_cancel__doc__},
    {"close", (PyCFunction)conn_close, METH_NOARGS, conn_close__doc__},
    {"fileno", (PyCFunction)conn_fileno, METH_NOARGS, conn_fileno__doc__},
    {"get_cast_hook", (PyCFunction)conn_get_cast_hook, METH_NOARGS,
     conn_get_cast_hook__doc__},
    {"set_cast_hook", (PyCFunction)conn_set_cast_hook, METH_O,
     conn_set_cast_hook__doc__},
    {"get_notice_receiver", (PyCFunction)conn_get_notice_receiver, METH_NOARGS,
     conn_get_notice_receiver__doc__},
    {"set_notice_receiver", (PyCFunction)conn_set_notice_receiver, METH_O,
     conn_set_notice_receiver__doc__},
    {"getnotify", (PyCFunction)conn_get_notify, METH_NOARGS,
     conn_get_notify__doc__},
    {"inserttable", (PyCFunction)(void (*)(void))conn_inserttable,
     METH_VARARGS | METH_KEYWORDS, conn_inserttable__doc__},
    {"transaction", (PyCFunction)conn_transaction, METH_NOARGS,
     conn_transaction__doc__},
    {"parameter", (PyCFunction)conn_parameter, METH_VARARGS,
     conn_parameter__doc__},
    {"date_format", (PyCFunction)conn_date_format, METH_NOARGS,
     conn_date_format__doc__},

    {"escape_literal", (PyCFunction)conn_escape_literal, METH_O,
     conn_escape_literal__doc__},
    {"escape_identifier", (PyCFunction)conn_escape_identifier, METH_O,
     conn_escape_identifier__doc__},
    {"escape_string", (PyCFunction)conn_escape_string, METH_O,
     conn_escape_string__doc__},
    {"escape_bytea", (PyCFunction)conn_escape_bytea, METH_O,
     conn_escape_bytea__doc__},
    {"encrypt_password", (PyCFunction)(void (*)(void))conn_encrypt_password,
     METH_VARARGS | METH_KEYWORDS, conn_encrypt_password__doc__},
    {"change_password", (PyCFunction)(void (*)(void))conn_change_password,
     METH_VARARGS | METH_KEYWORDS, conn_change_password__doc__},

    {"putline", (PyCFunction)conn_putline, METH_VARARGS, conn_putline__doc__},
    {"getline", (PyCFunction)conn_getline, METH_NOARGS, conn_getline__doc__},
    {"endcopy", (PyCFunction)conn_endcopy, METH_NOARGS, conn_endcopy__doc__},
    {"set_non_blocking", (PyCFunction)conn_set_non_blocking, METH_VARARGS,
     conn_set_non_blocking__doc__},
    {"is_non_blocking", (PyCFunction)conn_is_non_blocking, METH_NOARGS,
     conn_is_non_blocking__doc__},

    {"locreate", (PyCFunction)conn_locreate, METH_VARARGS,
     conn_locreate__doc__},
    {"getlo", (PyCFunction)conn_getlo, METH_VARARGS, conn_getlo__doc__},
    {"loimport", (PyCFunction)conn_loimport, METH_VARARGS,
     conn_loimport__doc__},

    {NULL, NULL, 0, NULL} /* sentinel */
};

static char conn__doc__[] = "PostgreSQL connection object";

/* Connection type definition */
PyTypeObject connType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pg.Connection",
    .tp_basicsize = sizeof(connObject),
    .tp_dealloc = (destructor)conn_dealloc,
    .tp_getattro = (getattrofunc)conn_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_doc = conn__doc__,
    .tp_traverse = (traverseproc)conn_traverse,
    .tp_clear = (inquiry)conn_clear,
    .tp_methods = conn_methods,
};
