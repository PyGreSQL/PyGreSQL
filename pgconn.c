/*
 * $Id: pgconn.c 985 2019-04-22 22:07:43Z cito $
 *
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The connection object - this file is part a of the C extension module.
 *
 * Copyright (c) 2019 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 *
 */

/* Deallocate connection object. */
static void
conn_dealloc(connObject *self)
{
    if (self->cnx) {
        Py_BEGIN_ALLOW_THREADS
        PQfinish(self->cnx);
        Py_END_ALLOW_THREADS
    }
    Py_XDECREF(self->cast_hook);
    Py_XDECREF(self->notice_receiver);
    PyObject_Del(self);
}

/* Get connection attributes. */
static PyObject *
conn_getattr(connObject *self, PyObject *nameobj)
{
    const char *name = PyStr_AsString(nameobj);

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
        char *r = PQhost(self->cnx);
        if (!r || r[0] == '/') /* Pg >= 9.6 can return a Unix socket path */
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
        return PyInt_FromLong(PQserverVersion(self->cnx));

    /* descriptor number of connection socket */
    if (!strcmp(name, "socket")) {
        return PyInt_FromLong(PQsocket(self->cnx));
    }

    /* PID of backend process */
    if (!strcmp(name, "backend_pid")) {
        return PyInt_FromLong(PQbackendPID(self->cnx));
    }

    /* whether the connection uses SSL */
    if (!strcmp(name, "ssl_in_use")) {
#ifdef SSL_INFO
        if (PQsslInUse(self->cnx)) {
            Py_INCREF(Py_True); return Py_True;
        }
        else {
            Py_INCREF(Py_False); return Py_False;
        }
#else
        set_error_msg(NotSupportedError, "SSL info functions not supported");
        return NULL;
#endif
    }

    /* SSL attributes */
    if (!strcmp(name, "ssl_attributes")) {
#ifdef SSL_INFO
        return get_ssl_attributes(self->cnx);
#else
        set_error_msg(NotSupportedError, "SSL info functions not supported");
        return NULL;
#endif
    }

    return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* Check connection validity. */
static int
_check_cnx_obj(connObject *self)
{
    if (!self || !self->valid || !self->cnx) {
        set_error_msg(OperationalError, "Connection has been closed");
        return 0;
    }
    return 1;
}

/* Create source object. */
static char conn_source__doc__[] =
"source() -- create a new source object for this connection";

static PyObject *
conn_source(connObject *self, PyObject *noargs)
{
    sourceObject *source_obj;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* allocates new query object */
    if (!(source_obj = PyObject_NEW(sourceObject, &sourceType))) {
        return NULL;
    }

    /* initializes internal parameters */
    Py_XINCREF(self);
    source_obj->pgcnx = self;
    source_obj->result = NULL;
    source_obj->valid = 1;
    source_obj->arraysize = PG_ARRAYSIZE;

    return (PyObject *) source_obj;
}

/* Base method for execution of both unprepared and prepared queries */
static PyObject *
_conn_query(connObject *self, PyObject *args, int prepared)
{
    PyObject *query_str_obj, *param_obj = NULL;
    PGresult* result;
    queryObject* query_obj;
    char *query;
    int encoding, status, nparms = 0;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* get query args */
    if (!PyArg_ParseTuple(args, "O|O", &query_str_obj, &param_obj)) {
        return NULL;
    }

    encoding = PQclientEncoding(self->cnx);

    if (PyBytes_Check(query_str_obj)) {
        query = PyBytes_AsString(query_str_obj);
        query_str_obj = NULL;
    }
    else if (PyUnicode_Check(query_str_obj)) {
        query_str_obj = get_encoded_string(query_str_obj, encoding);
        if (!query_str_obj) return NULL; /* pass the UnicodeEncodeError */
        query = PyBytes_AsString(query_str_obj);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method query() expects a string as first argument");
        return NULL;
    }

    /* If param_obj is passed, ensure it's a non-empty tuple. We want to treat
     * an empty tuple the same as no argument since we'll get that when the
     * caller passes no arguments to db.query(), and historic behaviour was
     * to call PQexec() in that case, which can execute multiple commands. */
    if (param_obj) {
        param_obj = PySequence_Fast(
            param_obj, "Method query() expects a sequence as second argument");
        if (!param_obj) {
            Py_XDECREF(query_str_obj);
            return NULL;
        }
        nparms = (int) PySequence_Fast_GET_SIZE(param_obj);

        /* if there's a single argument and it's a list or tuple, it
         * contains the positional arguments. */
        if (nparms == 1) {
            PyObject *first_obj = PySequence_Fast_GET_ITEM(param_obj, 0);
            if (PyList_Check(first_obj) || PyTuple_Check(first_obj)) {
                Py_DECREF(param_obj);
                param_obj = PySequence_Fast(first_obj, NULL);
                nparms = (int) PySequence_Fast_GET_SIZE(param_obj);
            }
        }
    }

    /* gets result */
    if (nparms) {
        /* prepare arguments */
        PyObject **str, **s;
        const char **parms, **p;
        register int i;

        str = (PyObject **) PyMem_Malloc(nparms * sizeof(*str));
        parms = (const char **) PyMem_Malloc(nparms * sizeof(*parms));
        if (!str || !parms) {
            PyMem_Free((void *) parms); PyMem_Free(str);
            Py_XDECREF(query_str_obj); Py_XDECREF(param_obj);
            return PyErr_NoMemory();
        }

        /* convert optional args to a list of strings -- this allows
         * the caller to pass whatever they like, and prevents us
         * from having to map types to OIDs */
        for (i = 0, s = str, p = parms; i < nparms; ++i, ++p) {
            PyObject *obj = PySequence_Fast_GET_ITEM(param_obj, i);

            if (obj == Py_None) {
                *p = NULL;
            }
            else if (PyBytes_Check(obj)) {
                *p = PyBytes_AsString(obj);
            }
            else if (PyUnicode_Check(obj)) {
                PyObject *str_obj = get_encoded_string(obj, encoding);
                if (!str_obj) {
                    PyMem_Free((void *) parms);
                    while (s != str) { s--; Py_DECREF(*s); }
                    PyMem_Free(str);
                    Py_XDECREF(query_str_obj);
                    Py_XDECREF(param_obj);
                    /* pass the UnicodeEncodeError */
                    return NULL;
                }
                *s++ = str_obj;
                *p = PyBytes_AsString(str_obj);
            }
            else {
                PyObject *str_obj = PyObject_Str(obj);
                if (!str_obj) {
                    PyMem_Free((void *) parms);
                    while (s != str) { s--; Py_DECREF(*s); }
                    PyMem_Free(str);
                    Py_XDECREF(query_str_obj);
                    Py_XDECREF(param_obj);
                    PyErr_SetString(
                        PyExc_TypeError,
                        "Query parameter has no string representation");
                    return NULL;
                }
                *s++ = str_obj;
                *p = PyStr_AsString(str_obj);
            }
        }

        Py_BEGIN_ALLOW_THREADS
        result = prepared ?
            PQexecPrepared(self->cnx, query, nparms,
                parms, NULL, NULL, 0) :
            PQexecParams(self->cnx, query, nparms,
                NULL, parms, NULL, NULL, 0);
        Py_END_ALLOW_THREADS

        PyMem_Free((void *) parms);
        while (s != str) { s--; Py_DECREF(*s); }
        PyMem_Free(str);
    }
    else {
        Py_BEGIN_ALLOW_THREADS
        result = prepared ?
            PQexecPrepared(self->cnx, query, 0,
                NULL, NULL, NULL, 0) :
            PQexec(self->cnx, query);
        Py_END_ALLOW_THREADS
    }

    /* we don't need the query and its params any more */
    Py_XDECREF(query_str_obj);
    Py_XDECREF(param_obj);

    /* checks result validity */
    if (!result) {
        PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
        return NULL;
    }

    /* this may have changed the datestyle, so we reset the date format
       in order to force fetching it newly when next time requested */
    self->date_format = date_format; /* this is normally NULL */

    /* checks result status */
    if ((status = PQresultStatus(result)) != PGRES_TUPLES_OK) {
        switch (status) {
            case PGRES_EMPTY_QUERY:
                PyErr_SetString(PyExc_ValueError, "Empty query");
                break;
            case PGRES_BAD_RESPONSE:
            case PGRES_FATAL_ERROR:
            case PGRES_NONFATAL_ERROR:
                set_error(ProgrammingError, "Cannot execute query",
                    self->cnx, result);
                break;
            case PGRES_COMMAND_OK:
                {   /* INSERT, UPDATE, DELETE */
                    Oid oid = PQoidValue(result);

                    if (oid == InvalidOid) {  /* not a single insert */
                        char *ret = PQcmdTuples(result);

                        if (ret[0]) {  /* return number of rows affected */
                            PyObject *obj = PyStr_FromString(ret);
                            PQclear(result);
                            return obj;
                        }
                        PQclear(result);
                        Py_INCREF(Py_None);
                        return Py_None;
                    }
                    /* for a single insert, return the oid */
                    PQclear(result);
                    return PyInt_FromLong(oid);
                }
            case PGRES_COPY_OUT: /* no data will be received */
            case PGRES_COPY_IN:
                PQclear(result);
                Py_INCREF(Py_None);
                return Py_None;
            default:
                set_error_msg(InternalError, "Unknown result status");
        }

        PQclear(result);
        return NULL; /* error detected on query */
    }

    if (!(query_obj = PyObject_NEW(queryObject, &queryType)))
        return PyErr_NoMemory();

    /* stores result and returns object */
    Py_XINCREF(self);
    query_obj->pgcnx = self;
    query_obj->result = result;
    query_obj->encoding = encoding;
    query_obj->current_row = 0;
    query_obj->max_row = PQntuples(result);
    query_obj->num_fields = PQnfields(result);
    query_obj->col_types = get_col_types(result, query_obj->num_fields);
    if (!query_obj->col_types) {
        Py_DECREF(query_obj);
        Py_DECREF(self);
        return NULL;
    }

    return (PyObject *) query_obj;
}

/* Database query */
static char conn_query__doc__[] =
"query(sql, [arg]) -- create a new query object for this connection\n\n"
"You must pass the SQL (string) request and you can optionally pass\n"
"a tuple with positional parameters.\n";

static PyObject *
conn_query(connObject *self, PyObject *args)
{
    return _conn_query(self, args, 0);
}

/* Execute prepared statement. */
static char conn_query_prepared__doc__[] =
"query_prepared(name, [arg]) -- execute a prepared statement\n\n"
"You must pass the name (string) of the prepared statement and you can\n"
"optionally pass a tuple with positional parameters.\n";

static PyObject *
conn_query_prepared(connObject *self, PyObject *args)
{
    return _conn_query(self, args, 1);
}

/* Create prepared statement. */
static char conn_prepare__doc__[] =
"prepare(name, sql) -- create a prepared statement\n\n"
"You must pass the name (string) of the prepared statement and the\n"
"SQL (string) request for later execution.\n";

static PyObject *
conn_prepare(connObject *self, PyObject *args)
{
    char *name, *query;
    int name_length, query_length;
    PGresult *result;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTuple(args, "s#s#",
        &name, &name_length, &query, &query_length))
    {
        PyErr_SetString(PyExc_TypeError,
                        "Method prepare() takes two string arguments");
        return NULL;
    }

    /* create prepared statement */
    Py_BEGIN_ALLOW_THREADS
    result = PQprepare(self->cnx, name, query, 0, NULL);
    Py_END_ALLOW_THREADS
    if (result && PQresultStatus(result) == PGRES_COMMAND_OK) {
        PQclear(result);
        Py_INCREF(Py_None);
        return Py_None; /* success */
    }
    set_error(ProgrammingError, "Cannot create prepared statement",
        self->cnx, result);
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
    char *name;
    int name_length;
    PGresult *result;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTuple(args, "s#", &name, &name_length)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method prepare() takes a string argument");
        return NULL;
    }

    /* describe prepared statement */
    Py_BEGIN_ALLOW_THREADS
    result = PQdescribePrepared(self->cnx, name);
    Py_END_ALLOW_THREADS
    if (result && PQresultStatus(result) == PGRES_COMMAND_OK) {
        queryObject *query_obj = PyObject_NEW(queryObject, &queryType);
        if (!query_obj)
            return PyErr_NoMemory();
        Py_XINCREF(self);
        query_obj->pgcnx = self;
        query_obj->result = result;
        query_obj->encoding = PQclientEncoding(self->cnx);
        query_obj->current_row = 0;
        query_obj->max_row = PQntuples(result);
        query_obj->num_fields = PQnfields(result);
        query_obj->col_types = get_col_types(result, query_obj->num_fields);
        return (PyObject *) query_obj;
    }
    set_error(ProgrammingError, "Cannot describe prepared statement",
        self->cnx, result);
    if (result)
        PQclear(result);
    return NULL; /* error */
}

#ifdef DIRECT_ACCESS
static char conn_putline__doc__[] =
"putline(line) -- send a line directly to the backend";

/* Direct access function: putline. */
static PyObject *
conn_putline(connObject *self, PyObject *args)
{
    char *line;
    int line_length;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* reads args */
    if (!PyArg_ParseTuple(args, "s#", &line, &line_length)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method putline() takes a string argument");
        return NULL;
    }

    /* sends line to backend */
    if (PQputline(self->cnx, line)) {
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
        return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

/* Direct access function: getline. */
static char conn_getline__doc__[] =
"getline() -- get a line directly from the backend";

static PyObject *
conn_getline(connObject *self, PyObject *noargs)
{
    char line[MAX_BUFFER_SIZE];
    PyObject *str = NULL;     /* GCC */

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* gets line */
    switch (PQgetline(self->cnx, line, MAX_BUFFER_SIZE)) {
        case 0:
            str = PyStr_FromString(line);
            break;
        case 1:
            PyErr_SetString(PyExc_MemoryError, "Buffer overflow");
            str = NULL;
            break;
        case EOF:
            Py_INCREF(Py_None);
            str = Py_None;
            break;
    }

    return str;
}

/* Direct access function: end copy. */
static char conn_endcopy__doc__[] =
"endcopy() -- synchronize client and server";

static PyObject *
conn_endcopy(connObject *self, PyObject *noargs)
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* ends direct copy */
    if (PQendcopy(self->cnx)) {
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
        return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}
#endif /* DIRECT_ACCESS */


/* Insert table */
static char conn_inserttable__doc__[] =
"inserttable(table, data) -- insert list into table\n\n"
"The fields in the list must be in the same order as in the table.\n";

static PyObject *
conn_inserttable(connObject *self, PyObject *args)
{
    PGresult *result;
    char *table, *buffer, *bufpt;
    int encoding;
    size_t bufsiz;
    PyObject *list, *sublist, *item;
    PyObject *(*getitem) (PyObject *, Py_ssize_t);
    PyObject *(*getsubitem) (PyObject *, Py_ssize_t);
    Py_ssize_t i, j, m, n;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "sO:filter", &table, &list)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Method inserttable() expects a string and a list as arguments");
        return NULL;
    }

    /* checks list type */
    if (PyList_Check(list)) {
        m = PyList_Size(list);
        getitem = PyList_GetItem;
    }
    else if (PyTuple_Check(list)) {
        m = PyTuple_Size(list);
        getitem = PyTuple_GetItem;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method inserttable() expects a list or a tuple"
            " as second argument");
        return NULL;
    }

    /* allocate buffer */
    if (!(buffer = PyMem_Malloc(MAX_BUFFER_SIZE)))
        return PyErr_NoMemory();

    /* starts query */
    sprintf(buffer, "copy %s from stdin", table);

    Py_BEGIN_ALLOW_THREADS
    result = PQexec(self->cnx, buffer);
    Py_END_ALLOW_THREADS

    if (!result) {
        PyMem_Free(buffer);
        PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->cnx));
        return NULL;
    }

    encoding = PQclientEncoding(self->cnx);

    PQclear(result);

    n = 0; /* not strictly necessary but avoids warning */

    /* feed table */
    for (i = 0; i < m; ++i) {
        sublist = getitem(list, i);
        if (PyTuple_Check(sublist)) {
            j = PyTuple_Size(sublist);
            getsubitem = PyTuple_GetItem;
        }
        else if (PyList_Check(sublist)) {
            j = PyList_Size(sublist);
            getsubitem = PyList_GetItem;
        }
        else {
            PyErr_SetString(
                PyExc_TypeError,
                "The second argument must contain a tuple or a list");
            return NULL;
        }
        if (i) {
            if (j != n) {
                PyMem_Free(buffer);
                PyErr_SetString(
                    PyExc_TypeError,
                    "Arrays contained in second arg must have same size");
                return NULL;
            }
        }
        else {
            n = j; /* never used before this assignment */
        }

        /* builds insert line */
        bufpt = buffer;
        bufsiz = MAX_BUFFER_SIZE - 1;

        for (j = 0; j < n; ++j) {
            if (j) {
                *bufpt++ = '\t'; --bufsiz;
            }

            item = getsubitem(sublist, j);

            /* convert item to string and append to buffer */
            if (item == Py_None) {
                if (bufsiz > 2) {
                    *bufpt++ = '\\'; *bufpt++ = 'N';
                    bufsiz -= 2;
                }
                else
                    bufsiz = 0;
            }
            else if (PyBytes_Check(item)) {
                const char* t = PyBytes_AsString(item);

                while (*t && bufsiz) {
                    if (*t == '\\' || *t == '\t' || *t == '\n') {
                        *bufpt++ = '\\'; --bufsiz;
                        if (!bufsiz) break;
                    }
                    *bufpt++ = *t++; --bufsiz;
                }
            }
            else if (PyUnicode_Check(item)) {
                PyObject *s = get_encoded_string(item, encoding);
                if (!s) {
                    PyMem_Free(buffer);
                    return NULL; /* pass the UnicodeEncodeError */
                }
                else {
                    const char* t = PyBytes_AsString(s);

                    while (*t && bufsiz) {
                        if (*t == '\\' || *t == '\t' || *t == '\n') {
                            *bufpt++ = '\\'; --bufsiz;
                            if (!bufsiz) break;
                        }
                        *bufpt++ = *t++; --bufsiz;
                    }
                    Py_DECREF(s);
                }
            }
            else if (PyInt_Check(item) || PyLong_Check(item)) {
                PyObject* s = PyObject_Str(item);
                const char* t = PyStr_AsString(s);

                while (*t && bufsiz) {
                    *bufpt++ = *t++; --bufsiz;
                }
                Py_DECREF(s);
            }
            else {
                PyObject* s = PyObject_Repr(item);
                const char* t = PyStr_AsString(s);

                while (*t && bufsiz) {
                    if (*t == '\\' || *t == '\t' || *t == '\n') {
                        *bufpt++ = '\\'; --bufsiz;
                        if (!bufsiz) break;
                    }
                    *bufpt++ = *t++; --bufsiz;
                }
                Py_DECREF(s);
            }

            if (bufsiz <= 0) {
                PyMem_Free(buffer); return PyErr_NoMemory();
            }

        }

        *bufpt++ = '\n'; *bufpt = '\0';

        /* sends data */
        if (PQputline(self->cnx, buffer)) {
            PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
            PQendcopy(self->cnx);
            PyMem_Free(buffer);
            return NULL;
        }
    }

    /* ends query */
    if (PQputline(self->cnx, "\\.\n")) {
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
        PQendcopy(self->cnx);
        PyMem_Free(buffer);
        return NULL;
    }

    if (PQendcopy(self->cnx)) {
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->cnx));
        PyMem_Free(buffer);
        return NULL;
    }

    PyMem_Free(buffer);

    /* no error : returns nothing */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Get transaction state. */
static char conn_transaction__doc__[] =
"transaction() -- return the current transaction status";

static PyObject *
conn_transaction(connObject *self, PyObject *noargs)
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    return PyInt_FromLong(PQtransactionStatus(self->cnx));
}

/* Get parameter setting. */
static char conn_parameter__doc__[] =
"parameter(name) -- look up a current parameter setting";

static PyObject *
conn_parameter(connObject *self, PyObject *args)
{
    const char *name;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* get query args */
    if (!PyArg_ParseTuple(args, "s", &name)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method parameter() takes a string as argument");
        return NULL;
    }

    name = PQparameterStatus(self->cnx, name);

    if (name)
        return PyStr_FromString(name);

    /* unknown parameter, return None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Get current date format. */
static char conn_date_format__doc__[] =
"date_format() -- return the current date format";

static PyObject *
conn_date_format(connObject *self, PyObject *noargs)
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

    return PyStr_FromString(fmt);
}

#ifdef ESCAPING_FUNCS

/* Escape literal */
static char conn_escape_literal__doc__[] =
"escape_literal(str) -- escape a literal constant for use within SQL";

static PyObject *
conn_escape_literal(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        encoding = PQclientEncoding(self->cnx);
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_literal() expects a string as argument");
        return NULL;
    }

    to = PQescapeLiteral(self->cnx, from, (size_t) from_length);
    to_length = strlen(to);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, to_length);
    else
        to_obj = get_decoded_string(to, to_length, encoding);
    if (to)
        PQfreemem(to);
    return to_obj;
}

/* Escape identifier */
static char conn_escape_identifier__doc__[] =
"escape_identifier(str) -- escape an identifier for use within SQL";

static PyObject *
conn_escape_identifier(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        encoding = PQclientEncoding(self->cnx);
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_identifier() expects a string as argument");
        return NULL;
    }

    to = PQescapeIdentifier(self->cnx, from, (size_t) from_length);
    to_length = strlen(to);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, to_length);
    else
        to_obj = get_decoded_string(to, to_length, encoding);
    if (to)
        PQfreemem(to);
    return to_obj;
}

#endif /* ESCAPING_FUNCS */

/* Escape string */
static char conn_escape_string__doc__[] =
"escape_string(str) -- escape a string for use within SQL";

static PyObject *
conn_escape_string(connObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        encoding = PQclientEncoding(self->cnx);
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_string() expects a string as argument");
        return NULL;
    }

    to_length = 2*from_length + 1;
    if ((Py_ssize_t) to_length < from_length) { /* overflow */
        to_length = from_length;
        from_length = (from_length - 1)/2;
    }
    to = (char *) PyMem_Malloc(to_length);
    to_length = PQescapeStringConn(self->cnx,
        to, from, (size_t) from_length, NULL);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, to_length);
    else
        to_obj = get_decoded_string(to, to_length, encoding);
    PyMem_Free(to);
    return to_obj;
}

/* Escape bytea */
static char conn_escape_bytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea";

static PyObject *
conn_escape_bytea(connObject *self, PyObject *data)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        encoding = PQclientEncoding(self->cnx);
        tmp_obj = get_encoded_string(data, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method escape_bytea() expects a string as argument");
        return NULL;
    }

    to = (char *) PQescapeByteaConn(self->cnx,
        (unsigned char *) from, (size_t) from_length, &to_length);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, to_length - 1);
    else
        to_obj = get_decoded_string(to, to_length - 1, encoding);
    if (to)
        PQfreemem(to);
    return to_obj;
}

#ifdef LARGE_OBJECTS

/* Constructor for large objects (internal use only) */
static largeObject *
large_new(connObject *pgcnx, Oid oid)
{
    largeObject *large_obj;

    if (!(large_obj = PyObject_NEW(largeObject, &largeType))) {
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
    lo_oid = lo_creat(self->cnx, mode);
    if (lo_oid == 0) {
        set_error_msg(OperationalError, "Can't create large object");
        return NULL;
    }

    return (PyObject *) large_new(self, lo_oid);
}

/* Init from already known oid. */
static char conn_getlo__doc__[] =
"getlo(oid) -- create a large object instance for the specified oid";

static PyObject *
conn_getlo(connObject *self, PyObject *args)
{
    int oid;
    Oid lo_oid;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i", &oid)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method getlo() takes an integer argument");
        return NULL;
    }

    lo_oid = (Oid) oid;
    if (lo_oid == 0) {
        PyErr_SetString(PyExc_ValueError, "The object oid can't be null");
        return NULL;
    }

    /* creates object */
    return (PyObject *) large_new(self, lo_oid);
}

/* Import unix file. */
static char conn_loimport__doc__[] =
"loimport(name) -- create a new large object from specified file";

static PyObject *
conn_loimport(connObject *self, PyObject *args)
{
    char *name;
    Oid lo_oid;

    /* checks validity */
    if (!_check_cnx_obj(self)) {
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s", &name)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method loimport() takes a string argument");
        return NULL;
    }

    /* imports file and checks result */
    lo_oid = lo_import(self->cnx, name);
    if (lo_oid == 0) {
        set_error_msg(OperationalError, "Can't create large object");
        return NULL;
    }

    return (PyObject *) large_new(self, lo_oid);
}

#endif /* LARGE_OBJECTS */

/* Reset connection. */
static char conn_reset__doc__[] =
"reset() -- reset connection with current parameters\n\n"
"All derived queries and large objects derived from this connection\n"
"will not be usable after this call.\n";

static PyObject *
conn_reset(connObject *self, PyObject *noargs)
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* resets the connection */
    PQreset(self->cnx);
    Py_INCREF(Py_None);
    return Py_None;
}

/* Cancel current command. */
static char conn_cancel__doc__[] =
"cancel() -- abandon processing of the current command";

static PyObject *
conn_cancel(connObject *self, PyObject *noargs)
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* request that the server abandon processing of the current command */
    return PyInt_FromLong((long) PQrequestCancel(self->cnx));
}

/* Get connection socket. */
static char conn_fileno__doc__[] =
"fileno() -- return database connection socket file handle";

static PyObject *
conn_fileno(connObject *self, PyObject *noargs)
{
    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

#ifdef NO_PQSOCKET
    return PyInt_FromLong((long) self->cnx->sock);
#else
    return PyInt_FromLong((long) PQsocket(self->cnx));
#endif
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
        Py_INCREF(Py_None); ret = Py_None;
    }
    else if (PyCallable_Check(func)) {
        Py_XINCREF(func); Py_XDECREF(self->cast_hook);
        self->cast_hook = func;
        Py_INCREF(Py_None); ret = Py_None;
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
conn_get_cast_hook(connObject *self, PyObject *noargs)
{
    PyObject *ret = self->cast_hook;;

    if (!ret)
        ret = Py_None;
    Py_INCREF(ret);

    return ret;
}

/* Set notice receiver callback function. */
static char conn_set_notice_receiver__doc__[] =
"set_notice_receiver(func) -- set the current notice receiver";

static PyObject *
conn_set_notice_receiver(connObject *self, PyObject *func)
{
    PyObject *ret = NULL;

    if (func == Py_None) {
        Py_XDECREF(self->notice_receiver);
        self->notice_receiver = NULL;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else if (PyCallable_Check(func)) {
        Py_XINCREF(func); Py_XDECREF(self->notice_receiver);
        self->notice_receiver = func;
        PQsetNoticeReceiver(self->cnx, notice_receiver, self);
        Py_INCREF(Py_None); ret = Py_None;
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
conn_get_notice_receiver(connObject *self, PyObject *noargs)
{
    PyObject *ret = self->notice_receiver;

    if (!ret)
        ret = Py_None;
    Py_INCREF(ret);

    return ret;
}

/* Close without deleting. */
static char conn_close__doc__[] =
"close() -- close connection\n\n"
"All instances of the connection object and derived objects\n"
"(queries and large objects) can no longer be used after this call.\n";

static PyObject *
conn_close(connObject *self, PyObject *noargs)
{
    /* connection object cannot already be closed */
    if (!self->cnx) {
        set_error_msg(InternalError, "Connection already closed");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    PQfinish(self->cnx);
    Py_END_ALLOW_THREADS

    self->cnx = NULL;
    Py_INCREF(Py_None);
    return Py_None;
}

/* Get asynchronous notify. */
static char conn_get_notify__doc__[] =
"getnotify() -- get database notify for this connection";

static PyObject *
conn_get_notify(connObject *self, PyObject *noargs)
{
    PGnotify *notify;

    if (!self->cnx) {
        PyErr_SetString(PyExc_TypeError, "Connection is not valid");
        return NULL;
    }

    /* checks for NOTIFY messages */
    PQconsumeInput(self->cnx);

    if (!(notify = PQnotifies(self->cnx))) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else {
        PyObject *notify_result, *tmp;

        if (!(tmp = PyStr_FromString(notify->relname))) {
            return NULL;
        }

        if (!(notify_result = PyTuple_New(3))) {
            return NULL;
        }

        PyTuple_SET_ITEM(notify_result, 0, tmp);

        if (!(tmp = PyInt_FromLong(notify->be_pid))) {
            Py_DECREF(notify_result);
            return NULL;
        }

        PyTuple_SET_ITEM(notify_result, 1, tmp);

        /* extra exists even in old versions that did not support it */
        if (!(tmp = PyStr_FromString(notify->extra))) {
            Py_DECREF(notify_result);
            return NULL;
        }

        PyTuple_SET_ITEM(notify_result, 2, tmp);

        PQfreemem(notify);

        return notify_result;
    }
}

/* Get the list of connection attributes. */
static PyObject *
conn_dir(connObject *self, PyObject *noargs)
{
    PyObject *attrs;

    attrs = PyObject_Dir(PyObject_Type((PyObject *) self));
    PyObject_CallMethod(
        attrs, "extend", "[sssssssssssss]",
        "host", "port", "db", "options", "error", "status", "user",
        "protocol_version", "server_version", "socket", "backend_pid",
        "ssl_in_use", "ssl_attributes");

    return attrs;
}

/* Connection object methods */
static struct PyMethodDef conn_methods[] = {
    {"__dir__", (PyCFunction) conn_dir,  METH_NOARGS, NULL},

    {"source", (PyCFunction) conn_source,
        METH_NOARGS, conn_source__doc__},
    {"query", (PyCFunction) conn_query,
        METH_VARARGS, conn_query__doc__},
    {"query_prepared", (PyCFunction) conn_query_prepared,
        METH_VARARGS, conn_query_prepared__doc__},
    {"prepare", (PyCFunction) conn_prepare,
        METH_VARARGS, conn_prepare__doc__},
    {"describe_prepared", (PyCFunction) conn_describe_prepared,
        METH_VARARGS, conn_describe_prepared__doc__},
    {"reset", (PyCFunction) conn_reset,
        METH_NOARGS, conn_reset__doc__},
    {"cancel", (PyCFunction) conn_cancel,
        METH_NOARGS, conn_cancel__doc__},
    {"close", (PyCFunction) conn_close,
        METH_NOARGS, conn_close__doc__},
    {"fileno", (PyCFunction) conn_fileno,
        METH_NOARGS, conn_fileno__doc__},
    {"get_cast_hook", (PyCFunction) conn_get_cast_hook,
        METH_NOARGS, conn_get_cast_hook__doc__},
    {"set_cast_hook", (PyCFunction) conn_set_cast_hook,
        METH_O, conn_set_cast_hook__doc__},
    {"get_notice_receiver", (PyCFunction) conn_get_notice_receiver,
        METH_NOARGS, conn_get_notice_receiver__doc__},
    {"set_notice_receiver", (PyCFunction) conn_set_notice_receiver,
        METH_O, conn_set_notice_receiver__doc__},
    {"getnotify", (PyCFunction) conn_get_notify,
        METH_NOARGS, conn_get_notify__doc__},
    {"inserttable", (PyCFunction) conn_inserttable,
        METH_VARARGS, conn_inserttable__doc__},
    {"transaction", (PyCFunction) conn_transaction,
        METH_NOARGS, conn_transaction__doc__},
    {"parameter", (PyCFunction) conn_parameter,
        METH_VARARGS, conn_parameter__doc__},
    {"date_format", (PyCFunction) conn_date_format,
        METH_NOARGS, conn_date_format__doc__},

#ifdef ESCAPING_FUNCS
    {"escape_literal", (PyCFunction) conn_escape_literal,
        METH_O, conn_escape_literal__doc__},
    {"escape_identifier", (PyCFunction) conn_escape_identifier,
        METH_O, conn_escape_identifier__doc__},
#endif /* ESCAPING_FUNCS */
    {"escape_string", (PyCFunction) conn_escape_string,
        METH_O, conn_escape_string__doc__},
    {"escape_bytea", (PyCFunction) conn_escape_bytea,
        METH_O, conn_escape_bytea__doc__},

#ifdef DIRECT_ACCESS
    {"putline", (PyCFunction) conn_putline,
        METH_VARARGS, conn_putline__doc__},
    {"getline", (PyCFunction) conn_getline,
        METH_NOARGS, conn_getline__doc__},
    {"endcopy", (PyCFunction) conn_endcopy,
        METH_NOARGS, conn_endcopy__doc__},
#endif /* DIRECT_ACCESS */

#ifdef LARGE_OBJECTS
    {"locreate", (PyCFunction) conn_locreate,
        METH_VARARGS, conn_locreate__doc__},
    {"getlo", (PyCFunction) conn_getlo,
        METH_VARARGS, conn_getlo__doc__},
    {"loimport", (PyCFunction) conn_loimport,
        METH_VARARGS, conn_loimport__doc__},
#endif /* LARGE_OBJECTS */

    {NULL, NULL} /* sentinel */
};

static char conn__doc__[] = "PostgreSQL connection object";

/* Connection type definition */
static PyTypeObject connType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pg.Connection",              /* tp_name */
    sizeof(connObject),           /* tp_basicsize */
    0,                            /* tp_itemsize */
    (destructor) conn_dealloc,    /* tp_dealloc */
    0,                            /* tp_print */
    0,                            /* tp_getattr */
    0,                            /* tp_setattr */
    0,                            /* tp_reserved */
    0,                            /* tp_repr */
    0,                            /* tp_as_number */
    0,                            /* tp_as_sequence */
    0,                            /* tp_as_mapping */
    0,                            /* tp_hash */
    0,                            /* tp_call */
    0,                            /* tp_str */
    (getattrofunc) conn_getattr,  /* tp_getattro */
    0,                            /* tp_setattro */
    0,                            /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    conn__doc__,                  /* tp_doc */
    0,                            /* tp_traverse */
    0,                            /* tp_clear */
    0,                            /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    0,                            /* tp_iter */
    0,                            /* tp_iternext */
    conn_methods,                 /* tp_methods */
};
