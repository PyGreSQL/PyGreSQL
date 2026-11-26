General PyGreSQL programming information
----------------------------------------

PyGreSQL consists of two parts: the "classic" PyGreSQL interface
provided by the :mod:`pg` module and the newer
DB-API 2.0 compliant interface provided by the :mod:`pgdb` module.

If you use only the standard features of the DB-API 2.0 interface,
it will be easier to switch from PostgreSQL to another database
for which a DB-API 2.0 compliant interface exists.

The "classic" interface may be easier to use for beginners, and it
provides some higher-level and PostgreSQL specific convenience methods.

.. seealso::

    **DB-API 2.0** (Python Database API Specification v2.0)
    is a specification for connecting to databases (not only PostgreSQL)
    from Python that has been developed by the Python DB-SIG in 1999.
    The authoritative programming information for the DB-API is :pep:`0249`.

Both Python modules utilize the same low-level C extension, which
serves as a wrapper for the "libpq" library, the C API to PostgreSQL.

This means you must have the libpq library installed as a shared library
on your client computer, in a version that is supported by PyGreSQL.
Depending on the client platform, you may have to set environment variables
like ``PATH`` or ``LD_LIBRARY_PATH`` so that PyGreSQL can find the library.

.. warning::

    Note that PyGreSQL is not thread-safe on the connection level. Therefore
    we recommend using `DBUtils <https://webwareforpython.github.io/DBUtils/>`_
    for multi-threaded environments, which supports both PyGreSQL interfaces.

The high-level `SQLAlchemy <https://www.sqlalchemy.org/>`_ SQL toolkit
and ORM included a dialect for using PyGreSQL as a database driver in
SQLAlchemy 1.1 to 1.4. This dialect has been removed in SQLAlchemy 2.0.
