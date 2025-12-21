PyGreSQL - Python interface for PostgreSQL
==========================================

PyGreSQL is a Python module that interfaces to a PostgreSQL database.
It wraps the lower level C API library libpq to allow easy use of the
powerful PostgreSQL features from Python.

PyGreSQL should run on most platforms where PostgreSQL and Python is running.
It is based on the PyGres95 code written by Pascal Andre.
D'Arcy J. M. Cain renamed it to PyGreSQL starting with version 2.0
and serves as the "BDFL" of PyGreSQL.
Christoph Zwerschke volunteered as another maintainer and has been the main 
contributor since version 3.8 of PyGreSQL.

The following Python versions are supported:

* PyGreSQL 4.x and earlier: Python 2 only
* PyGreSQL 5.x: Python 2 and Python 3
* PyGreSQL 6.x and newer: Python 3 only

The current version of PyGreSQL supports Python versions 3.8 to 3.14
and PostgreSQL versions 12 to 18 on the server.

Installation
------------

The simplest way to install PyGreSQL is to type::

    $ pip install PyGreSQL

For other ways of installing PyGreSQL and requirements,
see the documentation.

Note that PyGreSQL also requires the libpq shared library to be
installed and accessible on the client machine.

Documentation
-------------

The documentation is available at
`pygresql.github.io/ <http://pygresql.github.io/>`_ and at
`pygresql.readthedocs.io <https://pygresql.readthedocs.io/>`_,
where you can also find the documentation for older versions.
