About PyGreSQL
==============

**PyGreSQL** is an *open-source* `Python <https://www.python.org>`_ module
that interfaces to a `PostgreSQL <https://www.postgresql.org>`_ database.
It wraps the lower level C API library libpq to allow easy use of the
powerful PostgreSQL features from Python.

    | This software is copyright © 1995, Pascal Andre.
    | Further modifications are copyright © 1997-2008 by D'Arcy J.M. Cain.
    | Further modifications are copyright © 2009-2026
      by the PyGreSQL Development Team.
    | For licensing details, see the full :doc:`copyright`.

**PostgreSQL** is a highly scalable, SQL compliant, open source
object-relational database management system. With more than 35 years
of active development, it has become one of the most widely used databases
for enterprise level open source solutions. Best of all, PostgreSQL's
source code is available under the PostgreSQL License, a liberal open
source license similar to the BSD or MIT licenses.

**Python** is an interpreted, interactive, object-oriented
programming language. It is often compared to Tcl, Perl, Scheme or Java.
Python combines remarkable power with very clear syntax. It has modules,
classes, exceptions, very high level dynamic data types, and dynamic typing.
There are interfaces to many system calls and libraries, as well as to
various windowing systems (X11, Motif, Tk, Mac, MFC). New built-in modules
are easily written in C or C++. Python is also usable as an extension
language for applications that need a programmable interface.
The Python implementation is copyrighted but freely usable and distributable,
even for commercial use.

**PyGreSQL** runs on most platforms where PostgreSQL and Python are
running. It is based on the PyGres95 code written by Pascal Andre
(andre@chimay.via.ecp.fr). D'Arcy (darcy@pygresql.org) renamed it to PyGreSQL
starting with version 2.0 and serves as the "BDFL" of PyGreSQL.

The current version PyGreSQL |version| needs PostgreSQL 12 to 19, and Python
3.10 to 3.15. If you need to support Python 3.8 or 3.9, you can resort to
PyGreSQL 6.2.x. For older PostgreSQL or Python versions, you can resort to
older PyGreSQL versions, e.g. 6.1.x for PostgreSQL 10 and 11 or Python 3.7,
and 5.x for even older versions.
