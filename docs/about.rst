About PyGreSQL
==============

**PyGreSQL** is an *open-source* `Python <http://www.python.org>`_ module
that interfaces to a `PostgreSQL <http://www.postgresql.org>`_ database.
It wraps the lower level C API library libpq to allow easy use of the
powerful PostgreSQL features from Python.

    | This software is copyright © 1995, Pascal Andre.
    | Further modifications are copyright © 1997-2008 by D'Arcy J.M. Cain.
    | Further modifications are copyright © 2009-2025 by the PyGreSQL team.
    | For licensing details, see the full :doc:`copyright`.

**PostgreSQL** is a highly scalable, SQL compliant, open source
object-relational database management system. With more than 20 years
of development history, it is quickly becoming the de facto database
for enterprise level open source solutions.
Best of all, PostgreSQL's source code is available under the most liberal
open source license: the BSD license.

**Python** Python is an interpreted, interactive, object-oriented
programming language. It is often compared to Tcl, Perl, Scheme or Java.
Python combines remarkable power with very clear syntax. It has modules,
classes, exceptions, very high level dynamic data types, and dynamic typing.
There are interfaces to many system calls and libraries, as well as to
various windowing systems (X11, Motif, Tk, Mac, MFC). New built-in modules
are easily written in C or C++. Python is also usable as an extension
language for applications that need a programmable interface.
The Python implementation is copyrighted but freely usable and distributable,
even for commercial use.

**PyGreSQL** is a Python module that interfaces to a PostgreSQL database.
It wraps the lower level C API library libpq to allow easy use of the
powerful PostgreSQL features from Python.

PyGreSQL is developed and tested on a NetBSD system, but it also runs on
most other platforms where PostgreSQL and Python is running.  It is based
on the PyGres95 code written by Pascal Andre (andre@chimay.via.ecp.fr).
D'Arcy (darcy@druid.net) renamed it to PyGreSQL starting with
version 2.0 and serves as the "BDFL" of PyGreSQL.

The current version PyGreSQL |version| needs PostgreSQL 10 to 17, and Python
3.7 to 3.13. If you need to support older PostgreSQL or Python versions,
you can resort to the PyGreSQL 5.x versions that still support them.
