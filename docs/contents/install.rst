Installation
============

General
-------

You must first install Python and PostgreSQL on your system.
If you want to access remote databases only, you don't need to install
the full PostgreSQL server, but only the libpq C-interface library.
On Windows, this library is called ``libpq.dll`` and is for instance contained
in the PostgreSQL ODBC driver (search for "psqlodbc"). On Linux, it is called
``libpq.so`` and usually provided in a package called "libpq" or "libpq5".
On Windows, you also need to make sure that the directory that contains
``libpq.dll`` is part of your ``PATH`` environment variable.

The current version of PyGreSQL has been tested with Python versions
3.10 to 3.15, and PostgreSQL versions 12 to 19.

PyGreSQL needs libpq version 12 or newer. Some features are only available
when PyGreSQL has been built with a newer libpq, e.g. encrypted cancel
requests need libpq 17 or newer. The libpq that is used at runtime must not
be older than the libpq that PyGreSQL has been built with.

PyGreSQL will be installed as two packages named ``pg`` (for the classic
interface) and ``pgdb`` (for the DB API 2 compliant interface). The former
also contains a shared library called ``_pg.so`` (on Linux) or a DLL called
``_pg.pyd`` (on Windows) and a stub file ``_pg.pyi`` for this library.


Installing with Pip
-------------------

This is the most easy way to install PyGreSQL if you have "pip" installed.
Just run the following command in your terminal::

  pip install PyGreSQL

This will automatically try to find and download a distribution on the
`Python Package Index <https://pypi.python.org/>`_ that matches your operating
system and Python version and install it.

Note that you still need to have the libpq interface installed on your system
(see the general remarks above).


Installing from a Binary Distribution
-------------------------------------

If you don't want to use "pip", or "pip" doesn't find an appropriate
distribution for your computer, you can also try to manually download
and install a distribution.

When you download the source distribution, you will need to compile the
C extension, for which you need a C compiler installed.
If you don't want to install a C compiler or avoid possible problems
with the compilation, you can search for a pre-compiled binary distribution
of PyGreSQL on the Python Package Index or the PyGreSQL homepage.

You can currently download PyGreSQL as Linux RPM, NetBSD package and Windows
installer. Make sure the required Python version of the binary package matches
the Python version you have installed.

Install the package as usual on your system.

Note that the documentation is currently only included in the source package.


Installing from Source
----------------------

If you want to install PyGreSQL from Source, or there is no binary
package available for your platform, follow these instructions.

Make sure the Python header files and PostgreSQL client and server header
files are installed. These come usually with the "devel" packages on Unix
systems and the installer executables on Windows systems.

If you are using a precompiled PostgreSQL, you will also need the pg_config
tool. This is usually also part of the "devel" package on Unix, and will be
installed as part of the database server feature on Windows systems.

Building and installing with pip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download and unpack the PyGreSQL source tarball if you haven't already done so.
Then run the following command in the directory containing the sources in
order to build and install PyGreSQL::

    pip install .

Now you should be ready to use PyGreSQL.

You can also build a wheel first if you want to install it on a different
system. The wheel will be created in the ``dist`` directory::

    pip install build
    python -m build --wheel

By default, PyGreSQL is compiled with support for all features available in the
installed PostgreSQL version, and you will get warnings for the features that
are not supported in this version. You can also explicitly enable or disable
certain features by passing options to the ``build_ext`` command. For instance,
in order to build PyGreSQL without support for the memory size functions, run::

    pip install . -C--build-option=build_ext -C--build-option=--no-memory-size

The same options can be passed to ``python -m build``. Note that pip may reuse
a wheel that it has built and cached before. Add the ``--no-cache-dir`` option
to make sure that the build options are applied.

You can find out all possible build options with::

    python setup.py build_ext --help

Alternatively, you can also use the corresponding C preprocessor macros like
``MEMORY_SIZE`` directly (see the next section).

Note that if you build PyGreSQL with support for newer features that are not
available in the PQLib installed on the runtime system, you may get an error
when importing PyGreSQL, since these features are missing in the shared library
which will prevent Python from loading it.

Compiling Manually
~~~~~~~~~~~~~~~~~~

The source files for the C extension module are in the ``ext`` directory.
If you cannot or do not want to use pip, you can also compile the C extension
manually. In the directory containing the PyGreSQL sources, run something
like the following command::

    cc -fpic -shared -o pg/_pg$EXT_SUFFIX -DPYGRESQL_VERSION=$VERSION \
        -I$PYINC -I$PGINC ext/*.c -L$PGLIB -lpq

where you have to set::

    EXT_SUFFIX = file name suffix for Python extension modules
                 (something like .cpython-313-x86_64-linux-gnu.so)
    VERSION = the version of PyGreSQL (something like 6.3.0)
    PYINC = path to the Python include files
    PGINC = path to the PostgreSQL client include files
    PGLIB = path to the PostgreSQL client library

You can get these values for your system like this::

    EXT_SUFFIX=`python -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"`
    PYINC=`python -c "import sysconfig; print(sysconfig.get_path('include'))"`
    PGINC=`pg_config --includedir`
    PGLIB=`pg_config --libdir`

Some options may be added to the command::

    -DMEMORY_SIZE = support memory size function

Now test the new module. Something like the following should work::

    $ python

    >>> import pg
    >>> db = pg.DB('testdb', 'localhost')
    >>> db.query("SELECT 'pong' AS ping").getresult()
    [('pong',)]

Finally, copy the ``pg`` and ``pgdb`` directories to a directory in your
``PYTHONPATH``, for instance to the ``site-packages`` directory of your
Python installation.
