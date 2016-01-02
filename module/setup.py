#! /usr/bin/python
# $Id$

"""Setup script for PyGreSQL version 4.2

PyGreSQL is an open-source Python module that interfaces to a
PostgreSQL database. It embeds the PostgreSQL query library to allow
easy use of the powerful PostgreSQL features from a Python script.

Authors and history:
* PyGreSQL written 1997 by D'Arcy J.M. Cain <darcy@druid.net>
* based on code written 1995 by Pascal Andre <andre@chimay.via.ecp.fr>
* setup script created 2000/04 Mark Alexander <mwa@gate.net>
* tweaked 2000/05 Jeremy Hylton <jeremy@cnri.reston.va.us>
* win32 support 2001/01 by Gerhard Haering <gerhard@bigfoot.de>
* tweaked 2006/02-2010/02 by Christoph Zwerschke <cito@online.de>

Prerequisites to be installed:
* Python including devel package (header files and distutils)
* PostgreSQL libs and devel packages (header file of the libpq client)
* PostgreSQL pg_config tool (usually included in the devel package)
  (the Windows installer has it as part of the database server feature)

The supported versions are Python 2.4-2.7 and PostgreSQL 8.3-9.4.

Use as follows:
python setup.py build   # to build the module
python setup.py install # to install it

You can use MinGW or MinGW-w64 for building on Windows:
python setup.py build -c mingw32 install

See docs.python.org/doc/install/ for more information on
using distutils to install Python programs.

"""

version = '4.2'


import sys

if not (2, 4) <= sys.version_info[:2] <= (2, 7):
    raise Exception("Sorry, PyGreSQL %s"
        " does not support this Python version" % version)


import os
import platform
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext
from distutils.ccompiler import get_default_compiler
from distutils.sysconfig import get_python_inc, get_python_lib


def pg_config(s):
    """Retrieve information about installed version of PostgreSQL."""
    f = os.popen('pg_config --%s' % s)
    d = f.readline().strip()
    if f.close() is not None:
        raise Exception("pg_config tool is not available.")
    if not d:
        raise Exception("Could not get %s information." % s)
    return d


def pg_version():
    """Return the PostgreSQL version as a tuple of integers."""
    parts = []
    for part in pg_config('version').split()[-1].split('.'):
        if part.isdigit():
            part = int(part)
        parts.append(part)
    return tuple(parts or [8])


pg_version = pg_version()
py_modules = ['pg', 'pgdb']
libraries = ['pq']
# Make sure that the Python header files are searched before
# those of PostgreSQL, because PostgreSQL can have its own Python.h
include_dirs = [get_python_inc(), pg_config('includedir')]
library_dirs = [get_python_lib(), pg_config('libdir')]
define_macros = [('PYGRESQL_VERSION', version)]
undef_macros = []
extra_compile_args = ['-O2', '-Wall', '-Werror', '-funsigned-char']


class build_pg_ext(build_ext):
    """Customized build_ext command for PyGreSQL."""

    description = "build the PyGreSQL C extension"

    user_options = build_ext.user_options + [
        ('direct-access', None,
            "enable direct access functions"),
        ('large-objects', None,
            "enable large object support"),
        ('default-vars', None,
            "enable default variables use"),
        ('escaping-funcs', None,
            "enable string escaping functions")]

    boolean_options = build_ext.boolean_options + [
        'direct-access', 'large-objects', 'default-vars', 'escaping-funcs']

    def get_compiler(self):
        """Return the C compiler used for building the extension."""
        return self.compiler or get_default_compiler()

    def initialize_options(self):
        build_ext.initialize_options(self)
        self.direct_access = True
        self.large_objects = True
        self.default_vars = True
        self.escaping_funcs = pg_version[0] >= 9

    def finalize_options(self):
        """Set final values for all build_pg options."""
        build_ext.finalize_options(self)
        if self.direct_access:
            define_macros.append(('DIRECT_ACCESS', None))
        if self.large_objects:
            define_macros.append(('LARGE_OBJECTS', None))
        if self.default_vars:
            define_macros.append(('DEFAULT_VARS', None))
        if self.escaping_funcs and pg_version[0] >= 9:
            define_macros.append(('ESCAPING_FUNCS', None))
        if sys.platform == 'win32':
            bits = platform.architecture()[0]
            if bits == '64bit':  # we need to find libpq64
                for path in os.environ['PATH'].split(os.pathsep) + [
                        r'C:\Program Files\PostgreSQL\libpq64']:
                    library_dir = os.path.join(path, 'lib')
                    if not os.path.isdir(library_dir):
                        continue
                    lib = os.path.join(library_dir, 'libpqdll.')
                    if not (os.path.exists(lib + 'lib')
                            or os.path.exists(lib + 'a')):
                        continue
                    include_dir = os.path.join(path, 'include')
                    if not os.path.isdir(include_dir):
                        continue
                    if library_dir not in library_dirs:
                        library_dirs.insert(1, library_dir)
                    if include_dir not in include_dirs:
                        include_dirs.insert(1, include_dir)
                    libraries[0] += 'dll'  # libpqdll instead of libpq
                    break
            compiler = self.get_compiler()
            if compiler == 'mingw32':  # MinGW
                if bits == '64bit':  # needs MinGW-w64
                    define_macros.append(('MS_WIN64', None))
            elif compiler == 'msvc':  # Microsoft Visual C++
                libraries[0] = 'lib' + libraries[0]


setup(
    name="PyGreSQL",
    version=version,
    description="Python PostgreSQL Interfaces",
    long_description=__doc__.split('\n\n', 2)[1],  # first passage
    keywords="pygresql postgresql database api dbapi",
    author="D'Arcy J. M. Cain",
    author_email="darcy@PyGreSQL.org",
    url="http://www.pygresql.org",
    download_url="ftp://ftp.pygresql.org/pub/distrib/",
    platforms=["any"],
    license="Python",
    py_modules=py_modules,
    ext_modules=[Extension('_pg', ['pgmodule.c'],
        include_dirs=include_dirs, library_dirs=library_dirs,
        define_macros=define_macros, undef_macros=undef_macros,
        libraries=libraries, extra_compile_args=extra_compile_args)],
    zip_safe=False,
    cmdclass=dict(build_ext=build_pg_ext),
    test_suite='tests.discover',
    classifiers=[
        "Development Status :: 6 - Mature",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Python Software Foundation License",
        "Operating System :: OS Independent",
        "Programming Language :: C",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.4',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"]
)
