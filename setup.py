#!/usr/bin/python
#
# PyGreSQL - a Python interface for the PostgreSQL database.
#
# Copyright (c) 2020 by the PyGreSQL Development Team
#
# Please see the LICENSE.TXT file for specific restrictions.

"""Setup script for PyGreSQL version 5.1.2

PyGreSQL is an open-source Python module that interfaces to a
PostgreSQL database. It embeds the PostgreSQL query library to allow
easy use of the powerful PostgreSQL features from a Python script.

Authors and history:
* PyGreSQL written 1997 by D'Arcy J.M. Cain <darcy@druid.net>
* based on code written 1995 by Pascal Andre <andre@chimay.via.ecp.fr>
* setup script created 2000 by Mark Alexander <mwa@gate.net>
* improved 2000 by Jeremy Hylton <jeremy@cnri.reston.va.us>
* improved 2001 by Gerhard Haering <gerhard@bigfoot.de>
* improved 2006 to 2018 by Christoph Zwerschke <cito@online.de>

Prerequisites to be installed:
* Python including devel package (header files and distutils)
* PostgreSQL libs and devel packages (header file of the libpq client)
* PostgreSQL pg_config tool (usually included in the devel package)
  (the Windows installer has it as part of the database server feature)

PyGreSQL currently supports Python versions 2.6, 2.7 and 3.3 to 3.8,
and PostgreSQL versions 9.0 to 9.6 and 10 to 12.

Use as follows:
python setup.py build_ext # to build the module
python setup.py install   # to install it

See docs.python.org/doc/install/ for more information on
using distutils to install Python programs.

"""

import os
import platform
import re
import sys
import warnings
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext
from distutils.ccompiler import get_default_compiler
from distutils.sysconfig import get_python_inc, get_python_lib

version = '5.1.2'

if (not (2, 6) <= sys.version_info[:2] < (3, 0)
        and not (3, 3) <= sys.version_info[:2] < (4, 0)):
    raise Exception(
        "Sorry, PyGreSQL %s does not support this Python version" % version)

# For historical reasons, PyGreSQL does not install itself as a single
# "pygresql" package, but as two top-level modules "pg", providing the
# classic interface, and "pgdb" for the modern DB-API 2.0 interface.
# These two top-level Python modules share the same C extension "_pg".

py_modules = ['pg', 'pgdb']
c_sources = ['pgmodule.c']


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
    match = re.search(r'(\d+)\.(\d+)', pg_config('version'))
    if match:
        return tuple(map(int, match.groups()))
    return 9, 0


pg_version = pg_version()
libraries = ['pq']
# Make sure that the Python header files are searched before
# those of PostgreSQL, because PostgreSQL can have its own Python.h
include_dirs = [get_python_inc(), pg_config('includedir')]
library_dirs = [get_python_lib(), pg_config('libdir')]
define_macros = [('PYGRESQL_VERSION', version)]
undef_macros = []
extra_compile_args = ['-O2', '-funsigned-char', '-Wall', '-Wconversion']


class build_pg_ext(build_ext):
    """Customized build_ext command for PyGreSQL."""

    description = "build the PyGreSQL C extension"

    user_options = build_ext.user_options + [
        ('strict', None, "count all compiler warnings as errors"),
        ('direct-access', None, "enable direct access functions"),
        ('no-direct-access', None, "disable direct access functions"),
        ('direct-access', None, "enable direct access functions"),
        ('no-direct-access', None, "disable direct access functions"),
        ('large-objects', None, "enable large object support"),
        ('no-large-objects', None, "disable large object support"),
        ('default-vars', None, "enable default variables use"),
        ('no-default-vars', None, "disable default variables use"),
        ('escaping-funcs', None, "enable string escaping functions"),
        ('no-escaping-funcs', None, "disable string escaping functions"),
        ('ssl-info', None, "use new ssl info functions"),
        ('no-ssl-info', None, "do not use new ssl info functions")]

    boolean_options = build_ext.boolean_options + [
        'strict', 'direct-access', 'large-objects', 'default-vars',
        'escaping-funcs', 'ssl-info']

    negative_opt = {
        'no-direct-access': 'direct-access',
        'no-large-objects': 'large-objects',
        'no-default-vars': 'default-vars',
        'no-escaping-funcs': 'escaping-funcs',
        'no-ssl-info': 'ssl-info'}

    def get_compiler(self):
        """Return the C compiler used for building the extension."""
        return self.compiler or get_default_compiler()

    def initialize_options(self):
        build_ext.initialize_options(self)
        self.strict = False
        self.direct_access = None
        self.large_objects = None
        self.default_vars = None
        self.escaping_funcs = None
        self.ssl_info = None
        if pg_version < (9, 0):
            warnings.warn(
                "PyGreSQL does not support the installed PostgreSQL version.")

    def finalize_options(self):
        """Set final values for all build_pg options."""
        build_ext.finalize_options(self)
        if self.strict:
            extra_compile_args.append('-Werror')
        if self.direct_access is None or self.direct_access:
            define_macros.append(('DIRECT_ACCESS', None))
        if self.large_objects is None or self.large_objects:
            define_macros.append(('LARGE_OBJECTS', None))
        if self.default_vars is None or self.default_vars:
            define_macros.append(('DEFAULT_VARS', None))
        if self.escaping_funcs is None or self.escaping_funcs:
            if pg_version >= (9, 0):
                define_macros.append(('ESCAPING_FUNCS', None))
            else:
                (warnings.warn if self.escaping_funcs is None else sys.exit)(
                    "The installed PostgreSQL version"
                    " does not support the newer string escaping functions.")
        if self.ssl_info is None or self.ssl_info:
            if pg_version >= (9, 5):
                define_macros.append(('SSL_INFO', None))
            else:
                (warnings.warn if self.ssl_info is None else sys.exit)(
                    "The installed PostgreSQL version"
                    " does not support ssl info functions.")
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
                extra_compile_args[1:] = [
                    '-J', '-W3', '-WX',
                    '-Dinline=__inline']  # needed for MSVC 9


setup(
    name="PyGreSQL",
    version=version,
    description="Python PostgreSQL Interfaces",
    long_description=__doc__.split('\n\n', 2)[1],  # first passage
    long_description_content_type = 'text/plain',
    keywords="pygresql postgresql database api dbapi",
    author="D'Arcy J. M. Cain",
    author_email="darcy@PyGreSQL.org",
    url="http://www.pygresql.org",
    download_url="http://www.pygresql.org/download/",
    platforms=["any"],
    license="PostgreSQL",
    py_modules=py_modules,
    ext_modules=[Extension(
        '_pg', c_sources,
        include_dirs=include_dirs, library_dirs=library_dirs,
        define_macros=define_macros, undef_macros=undef_macros,
        libraries=libraries, extra_compile_args=extra_compile_args)],
    zip_safe=False,
    cmdclass=dict(build_ext=build_pg_ext),
    test_suite='tests.discover',
    classifiers=[
        "Development Status :: 6 - Mature",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: PostgreSQL License",
        "Operating System :: OS Independent",
        "Programming Language :: C",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"]
)
