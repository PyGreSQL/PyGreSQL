#!/usr/bin/env python
# $Id$

"""Setup script for PyGreSQL version 4.1

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

The supported versions are Python 2.5-2.7 and PostgreSQL 8.3-9.2.

Use as follows:
python setup.py build   # to build the module
python setup.py install # to install it

You can use MinGW or MinGW-w64 for building on Windows:
python setup.py build -c mingw32 install

See docs.python.org/doc/install/ for more information on
using distutils to install Python programs.

"""

version = '4.1'


import sys

if not (2, 3) <= sys.version_info[:2] < (3, 0):
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


def pg_config(s):
    """Retrieve information about installed version of PostgreSQL."""
    f = os.popen('pg_config --%s' % s)
    d = f.readline().strip()
    if f.close() is not None:
        raise Exception("pg_config tool is not available.")
    if not d:
        raise Exception("Could not get %s information." % s)
    return d


py_modules = ['pg', 'pgdb']
libraries = ['pq']
include_dirs = [pg_config('includedir')]
library_dirs = [pg_config('libdir')]
define_macros = [('PYGRESQL_VERSION', version)]
undef_macros = []
extra_compile_args = ['-O2']


class build_pg_ext(build_ext):
    """Customized build_ext command for PyGreSQL."""

    description = "build the PyGreSQL C extension"

    user_options = build_ext.user_options + [
        ('direct-access', None,
            "enable direct access functions"),
        ('large-objects', None,
            "enable large object support"),
        ('default-vars', None,
            'enable default variables use')]

    boolean_options = build_ext.boolean_options + [
        'direct-access', 'large-objects', 'default-vars']

    def get_compiler(self):
        """Return the C compiler used for building the extension."""
        return self.compiler or get_default_compiler()

    def initialize_options(self):
        build_ext.initialize_options(self)
        self.direct_access = 1
        self.large_objects = 1
        self.default_vars = 1

    def finalize_options(self):
        """Set final values for all build_pg options."""
        build_ext.finalize_options(self)
        if self.direct_access:
            define_macros.append(('DIRECT_ACCESS', None))
        if self.large_objects:
            define_macros.append(('LARGE_OBJECTS', None))
        if self.default_vars:
            define_macros.append(('DEFAULT_VARS', None))
        if sys.platform == 'win32':
            bits = platform.architecture()[0]
            if bits == '64bit': # we need to find libpq64
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
                        library_dirs.insert(0, library_dir)
                    if include_dir not in include_dirs:
                        include_dirs.insert(0, include_dir)
                    libraries[0] += 'dll' # libpqdll instead of libpq
                    break
            compiler = self.get_compiler()
            if compiler == 'mingw32': # MinGW
                if bits == '64bit': # needs MinGW-w64
                    define_macros.append(('MS_WIN64', None))
            elif compiler == 'msvc': # Microsoft Visual C++
                libraries[0] = 'lib' + libraries[0]


setup(
    name="PyGreSQL",
    version=version,
    description="Python PostgreSQL Interfaces",
    long_description=__doc__.split('\n\n', 2)[1], # first passage
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
    cmdclass=dict(build_ext=build_pg_ext),
    classifiers=[
        "Development Status :: 6 - Mature",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Python Software Foundation License",
        "Operating System :: OS Independent",
        "Programming Language :: C",
        "Programming Language :: Python",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"]
)
