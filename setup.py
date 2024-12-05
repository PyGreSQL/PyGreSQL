#!/usr/bin/python

"""Driver script for building PyGreSQL using setuptools.

You can build the PyGreSQL distribution like this:

    pip install build
    python -m build -C strict -C memory-size
"""

import os
import platform
import re
import sys
import warnings
from distutils.ccompiler import get_default_compiler
from distutils.sysconfig import get_python_inc, get_python_lib

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


def project_version():
    """Read the PyGreSQL version from the pyproject.toml file."""
    with open('pyproject.toml') as f:
        for d in f:
            if d.startswith("version ="):
                version = d.split("=")[1].strip().strip('"')
                return version
    raise Exception("Cannot determine PyGreSQL version")


def project_readme():
    """Get the content of the README file."""
    with open('README.rst') as f:
        return f.read()


version = project_version()

if not (3, 7) <= sys.version_info[:2] < (4, 0):
    raise Exception(
        f"Sorry, PyGreSQL {version} does not support this Python version")

long_description = project_readme()


# For historical reasons, PyGreSQL does not install itself as a single
# "pygresql" package, but as two top-level modules "pg", providing the
# classic interface, and "pgdb" for the modern DB-API 2.0 interface.
# These two top-level Python modules share the same C extension "_pg".

def pg_config(s):
    """Retrieve information about installed version of PostgreSQL."""
    f = os.popen(f'pg_config --{s}')  # noqa: S605
    d = f.readline().strip()
    if f.close() is not None:
        raise Exception("pg_config tool is not available.")
    if not d:
        raise Exception(f"Could not get {s} information.")
    return d


def pg_version():
    """Return the PostgreSQL version as a tuple of integers."""
    match = re.search(r'(\d+)\.(\d+)', pg_config('version'))
    if match:
        return tuple(map(int, match.groups()))
    return 10, 0


pg_version = pg_version()
libraries = ['pq']
# Make sure that the Python header files are searched before
# those of PostgreSQL, because PostgreSQL can have its own Python.h
include_dirs = [get_python_inc(), pg_config('includedir')]
library_dirs = [get_python_lib(), pg_config('libdir')]
define_macros = [('PYGRESQL_VERSION', version)]
undef_macros = []
extra_compile_args = ['-O2', '-funsigned-char', '-Wall', '-Wconversion']


class build_pg_ext(build_ext):  # noqa: N801
    """Customized build_ext command for PyGreSQL."""

    description = "build the PyGreSQL C extension"

    user_options = [*build_ext.user_options,  # noqa: RUF012
        ('strict', None, "count all compiler warnings as errors"),
        ('memory-size', None, "enable memory size function"),
        ('no-memory-size', None, "disable memory size function")]

    boolean_options = [*build_ext.boolean_options,  # noqa: RUF012
        'strict', 'memory-size']

    negative_opt = {  # noqa: RUF012
        'no-memory-size': 'memory-size'}

    def get_compiler(self):
        """Return the C compiler used for building the extension."""
        return self.compiler or get_default_compiler()

    def initialize_options(self):
        """Initialize the supported options with default values."""
        build_ext.initialize_options(self)
        self.strict = False
        self.memory_size = None
        supported = pg_version >= (10, 0)
        if not supported:
            warnings.warn(
                "PyGreSQL does not support the installed PostgreSQL version.",
                stacklevel=2)

    def finalize_options(self):
        """Set final values for all build_pg options."""
        build_ext.finalize_options(self)
        if self.strict:
            extra_compile_args.append('-Werror')
        wanted = self.memory_size
        supported = pg_version >= (12, 0)
        if (wanted is None and supported) or wanted:
            define_macros.append(('MEMORY_SIZE', None))
            if not supported:
                warnings.warn(
                    "The installed PostgreSQL version"
                    " does not support the memory size function.",
                    stacklevel=2)
        if sys.platform == 'win32':
            libraries[0] = 'lib' + libraries[0]
            if os.path.exists(os.path.join(
                    library_dirs[1], libraries[0] + 'dll.lib')):
                libraries[0] += 'dll'
            compiler = self.get_compiler()
            if compiler == 'mingw32':  # MinGW
                if platform.architecture()[0] == '64bit':  # needs MinGW-w64
                    define_macros.append(('MS_WIN64', None))
            elif compiler == 'msvc':  # Microsoft Visual C++
                extra_compile_args[1:] = [
                    '-J', '-W3', '-WX', '-wd4391',
                    '-Dinline=__inline']  # needed for MSVC 9


setup(
    name='PyGreSQL',
    version=version,
    description='Python PostgreSQL Interfaces',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    keywords='pygresql postgresql database api dbapi',
    author="D'Arcy J. M. Cain",
    author_email="darcy@PyGreSQL.org",
    url='https://pygresql.github.io/',
    download_url='https://pygresql.github.io/download/',
    project_urls={
        'Documentation': 'https://pygresql.github.io/contents/',
        'Issue Tracker': 'https://github.com/PyGreSQL/PyGreSQL/issues/',
        'Mailing List': 'https://mail.vex.net/mailman/listinfo/pygresql',
        'Source Code': 'https://github.com/PyGreSQL/PyGreSQL'},
    classifiers=[
        'Development Status :: 6 - Mature',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: PostgreSQL License',
        'Operating System :: OS Independent',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    license='PostgreSQL',
    test_suite='tests.discover',
    zip_safe=False,
    packages=["pg", "pgdb"],
    package_data={"pg": ["py.typed"], "pgdb": ["py.typed"]},
    ext_modules=[Extension(
        'pg._pg', ["ext/pgmodule.c"],
        include_dirs=include_dirs, library_dirs=library_dirs,
        define_macros=define_macros, undef_macros=undef_macros,
        libraries=libraries, extra_compile_args=extra_compile_args)],
    cmdclass=dict(build_ext=build_pg_ext),
)
