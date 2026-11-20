#!/usr/bin/python

"""Driver script for building PyGreSQL using setuptools.

You can build the PyGreSQL distribution like this:

    pip install build
    python -m build

For development, you can build the C extension in place like this:

    python setup.py build_ext --inplace --strict --pedantic

The custom build_ext options are --strict (treat compiler warnings as
errors), --pedantic (enable extra compiler warnings) and --memory-size
or --no-memory-size (enabled by default if supported). Note that build
frontends do not pass these options through as simple config settings.
You need to pass them as build options instead, like this:

    python -m build -C--build-option=build_ext -C--build-option=--strict
"""

import os
import platform
import re
import sys
import sysconfig
import warnings

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

min_py_version = 3, 10  # supported: Python >= 3.10
max_py_version = 4, 0  # and < 4.0
min_pg_version = 12, 0  # supported: PostgreSQL >= 12.0


def project_version():
    """Read the PyGreSQL version from the pyproject.toml file."""
    with open('pyproject.toml') as f:
        for d in f:
            if d.startswith("version ="):
                version = d.split("=")[1].strip().strip('"')
                return version
    raise Exception("Cannot determine PyGreSQL version")


version = project_version()

if not min_py_version <= sys.version_info[:2] < max_py_version:
    raise Exception(
        f"Sorry, PyGreSQL {version} does not support this Python version")


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
    return min_pg_version


pg_version = pg_version()
libraries = ['pq']
# Make sure that the Python header files are searched before
# those of PostgreSQL, because PostgreSQL can have its own Python.h
include_dirs = [sysconfig.get_path("include"), pg_config('includedir')]
library_dirs = [sysconfig.get_path("purelib"), pg_config('libdir')]
define_macros = [('PYGRESQL_VERSION', version)]
undef_macros = []
extra_compile_args = ['-O2', '-funsigned-char', '-Wall', '-Wconversion']


class BuildPgExt(build_ext):
    """Customized build_ext command for PyGreSQL."""

    description = "build the PyGreSQL C extension"

    user_options = [*build_ext.user_options,  # noqa: RUF012
        ('strict', None, "count all compiler warnings as errors"),
        ('pedantic', None, "enable extra compiler warnings"),
        ('memory-size', None, "enable memory size function"),
        ('no-memory-size', None, "disable memory size function")]

    boolean_options = [*build_ext.boolean_options,  # noqa: RUF012
        'strict', 'pedantic', 'memory-size']

    negative_opt = {  # noqa: RUF012
        'no-memory-size': 'memory-size'}

    def initialize_options(self):
        """Initialize the supported options with default values."""
        super().initialize_options()
        self.strict = False
        self.pedantic = False
        self.memory_size = None
        supported = pg_version >= min_pg_version
        if not supported:
            warnings.warn(
                "PyGreSQL does not support the installed PostgreSQL version.",
                stacklevel=2)

    def finalize_options(self):
        """Set values for all build_pg options.

        Some values are set in build_extensions() since they depend
        on the compiler version which is not yet known at this point.
        """
        super().finalize_options()
        if self.pedantic:
            extra_compile_args.extend(['-Wextra', '-pedantic'])
        if self.strict:
            extra_compile_args.append('-Werror')
        wanted = self.memory_size
        supported = pg_version >= min_pg_version
        if (wanted is None and supported) or wanted:
            define_macros.append(('MEMORY_SIZE', None))
            if not supported:
                warnings.warn(
                    "The installed PostgreSQL version"
                    " does not support the memory size function.",
                    stacklevel=2)

    def build_extensions(self):
        """Build the PyGreSQL C extension."""
        # Adjust settings for Windows platforms
        if sys.platform == 'win32':
            libraries[0] = 'lib' + libraries[0]
            if os.path.exists(os.path.join(
                    library_dirs[1], libraries[0] + 'dll.lib')):
                libraries[0] += 'dll'
            compiler_type = self.compiler.compiler_type
            if compiler_type == 'mingw32':  # MinGW
                if platform.architecture()[0] == '64bit':  # needs MinGW-w64
                    define_macros.append(('MS_WIN64', None))
            elif compiler_type == 'msvc':  # Microsoft Visual C++
                extra_compile_args[1:] = ['-J', '-W3', '-WX', '-wd4391']
        super().build_extensions()


# All project metadata is defined in pyproject.toml.
# We only need to specify the C extension module and how to build it here.

setup(
    ext_modules=[Extension(
        'pg._pg', [
            "ext/pgmodule.c",
            "ext/pginternal.c",
            "ext/pgconn.c",
            "ext/pgquery.c",
            "ext/pgsource.c",
            "ext/pgnotice.c",
            "ext/pglarge.c",
        ],
        include_dirs=include_dirs, library_dirs=library_dirs,
        define_macros=define_macros, undef_macros=undef_macros,
        libraries=libraries, extra_compile_args=extra_compile_args)],
    cmdclass=dict(build_ext=BuildPgExt),
)
