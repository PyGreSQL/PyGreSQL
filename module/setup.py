#!/usr/bin/env python
# $Id: setup.py,v 1.18 2006-02-17 20:51:45 cito Exp $

"""Setup script for PyGreSQL version 3.8

Authors and history:
* PyGreSQL written 1997 by D'Arcy J.M. Cain <darcy@druid.net>
* based on code written 1995 by Pascal Andre <andre@chimay.via.ecp.fr>
* setup script created 2000/04 Mark Alexander <mwa@gate.net>
* tweaked 2000/05 Jeremy Hylton <jeremy@cnri.reston.va.us>
* win32 support 2001/01 by Gerhard Haering <gerhard@bigfoot.de>
* tweaked 2006/02 Christoph Zwerschke <cito@online.de>

Prerequisites to be installed:
* Python including devel package (header files and distutils)
* PostgreSQL libs and devel packages (header files of client and server)
* PostgreSQL pg_config tool (usually included in the devel package)
  (the Windows installer has it as part of the database server feature)

Tested with Python 2.4.2 and PostGreSQL 8.1.2.

Use as follows:
python setup.py build   # to build the module
python setup.py install # to install it

For Win32, you should have the Microsoft Visual C++ compiler and
the Microsoft .NET Framework SDK installed and on your search path.
If you want to use the free Microsoft Visual C++ Toolkit 2003 compiler,
you need to patch distutils (www.vrplumber.com/programming/mstoolkit/).
Alternatively, you can use MinGW (www.mingw.org) for building on Win32:
python setup.py build -c mingw32 install # use MinGW
You should edit the file "%MinGWpath%/lib/gcc/%MinGWversion%/specs"
and change the entry that reads -lmsvcrt to -lmsvcr71 if using MinGW.

See www.python.org/doc/current/inst/ for more information
on using distutils to install Python programs.
"""

from distutils.core import setup
from distutils.extension import Extension
import sys, os

def pg_config(s):
	"""Retrieve information about installed version of PostgreSQL."""
	f = os.popen("pg_config --%s" % s)
	d = f.readline().strip()
	if f.close() is not None:
		raise Exception, "pg_config tool is not available."
	if not d:
		raise Exception, "Could not get %s information." % s
	return d

def mk_include():
	"""Create a temporary local include directory.

	The directory will contain a copy of the PostgreSQL server header files,
	where all features which are not necessary for PyGreSQL are disabled.
	"""
	os.mkdir('include')
	for f in os.listdir(pg_include_dir_server):
		if not f.endswith('.h'):
			continue
		d = file(os.path.join(pg_include_dir_server, f)).read()
		if f == 'pg_config.h':
			d += '\n'
			d += '#undef ENABLE_NLS\n'
			d += '#undef USE_REPL_SNPRINTF\n'
			d += '#undef USE_SSL\n'
		file(os.path.join('include', f), 'w').write(d)

def rm_include():
	"""Remove the temporary local include directory."""
	if os.path.exists('include'):
		for f in os.listdir('include'):
			os.remove(os.path.join('include', f))
		os.rmdir('include')

pg_include_dir = pg_config('includedir')
pg_include_dir_server = pg_config('includedir-server')

rm_include()
mk_include()

include_dirs = ['include', pg_include_dir,  pg_include_dir_server]

pg_libdir = pg_config('libdir')
library_dirs = [pg_libdir]

libraries=['pq']

if sys.platform == "win32":
	include_dirs.append(os.path.join(pg_include_dir_server, 'port/win32'))

setup(
	name = "PyGreSQL",
	version = "3.8",
	description = "Python PostgreSQL Interfaces",
	author = "D'Arcy J. M. Cain",
	author_email = "darcy@PyGreSQL.org",
	url = "http://www.pygresql.org",
	license = "Python",
	py_modules = ['pg', 'pgdb'],
	ext_modules = [Extension(
		'_pg', ['pgmodule.c'],
		include_dirs = include_dirs,
		library_dirs = library_dirs,
		libraries = libraries,
		extra_compile_args = ['-O2'],
		)],
	)

rm_include()
