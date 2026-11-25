# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'PyGreSQL'
author = 'The PyGreSQL Development Team'
copyright = '2026, ' + author

def project_version():
    with open('../pyproject.toml') as f:
        for d in f:
            if d.startswith("version ="):
                version = d.split("=")[1].strip().strip('"')
                return version
    raise Exception("Cannot determine PyGreSQL version")

version = release = project_version()

language = 'en'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx']

# link references to Python names to the Python documentation
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# List of pages which are included in other pages and therefore should
# not appear in the toctree.
exclude_patterns += [
    'download/download.rst', 'download/files.rst',
    'community/mailinglist.rst', 'community/source.rst',
    'community/issues.rst', 'community/support.rst',
    'community/homes.rst']

# ignore certain warnings
# (pseudo types and some names that are not documented as targets)
nitpicky = True
nitpick_ignore = [
    ('py:' + t, n) for t, names in {
        'attr': ('arraysize', 'error', 'sqlstate', 'DatabaseError.sqlstate'),
        'class': ('callable', 'callables', 'class', 'function', 'iterable',
                  'False', 'namedtuple', 'namedtuples',
                  'bytes/str', 'first field', 'path-like object',
                  'Notice', 'DATETIME'),
        'data': ('defbase', 'defhost', 'defopt', 'defpasswd', 'defport',
                 'defuser'),
        'exc': ('pg.DatabaseError', 'pg.InterfaceError', 'pg.InternalError',
                'pg.InvalidResultError',
                'pg.MultipleResultsError', 'pg.NoResultError',
                'pg.OperationalError', 'pg.ProgrammingError'),
        'meth': ('cur.execute',
                 'DB.close', 'DB.connection_handler', 'DB.get_regtypes',
                 'DB.inserttable', 'DB.reopen'),
    }.items() for n in names]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

html_title = f'PyGreSQL {version}'

# the canonical location of the documentation (latest final release),
# also used by the copies of the main branch at pygresql.github.io and
# pygresql.readthedocs.io
html_baseurl = 'https://pygresql.org/'

html_logo = '_static/pygresql.png'
html_favicon = '_static/favicon.ico'
