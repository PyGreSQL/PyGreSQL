# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'PyGreSQL'
author = 'The PyGreSQL team'
copyright = '2024, ' + author

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

extensions = ['sphinx.ext.autodoc']

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
# (references to some of the Python names do not resolve correctly)
nitpicky = True
nitpick_ignore = [
    ('py:' + t, n) for t, names in {
        'attr': ('arraysize', 'error', 'sqlstate', 'DatabaseError.sqlstate'),
        'class': ('bool', 'bytes', 'callable', 'callables', 'class',
                  'dict', 'float', 'function', 'int', 'iterable',
                  'list', 'object', 'set', 'str', 'tuple',
                  'False', 'True', 'None',
                  'namedtuple', 'namedtuples',
                  'decimal.Decimal',
                  'bytes/str', 'list of namedtuples', 'tuple of callables',
                  'first field', 'type of first field',
                  'Notice', 'DATETIME'),
        'data': ('defbase', 'defhost', 'defopt', 'defpasswd', 'defport',
                 'defuser'),
        'exc': ('Exception', 'IndexError', 'IOError', 'KeyError',
                'MemoryError', 'SyntaxError', 'TypeError', 'ValueError',
                'pg.InternalError', 'pg.InvalidResultError',
                'pg.MultipleResultsError', 'pg.NoResultError',
                'pg.OperationalError', 'pg.ProgrammingError'),
        'func': ('len', 'json.dumps', 'json.loads'),
        'meth': ('datetime.strptime',
                 'cur.execute',
                 'DB.close', 'DB.connection_handler', 'DB.get_regtypes',
                 'DB.inserttable', 'DB.reopen'),
        'obj': ('False', 'True', 'None')
    }.items() for n in names]



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

html_title = f'PyGreSQL {version}'

html_logo = '_static/pygresql.png'
html_favicon = '_static/favicon.ico'
