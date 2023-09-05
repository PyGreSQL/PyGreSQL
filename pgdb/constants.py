"""The DB API 2 module constants."""

# compliant with DB API 2.0
apilevel = '2.0'

# module may be shared, but not connections
threadsafety = 1

# this module use extended python format codes
paramstyle = 'pyformat'

# shortcut methods have been excluded from DB API 2 and
# are not recommended by the DB SIG, but they can be handy
shortcutmethods = 1
