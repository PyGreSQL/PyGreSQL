#!/usr/bin/env python
#
# Test the PyGreSQL inserttable() function.
# You need a "test" database ("createdb test").
# Christoph Zwerschke, 2005-12-27
#

from pg import DB, ProgrammingError

print
print "PygreSQL inserttable() test."

# Verify inserttable() works with German locale as well:
import locale
try:
    locale.setlocale(locale.LC_ALL, 'de_DE')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')

db = DB('test')

try:
    db.query("drop table inserttable")
except ProgrammingError:
    pass

db.query("""create table inserttable(
    lnr int,
    b boolean,
    d date,
    t1 char(10),
    t2 varchar(10),
    t3 text,
    i1 smallint,
    i2 integer,
    i3 bigint,
    n numeric,
    f1 real,
    f2 double precision)
""")

table = [
[ 1, False, None,
    'Hello', 'World', 'Hello World!',
    1, 2, 3,
    4.5, 5.25, 6.75 ],
[ 2, True, '1910-06-22',
    'HelloWorld', 'HelloWorld',  'HW!',
    32767, 2147483647L, 9223372036854775807L,
    1234567890123456789L, 123.456, 123.4567891234 ],
[ 3, None, 'now()',
    'Hi\t\tWorld\n', 'Hi\t\tWorld\n',  'Hi\t\\\tWorld!\n',
    -1, -2, -3,
    -4.5, -5.25, -6.75 ],
]

print
print "Inserting..."
db.inserttable('inserttable', table)
print "Done!"

result = db.query(
    "select * from inserttable order by 1").getresult()

print
for row_t, row_r in zip(table, result):
    print "-" *46
    for col_t, col_r in zip(row_t, row_r):
        print "|%22r|%22r|" % (col_t, col_r)
print "-" *46
print
