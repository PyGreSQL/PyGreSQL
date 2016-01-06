Introduction
============

You may either choose to use the "classic" PyGreSQL interface provided by
the :mod:`pg` module or else the newer DB-API 2.0 compliant interface
provided by the :mod:`pgdb` module.

The following part of the documentation covers only the older :mod:`pg` API.

The :mod:`pg` module handles three types of objects,

- the :class:`pgobject`, which handles the connection
  and all the requests to the database,
- the :class:`pglarge` object, which handles
  all the accesses to PostgreSQL large objects,
- the :class:`pgqueryobject` that handles query results

and it provides a convenient wrapper class :class:`DB`
for the :class:`pgobject`.

.. seealso::

    If you want to see a simple example of the use of some of these functions,
    see the :doc:`../examples` page.
