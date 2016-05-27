Using :py:mod:`Goblin<goblin>`
==============================

:py:mod:`Goblin<goblin>` aims to provide an easy to use, intuitive API, without
sacrificing the flexibility enabled by extensive user configuration. It is designed
to 'just work' out of the box with most versions of Python (2.7+) using the
:py:class:`tornado.ioloop.IOLoop` and :py:class:`tornado.concurrent.Future` class.
But as the Python community moves forward with projects like :py:mod:`asyncio`,
:py:mod:`trollius`, :py:mod:`aiohttp` (based on :py:mod:`asyncio`), and `curio`_,
developers want to be able to choose the async API, event loop and future
implementation that works with their stack. To enable this, :py:mod:`Goblin<goblin>`
provides a pluggable client implementation that allows the user to choose the client,
event loop, and future classes, provided that certain compatibility and API requirements
are kept in mind.


This document aims to present an overview of the core functionality of :py:mod:`Goblin<goblin>`
in the simplest manner possible. In future versions, each of these sections will be
expanded into a guide covering the complete functionality provided by :py:mod:`Goblin<goblin>`.
For a full reference, please see the :ref:`API docs<api-docs>`.

Setting up :py:mod:`Goblin<goblin>`
-----------------------------------

In order to talk to the `Gremlin Server`_, :py:mod:`Goblin<goblin>` needs a
:py:class:`Future`, which can be any future implementation with a compatible API,
and a :py:class:`Pool`, which is typically inherits from
:py:class:`gremlinclient.Pool<gremlinclient.pool.Pool>`. For a simple
application, you can use :py:func:`goblin.connection.setup`, which allows
you to define a :py:class:`Future` and :py:class:`Pool` as :py:mod:`goblin.connection`
constants that will be used throughout :py:mod:`Goblin<goblin>`::

    >>> import asyncio
    >>> from gremlinclient import aiohttp_client
    >>> from goblin import connection

    >>> connection.setup(
    ...     pool_class=aiohttp_client.Pool, future_class=asyncio.Future)


The function :py:func:`goblin.connection.setup` provides a wide variety of configuration
options, please refer to the :ref:`API docs<goblin.connection.setup>` for a
complete description.

For more involved applications, it is often desirable to manage connection pool
and futures explicitly. :py:mod:`Goblin<goblin>` allows these parameters to be
passed as keyword arguments to any caller of :py:func:`goblin.connection.execute_query`.

After using :py:func:`goblin.connection.setup`, it is important to call
:py:func:`goblin.connection.tear_down` to clean up any remaining connections.
Depending on the connection pool implementation, this method may or may not return a
:py:class:`Future`. Using :py:class:`gremlinclient.aiohttp_client.Pool<gremlinclient.aiohttp_client.client.Pool>`::

    >>> yield from connection.tear_down()

All of the following examples assume a
:py:class:`gremlinclient.aiohttp_client.Pool<gremlinclient.aiohttp_client.client.Pool>` and
:py:class:`asyncio.Future`


Creating :py:mod:`models`
----------------------------------------
The core functionality of :py:mod:`Goblin<goblin>` lies in the
:py:mod:`models` module, which allows you to define Python
classes (:py:class:`vertices<goblin.models.vertex.Vertex>`,
:py:class:`edges<goblin.models.edge.Edge>`, and
:py:mod:`properties<goblin.properties.properties>`)
that are mapped to graph elements.

using the Gremlin query language::

    >>> from goblin import models
    >>> from goblin import properties

    >>> class User(models.Vertex):
    ...     name = properties.String()
    ...     email = properties.Email()
    ...     url = properties.Url()

    >>> class Follows(models.Edge):
            pass  # Edge can have properties just like Vertex


We can then use the method :py:meth:`create<goblin.models.element.create>` to
create nodes and edges::

    >>> joe = yield from User.create(name='joe', email='joe@joe.com',
    ...                              url='http://joe.com')
    >>> bob = yield from User.create(name='bob', email='bob@bob.com',
    ...                              url='http://bob.com')
    >>> joe_follows_bob = yield from Follows.create(joe, bob)

This creates two vertices with the label "user" and one edge with the label "follows"
in the graphdb.

Elements can be retrieved from the graphdb using class methods provided by the
element implementations. These methods include :py:meth:`get<goblin.models.element.get>`
which allows you to retrieve an element by id, and
:py:meth:`all<goblin.models.element.all>`, which retrieves all elements with a label
corresponding to the model class from the database::

    >>> joe = yield from User.get(joe.id)
    >>> stream = yield from User.all()
    >>> while True:
    ...     resp = yield from stream.read()
    ...     if resp is None:
    ...         break
    ...     print(resp)

Instances of graph elements (Vertices and Edges) provide methods that
allow you to delete and update properties.

    >>> josep = yield from joe.save(name='Josep')
    >>> yield from josep.delete()

Graph element instances also provide an API that allows you to access and modify
neighbor elements, but this API is under review and may be deprecated in favor of
the vertex centric query API and the proposed edge centric query API.


Using the :py:class:`Relationship<goblin.relationships.relationship.Relationship>` class
--------------------------------------------------------------------------------

In an effort to provide a more convenient API, :py:mod:`Goblin<goblin>` provides
the :py:class:`Relationship<goblin.relationships.relationship.Relationship>` class, which allows
you to explicitly define relationships between vertex classes::

    >>> from goblin import models
    >>> from goblin import properties
    >>> from goblin import relationships

    >>> class WorksIn(models.Edge):
            pass

    >>> class Department(models.Vertex):
    ...     name = properties.String()

    >>> class Employee(models.Vertex)
    ...     name = properties.String()
    ...     email = properties.Email()
    ...     department = relationships.Relationship(WorksIn, Department)


You can then use the :py:obj:`department` relationship to easily create edges
of type `WorksIn` and vertices of type `Department`::

    >>> joe = Employee.create(name='joe', email="joe@joe.com")
    >>> joe_works_in, r_and_d = yield from joe.department.create(
    ...     vertex_params={'name': 'R&D'})

The :py:class:`Relationship<goblin.relationships.relationship.Relationship>` class
provides several other methods for convenience as well. For a full reference,
please see the :ref:`API docs<goblin.relationships.relationship.Relationship>`

The :py:class:`V<goblin.models.query.V>` ertex centric query API
---------------------------------------------------------------

To emulate Gremlin style traversals, :py:mod:`Goblin<goblin>` provides the class
:py:class:`V<goblin.models.query.V>`, which provides an interface for step based
graph traversals using method chaining. Unlike Gremlin, the
class :py:class:`V<goblin.models.query.V>` requires that the user pass a vertex or
vertex id as a starting point for the traversal. For example::

    >>> from goblin.models import V
    >>> dep = yield from V(joe).out_step().get()
    >>> r_and_d = yield from V(joe).\
    ...     out_step().\
    ...     has(Department.get_property_by_name('name'), 'R&D').\
    ...     get()

There are three things to note in the above example:

1. The Gremlin steps `in` and `out` have been renamed as `in_step` and `out_step`
   due to the fact that `in` is a reserved word in Python
2. All traversal must end with :py:meth:`get<goblin.models.query.V.get>`.
3. The `has` step requires that you use :py:meth:`get_property_by_name` method
   to retrieve the correct property key.

Furthermore, it should be noted that the camel case used in Gremlin steps has
been replaced with the underscores more commonly used with Python methods: `inV` -> `in_v`.

For a full list of steps, please see the :ref:`API docs<goblin.models.query.V>`


Coming soon, detailed guides...


.. _curio: https://github.com/dabeaz/curio
.. _Gremlin Server: http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#gremlin-server
