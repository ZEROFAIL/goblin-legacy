# Goblin OGM for TinkerPop3 Gremlin Server

[![Build Status](https://travis-ci.org/ZEROFAIL/goblin.svg?branch=dev)](https://travis-ci.org/ZEROFAIL/goblin)
[![Coverage Status](https://coveralls.io/repos/github/ZEROFAIL/goblin/badge.svg?branch=dev)](https://coveralls.io/github/ZEROFAIL/goblin?branch=dev)
[![Gitter chat](https://badges.gitter.im/ZEROFAIL/goblin.svg)](https://gitter.im/ZEROFAIL/goblin?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Documentation Status](https://readthedocs.org/projects/goblin/badge/?version=latest)](http://goblin.readthedocs.io/en/latest/?badge=latest)

This project began as the ZEROFAIL fork of ``mogwai``, a [TinkerPop3](https://tinkerpop.incubator.apache.org/) [Gremlin Server](http://tinkerpop.apache.org/docs/3.1.1-incubating/reference/#gremlin-server) compatible port of Cody Lee's original Python object graph mapper for Titan 0.5.x.

As Gremlin approached The TinkerPop, ``mogwai`` felt left behind and the closer he got, the more his world dissolved. He realized that all that he realized was just a realization and that all realized realizations are just as real as the need to evolve into something else - ``goblin`` was born...

``Goblin`` uses @davebshow [gremlinclient](https://github.com/davebshow/gremlinclient) for asynchronous websocket based communication with the Gremlin Server, and is therefore designed to be multi-platform, allowing the user to choose between [Tornado](http://www.tornadoweb.org/en/stable/), [Trollius](http://trollius.readthedocs.org/), or [Asyncio](https://docs.python.org/3/library/asyncio.html). It aims to provide full support for all TinkerPop3 enabled graph databases; however, it is currently only tested against [Titan:db 1.x](http://s3.thinkaurelius.com/docs/titan/1.0.0/index.html). This project is under active development.

### Basic Example

```python
from tornado import gen
from tornado.ioloop import IOLoop
from goblin import properties
from goblin import connection
from goblin.models import Vertex, Edge, V


class User(Vertex):
    name = properties.String()


class Follows(Edge):
    pass


@gen.coroutine
def go():
    goblin = yield User.create(name="Goblin")
    gremlin = yield User.create(name="Gremlin")
    gob_follows_grem = yield Follows.create(goblin, gremlin)
    # Find gremlin's followers
    stream = yield V(gremlin).in_step().get()
    followers = yield stream.read()
    return followers


connection.setup("ws://localhost:8182")
loop = IOLoop.current()
try:
    followers = loop.run_sync(go)
finally:
    connection.tear_down()
    loop.close()
```

### Example Usage:

Download Titan, unzip, and fire it up:

```sh
$ wget http://s3.thinkaurelius.com/downloads/titan/titan-1.0.0-hadoop1.zip
$ unzip titan-1.0.0-hadoop1.zip
$ cd titan-1.0.0-hadoop1/
$ ./bin/titan.sh start
```

The following example uses Python 3.5. On Ubuntu, you can install Python 3.5 using deadsnakes:

```sh
$ sudo add-apt-repository ppa:fkrull/deadsnakes
$ sudo apt-get update
$ sudo apt-get install python3.5
```

Then, if you have virtualenvwrapper, you can do something like this:

```sh
$ mkvirtualenv -p /usr/bin/python3.5 goblin
$ pip install git+https://github.com/ZEROFAIL/goblin.git@dev#egg=goblin
```


Then you are ready to go. Goblin is easy to use, and while it's not yet feature complete, it's already working pretty well:

```python
import asyncio
import datetime

from pytz import utc

from tornado.platform.asyncio import AsyncIOMainLoop

from goblin import properties
from goblin.connection import setup, tear_down
from goblin.models import Vertex, Edge, V
from goblin.relationships import Relationship


# Define edge models
class WorksFor(Edge):
    start_date = properties.DateTime()


class MemberOf(Edge):
    since = properties.DateTime()


class BelongsTo(Edge):
    pass


# Define vertex models
class Organization(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()


class Department(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()
    belongs_to = Relationship(BelongsTo, Organization)


class Person(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()
    works_for = Relationship(WorksFor, Organization)
    member_of = Relationship(MemberOf, Department)


async def main():
    # Start by creating a graph.

    # First create some nodes
    zfail = await Organization.create(
        name="zfail", email="zfail@zfail.com", url="https://zfail.com")
    west = await Organization.create(
        name="west", email="west@west.com", url="https://west.com")

    r_and_d = await Department.create(
        name="r_and_d", email="randd@somemail.com", url="https://randd.com")
    c_and_p = await Department.create(
        name="c_and_p", email="candp@somemail.com", url="https://candp.com")

    jon = await Person.create(
        name="jon", email="jon@jon.com", url="https://jon.com/")
    dave = await Person.create(
        name="dave", email="dave@dave.com", url="https://dave.com/")
    leif = await Person.create(
        name="leif", email="leif@leif.com", url="https://leif.com/")

    # Create some edges
    r_and_d_belongs_to = await BelongsTo.create(r_and_d, zfail)
    c_and_p_belongs_to = await BelongsTo.create(c_and_p, west)

    jon_works_for = await WorksFor.create(
        jon, zfail, start_date=datetime.datetime(2014, 1, 1, tzinfo=utc))
    jon_member_of = await MemberOf.create(
        jon, r_and_d, since=datetime.datetime(2014, 1, 1, tzinfo=utc))

    leif_works_for = await WorksFor.create(
        leif, zfail, start_date=datetime.datetime(2014, 1, 1, tzinfo=utc))
    leif_member_of = await MemberOf.create(
        leif, r_and_d, since=datetime.datetime(2014, 1, 1, tzinfo=utc))

    dave_works_for = await WorksFor.create(
        dave, west, start_date=datetime.datetime(2014, 1, 1, tzinfo=utc))
    dave_member_of = await MemberOf.create(
        dave, c_and_p, since=datetime.datetime(2014, 1, 1, tzinfo=utc))

    # You can also create nodes and edges at the same time
    # using the relationship properties
    dave_member_of_dep, a_and_h = await dave.member_of.create(
        edge_params={"since": datetime.datetime(2014, 1, 1, tzinfo=utc)},
        vertex_params={"name": "a_and_h", "email": "aandh@somemail.com",
                       "url": "https://aandh.com"})

    try:
        # Ok, now lets try out some of the Vertex methods these methods,
        # like all query methods return a gremlinclient.Stream object
        stream = await jon.outV()
        jons_out_v = await stream.read()
        print("These are Jon's neighbours:\n\n{}\n\n{}\n".format(
            jons_out_v[0], jons_out_v[1]))

        stream = await dave.outE()
        daves_out_e = await stream.read()
        print("These are Daves's rels:\n\n{}\n\n{}\n".format(
            daves_out_e[0], daves_out_e[1]))

        # Ok, how about some more complex queries?
        stream = await V(jon).out_step().in_step().get()
        jons_coworkers = await stream.read()
        print("These are Jons's coworkers:\n\n{}\n".format(
            jons_coworkers[0]))

    # Clean up
    finally:
        await r_and_d_belongs_to.delete()
        await c_and_p_belongs_to.delete()
        await jon_works_for.delete()
        await jon_member_of.delete()
        await leif_works_for.delete()
        await leif_member_of.delete()
        await dave_works_for.delete()
        await dave_member_of.delete()
        await dave_member_of_dep.delete()
        await zfail.delete()
        await west.delete()
        await r_and_d.delete()
        await c_and_p.delete()
        await jon.delete()
        await dave.delete()
        await leif.delete()
        await a_and_h.delete()


if __name__ == "__main__":
    setup("ws://localhost:8182", future_class=asyncio.Future)
    AsyncIOMainLoop().install()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    tear_down()  # maybe this should be teardown
    loop.close()

```

Shut down Titan:

```sh
$ ./bin/titan.sh stop
```

To download and run the above example:

```sh
$ git clone https://gist.github.com/e6d622c003c8e0d4dc0d.git
$ cd e6d622c003c8e0d4dc0d/
$ python goblin_example.py
```

#Contributing
Development of `goblin` happens at Github. We very much welcome your contribution
of course. To do so, simply follow these guidelines:

* Fork `goblin` on github
* Create a topic branch ``git checkout -b my_topic_branch``
* Push to your branch ``git push origin my_topic_branch``
* Create a pull request against the `dev` branch.
* Alternatively, if you need to report a bug or an unexpected behaviour, make sure
  to include a [mcve](http://stackoverflow.com/help/mcve) in your issue.

A good `pull` request should:

* Cover one bug fix or new feature only
* Include tests to cover the new code (inside the ``tests`` directory)
* Test coverage should only increase
* Preferably have one commit only (you can use [rebase](https://help.github.com/articles/about-git-rebase) to combine several
  commits into one)
* Make sure all tests pass

# Using vagrant environment

You can use [vagrant](https://www.vagrantup.com/) to start a development environment with cassandra, elasticsearch and titan. It uses docker for running the necessary services.

Start the environment with:

```
$ vagrant up
```

Run the integrated tests with:

```
$ vagrant ssh -c 'cd /vagrant; ./run_tests.sh'
```

# Integration with Python Asynchronous Frameworks

`goblin` is designed to interact smoothly with a variety of async frameworks such as [aiohttp](http://aiohttp.readthedocs.org/en/stable/), [Tornado](http://www.tornadoweb.org/en/stable/), and [Pulsar](http://pythonhosted.org/pulsar/). The following examples demonstrate some simple integration with Pulsar using the `gremlinclinet.aiohttp` module.

Install dependencies:

```
$ pip install aiohttp
$ pip install pulsar
$ pip install git+https://github.com/ZEROFAIL/goblin.git@dev#egg=goblin
```

Tornado is installed by default with `goblin`, but since we aren't using it here it can be uninstalled:

```
$ pip uninstall tornado
```

`Pulsar` uses an actor based model as its basic building blocks. The following example shows how to create a simple application that uses actors to create vertices:

```python
import asyncio
import pulsar

from goblin import properties
from goblin.connection import setup, tear_down
from goblin.models import Vertex
from gremlinclient.aiohttp_client import Pool


nodes = [
    {"name": "dave", "email": "dave@dave.com", "url": "https://dave.com/"},
    {"name": "jon", "email": "jon@jon.com", "url": "https://jon.com/"},
    {"name": "leif", "email": "leif@leif.com", "url": "https://leif.com/"}]


class Person(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()


@pulsar.command()
@asyncio.coroutine
def create_person(request, message):
    name = message["name"]
    url = message["url"]
    email = message["email"]
    person = yield from Person.create(name=name, url=url, email=email)
    request.actor.logger.info("Created: {}".format(person))
    return person


class Creator:

    def __init__(self):
        # Allow passing of config args
        setup('ws://localhost:8182', pool_class=Pool, future=asyncio.Future)
        cfg = pulsar.Config()
        cfg.parse_command_line()
        # Arbiter controls the main event loop in master process
        arbiter = pulsar.arbiter(cfg=cfg)
        self.cfg = arbiter.cfg
        # Conforms to the Pulsar definition of an async object
        self._loop = arbiter._loop
        self._loop.call_later(1, pulsar.ensure_future, self())
        arbiter.start()

    @asyncio.coroutine
    def __call__(self, actor=None):
        if actor is None:
            # This creates an actor in its own process with its own loop
            actor = yield from pulsar.spawn(name="creator")
        if nodes:
            node = nodes.pop()
            self._loop.logger.info("Creating: {}".format(node["name"]))
            # Send the task of creating a person to the actor
            yield from pulsar.send(actor, 'create_person', node)
            self._loop.call_soon(pulsar.ensure_future, self(actor))
        else:
            # Stop the event loop
            yield from tear_down()
            pulsar.arbiter().stop()
```

Run this example as follows:

```
$ git clone https://gist.github.com/322377bf995ddf768bdf.git
$ cd 322377bf995ddf768bdf/
$ python titan_pulsar.py
```

This is pretty low level, but `Pulsar` provides a higher level `Application` interface and ships with several batteries included apps out of the box. Here, we see how to create a JSON-RPC service with Titan:db...

```python
"""
Basic JSON-RPC WSGI server with Pulsar. Could easily implement custom
RPC and serve on sockets/websockets.
"""
import asyncio

from pulsar import ensure_future
from pulsar.apps import rpc, wsgi
from pulsar.apps.wsgi.utils import LOGGER
from pulsar.utils.httpurl import JSON_CONTENT_TYPES

from goblin import properties
from goblin.connection import setup, tear_down
from goblin.models import Vertex
from gremlinclient.aiohttp_client import Pool


class Person(Vertex):
    name = properties.String()
    email = properties.Email()
    url = properties.URL()


class TitanRPC(rpc.JSONRPC):
    """RPC methods are defined here"""
    def rpc_create_person(self, request, name, email, url):
        person = yield from Person.create(name=name, url=url, email=email)
        LOGGER.info("Created: {}".format(person))
        return [person.name, person.id]


class TitanRPCSite(wsgi.LazyWsgi):
    """Handler for the RPCServer"""

    def __init__(self):
        setup("ws://localhost:8182", pool_class=Pool, future=asyncio.Future)

    def setup(self, environ):
        commands = rpc.PulsarServerCommands()
        json_handler = commands.putSubHandler('titan', TitanRPC())
        middleware = wsgi.Router("/", post=json_handler,
                                 accept_content_types=JSON_CONTENT_TYPES)
        response = [wsgi.GZipMiddleware(200)]
        return wsgi.WsgiHandler(middleware=[wsgi.wait_for_body_middleware,
                                            middleware],
                                response_middleware=response,
                                async=True)


class TitanRPCServer(wsgi.WSGIServer):
    """Adds a hook that closes pool when the server is stopped."""
    def monitor_stopping(self, monitor):
        loop = monitor._loop
        loop.call_soon(ensure_future, tear_down())


def server(callable=None, **params):
    return TitanRPCServer(TitanRPCSite(), **params)

```

Then to access the server, a simple client:

```python
"""
Simple client for the JSONRPC Server
"""

import asyncio
from pulsar.apps import rpc

proxy = rpc.JsonProxy("http://localhost:8060")


@asyncio.coroutine
def main():
    name, vid = yield from proxy.titan.create_person(
        "jon", "jon@jon.com", "https://jon.com/")
    print("Created vertex {} named {}".format(vid, name))
```

To run this example, first run the server:

```
$ git clone https://gist.github.com/ab8a034d31d8776f9c04.git
$ cd ab8a034d31d8776f9c04/
$ python titan_rpc_server.py
```

Then open a new terminal and navigate to the same directory:

```
$ cd ab8a034d31d8776f9c04/
$ python titan_rpc_client.py
```
