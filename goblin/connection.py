from __future__ import unicode_literals
import logging
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from goblin.constants import (TORNADO_CLIENT_MODULE, AIOHTTP_CLIENT_MODULE,
                              SECURE_SCHEMES, INSECURE_SCHEMES)
from goblin.exceptions import GoblinConnectionError


logger = logging.getLogger(__name__)


def execute_query(query, bindings=None, app=None, pool=None, future_class=None,
                  graph_name=None, traversal_source=None, username="",
                  password="", handler=None, request_id=None, *args, **kwargs):
    """
    Execute a raw Gremlin query with the given parameters passed in.

    :param str query: The Gremlin query to be executed
    :param dict bindings: Bindings for the Gremlin query
    :param `gremlinclient.pool.Pool` pool: Pool that provides connection used
        in query
    :param str graph_name: graph name as defined in server configuration.
        Defaults to "graph"
    :param str traversal_source: traversal source name as defined in the
        server configuration. Defaults to "g"
    :param str username: username as defined in the Tinkerpop credentials
        graph.
    :param str password: password for username as definined in the Tinkerpop
        credentials graph
    :param func handler: Handles preprocessing of query results

    :returns: Future
    """
    import ipdb; ipdb.set_trace()
    if pool is None and app:
        pool = app.config["CONNECTION_POOL"]

    if not pool:
        raise GoblinConnectionError(("Please create ``Goblin`` instance or "
                                     "pass pool explicitly"))

    if future_class is None:
        future_class = pool.future_class

    if graph_name is None and app:
        graph_name = app.config["GRAPH"]
    else:
        graph_name = "graph"

    if traversal_source is None and app:
        traversal_source = app.config["TRAVERSAL_SOURCE"]
    else:
        traversal_source = "g"

    aliases = {"graph": graph_name, "g": traversal_source}

    future = future_class()
    future_conn = pool.acquire()

    def on_connect(f):
        try:
            conn = f.result()

        except Exception as e:
            future.set_exception(e)
        else:
            stream = conn.send(
                query, bindings=bindings, aliases=aliases, handler=handler,
                request_id=request_id)
            future.set_result(stream)

    future_conn.add_done_callback(on_connect)

    return future


def generate_spec():  # pragma: no cover
    pass


def sync_spec():  # pragma: no cover
    pass


def pop_execute_query_kwargs(keyword_arguments):
    """ pop the optional execute query arguments from arbitrary kwargs;
        return non-None query kwargs in a dict
    """
    query_kwargs = {}
    for key in ('graph_name', 'traversal_source', 'pool',
                'request_id', 'future_class'):
        val = keyword_arguments.pop(key, None)
        if val is not None:
            query_kwargs[key] = val
    return query_kwargs
