from __future__ import unicode_literals
import logging
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from goblin.constants import (TORNADO_CLIENT_MODULE, AIOHTTP_CLIENT_MODULE,
                              SECURE_SCHEMES, INSECURE_SCHEMES)


logger = logging.getLogger(__name__)


def _get_pool_class():
    try:
        from gremlinclient.tornado_client import Pool
    except ImportError:
        try:
            from gremlinclient.aiohttp_client import Pool
        except ImportError:
            raise ImportError(
                "Install appropriate client or pass pool explicitly")
    return Pool


def _get_connector(config):
    if config["SCHEME"] in SECURE_SCHEMES:
        if config["SSL_CONTEXT"] is None:
            raise ValueError("Please pass ssl_context for secure protocol")

        if config["CLIENT_MODULE"] == AIOHTTP_CLIENT_MODULE:
            import aiohttp
            connector = aiohttp.TCPConnector(ssl_context=ssl_context,
                                             loop=loop)

        elif config["CLIENT_MODULE"] == TORNADO_CLIENT_MODULE:
            from functools import partial
            from tornado import httpclient
            connector = partial(
                httpclient.HTTPRequest, ssl_options=sslcontext)
        else:
            raise ValueError("Unknown client module")
    elif config["SCHEME"] in INSECURE_SCHEMES:
        connector = None
    else:
        raise ValueError("Unknown protocol")
    return connector


class Goblin(object):

    default_config = {
        "URL": "",
        "LOOP": None,
        "CONNECTION_POOL": None,
        "POOL_SIZE": 256,
        "SSL_CONTEXT": None,
        "USERNAME": "",
        "PASSWORD": "",
        "FUTURE_CLASS": None,
        "GRAPH_NAME": "graph",
        "TRAVERSAL_SOURCE": "g",
        "LOADED_MODELS": [],
        "SCHEME": None,
        "NETLOC": None,
        "CLIENT_MODULE": None,
        "CONNECTOR": None}

    def __init__(self, url, **config):
        # Allow user to directly pass config
        self.default_config.update(config)

        # Set up required URL info
        self.default_config["URL"] = url
        parsed_url = urlparse(url)
        self.default_config["SCHEME"]= parsed_url.scheme
        self.default_config["NETLOC"] = parsed_url.netloc

        # Get connector for pool if necessary
        if self.default_config["CONNECTOR"] is None:
            connector = _get_connector(self.default_config)
            self.default_config["CONNECTOR"] = connector

        # Setup pool
        if self.default_config["CONNECTION_POOL"] is None:
            pool_class = _get_pool_class()
            # Get module from pool
            client_module = pool_class.__module__.split('.')[1]
            self.default_config["CLIENT_MODULE"] = client_module
            connection_pool = pool_class(
                url, maxsize=self.default_config["POOL_SIZE"],
                username=self.default_config["USERNAME"],
                password=self.default_config["PASSWORD"],
                force_release=True,
                future_class=self.default_config["FUTURE_CLASS"],
                loop=self.default_config["LOOP"],
                connector=self.default_config["CONNECTOR"])
            self.default_config["CONNECTION_POOL"] = connection_pool

        # Get future_class from pool if necessary
        if self.default_config["FUTURE_CLASS"] is None:
            future_class = self.default_config["CONNECTION_POOL"].future_class
            self.default_config["FUTURE_CLASS"] = future_class

        # Set up config obj
        self.config = Config(self.default_config)

    def register(self, element_class):
        # Set app as a class attribute - limitation: limits model to one app
        element_class.app = property(lambda s: self)
        if element_class not in self.config["LOADED_MODELS"]:
            self.config["LOADED_MODELS"].append(element_class)

    def tear_down(self):
        return self.config["CONNECTION_POOL"].close()


class Config(dict):

    def __init__(self, config):
        super(Config, self).__init__(config)

    def from_object(self):
        pass

    def from_envvar(self):
        pass
