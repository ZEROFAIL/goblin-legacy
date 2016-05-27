from __future__ import unicode_literals
import logging
import warnings
from functools import wraps

from goblin import connection
from goblin._compat import array_types, string_types
from goblin.constants import IN, OUT, BOTH
from goblin.exceptions import GoblinRelationshipException
from goblin.gremlin import GremlinMethod
from goblin.tools import LazyImportClass


logger = logging.getLogger(__name__)


def requires_vertex(method):
    @wraps(method)
    def method_wrapper(self, *args, **kwargs):
        if self.top_level_vertex:
            return method(self, *args, **kwargs)
        else:
            raise GoblinRelationshipException("No Vertex Instantiated")
    return method_wrapper


class Relationship(object):

    """
    Define incoming and outgoing relationships that exist. Also enforce
    schema IN, OUT and BOTH directions

    Warn if queries return schema violations.
    """

    def __init__(self, edge_class, vertex_class, direction):
        from goblin.models import Edge, Vertex
        self.edge_classes = self._create_class_tuple(
            edge_class, Edge)
        self.vertex_classes = self._create_class_tuple(
            vertex_class, Vertex)
        assert direction in (IN, OUT, BOTH), \
            "Direction of Relationship must be of one in (%s, %s, %s)" % (
                IN, OUT, BOTH)
        self.direction = direction
        self.top_level_vertex_class = None
        self.top_level_vertex = None

    def _setup_instantiated_vertex(self, vertex):
        self.top_level_vertex = vertex
        self.top_level_vertex_class = vertex.__class__

    def _create_class_tuple(self, model_class, element_type):
        """
        Take in an string, array of classes, or a single class and make a
        tuple of said referenced classes

        :param model_class: Input to be transformed into reference class(es)
        :type model_class: string_types | array_types | goblin.models.Edge |
            goblin.models.Vertex
        :param element_type: Enforce a specific model type? If not provided,
            everything that is resolved passes, otherwise if a type is given,
            the classes are filtered out that don't match.
        :type element_type: None | goblin.models.Vertex | goblin.models.Edge
        :rtype: tuple[element_type | Object]
        """
        if isinstance(model_class, string_types):
            model_classes = (LazyImportClass(model_class), )
        elif isinstance(model_class, array_types):
            model_classes = []
            for mc in model_class:
                if isinstance(mc, string_types):
                    mc = LazyImportClass(mc)
                if not (isinstance(mc, LazyImportClass) or not
                            issubclass(mc, element_type)):
                    warnings.warn(
                        "Relationship constraint is not derived from %s and will be ignored!" % element_type,
                        category=SyntaxWarning)
                else:
                    model_classes.append(mc)
            model_classes = tuple(model_classes)
        else:
            model_classes = (model_class, )
        return model_classes

    @requires_vertex
    def vertices(self, limit=None, **kwargs):
        """ Query and return all Vertices attached to the current Vertex

        TODO: fix this, the instance method isn't properly setup
        :param limit: Limit the number of returned results
        :type limit: int | long
        :param offset: Query offset of the number of paginated results
        :type offset: int | long
        :param callback: (Optional) Callback function to handle results
        :type callback: method
        :rtype: List[goblin.models.Edge] | Object
        """
        script, bindings = self._vertices()
        return self._get_elements(script, bindings, limit=limit, **kwargs)

    @requires_vertex
    def edges(self, limit=None, **kwargs):
        """ Query and return all Edges attached to the current Vertex

        TODO: fix this, the instance method isn't properly setup
        :param limit: Limit the number of returned results
        :type limit: int | long
        :param offset: Query offset of the number of paginated results
        :type offset: int | long
        :param callback: (Optional) Callback function to handle results
        :type callback: method
        :rtype: List[goblin.models.Edge] | Object
        """
        script, bindings = self._edges()
        return self._get_elements(script, bindings, limit=limit, **kwargs)

    def _get_elements(self, script, bindings, limit=None, **kwargs):
        """ Query and return all Vertices attached to the current Vertex

        :param limit: Limit the number of returned results
        :type limit: int | long
        :param offset: Query offset of the number of paginated results
        :type offset: int | long
        :param callback: (Optional) Callback function to handle results
        :type callback: method
        :rtype: List[goblin.models.Vertex] | Object
        """
        from goblin.models.element import Element

        deserialize = kwargs.pop('deserialize', True)
        def result_handler(results):
            if not results:
                results = []
            if deserialize:
                results = [Element.deserialize(r) for r in results]
            return results

        return connection.execute_query(script, bindings=bindings,
                                        handler=result_handler, **kwargs)

    def _vertices(self):
        if self.direction == OUT:
            vertex = IN
        elif self.direction == IN:
            vertex = OUT
        else:
            vertex = 'other'
        vlabels = [v.get_label() for v in self.vertex_classes]
        script, bindings = self._edges()
        script += ".%sV().hasLabel(*vlabels)" % (vertex, )
        bindings.update({"vlabels": vlabels})
        return script, bindings

    def _edges(self):
        if self.direction == OUT:
            edge = OUT
        elif self.direction == IN:
            edge = IN
        else:
            edge = BOTH
        elabels = [e.get_label() for e in self.edge_classes]
        script = "g.V(vid).%sE(*elabels)" % (edge, )
        bindings = {"vid": self.top_level_vertex.id, "elabels": elabels}
        return script, bindings

    def allowed(self, edge_type, vertex_type):
        """
        Check whether or not the allowed Edge and Vertex type are
        compatible with the schema defined

        :param edge_type: Edge Class
        :type: goblin.models.Edge
        :param vertex_type: Vertex Class
        :type: goblin.models.Vertex
        :rtype: bool
        """
        if (edge_type in self.edge_classes and
                vertex_type in self.vertex_classes):
            return True
        else:
            return False

    def _create_entity(self, model_cls, model_params, outV=None, inV=None):
        """ Create Vertex and Edge between current Vertex and New Vertex

        :param model_cls: Vertex or Edge Class for the relationship
        :type model_cls: goblin.models.Vertex | goblin.models.Edge
        :param model_params: Vertex or Edge class parameters for instantiating
            the model
        :type model_params: dict
        :param outV: Outgoing Vertex if creating an Edge between two vertices
            (otherwise ignored)
        :type outV: goblin.models.Vertex
        :param inV: Incoming Vertex if creating an Edge between two vertices
            (otherwise ignored)
        :type inV: goblin.models.Vertex
        :rtype: goblin.models.Vertex | goblin.models.Edge
        """
        if isinstance(model_cls, LazyImportClass):
            model_cls = model_cls.klass

        create_cls = model_cls._get_factory()

        from goblin.models.edge import Edge
        if issubclass(model_cls, Edge):
            return create_cls(outV=outV, inV=inV, **model_params)
        else:
            return create_cls(**model_params)

    @requires_vertex
    def create(self, edge_params={}, vertex_params={}, edge_type=None,
               vertex_type=None, callback=None, **kwargs):
        """ Creates a Relationship defined by the schema

        :param edge_params: (Optional) Parameters passed to the instantiation
            method of the Edge
        :type edge_params: dict
        :param vertex_params: (Optional) Parameters passed to the instantiation
            method
        :type vertex_params: dict
        :param edge_type: (Optional) Edge class type, otherwise it defaults to
            the first Edge type known
        :type edge_type: goblin.models.Edge | None
        :param edge_type: (Optional) Vertex class type, otherwise it defaults
            to the first Vertex type known
        :type edge_type: goblin.models.Vertex | None
        :param callback: (Optional) Callback function to handle results
        :type callback: method
        :rtype: tuple(goblin.models.Edge, goblin.models.Vertex) | Object
        """
        # if not self.top_level_vertex:
        #    raise GoblinRelationshipException("No existing vertex known, haveyou created a vertex?")
        if not vertex_type:
            vertex_type = self.vertex_classes[0]
        if not edge_type:
            edge_type = self.edge_classes[0]
        if not self.allowed(edge_type, vertex_type):
            raise GoblinRelationshipException(
                "That is not a valid relationship setup: %s <-%s-> %s" % (
                    edge_type, self.direction, vertex_type))

        future = connection.get_future(kwargs)
        if isinstance(vertex_type, string_types):

            top_level_module = self.top_level_vertex.__module__
        new_vertex_future = self._create_entity(vertex_type, vertex_params)

        def on_vertex(f):
            try:
                new_vertex = f.result()
            except Exception as e:
                future.set_exception(e)
            else:
                if self.direction == IN:
                    outV = new_vertex
                    inV = self.top_level_vertex
                else:
                    outV = self.top_level_vertex
                    inV = new_vertex

                new_edge_future = self._create_entity(
                    edge_type, edge_params, outV=outV, inV=inV)

                def on_edge(f2):
                    try:
                        new_edge = f2.result()
                    except Exception as e:
                        future.set_exception(e)
                    else:
                        if callback:
                            try:
                                result = callback(new_edge, new_vertex)
                            except Exception as e:
                                future.set_exception(e)
                        else:
                            result = (new_edge, new_vertex)
                        future.set_result(result)

                new_edge_future.add_done_callback(on_edge)

        new_vertex_future.add_done_callback(on_vertex)

        return future
