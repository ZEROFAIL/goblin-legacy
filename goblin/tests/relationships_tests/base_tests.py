from __future__ import unicode_literals
from nose.plugins.attrib import attr

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.testing import gen_test

from goblin.models import Vertex, Edge
from goblin.properties import String, Integer
from goblin.exceptions import GoblinRelationshipException
from goblin.tests.base import (
    BaseGoblinTestCase, TestEdgeModel, TestVertexModel, counter)
from goblin.relationships.relationship import Relationship


class TestVertex2Model(Vertex):
    label = 'test_vertex2_model'

    name = String(default='test_vertex')
    test_val = Integer(default=counter)


class TestEdge2Model(Edge):
    label = 'test_edge2_model'

    name = String(default='test_edge')
    test_val = Integer(default=counter)


@attr('unit', 'relationship')
class GraphRelationshipBaseTestCase(BaseGoblinTestCase):
    """ Test Relationship Functionality """

    @classmethod
    def setUpClass(cls):
        super(GraphRelationshipBaseTestCase, cls).setUpClass()
        cls.relationship_base_cls = Relationship
        cls.edge_model = TestEdgeModel
        cls.vertex_model = TestVertexModel

    @classmethod
    def tearDownClass(cls):
        super(GraphRelationshipBaseTestCase, cls).tearDownClass()

    @gen_test
    def test_instantiation(self):
        """ Test that the Relationship is properly Instantiated """

        # setup relationship
        relationship = self.relationship_base_cls(self.edge_model,
                                                  self.vertex_model,
                                                  'both')
        self.assertIsNone(relationship.top_level_vertex_class)
        self.assertIsNone(relationship.top_level_vertex)

        with self.assertRaises(GoblinRelationshipException):
            yield relationship.create({}, {})

    @gen_test
    def test_relationship_io(self):
        """
        Test Relationship GraphDB interaction for querying Edges and Vertices
        """
        # setup relationship
        vertex_start = yield TestVertexModel.create(name='test relationship')
        relationship = self.relationship_base_cls(self.edge_model,
                                                  self.vertex_model,
                                                  'both')
        relationship.top_level_vertex = vertex_start
        relationship.top_level_vertex_class = self.vertex_model

        stream = yield relationship.vertices()
        vertices = yield stream.read()
        self.assertEqual(len(vertices), 0)
        v2 = yield self.vertex_model.create(name='other side relationship')
        e1 = yield self.edge_model.create(v2, vertex_start)
        try:
            stream = yield relationship.vertices()
            vertices = yield stream.read()
            self.assertEqual(len(vertices), 1)

            stream = yield relationship.edges()
            edges = yield stream.read()
            self.assertEqual(len(edges), 1)
        finally:
            yield e1.delete()
            yield v2.delete()

    def test_relationship_control(self):
        """ Test Relationship Constraint system """

        # setup relationship
        relationship = self.relationship_base_cls(self.edge_model,
                                                  self.vertex_model, 'both')
        self.assertTrue(relationship.allowed(self.edge_model,
                                             self.vertex_model))

        class BadEdgeModel(object):
            pass

        class BadVertexModel(object):
            pass

        self.assertFalse(relationship.allowed(self.edge_model, BadVertexModel))
        self.assertFalse(relationship.allowed(BadEdgeModel, self.vertex_model))
        self.assertFalse(relationship.allowed(BadEdgeModel, BadVertexModel))

    # def test_relationship_query(self):
    #     """ Test Relationship Query system
    #
    #     The Query tests are in a separate tests, we only care that we get the same instantiated query class and
    #     functionally operates
    #     """
    #
    #     # setup relationship
    #     relationship = self.relationship_base_cls(self.edge_model, self.vertex_model)
    #     relationship.top_level_vertex = vertex_start
    #     relationship.top_level_vertex_class = self.vertex_model
    #
    #     v2 = yield self.vertex_model.create(name='other side relationship')
    #     e1 = yield self.edge_model.create(v2, vertex_start)
    #
    #     from goblin.models import Query, IN
    #
    #     # default query
    #     query = relationship.query()
    #     self.assertIsInstance(query, Query)
    #     result = query.direction(IN)._get_partial()
    #     self.assertEqual(result, "g.v(id).query().labels('test_edge_model').direction(IN)")
    #
    #     # specified edge_types query
    #     query = relationship.query(edge_types=self.edge_model)
    #     self.assertIsInstance(query, Query)
    #     result = query.direction(IN)._get_partial()
    #     self.assertEqual(result, "g.v(id).query().labels('test_edge_model').direction(IN)")
    #
        # yield e1.delete()
        # yield v2.delete()

    @gen_test
    def test_relationship_creation(self):
        """ Test Relationship Vertex and Edge Creation mechanism """

        # setup relationship
        vertex_start = yield TestVertexModel.create(name='test relationship')
        relationship = self.relationship_base_cls(TestEdge2Model,
                                                  TestVertex2Model, 'both')
        relationship.top_level_vertex = vertex_start
        relationship.top_level_vertex_class = self.vertex_model

        e1, v2 = yield relationship.create(
            edge_params={}, vertex_params={'name': 'other side relationship'})
        try:
            self.assertIsInstance(e1, Edge)
            self.assertIsInstance(v2, Vertex)

            stream = yield relationship.vertices()
            vertex_result = yield stream.read()
            self.assertEqual(len(vertex_result), 1)
            self.assertEqual(vertex_result[0], v2)

            stream = yield relationship.edges()
            edge_result = yield stream.read()
            self.assertEqual(len(edge_result), 1)
            self.assertEqual(edge_result[0], e1)
        finally:
            yield e1.delete()
            yield v2.delete()
            yield vertex_start.delete()
