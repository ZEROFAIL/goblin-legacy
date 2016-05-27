from __future__ import unicode_literals
from nose.plugins.attrib import attr
from copy import deepcopy

from tornado.testing import gen_test
from goblin._compat import print_
from goblin.models import Vertex, Edge
from goblin.properties import String, Integer
from goblin.exceptions import GoblinRelationshipException
from goblin.tests.base import BaseGoblinTestCase, TestVertexModel, counter
from goblin.relationships.relationship import Relationship


class TestRelationshipEdgeModel(Edge):
    label = 'test_relationship_edge_model'

    name = String(default='test_edge')
    test_val = Integer(default=counter)


class TestRelationshipStringPlaceholderVertexModel(Vertex):
    label = 'test_placeholder_inspection'

    name = String(default='test_vertex')
    test_val = Integer(default=counter)

    relation = Relationship(
        TestRelationshipEdgeModel,
        "goblin.tests.relationships_tests.vertex_relationship_io_tests.AnotherTestVertexModel",
        'out')


class AnotherTestVertexModel(Vertex):
    label = 'another_test_vertex_model'
    name = String(default='test_vertex')
    test_val = Integer(default=counter)


class TestRelationshipVertexModel(Vertex):
    label = 'test_relationship_vertex_model'

    name = String(default='test_vertex')
    test_val = Integer(default=counter)

    relation = Relationship(TestRelationshipEdgeModel, TestVertexModel, 'out')


@attr('unit', 'relationship')
class GraphRelationshipVertexIOTestCase(BaseGoblinTestCase):
    """ Test Relationship Vertex IO Functionality """

    @classmethod
    def setUpClass(cls):
        super(GraphRelationshipVertexIOTestCase, cls).setUpClass()
        cls.relationship_base_cls = Relationship
        cls.edge_model = TestRelationshipEdgeModel
        cls.vertex_model = TestRelationshipVertexModel
        cls.placeholder_model = TestRelationshipStringPlaceholderVertexModel

    @gen_test
    def test_instantiation(self):
        """ Test that the Relationship is properly Instantiated """
        v1 = yield self.vertex_model.create(name='test relationship')
        try:
            # setup relationship
            self.assertIsNotNone(v1.relation.top_level_vertex_class)
            self.assertIsNotNone(v1.relation.top_level_vertex)
            self.assertEqual(v1.relation.top_level_vertex, v1)
        finally:
            yield v1.delete()

    @gen_test
    def test_follow_through(self):
        """ Test that the Relationship property functions """

        v1 = yield self.vertex_model.create(name='test relationship')
        e1, v2 = yield v1.relation.create(
            vertex_params={'name': 'new relation'})
        try:
            stream = yield v1.outE(TestRelationshipEdgeModel)
            e1q = yield stream.read()
            e1q = e1q[0]
            stream = yield v1.outV(TestRelationshipEdgeModel)
            v2q = yield stream.read()
            v2q = v2q[0]
            self.assertEqual(e1, e1q)
            self.assertEqual(v2, v2q)
        finally:
            yield e1.delete()
            yield v1.delete()
            yield v2.delete()

    @gen_test
    def test_placeholder_inspection(self):
        """ Test that the Relationship property functions """

        v1 = yield self.placeholder_model.create(name='test placeholder')
        e1, v2 = yield v1.relation.create(
            vertex_params={'name': 'another new relation'})
        try:
            stream = yield v1.outE(TestRelationshipEdgeModel)
            e1q = yield stream.read()
            e1q = e1q[0]
            stream = yield v1.outV(TestRelationshipEdgeModel)
            v2q = yield stream.read()
            v2q = v2q[0]
            self.assertEqual(e1, e1q)
            self.assertEqual(v2, v2q)
        finally:
            yield e1.delete()
            yield v1.delete()
            yield v2.delete()

    @attr('relationship_isolation')
    @gen_test
    def test_relationship_isolation(self):
        """ Test that the relationship adheres to instance methods """

        v11 = yield self.vertex_model.create(name='test1')
        e1, v12 = yield v11.relation.create(
            vertex_params={'name': 'new_relation_1'})
        v21 = yield self.vertex_model.create(name='test2')
        e2, v22 = yield v21.relation.create(
            vertex_params={'name': 'new_relation_2'})
        try:
            stream = yield v11.relation.vertices()
            verts = yield stream.read()
            r11 = deepcopy(verts)
            print_("Vertex 1-1 relationships: {}".format(r11))

            stream = yield v21.relation.vertices()
            verts = yield stream.read()
            r2 = deepcopy(verts)
            print_("Vertex 2-1 relationships: {}".format(r2))

            with self.assertRaises(AssertionError):
                self.assertListEqual(r11, r2)

            stream = yield v11.relation.vertices()
            verts = yield stream.read()
            r12 = deepcopy(verts)
            print_("Vertex 1-1 relationships again: {}".format(r12))
            with self.assertRaises(AssertionError):
                self.assertListEqual(r2, r12)

            self.assertListEqual(r11, r12)
        finally:
            yield v11.delete()
            yield v12.delete()
            yield v21.delete()
            yield v22.delete()
