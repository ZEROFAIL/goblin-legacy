from __future__ import unicode_literals
from nose.plugins.attrib import attr

from tornado.testing import gen_test

from .base_tests import GraphPropertyBaseClassTestCase
from goblin.properties.properties import Float, Double
from goblin.models import Vertex
from goblin._compat import print_


@attr('unit', 'property', 'property_float')
class FloatPropertyTestCase(GraphPropertyBaseClassTestCase):
    klass = Float
    good_cases = (1.1, 0.0, None)
    bad_cases = ('', 'a', 1, [], [1], {}, {'a': 1})


class FloatTestVertex(Vertex):
    element_type = 'test_float_vertex'

    test_val = Float()


@attr('unit', 'property', 'property_float')
class FloatVertexTestCase(GraphPropertyBaseClassTestCase):

    @gen_test
    def test_float_io(self):
        print_("creating vertex")
        dt = yield FloatTestVertex.create(test_val=1.1)
        print_("getting vertex from vertex: %s" % dt)
        dt2 = yield FloatTestVertex.get(dt._id)
        print_("got vertex: %s\n" % dt2)
        self.assertEqual(dt2.test_val, dt.test_val)
        print_("deleting vertex")
        yield dt2.delete()

        dt = yield FloatTestVertex.create(test_val=2.2)
        print_("\ncreated vertex: %s" % dt)
        dt2 = yield FloatTestVertex.get(dt._id)
        print_("Got vertex: %s" % dt2)
        self.assertEqual(dt2.test_val, 2.2)
        print_("deleting vertex")
        yield dt2.delete()


@attr('unit', 'property', 'property_double')
class DoublePropertyTestCase(FloatPropertyTestCase):
    klass = Double


class DoubleTestVertex(Vertex):
    element_type = 'test_double_vertex'

    test_val = Double()


@attr('unit', 'property', 'property_double')
class DoubleVertexTestCase(GraphPropertyBaseClassTestCase):

    @gen_test
    def test_double_io(self):
        print_("creating vertex")
        dt = yield DoubleTestVertex.create(test_val=1.1)
        print_("getting vertex from vertex: %s" % dt)
        dt2 = yield DoubleTestVertex.get(dt._id)
        print_("got vertex: %s\n" % dt2)
        self.assertEqual(dt2.test_val, dt.test_val)
        print_("deleting vertex")
        yield dt2.delete()

        dt = yield DoubleTestVertex.create(test_val=2.2)
        print_("\ncreated vertex: %s" % dt)
        dt2 = yield DoubleTestVertex.get(dt._id)
        print_("Got vertex: %s" % dt2)
        self.assertEqual(dt2.test_val, 2.2)
        print_("deleting vertex")
        yield dt2.delete()
