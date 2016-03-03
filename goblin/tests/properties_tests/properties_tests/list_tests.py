from __future__ import unicode_literals
from nose.plugins.attrib import attr

from tornado.testing import gen_test
from .base_tests import GraphPropertyBaseClassTestCase
from goblin.properties.properties import List
from goblin.models import Vertex
from goblin._compat import print_


@attr('unit', 'property', 'property_list')
class ListPropertyTestCase(GraphPropertyBaseClassTestCase):
    klass = List
    good_cases = ([], ['val'], (), tuple('val'), None)
    bad_cases = (0, 1.1, 'val', {}, {'value': 1})


class ListTestVertex(Vertex):
    element_type = 'test_list_vertex'

    test_val = List()


#
# @attr('unit', 'property', 'property_list')
# class ListVertexTestCase(GraphPropertyBaseClassTestCase):
#
#     @gen_test
#     def test_list_io(self):
#         print_("creating vertex")
#         dt = yield ListTestVertex.create(test_val=['a', 'b'])
#         print_("getting vertex from vertex: %s" % dt)
#         dt2 = yield ListTestVertex.get(dt._id)
#         print_("got vertex: %s\n" % dt2)
#         self.assertEqual(dt2.test_val, dt.test_val)
#         print_("deleting vertex")
#         yield dt2.delete()
#
#         dt = yield ListTestVertex.create(test_val=[1, 2])
#         print_("\ncreated vertex: %s" % dt)
#         dt2 = yield ListTestVertex.get(dt._id)
#         print_("Got vertex: %s" % dt2)
#         self.assertEqual(dt2.test_val, [1, 2])
#         print_("deleting vertex")
#         yield dt2.delete()
