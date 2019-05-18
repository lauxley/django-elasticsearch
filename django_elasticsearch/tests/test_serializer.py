from django.test import TestCase

from django_elasticsearch.utils import dict_depth
from django_elasticsearch.managers import es_client
from django_elasticsearch.tests.utils import withattrs
from django_elasticsearch.serializers import EsJsonSerializer
from django_elasticsearch.serializers import EsSimpleJsonSerializer

from test_app.models import Dummy
from test_app.models import TestAllFieldsModel


class CustomSerializer(EsJsonSerializer):
    def serialize_char(self, instance, field_name):
        return u'FOO'


class EsJsonSerializerTestCase(TestCase):
    def setUp(self):
        TestAllFieldsModel.es.flush()
        self.target = Dummy.objects.create()
        self.instance = TestAllFieldsModel.objects.create(fk=self.target,
                                                  oto=self.target)
        # to test for infinite nested recursion
        self.instance.fkself = self.instance
        self.instance.save()

        self.instance.es.do_index()
        TestAllFieldsModel.es.do_update()

    def tearDown(self):
        super(EsJsonSerializerTestCase, self).tearDown()
        es_client.indices.delete(index=TestAllFieldsModel.es.get_index())

    def test_serialize(self):
        obj = self.instance.es.serialize()
        self.assertTrue(isinstance(obj, basestring))

    @withattrs(TestAllFieldsModel.Elasticsearch, 'serializer_class',
               'django_elasticsearch.serializers.EsJsonSerializer')
    def test_dynamic_serializer_import(self):
        obj = self.instance.es.serialize()
        self.assertTrue(isinstance(obj, basestring))

    def test_deserialize(self):
        instance = TestAllFieldsModel.es.deserialize({'char': 'test'})
        self.assertEqual(instance.char, 'test')
        self.assertRaises(ValueError, instance.save)

    @withattrs(TestAllFieldsModel.Elasticsearch, 'serializer_class', CustomSerializer)
    def test_custom_serializer(self):
        json = self.instance.es.serialize()
        self.assertIn('"char": "FOO"', json)

    def test_nested_fk(self):
        # if the target model got a Elasticsearch.serializer, we use it
        u = TestAllFieldsModel.es.all()[0]
        self.assertTrue('fk' in u)
        self.assertTrue(type(u['fk']) is dict)

    def test_nested_oto(self):
        # if the target model got a Elasticsearch.serializer, we use it
        u = TestAllFieldsModel.es.all()[0]
        self.assertTrue('oto' in u)
        self.assertTrue(type(u['oto']) is dict)

    @withattrs(TestAllFieldsModel.Elasticsearch, 'fields', ['fkself',])
    def test_self_fk_depth_test(self):
        TestAllFieldsModel.es.serializer = None  # reset cache
        serializer = TestAllFieldsModel.es.get_serializer(max_depth=3)
        obj = serializer.format(self.instance)
        self.assertEqual(dict_depth(obj), 3)

    def test_nested_m2m(self):
        u = TestAllFieldsModel.es.all()[0]
        self.assertTrue('mtm' in u)
        self.assertTrue(type(u['mtm']) is list)

    @withattrs(TestAllFieldsModel.Elasticsearch, 'fields', ['abstract_prop', 'abstract_method'])
    def test_abstract_field(self):
        serializer = TestAllFieldsModel.es.get_serializer()
        obj = serializer.format(self.instance)
        expected = {'abstract_method': 'woot', 'abstract_prop': 'weez'}
        self.assertEqual(obj, expected)

    @withattrs(TestAllFieldsModel.Elasticsearch, 'fields', ['foo',])
    def test_unknown_field(self):
        with self.assertRaises(AttributeError):
            self.instance.es.serialize()

    def test_specific_field_method(self):
        serializer = TestAllFieldsModel.es.get_serializer()
        obj = serializer.format(self.instance)
        self.assertEqual(obj["bigint"], 42)

        instance = TestAllFieldsModel.es.deserialize(obj)
        self.assertEqual(instance.bigint, 45)

    def test_type_specific_field_method(self):
        serializer = TestAllFieldsModel.es.get_serializer()
        obj = serializer.format(self.instance)
        self.assertTrue(type(obj["datetf"]) is dict)

        instance = TestAllFieldsModel.es.deserialize({"datetf": obj["datetf"]})
        self.assertEqual(instance.datetf, self.instance.datetf)

    @withattrs(TestAllFieldsModel.Elasticsearch, 'serializer_class', EsSimpleJsonSerializer)
    def test_simple_serializer(self):
        results = TestAllFieldsModel.es.deserialize([{'id': self.instance.pk},])
        self.assertTrue(self.instance in results)
