import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.test.utils import override_settings
from django.contrib.auth.models import Group
from django.template import Template, Context

from django_elasticsearch.client import es_client
from django_elasticsearch.managers import EsQueryset
from django_elasticsearch.tests.utils import withattrs

from test_app.models import TestModel


class EsQuerysetTestCase(TestCase):
    def setUp(self):
        # create a bunch of documents
        TestModel.es.flush()
        
        self.t1 = TestModel.objects.create(username=u"woot woot",
                                           first_name=u"John",
                                           last_name=u"Smith",
                                           email='johnsmith@host.com')
        self.group = Group.objects.create(name='agroup')
        self.t1.groups.add(self.group)

        self.t2 = TestModel.objects.create(username=u"woot",
                                           first_name=u"Jack",
                                           last_name=u"Smith",
                                           last_login=datetime.now() + timedelta(seconds=1),
                                           date_joined=datetime.now() + timedelta(seconds=1))
        self.t3 = TestModel.objects.create(username=u"BigMama",
                                           first_name=u"Mama",
                                           last_name=u"Smith",
                                           last_login=datetime.now() + timedelta(seconds=2),
                                           date_joined=datetime.now() + timedelta(seconds=2))
        self.t4 = TestModel.objects.create(username=u"foo",
                                           first_name=u"Foo",
                                           last_name=u"Bar",
                                           last_login=datetime.now() + timedelta(seconds=3),
                                           date_joined=datetime.now() + timedelta(seconds=3))

        self.t1.es.do_index()
        self.t2.es.do_index()
        self.t3.es.do_index()
        self.t4.es.do_index()

        TestModel.es.do_update()

    def tearDown(self):
        super(EsQuerysetTestCase, self).tearDown()
        es_client.indices.delete(index=TestModel.es.get_index())

    def test_all(self):
        contents = TestModel.es.search("").deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

    def test_slice(self):
        content = TestModel.es.deserialize(TestModel.es.all()[0])
        self.assertEqual(self.t1, content)

    def test_repr(self):
        contents = str(TestModel.es.queryset.order_by('id').deserialize())
        expected = str(list(TestModel.objects.all()))
        self.assertEqual(contents, expected)

    def test_use_cache(self):
        with patch('django_elasticsearch.client.es_client.search', MagicMock(side_effect=es_client.search)) as mocked:
            qs = TestModel.es.search("")
            # eval
            list(qs)
            # use cache
            list(qs)
            # use cache also
            self.t1 in qs
            
            self.assertEqual(len(mocked.mock_calls), 1)
            
            # re-eval
            list(qs[0:5])
            self.assertEqual(len(mocked.mock_calls), 2)

            # and also
            qs[0]
            self.assertEqual(len(mocked.mock_calls), 3)
            
            with self.assertRaises(IndexError):
                qs[4]  # only 3 instances
                
            self.assertEqual(len(mocked.mock_calls), 4)

    def test_facets(self):
        qs = TestModel.es.queryset.facet(['last_name'])
        expected = [{u'doc_count': 3, u'key': u'Smith'},
                    {u'doc_count': 1, u'key': u'Bar'}]
        self.assertEqual(qs.facets['doc_count'], 4)
        self.assertEqual(qs.facets['last_name']['buckets'], expected)

    def test_non_global_facets(self):
        qs = TestModel.es.search("Foo").facet(['last_name'], use_globals=False)
        expected = [{u'doc_count': 1, u'key': u'Bar'}]
        self.assertEqual(qs.facets['last_name']['buckets'], expected)

    def test_suggestions(self):
        qs = TestModel.es.search('smath').suggest(['last_name',], limit=3)
        expected = {
            u'last_name': [
                {u'length': 5,
                 u'offset': 0,
                 u'options': [],
                 u'text': u'smath'}]}
        self.assertEqual(expected, qs.suggestions)

    def test_count(self):
        self.assertEqual(TestModel.es.count(), 4)
        self.assertEqual(TestModel.es.search("").filter(last_name=u"Smith").count(), 3)
        self.assertEqual(TestModel.es.search("John").count(), 1)

    def test_count_after_reeval(self):
        # regression test
        q = TestModel.es.all()
        self.assertEqual(q.count(), 4)
        q = q.filter(first_name="John")
        self.assertEqual(q.count(), 1)

    def test_ordering(self):
        contents = TestModel.es.search("").order_by('first_name').deserialize()
        self.assertEqual(contents[0], self.t4)
        self.assertEqual(contents[1], self.t2)
        self.assertEqual(contents[2], self.t1)
        self.assertEqual(contents[3], self.t3)

    def test_default_ordering(self):
        qs = TestModel.objects.all()
        qes = TestModel.es.all().deserialize()
        self.assertEqual(list(qs), list(qes))

    @withattrs(TestModel.Elasticsearch, 'ordering', ['first_name',])
    def test_model_ordering(self):
        qs = TestModel.es.queryset.all()
        contents = qs.deserialize()
        self.assertEqual(contents[0], self.t4)
        self.assertEqual(contents[1], self.t2)
        self.assertEqual(contents[2], self.t1)
        self.assertEqual(contents[3], self.t3)

    def test_get(self):
        data = self.t1.es.get()
        self.assertEqual(data['id'], self.t1.id)

        data = TestModel.es.get(pk=self.t1.id)
        self.assertEqual(data['id'], self.t1.id)

        with self.assertRaises(AttributeError):
            TestModel.es.queryset.get()

    def test_filtering(self):
        contents = TestModel.es.filter(last_name=u"Smith").deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 not in contents)

    def test_multiple_filters(self):
        contents = TestModel.es.filter(last_name=u"Smith", first_name=u"Jack").deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

    def test_filter_range(self):
        contents = TestModel.es.filter(id__gt=self.t2.id).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 not in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

        contents = TestModel.es.filter(id__lt=self.t2.id).deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 not in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

        contents = TestModel.es.filter(id__gte=self.t2.id).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

        contents = TestModel.es.filter(id__lte=self.t2.id).deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

        contents = TestModel.es.filter(id__range=(self.t2.id, self.t3.id)).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 not in contents)

    def test_isnull_lookup(self):
        # Note: it works because we serialize empty string emails to the null value
        qs = TestModel.es.filter(email__isnull=False).deserialize()
        self.assertEqual(qs.count(), 1)
        self.assertTrue(self.t1 in qs)

        qs = TestModel.es.exclude(email__isnull=False).deserialize()
        self.assertEqual(qs.count(), 3)
        self.assertFalse(self.t1 in qs)

    @withattrs(TestModel.Elasticsearch, 'fields', ['id', 'date_joined_exp'])
    def test_sub_object_lookup(self):
        qs = TestModel.es.filter(date_joined_exp__iso=self.t1.date_joined).deserialize()
        self.assertEqual(len(qs), 1)
        self.assertTrue(self.t1 in qs)

        qs = TestModel.es.filter(date_joined_exp__iso__isnull=False)
        self.assertEqual(qs.count(), 4)

    def test_nested_filter(self):
        qs = TestModel.es.filter(groups__value='agroup')
        self.assertEqual(qs.count(), 1)
        qs = TestModel.es.filter(groups=self.group)
        self.assertEqual(qs.count(), 1)

    @withattrs(TestModel.Elasticsearch, 'fields', ['id', 'date_joined'])
    def test_filter_date_range(self):
        contents = TestModel.es.filter(date_joined__gte=self.t2.date_joined).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

    def test_excluding(self):
        contents = TestModel.es.exclude(first_name=u"Jack").deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 not in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

        qs = TestModel.es.all().exclude(first_name__not=u"Jack")
        contents = qs.deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

        with self.assertRaises(NotImplementedError):
            TestModel.es.exclude(id__range=(0, 1))

    def test_excluding_lookups(self):
        contents = TestModel.es.exclude(id__gt=self.t2.id).deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

        contents = TestModel.es.exclude(id__lt=self.t2.id).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

        contents = TestModel.es.exclude(id__gte=self.t2.id).deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 not in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

        contents = TestModel.es.exclude(id__lte=self.t2.id).deserialize()
        self.assertTrue(self.t1 not in contents)
        self.assertTrue(self.t2 not in contents)
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 in contents)

    def test_chain_filter_exclude(self):
        contents = TestModel.es.filter(last_name=u"Smith").exclude(first_name=u"Jack").deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 not in contents)  # excluded
        self.assertTrue(self.t3 in contents)
        self.assertTrue(self.t4 not in contents)  # not a Smith

    @withattrs(TestModel.Elasticsearch, 'fields', ['id', 'username'])
    def test_contains(self):
        contents = TestModel.es.filter(username__contains='woot').deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t2 in contents)
        self.assertTrue(self.t3 not in contents)
        self.assertTrue(self.t4 not in contents)

    def test_should_lookup(self):
        contents = TestModel.es.all().filter(last_name__should=u"Smith").deserialize()
        self.assertTrue(self.t1 in contents)
        self.assertTrue(self.t4 not in contents)

    def test_nonzero(self):
        self.assertTrue(TestModel.es.all())

    def test_response(self):
        r = TestModel.es.all().response
        # Note: don't make assumptions about what is returned for now
        self.assertTrue(type(r) is dict)

    def test_clone_query(self):
        q = TestModel.es.all()
        q2 = q.filter(first_name="John")
        q3 = q.filter(first_name="Jack")

        self.assertEqual(q.count(), 4)
        self.assertEqual(q2.count(), 1)
        self.assertEqual(q3.count(), 1)

    @override_settings(ELASTICSEARCH_CONNECTION_KWARGS={'max_retries': 0})
    def test_custom_client_connection_kwargs(self):
        # naive way to test this,
        # would be cool to find a way to test that it's actually taken into account
        from django_elasticsearch import client as test_client
        import importlib
        importlib.reload(test_client)
        self.assertTrue(test_client.es_client.ping())

    def test_extra(self):
        q = TestModel.es.search("Jack").extra({
            "highlight": {
                "fields" : {
                    "first_name" : {}
                }
            }
        })

        self.assertTrue(q.count(), 2)
        hl = q.response['hits']['hits'][0]['highlight']['first_name'][0]
        self.assertEqual(hl, '<em>Jack</em>')

        # make sure it didn't break the query otherwise
        self.assertTrue(q.deserialize())

    # some attributes were missing on the queryset
    # raising an AttributeError when passed to a template
    def test_qs_attributes_from_template(self):
        qs = self.t1.es.all().order_by('id')
        t = Template("{% for e in qs %}{{e.username}}. {% endfor %}")
        expected = u'woot woot. woot. BigMama. foo. '
        result = t.render(Context({'qs': qs}))
        self.assertEqual(result, expected)

    def test_prefetch_related(self):
        with self.assertRaises(NotImplementedError):
            TestModel.es.all().prefetch_related()

    def test_range_plus_must(self):
        q = TestModel.es.filter(date_joined__gt='now-10d').filter(first_name="John")
        self.assertEqual(q.count(), 1)
