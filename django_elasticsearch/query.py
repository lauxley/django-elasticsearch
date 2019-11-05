import copy
import logging
from datetime import datetime

from django.conf import settings
from django.db.models import Model
from django.db.models.query import QuerySet
from django.db.models.query import REPR_OUTPUT_SIZE

from django_elasticsearch.client import es_client
from django_elasticsearch.utils import nested_update

logger = logging.getLogger(__name__)


class EsQueryset(QuerySet):
    """
    Fake Queryset that is supposed to act somewhat like a django Queryset.
    """
    MODE_SEARCH = 1
    MODE_MLT = 2

    def __init__(self, model, fuzziness=None):
        self.model = model
        self.index = model.es.index

        # config
        self.mode = self.MODE_SEARCH
        self.mlt_kwargs = None
        self.filters = {}
        self.extra_body = None
        self.facets_fields = None
        self.suggest_fields = None
        self.suggest_limit = None
        
        # model.Elasticsearch.ordering -> model._meta.ordering -> _score
        if hasattr(self.model.Elasticsearch, 'ordering'):
            self.ordering = self.model.Elasticsearch.ordering
        else:
            self.ordering = getattr(self.model._meta, 'ordering', None)
        
        if fuzziness is None:  # beware, could be 0
            self.fuzziness = getattr(settings, 'ELASTICSEARCH_FUZZINESS', 0.5)
        else:
            self.fuzziness = fuzziness
        
        self.ndx = None
        self._query = ''
        self._deserialize = False

        self._start = 0
        self._stop = None

        # results
        self._suggestions = None
        self._facets = None
        self._result_cache = []  # store
        self._total = None

    def __deepcopy__(self, memo):
        """
        Deep copy of a QuerySet doesn't populate the cache
        """
        obj = self.__class__(self.model)
        for k, v in self.__dict__.items():
            if k not in ['_result_cache', '_facets', '_suggestions', '_total']:
                obj.__dict__[k] = copy.deepcopy(v, memo)
        return obj

    def _clone(self):
        # copy everything but the results cache
        clone = copy.deepcopy(self)  # deepcopy because .filters is immutable
        # clone._suggestions = None
        # clone._facets = None
        clone._result_cache = []  # store
        clone._total = None
        return clone

    def __iter__(self):
        self.do_search()
        for r in self._result_cache:
            yield r

    def __str__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return str(data)

    def __getitem__(self, ndx):
        self._result_cache = []
        
        if type(ndx) is slice:
            self._start = ndx.start or 0  # in case it is None because [:X]
            self._stop = ndx.stop
        elif type(ndx) is int:
            self._start = ndx
            self._stop = ndx + 1
        
        self.do_search()
        
        if type(ndx) is slice:
            return self._result_cache
        elif type(ndx) is int:
            # Note: 0 because we only fetch the right one
            return self._result_cache[0]

    def __contains__(self, instance):
        self.do_search()
        return instance.id in [e.id if self._deserialize else e["id"]
                               for e in self._result_cache]

    def __and__(self, other):
        raise NotImplementedError

    def __or__(self, other):
        raise NotImplementedError

    def __nonzero__(self):
        self.count()
        return self._total != 0

    def __len__(self):
        self.do_search()
        return len(self._result_cache)

    def make_search_body(self):
        search = {}
        if self._query:
            search = {'must': [{
                'multi_match' : {
                    'query': self._query,
                    'fields': self.model.es.get_search_fields(),
                    'fuzziness': self.fuzziness if self.fuzziness is not None else 'AUTO'
                }
            }]}
        elif not self.filters:
            search = {'must': [{'match_all': {}}]}
        
        if self.filters:
            for field, value in self.filters.items():
                try:
                    value = value  #.lower()
                except AttributeError:
                    pass
                
                field, operator = self.sanitize_lookup(field)
                field_ = field.split('.')[0]
                mapping = self.model.es.mapping['properties']
                is_nested = field_ in mapping and mapping[field_]['type'] == 'nested'
                
                if is_nested and isinstance(value, Model):
                    field_name = field + ".id"
                    value = value.id
                else:
                    field_name = field
                
                if isinstance(value, datetime):
                    value = value.isoformat()
                
                if operator in ['must', 'must_not', 'should']:
                    mode = 'match' if (field_ in mapping and mapping[field_]['type'] == 'text') else 'term'
                    filtr = {operator: [{mode: {field_name: value}}]}
                
                elif operator in ['gt', 'gte', 'lt', 'lte']:
                    filtr = {'must': [{'range': {field_name: {operator: value}}}]}
                
                elif operator == 'range':
                    filtr = {'must': [{'range': {field_name: {'gte': value[0],'lte': value[1]}}}]}
                
                elif operator == 'isnull':
                    if value:
                        filtr = {'must_not': [{'exists': {'field': field_name}}]}
                    else:
                        filtr = {'must': [{'exists': {'field': field_name}}]}
                
                elif operator == 'exists':
                    if value:
                        filtr = {'must': [{'exists': {'field': field_name}}]}
                    else:
                        filtr = {'must_not': [{'exists': {'field': field_name}}]}
                
                else:
                    raise ValueError("Unrecognized lookup '{operator}'".format(operator=operator))
                
                if is_nested:
                    filtr = {'must': [{'nested': {'path': field.split('.')[0],
                                                  'query': {'bool': filtr}}}]}

                nested_update(search, filtr)

        body = {"query": {'bool': search}}
        logger.info('Search query {}.'.format(body))
        return body

    @property
    def is_evaluated(self):
        return len(self._result_cache) > 0

    @property
    def response(self):
        self.do_search()
        return self._response

    def _fetch_all(self):
        self.do_search()

    def do_search(self):
        if self.is_evaluated:
            return
        
        body = self.make_search_body()
        if self.facets_fields:
            aggs = {}

            for field in self.facets_fields:
                aggs[field] = {'terms': {'field': field}}
                # if self.global_facets:
                #     aggs[field]['filter'] = body['query']['filtered']
            if self.facets_limit:
                aggs[field]['terms']['size'] = self.facets_limit

            body['aggs'] = aggs
        
        if self.suggest_fields:
            suggest = {}
            for field_name in self.suggest_fields:
                suggest[field_name] = {"text": self._query,
                                       "term": {"field": field_name}}
                if self.suggest_limit:
                    suggest[field_name]["term"]["size"] = self.suggest_limit
            body['suggest'] = suggest
        
        if self.ordering:
            body['sort'] = [{f: "asc"} if f[0] != '-' else {f[1:]: "desc"}
                            for f in self.ordering] + ["_score"]
        
        search_params = {
            'index': [self.index,]
        }
        if self._start:
            search_params['from'] = self._start
        if self._stop:
            search_params['size'] = self._stop - self._start
        
        if self.extra_body:
            body.update(self.extra_body)
        search_params['body'] = body
        self._body = body
        
        if self.mode == self.MODE_MLT:
            # change include's defaults to False
            # search_params['include'] = self.mlt_kwargs.pop('include', False)
            # # update search params names
            # search_params.update(self.mlt_kwargs)
            
            search_params['more_like_this'] = {
                'fields': self.mlt_kwargs.get('fields', '_all'),
                'like': [{
                    # '_index': self.index,
                    '_id': self.mlt_kwargs.get('id')
                }],
                'min_term_frequency': self.mlt_kwargs.get('min_term_frequency', 1),
                'max_term_frequency': self.mlt_kwargs.get('max_term_frequency', 10),
            }
            # search_params.update(self.mlt_kwargs)
            # for param in ['type', 'indices', 'types', 'scroll', 'size', 'from']:
            #     if param in search_params:
            #         search_params['search_{0}'.format(param)] = search_params.pop(param)
        else:
            if 'from' in search_params:
                search_params['from_'] = search_params.pop('from')
        r = es_client.search(**search_params)

        self._response = r
        if self.facets_fields:
            # if self.global_facets:
            #     self._facets = r['aggregations']['global_count']
            # else:
            self._facets = r['aggregations']

        self._suggestions = r.get('suggest')
        if self._deserialize:
            self._result_cache = [self.model.es.deserialize(e['_source'])
                                  for e in r['hits']['hits']]
        else:
            self._result_cache = [e['_source'] for e in r['hits']['hits']]
        self._max_score = r['hits']['max_score']
        
        self._total = r['hits']['total']['value']
        
        return

    def search(self, *args, **kwargs):
        """
        By default search upon all analyzed fields:
        search('query')
        Do the match on a specific field:
        search(field1='query', field2='query2')
        """
        if (not args and not kwargs) or (args and len(args) > 1):
            raise TypeError("Invalid arguments for search() supply one query or query on a subset of fields: search('query') or search(field='query')")

        clone = self._clone()
        if len(args):
            clone._query = args[0]
        if len(kwargs):
            clone.filters.update(kwargs)
        
        return clone

    def facet(self, fields, limit=None, use_globals=True):
        # TODO: bench global facets !!
        clone = self._clone()
        clone.facets_fields = fields
        clone.facets_limit = limit
        clone.global_facets = use_globals
        return clone

    def suggest(self, fields, limit=None):
        clone = self._clone()
        clone.suggest_fields = fields
        clone.suggest_limit = limit
        return clone

    def order_by(self, *fields):
        clone = self._clone()
        clone.ordering = fields
        return clone

    def filter(self, **kwargs):
        clone = self._clone()
        clone.filters.update(kwargs)
        return clone

    def sanitize_lookup(self, lookup):
        valid_operators = ['must', 'must_not', 'should',
                           'range', 'gt', 'lt', 'gte', 'lte',
                           'exists', 'isnull']
        words = lookup.split('__')
        fields = [word for word in words
                  if word not in valid_operators]
        operator = 'must'
        
        if words[-1] in valid_operators:
            operator = words[-1]
        if operator == 'isnull':
            logger.warning('isnull is not an Elasticsearch lookup and may be drop in the future to avoid confusion, please use exists instead.')
        
        return '.'.join(fields), operator
    
    def exclude(self, **kwargs):
        clone = self._clone()

        filters = {}
        for lookup, value in kwargs.items():
            field, operator = self.sanitize_lookup(lookup)
            if operator == 'must':
                filters['{0}__must_not'.format(field)] = value
            elif operator == 'must_not':
                filters[field] = value
            elif operator == 'should':
                # Note: a bit unclear what exclude..should means
                filters['{0}__must_not'.format(field)] = value
            elif operator in ['gt', 'gte', 'lt', 'lte']:
                inverse_map = {'gt': 'lte', 'gte': 'lt', 'lt': 'gte', 'lte': 'gt'}
                filters['{0}__{1}'.format(field, inverse_map[operator])] = value
            elif operator == 'exists':
                filters[lookup] = not value
            elif operator == 'isnull':
                filters[lookup] = not value
            elif operator == 'range':
                filters[lookup] = (value[1], value[0])
            else:
                raise NotImplementedError("{0} is not a valid *exclude* lookup type.".format(operator))
        
        clone.filters.update(filters)
        return clone

    ## getters
    def all(self):
        clone = self._clone()
        return clone

    def get(self, **kwargs):
        pk = kwargs.get('pk', None) or kwargs.get('id', None)

        if pk is None:
            # maybe it's in a filter, like in django.views.generic.detail
            pk = self.filters.get('pk', None) or self.filters.get('id', None)

        if pk is None:
            raise AttributeError("EsQueryset.get needs to get passed a 'pk' or 'id' parameter.")

        r = es_client.get(index=self.index, id=pk)
        self._response = r

        if self._deserialize:
            return self.model.es.deserialize(r['_source'])
        else:
            return r['_source']

    def mlt(self, id, **kwargs):
        self.mode = self.MODE_MLT
        self.mlt_kwargs = kwargs
        self.mlt_kwargs['id'] = id
        return self

    def complete(self, field_name, query):
        resp = es_client.search(index=[self.index,],
                                body={
                                    'suggest': {
                                        field_name: {
                                            'prefix': query ,
                                            'completion': {'field': field_name}
                                        }
                                    }
                                })
        try:
            return [r['text'] for r in resp['suggest'][field_name][0]['options']]
        except KeyError:
            # no results??
            return []
    
    def update(self):
        raise NotImplementedError("Db operational methods have been "
                                  "disabled for Elasticsearch Querysets.")

    def delete(self):
        raise NotImplementedError("Db operational methods have been "
                                  "disabled for Elasticsearch Querysets.")

    @property
    def facets(self):
        self.do_search()
        return self._facets

    @property
    def suggestions(self):
        self.do_search()
        return self._suggestions

    def count(self):
        # if we pass a body without a query, elasticsearch complains
        if self._total:
            return self._total
        if self.mode == self.MODE_MLT:
            # Note: there is no count on the mlt api, need to fetch the results
            self.do_search()
        else:
            body = self.make_search_body() or None            
            r = es_client.count(
                index=self.index,
                body=body)
            self._total = r['count']
        return self._total

    def deserialize(self):
        self._deserialize = True
        return self

    def extra(self, body):
        # Note: will .update() the body of the query
        # so it is possible to override anything
        clone = self._clone()
        clone.extra_body = body
        return clone

    def prefetch_related(self):
        raise NotImplementedError(".prefetch_related is not available for an EsQueryset.")
