from datetime import datetime
from collections import Mapping
from pprint import pprint

from elasticsearch import Elasticsearch, helpers


__all__ = ['EchoOutPut', 'ElasticOutPut']


def EchoOutPut(cache):
    """
    only print
    """
    for l in cache:
        pprint(l)


class ElasticOutPut:
    """
    elasticsearch output
    """
    def __init__(self, index, *args, doc_type='log', **kwargs):
        self.es = Elasticsearch(*args, **kwargs)
        self.index = index
        self.doc_type = doc_type

    def _escache(self, cache):
        for i in cache:
            t = datetime.utcnow()
            if isinstance(i, Mapping):
                t = i.get('@timestamp', t)
                index = t.strftime(self.index)
                yield {'_index': index, '_type': self.doc_type, '_source': i}
            else:
                data = {'@timestamp': t, 'message': str(i)}
                index = t.strftime(self.index)
                yield {'_index': index, '_type': self.doc_type, '_source': data}

    def __call__(self, cache):
        helpers.bulk(self.es, self._escache(cache))
