from datetime import datetime
from collections import Mapping
import logging

from elasticsearch import Elasticsearch, helpers


class ElasticOutPut:
    def __init__(self, index, *args, doc_type='log', **kwargs):
        self.es = Elasticsearch(*args, **kwargs)
        self.index = index
        self.doc_type = doc_type

    def _escache(self, cache):
        for i in cache:
            t = datetime.utcnow()
            if isinstance(i, str):
                data = {'@timestamp': t, 'message': i}
                index = t.strftime(self.index)
                yield {'_index': index, '_type': self.doc_type, '_source': data}
            elif isinstance(i, Mapping):
                t = i.get('@timestamp', t)
                index = t.strftime(self.index)
                yield {'_index': index, '_type': self.doc_type, '_source': i}
            else:
                logging.error('unsupport format')
                break

    def __call__(self, cache):
        helpers.bulk(self.es, self._escache(cache))
