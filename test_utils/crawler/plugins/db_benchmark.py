import logging

from django.conf import settings
from django.db import connection

from base import Plugin

LOG = logging.getLogger('crawler')


class DBBenchmark(Plugin):
    """
    Database benchmark - provides information about request queries - total number,
    duplicates and total time.
    """

    def __init__(self):
        super(DBBenchmark, self).__init__()
        self._duplicated_q = 0

    def pre_request(self, sender, **kwargs):
        settings.DEBUG = True

    def post_request(self, sender, **kwargs):
        url = kwargs['url']
        queries = connection.queries
        settings.DEBUG = False

        q_len = len(queries)
        q_time = sum([float(q['time']) for q in queries])
        LOG.info("%s %d queries, took %f", url, q_len, q_time)

        unique_q_len = len(set([q['sql'] for q in queries]))
        if q_len > unique_q_len:
            LOG.warning("%s has %d duplicated queries", url, q_len-unique_q_len)
            self._duplicated_q += 1

    def finish_run(self, sender, **kwargs):
        LOG.info("%d duplicated queries detected.", self._duplicated_q)

PLUGIN = DBBenchmark
