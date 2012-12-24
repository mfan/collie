import redis
from utils import getuid

## default expiration is one months
CRAWL_CACHE_DEFAULT_EXPIRATION = 86400 * 30

class CrawlCache(object):

    def __init__(self, redis_cli=None):
        if redis_cli:
            self.redis = redis_cli
        else:
            self.redis = redis.Redis()

    ## status = 'OK' or 'FAILED'
    ## code = HTTP error code, or error info for failures.
    def add(self, url, crawl_time, status, code=None, expiration=None):
        key = 'crawl:cache:%s' % getuid(url)

        value = '%s|%s' % (crawl_time, status)
        if code:
            value += '|%s' % code

        if not expiration:
            expiration = CRAWL_CACHE_DEFAULT_EXPIRATION

        return self.redis.setex(key, value, expiration)

