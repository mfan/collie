#!/usr/bin/python

import sys
import time
import requests
from requests.exceptions import *
import ujson

DEFAULT_HTTP_TIMEOUT = 30

class Downloader(object):
    def __init__(self, content_store, crawl_cache):
        self.store = content_store
        self.cache = crawl_cache
    
    def get(self, url):
        status = ''
        code = ''
        header = content = None
        start_time = time.time()
        host_error = False
        try:
            resp = requests.get(url, timeout=DEFAULT_HTTP_TIMEOUT)
            header = resp.headers
            if resp.history:
                ## add redirect_path
                redirect_path = []
                for i in resp.history:
                    redirect_path.append((i.status_code, i.headers['location']))
                header.update({'redirect_path':ujson.dumps(redirect_path)})
            if resp.encoding:
                header.update({'encoding': resp.encoding})
            if resp.ok:
                status = 'OK'
            else:
                status = 'FAILED'
            if resp.status_code:
                code = str(resp.status_code)
            header.update({'code':code, 'status': status})

            content = resp.content

            ## host wide temporary failures. 
            if resp.status_code == 403 or resp.status_code > 500:
                host_error = True

            print '%s: header: %r' % (url, header)
            if content:
                print '%s: content: %s bytes: %r' % (url, len(content), content[:50])
        except RequestException as x:
            status = 'FAILED'
            code = '%s' % x   ### TODO: need to parse and categorize errors: DNS error, timeout, connection failure, etc.
            header = { 'status': status, 'soft_error': 'T', 'error_type': 'request_exceptions', 'error_detail': '%s' % x }
            print '%s: FAILED: header: %r' % (url, header)
            ret = host_error
        finally:
            end_time = time.time()

            ## record rtt: round-trip-time
            rtt = end_time - start_time
            header.update({'rtt':str(rtt)})

            ## store into content store
            self.store.append(url, header=header, content=content)
            ## store into crawl cache
            self.cache.add(url,end_time, status, code)

            ## 'host_error' indicating if we need to abort the batch
            return start_time, end_time, host_error


if __name__ == "__main__":
    import redis
    from contents import ContentStore
    from crawl_cache import CrawlCache

    r = redis.Redis()
    c = ContentStore()
    cache = CrawlCache(r)
    dl = Downloader(c, cache)
    start_time, end_time = dl.get("http://mail.google.com/")
    print "download takes %s seconds." % (end_time - start_time)
