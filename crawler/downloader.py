#!/usr/bin/python

import sys
import requests
from requests.exceptions import *
import ujson
from contents import ContentStore

DEFAULT_HTTP_TIMEOUT = 30

class Downloader(object):
    def __init__(self):
        self.store = ContentStore()
    
    def get(self, url):
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
            header.update({'code':str(resp.status_code), 'status': status})

            content = resp.content

            print '%s: header: %r' % (url, header)
            if content:
                print '%s: content: %s bytes: %r' % (url, len(content), content[:50])
            self.store.append(url, header=header, content=content)
        except RequestException as x:
            header = { 'status': 'FAILED', 'soft_error': 'T', 'error_type': 'request_exceptions', 'error_detail': '%s' % x }
            print '%s: FAILED: header: %r' % (url, header)
            self.store.append(url, header=header)


if __name__ == "__main__":
    dl = Downloader()
    dl.get("http://mail.google.com/")
