#!/usr/bin/python

import sys
import urllib2
from redirect import RedirectHandler
from contents import ContentStore
import ujson

class Downloader(object):
    def __init__(self):
        ## add redirection tracing.
        opener = urllib2.build_opener(RedirectHandler)
        urllib2.install_opener(opener)
        self.store = ContentStore()
    
    def get(self, url):
        redirected=False

        try:
            f = urllib2.urlopen(url)
            header = f.headers.dict
            content = f.read()
            if hasattr(f, 'redirect_path'):
                ## add redirect_path
                header.update({'redirect_path':ujson.dumps(f.redirect_path)})
                redirected = True
            header.update({'code':f.code, 'status': 'OK'})
            print '%s: header: %r' % (url, header)
            print '%s: content: %s bytes: %r' % (url, len(content), content[:50])
            self.store.append(url, header=header, content=content, redirected=redirected)
        except:
            e = sys.exc_info()
            print '%s: failed:' % (url,)
            error_info = ''
            for i in e:
                print '\t%s: %s' % (url, i)
                error_info += "%s;" % i
            header = {'error_info': error_info}
            self.store.append(url, status='FAILED', code=None, header=header)

if __name__ == "__main__":
    dl = Downloader()
    dl.get("http://dajafa.com")
    dl.get("http://dajafa.com/region/new-jersey/")
