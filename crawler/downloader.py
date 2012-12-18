#!/usr/bin/python

import sys
import urllib2
from redirect import RedirectHandler

class Downloader(object):
    def __init__(self):
        ## add redirection tracing.
        opener = urllib2.build_opener(RedirectHandler)
        urllib2.install_opener(opener)
    
    def get(self, url):
        redirected=False

        try:
            f = urllib2.urlopen(url)
            header = f.headers.dict
            content = f.read()
            if hasattr(f, 'redirect_path'):
                ## add redirect_path
                header.update({'redirect_path':f.redirect_path})
                redirected = True
            header.update({'code':f.code, 'status': 'OK'})
            print '%s: header: %r' % (url, header)
            print '%s: content: %s bytes: %r' % (url, len(content), content[:50])

        except:
            e = sys.exc_info()
            print '%s: failed:' % (url,)
            for i in e:
                print '\t%s: %s' % (url, i)


if __name__ == "__main__":
    dl = Downloader()
    dl.get("http://www.facebook.com")
