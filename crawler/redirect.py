import sys
import urllib2

class RedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_301(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_301( 
            self, req, fp, code, msg, headers)

        newurl = headers.dict['location']
        redirect_path = [(code, newurl)]
        if hasattr(result, 'redirect_path'):
            result.redirect_path.insert(0, redirect_path)
        else:
            result.redirect_path = redirect_path

        return result
    http_error_300=http_error_301
    http_error_302=http_error_301
    http_error_303=http_error_301
    http_error_307=http_error_301


if __name__ == "__main__":
    opener = urllib2.build_opener(RedirectHandler)
    urllib2.install_opener(opener)
    urls = ['http://www.facebook.com', 'http://www.msn.com', 'http://gmail.com/']
    for url in urls:
        try:
            f = urllib2.urlopen(url)

            if hasattr(f, 'redirect_path'):
                print "redirect_path: ", f.redirect_path

            print "url: %s" % f.url
            print "content: %s" % f.read()[:50]
        except urllib2.HTTPError, x:
            print "failed: %s" % x

