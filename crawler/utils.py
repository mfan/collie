from md5 import md5

##
## url shall be normalized and validated
##
def url2key(url):
    l = url.split('/', 3)
    netloc = l[2]
    old = netloc.split(':')[0]
    new = '.'.join(reversed(old.split('.')))
    l[2] = netloc.replace(old, new)
    
    key = '/'.join(l)

    return key

def key2url(key):
    return url2key(key)

##
## url shall be normalized and validated
##
def getuid(url):
    return md5(url).hexdigest()


