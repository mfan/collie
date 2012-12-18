from md5 import md5

import urlnorm

def url2key(url):

    ## normalize and validate the url
    try:
        url = urlnorm.norm(url)
    except urlnorm.InvalidUrl:
        return None

    l = url.split('/', 3)
    netloc = l[2]
    old = netloc.split(':')[0]
    new = '.'.join(reversed(old.split('.')))
    l[2] = netloc.replace(old, new)
    
    key = '/'.join(l)

    return key

def key2url(key):
    return url2key(key)

def getuid(url):
    ## normalize and validate the url
    try:
        url = urlnorm.norm(url)
        return md5(url).hexdigest()
    except urlnorm.InvalidUrl:
        raise ValueError("invalid url")


