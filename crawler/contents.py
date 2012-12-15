import happybase
import urlnorm
from .utils import url2key

CONTENT_TABLE_NAME = 'web'
COLUMN_FAMILY_META = 'h'
COLUMN_FAMILY_PAGE = 'p'
COLUMN_FAMILY_XAPP = 'x'

def prefix_key(prefix, dict_data): return dict("%s:%s" % (prefix, str(k)) , v)) for k, v in dict_data.iteritems())

class ContentStore(object):
    def __init__(self, *args, **kwargs):
        '''
        ContentStore takes the same named arguments for happybase.Connection().
        All parameters are passed to happybase.Connection transparently.
        '''
        self.connection = happybase.Connection(*args, **kwargs)
        self.name = CONTENT_TABLE_NAME
        self.table = self.connection.table(self.name)

    def append(self, url, status='OK', header=None, content=None, code='200', redirected=None):
        '''
        ContentStore API to crawler is a simple append store. 
         * url: the url being downloaded
         * status: 'OK', or 'FAIL'
         * header: HTTP headers, a dictionary, preferable from urllib2.addinfourl.dict
         * content: page content
         * code: HTTP codes, e.g. 200, 404, etc.
         * redirected: if redirected, it's a list of string '30x [space] url', for example: 
         *   ('301 http://example.com/redirected1', '302 http://example.com/redirected2', ...)
         *   redirections will be stored as 
         *     field: 'h:RedirectionPath'
         *     value: '\t'.join(redirected)
         *   for processing, two-level split using '\t' and ' ' shall work.
        '''
        
        ## cook the url into row-key
        ## 1. normalize the url
        url = urlnorm(url)

        ## 2. row-key
        key = url2key(url)

        ## 3. row-data
        value = {}

        if header:
            value.update(prefix_key(COLUMN_FAMILY_META, header)) 

        if content:
            value.update(prefix_key(COLUMN_FAMILY_PAGE, {'c':content}))

        assert(status in ('OK', 'FAIL'))
        value.update(prefix_key(COLUMN_FAMILY_META, {'status':status}))

        if code:
            value.update(prefix_key(COLUMN_FAMILY_META, {'code':code}))

        ## TODO: redirection is not fully supported yet. 
        if redirected:
            pass

        ## mfan stopped here.
