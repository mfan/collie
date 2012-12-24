import happybase
import gevent
from gevent.queue import Queue
from gevent import Greenlet

from utils import url2key

CONTENT_TABLE_NAME = 'web'
COLUMN_FAMILY_META = 'h'
COLUMN_FAMILY_PAGE = 'p'
COLUMN_FAMILY_XAPP = 'x'

def prefix_key(prefix, dict_data): return dict(("%s:%s" % (prefix, str(k)) , v) for k, v in dict_data.iteritems())

class ContentStore(Greenlet):
    def __init__(self, *args, **kwargs):
        '''
        ContentStore takes the same named arguments for happybase.Connection().
        All parameters are passed to happybase.Connection transparently.
        '''
        self.inbox = Queue()

        self.connection = happybase.Connection(*args, **kwargs)
        self.name = CONTENT_TABLE_NAME
        self.table = self.connection.table(self.name)

        Greenlet.__init__(self)

    def append(self, url, header=None, content=None):
        return self.inbox.put((url, header, content))

    def _send(self, url, header=None, content=None):
        '''
        ContentStore API to crawler is a simple append store. 
         * url: the url being downloaded
         * status: 'OK', or 'FAIL'
         * header: HTTP headers, a dictionary, with 'redirect_path'
         * content: page content
         * code: HTTP codes, e.g. 200, 404, etc.
        '''
        
        ## cook the url into row-key
        key = url2key(url)

        ## prepare row-data
        value = {}

        if header:
            value.update(prefix_key(COLUMN_FAMILY_META, header)) 
        if 'redirect_path' in header:
            value.update(prefix_key(COLUMN_FAMILY_META, {'redirected':'T'}))

        if content:
            value.update(prefix_key(COLUMN_FAMILY_PAGE, {'c':content}))

        ## store the row into hbase.
        self.table.put(key, value)

    def _run(self):
        self.running = True

        while self.running:
            record = self.inbox.get()
            self._send(*record)
