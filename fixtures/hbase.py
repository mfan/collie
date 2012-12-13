import happybase
 
c = happybase.Connection('127.0.0.1')
 
##
## create web page store table
##  
##  'h' column family: 'Headers', including HTTP headers, and other downloading info about the web page.
##  'p' column family: 'Page Content', the HTML page content.
##  'x' column family: 'Application related extensions', application related, additional fields.
## 
##  note the versions of 'h' and 'p' shall be aligned, using the same timestamp.
##
c.create_table('web', {
    'h':dict(max_versions=3, compression='SNAPPY', block_cache_enabled=True),
    'p':dict(max_versions=3, compression='SNAPPY', block_cache_enabled=True),
    'x':dict(max_versions=1, compression='SNAPPY', block_cache_enabled=True),
  })
 
