import happybase
import time, random
 
# Number of test iterations
ITERATIONS = 10000
 
# Make the HBase c
c = happybase.Connection('127.0.0.1')
 
# Create the test table, with a column family
c.create_table('web', {
    'h':dict(max_versions=3, compression='SNAPPY', block_cache_enabled=True),
    'p':dict(max_versions=3, compression='SNAPPY', block_cache_enabled=True),
    'x':dict(max_versions=3, compression='SNAPPY', block_cache_enabled=True)
  })
 
