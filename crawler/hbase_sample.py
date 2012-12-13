import happybase
import time, random
 
# Number of test iterations
ITERATIONS = 10000
 
# Make the HBase connection
connection = happybase.Connection('127.0.0.1')
 
# Create the test table, with a column family
connection.create_table('mytable', {'cf':{}})
 
# Open the table
table = connection.table('mytable')
 
# Iterate with a counter increment
starttime = time.clock()
for i in range(ITERATIONS):
  number = str(random.randint(1, 10))
  table.counter_inc('row-key', 'cf:counter' + str(number))
 
totaltime = time.clock() - starttime
print "Counter for " + str(ITERATIONS) + " times took: " + str(totaltime) + " seconds"
 
# Iterate with a put
starttime = time.clock()
for i in range(ITERATIONS):
  number = str(random.randint(1, 10))
  table.put('row-key', {'cf:number' + str(number): '10'})
 
totaltime = time.clock() - starttime
print "Put for " + str(ITERATIONS) + " times took: " + str(totaltime) + " seconds"
 
# Disable and delete the table
connection.disable_table('mytable')
connection.delete_table('mytable')
