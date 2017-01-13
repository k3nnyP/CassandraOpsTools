#!/bin/python
import os
import time
import socket
import threading
import Queue
import sys
import re
# Set STDOUT buffer to 0
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

username='cassandra'
password='cassandra'
collected=time.time()
msg = ""
graphite_host='xxxxxx.xxxx'
graphite_port=2003
nt='/usr/local/cassandra/bin/nodetool'
threads = {}
infoTimeout=45
q = Queue.Queue();

#
# nodetool info proc for threads
#
def nt_info(cn, dc, node, collected ):
  cmd = 'timeout %d %s -u %s -pw %s -h %s info ' % ( infoTimeout, nt, username, password, node )
  with os.popen(cmd) as pipe:
    for line in pipe:
      if line.startswith('Gossip active'):
        a = line.split(':')
        status = a[1].lstrip().rstrip()
        if status == 'true':
          status = 1
        elif status == 'false':
          status = 0
        msg = "cassandra.%s.%s.%s.GossipActive %d %d" % (cn,dc,node.replace('.','_'),status,collected)
        q.put(msg)
      elif line.startswith('Thrift active'):
        a = line.split(':')
        status = a[1].lstrip().rstrip()
        if status == 'true':
          status = 1
        elif status == 'false':
          status = 0
        msg = "cassandra.%s.%s.%s.ThriftActive %d %d" % (cn,dc,node.replace('.','_'),status,collected)
        q.put(msg)
      elif line.startswith('Native Transport active'):
        a = line.split(':')
        status = a[1].lstrip().rstrip()
        if status == 'true':
          status = 1
        elif status == 'false':
          status = 0
        msg = "cassandra.%s.%s.%s.NativeTransportActive %d %d" % (cn,dc,node.replace('.','_'),status,collected)
        q.put(msg)
      elif line.startswith('Heap Memory'):
        a = re.split(':|/',line) 
        heapUsed = float(a[1].lstrip().rstrip())
        heapMax = float(a[2].lstrip().rstrip())
        heapPerc = (heapUsed/heapMax)*100
        msg = "cassandra.%s.%s.%s.heapUsed %d %d" % (cn,dc,node.replace('.','_'),heapUsed,collected)
        q.put(msg)
        msg = "cassandra.%s.%s.%s.heapMax %d %d" % (cn,dc,node.replace('.','_'),heapMax,collected)
        q.put(msg)
        msg = "cassandra.%s.%s.%s.heapUsedPerc %d %d" % (cn,dc,node.replace('.','_'),heapPerc,collected)
        q.put(msg)
  return

# Get Cluster Name
with os.popen(nt + ' -u ' + username + ' -pw ' + password + ' describecluster | egrep \'(Name)\' ') as pipe:
  for line in pipe:
    a = line.split(':')
    cn = a[1].lstrip().rstrip()

# Get Cluster Node Status
with os.popen(nt + ' -u ' + username + ' -pw ' + password + ' status | egrep \'(Datacenter|^[UD][NLJM])\'') as pipe:
  for line in pipe:
    if line.startswith('Datacenter'):
      a = line.split(':')
      dc = a[1].lstrip().rstrip()
    else:
      a = line.split()
      node=a[1]
      if a[0].startswith('U'):
        down = 0
      elif a[0].startswith('D'):
        down = 1
      if a[0][1] is 'N':
        #Normal
        status = 0
      elif a[0][1] is 'L':
        #Leaving
        status = 1
      elif a[0][1] is 'J':
        #Joining
        status = 2
      elif a[0][1] is 'J':
        #Moving
        status = 3
      msg = msg + "cassandra.%s.%s.%s.down %d %d\n" % (cn,dc,node.replace('.','_'),down,collected)
      msg = msg + "cassandra.%s.%s.%s.status %d %d\n" % (cn,dc,node.replace('.','_'),status,collected)
      #
      # Launch thread to run nodetool info
      #
      threads[node] = threading.Thread(target=nt_info, args=(cn, dc, node, collected,))
      print "[ " + node + " ] - Starting"
      threads[node].start()

# Wait for all threads to complete
while ( threads.__len__() > 0 ):
  for t in threads.keys():
    threads[t].join(2)
    if not threads[t].isAlive():
      del threads[t]
      print "[ " + t + " ] - Complete"
while not q.empty():
  msg = msg + q.get() + "\n"

# Send message to graphite
print "Sending to Graphite \n" + msg + "\n\n"
sock = socket.socket()
sock.connect((graphite_host, graphite_port))
sock.sendall(msg)
sock.close()
print "Exiting."




