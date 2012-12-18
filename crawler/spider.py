#!/usr/bin/python
"""Stateless downloader.
"""
from __future__ import with_statement
import os
import sys
from sys import argv
import gevent
from gevent import socket
from gevent.pool import Pool
from gevent.event import AsyncResult
from gevent.queue import Queue, Empty
import commands

from sched import Scheduler, SLOT_HEARTBEAT_INTERVAL

from gevent import monkey
monkey.patch_socket()
import urllib2
from redirect import RedirectHandler
from downloader import Downloader

sched = Scheduler()
fetcher = Downloader()

def download(url):
    print 'processing %s' % url
    result = fetcher.get(url)
    print result

    ### 
    ### process the downloaded data:
    ###  * store the (http_header, data) or failure code into HBase,
    ###  * update crawl cache.

def slot(machine_id, pack_id, slot_id):
    """
    """
    ## get batch from URL store
    slot_key = "slot:%s:%s:%s" % (machine_id, pack_id, slot_id)
    batch = []
    while True:
        qps, batch = sched.getBatch(slot_key)
        if batch:
            print '+++++++++++++++++++++++++++++++++++++++++: %s : %s' % (qps, batch)
            for url in batch:
                download(url)
            sched.ackBatch(slot_key)
        else:
            gevent.sleep(3)

def monitor(slot_list):
    """
    write a heartbeat signature to redis every SLOT_HEARTBEAT_INTERVAL.
    Note we can also report stats to scheduler here, for example:
      * round trip info for recent downloads.
      * failure rate per host ...
    """
    while True:
        gevent.sleep(SLOT_HEARTBEAT_INTERVAL)
        sched.reportSlots(slot_list)

def main():
    if len(argv) != 2:
        print >> sys.stdout, "Usage: %s <pack_id>" % (os.path.basename(argv[0]), )
        sys.exit(-1)

    MAX_WORKER = 5
    pool = Pool(MAX_WORKER+1)

    ## machine id
    ip = commands.getoutput("ifconfig").split("\n")[1].split()[1].split(':')[1]
    if not ip:
        raise "Need to get IP address as machine Id!"

    ## pack id
    pack_id = 0
    try:
        pack_id = int(argv[1])
    except ValueError:
        print >> sys.stdout, "Usage: %s <pack_id>" % (os.path.basename(argv[0]), )
        sys.exit(-1)

    ## set up machine table
    sched.addMachine(ip=ip, external_ip=ip, name='e6520', external_name='e6520', num_pack=1, num_slot=5, enabled=True)

    sched.restart()

    slot_list = []
    for i in range(1, MAX_WORKER + 1):
        slot_key = "slot:%s:%s:%s" % (ip, pack_id, i)
        slot_list.append(slot_key)
        pool.spawn(slot, ip, pack_id, i)

    ## register downloader slots
    sched.addSlots(slot_list)

    ## local management thread
    pool.spawn(monitor, slot_list)

    pool.join()


if __name__ == "__main__":
    main()

