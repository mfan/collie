#!/usr/bin/python
"""Stateless downloader.
"""
from __future__ import with_statement
import os
import sys
import time
from sys import argv
import gevent
from gevent import socket
from gevent.pool import Pool
from gevent.event import AsyncResult
from gevent.queue import Queue, Empty
import commands

from gevent import monkey
monkey.patch_socket()

from sched import Scheduler, SLOT_HEARTBEAT_INTERVAL
from downloader import Downloader

sched = Scheduler()
fetcher = Downloader()

def download(url, qps):
    start = time.time() 
    fetcher.get(url)
    end = time.time()

    ## we treat sleep_time less than 1ms as 0, just yield once.
    sleep_time = 1.0/qps - (end - start)
    if sleep_time < 0.1: sleep_time = 0

    ## we always yield for gevent scheduling after download.
    gevent.sleep(sleep_time)

def slot(machine_id, pack_id, slot_id):
    """
    """
    ## get batch from URL store
    slot_key = "slot:%s:%s:%s" % (machine_id, pack_id, slot_id)
    batch = []
    while True:
        qps, batch = sched.getBatch(slot_key)
        if batch:
            print '+++++++++++++++++++++++++++++++++++++++++: %s : %s of urls in batch' % (qps, len(batch))
            for url in batch:
                download(url, qps)
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
    lines = commands.getoutput("ifconfig").split("\n")
    inets = [line for line in lines if line.strip().startswith('inet')]
    ip = None
    for i in inets:
        ip = i.split()[1].split(':')[1]
        if ip: break
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

