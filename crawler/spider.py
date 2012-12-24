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
import redis
from contents import ContentStore
from crawl_cache import CrawlCache
from downloader import Downloader

SPIDER_MAX_WORKER = 200

##
## Note: so far always make the spider pack 'enabled'.
##
class SpiderPack(object):
    def __init__(self, machine_id, pack_id, num_slot, **kwargs):
        self.machine_id = machine_id
        self.pack_id = pack_id

        if num_slot < 0 or num_slot > SPIDER_MAX_WORKER:
            raise ValueError("SpiderPack: invalid number of downloading slots: %s." % num_slot)
        self.slots = num_slot

        self.sched = kwargs.get('scheduler', Scheduler())
        self.redis = kwargs.get('redis', redis.Redis())
        self.store = kwargs.get('content_store', ContentStore())
        self.crawl_cache = kwargs.get('crawl_cache', CrawlCache(self.redis))

        self.downloader = Downloader(self.store, self.crawl_cache)
        
        ##
        ## two extra greenlets:
        ##  * ContentStore: an actor for handling Hbase append().
        ##  * local _monitor: to report the spider works.
        ##
        self.pool = Pool(self.slots+2)
        
    def run(self):

        slot_list = []
        for i in range(1, self.slots + 1):
            slot_key = "slot:%s:%s:%s" % (self.machine_id, self.pack_id, i)
            slot_list.append(slot_key)
            self.pool.spawn(self._worker, i)

        ## register downloader slots
        self.sched.addSlots(slot_list)

        ## local management thread
        self.pool.spawn(self._monitor, slot_list)
        ## content store actor thread
        self.pool.add(self.store)
        self.pool.start(self.store)

        self.pool.join()


    def _download(self, url, qps):
        start, end, need_abort = self.downloader.get(url)
        if need_abort: return need_abort

        crawl_delay = 1.0/qps

        ## we treat sleep_time less than 1ms as 0, just yield once.
        ##
        sleep_time = crawl_delay - (end - start)
        if sleep_time < 0.1:
            sleep_time = 0
        else:
            ## Downloader return 'end' time doesn't take into
            ## account of the overhead for content store and crawl cache
            ## communications. we need to compensate for that 
            ## if rtt is too small.
            end = time.time()
            sleep_time = crawl_delay - (end - start)
            if sleep_time < 0.1: sleep_time = 0

        ## we always yield for gevent scheduling after download.
        gevent.sleep(sleep_time)
        return need_abort

    def _worker(self, slot_id):
        """
        downloading worker thread.
        """
        slot_key = "slot:%s:%s:%s" % (self.machine_id, self.pack_id, slot_id)
        while True:
            qps, batch = self.sched.getBatch(slot_key)
            if batch:
                print '+++++++++++++++++++++++++++++++++++++++++: %s : %s of urls in batch' % (qps, len(batch))
                need_abort = False
                for url in batch:
                    aborted = self._download(url, qps)
                    if aborted: break
                self.sched.ackBatch(slot_key, aborted)
            else:
                gevent.sleep(3)

    def _monitor(self, slot_list):
        """
        write a heartbeat signature to redis every SLOT_HEARTBEAT_INTERVAL.
        Note we can also report stats to scheduler here, for example:
          * round trip info for recent downloads.
          * failure rate per host ...
        """
        while True:
            gevent.sleep(SLOT_HEARTBEAT_INTERVAL)
            self.sched.reportSlots(slot_list)


if __name__ == "__main__":
    if len(argv) != 2:
        print >> sys.stdout, "Usage: %s <pack_id>" % (os.path.basename(argv[0]), )
        sys.exit(-1)

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

    pack = SpiderPack(ip, pack_id, 5)
    pack.run()


