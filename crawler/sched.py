"""
  Job scheduler.
"""

import logging
import math
from time import time, sleep
import redis

from jobs import SITE_DEADZONE_OFFSET, SITE_DEADZONE_BORDER
from jobs import SITE_DEFAULT_QPS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis()

##
## Slot alive report interval
##
SLOT_HEARTBEAT_INTERVAL = 60
SLOT_DELETE_INTERVAL = 60 * 6

SCHED_LOCK_DEFAULT = 60

##
## Scheduler state
##
SCHED_STATE_INIT = 0
SCHED_STATE_RUNNING = 1
SCHED_STATE_STOPPED = 2

class SchedulerException(Exception):
    def __init__(self, notice):
        self.notice = notice

    def __str__(self):
        return repr(self.notice)

class SlotAlreadyExist(SchedulerException):
    pass

class SlotDoesntExist(SchedulerException):
    pass

class BatchDoesntExist(SchedulerException):
    pass

class BatchAlreadyExist(SchedulerException):
    pass

class MachineInvalidConfig(SchedulerException):
    pass

class MachineDoesntExist(SchedulerException):
    pass

class MachineAlreadyExist(SchedulerException):
    pass

class InternalError(SchedulerException):
    pass

def lock(lock_key, current=time()):
    lock_value = current + SCHED_LOCK_DEFAULT
    set = r.setnx(lock_key, lock_value)
    logger.debug("lock(): current=%s, lock_value=%s, old_lock_value=%s" % (current, lock_value, set))
    ## locked, need to check if it's expired
    if not set:
        if current < float(r.get(lock_key)):
            ## already locked, no need to do anything here
            return
        else:
            ## expired lock, try to grab it
            old_value = float(r.getset(lock_key, lock_value))
            ## the lock might be acquired by someone else
            if current < old_value:
                return
    return lock_value

def unlock(lock_key, lock_value, current=time()):
    logger.debug("unlock(): lock_key=%s, lock_value=%s, now=%s" % (lock_key, lock_value, current))
    if current < lock_value:
        r.delete(lock_key)
    else:
        raise InternalError("unlock(): the lock(%s) takes more than %s secs!!!" % (lock_key, SCHED_LOCK_DEFAULT))

##
## stores with fixed keys
##
schedule_state_key = "scheduling:ctrl:state"
schedule_slot_key = "scheduling:slots"
crawl_slot_key = "crawling:slots"
crawl_batch_key = "crawling:batches"

slot_table_key = "slot:list"
idle_slot_key = "slot:idlelist"

host_list_key = "host:list"
mach_list_key = "machine:list"

lock_key = "scheduling:lock"

class Scheduler(object):

    def __init__(self):
        self.enabled = False

        if self.status() == SCHED_STATE_RUNNING:
            self.enabled = True

    def status(self):
        ##
        ## grab the schedule lock
        ##
        while True:
            now = time()
            lock_value = lock(lock_key, now)
            if not lock_value:
                sleep(1)
            else:
                break

        state = r.get(schedule_state_key)
        if not state:
            state = SCHED_STATE_INIT

        unlock(lock_key, lock_value)

        return int(state)

    def restart(self):
        ##
        ## grab the schedule lock
        ##
        while True:
            now = time()
            lock_value = lock(lock_key, now)
            if not lock_value:
                sleep(1)
            else:
                break

        ## 
        ## check scheduler control state
        ##
        old_state = r.getset(schedule_state_key, SCHED_STATE_RUNNING)
        if not old_state or int(old_state) != SCHED_STATE_STOPPED:
            self._stop()
            assert(int(r.get(schedule_state_key)) == SCHED_STATE_STOPPED)

        r.set(schedule_state_key, SCHED_STATE_RUNNING)
        self.enabled = True

        unlock(lock_key, lock_value)

    def stop(self):
        ##
        ## grab the schedule lock
        ##
        while True:
            now = time()
            lock_value = lock(lock_key, now)
            if not lock_value:
                sleep(1)
            else:
                break

        self._stop()

        unlock(lock_key, lock_value)

    def _stop(self):
        self.enabled = False

        ## 
        ## check scheduler control state
        ##
        old_state = r.getset(schedule_state_key, SCHED_STATE_STOPPED)
        if not old_state and int(old_state) == SCHED_STATE_STOPPED:
            logger.debug("stop(): exit: scheduler is being or already stopped.")
            return

        ##
        ## sleep a couple of seconds, wait for downloader to cool down.
        ##
        #sleep(60)

        ##
        ## check schedule slot table and clean up all the entries
        ##
        slot_dict = r.hgetall(schedule_slot_key)
        for slot, record in slot_dict.iteritems():
            prefix, machine, pack, fetcher = slot.split(':')
            if not prefix or not machine or not pack or not fetcher:
                raise InternalError("corrupted slot data (slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

            batch, qps, list_key = record.split('|')
            if not batch or not qps or not list_key:
                raise InternalError("corrupted scheduling record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

            prefix, hostname, remains = batch.split(':', 2)
            if not prefix or not hostname or not remains:
                raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))
            host_key = "host:%s" % hostname

            score = pack * 100 + fetcher
            ##
            ## update the job store and schedule store
            ## 1. add slot back into idle slot list
            ## 2. add batch into host pending list
            ## 3. recover the host's QPS
            ## 4. remove slot from scheduling slot table
            ret = r.pipeline()                              \
                .zadd(idle_slot_key, slot, score)           \
                .lpush(list_key, batch)                     \
                .hincrby(host_key, 'qps', SITE_DEFAULT_QPS) \
                .hdel(schedule_slot_key, slot)              \
                .execute()

            ## move the host out of DEADZONE if its qps is 0 or below.
            qps = int(r.hget(host_key, 'qps'))
            if qps <= SITE_DEFAULT_QPS:
                ## bring the host out of DEADZONE
                r.zincrby(host_list_key, host_key, 0 - SITE_DEADZONE_OFFSET)

            logger.debug("stop: from table %s: recovered slot(%s) with batch(%s)." % (schedule_slot_key, slot, batch))

        ##
        ## check crawling slot and batch table and clean up all the entries.
        ##
        crawl_dict = r.hgetall(crawl_slot_key)
        for slot, batch_record in crawl_dict.iteritems():
            if not slot or not batch_record:
                raise InternalError("corrupted record for slot(%s) in table %s" % (slot, crawl_slot_key))

            prefix, machine, pack, fetcher = slot.split(':')
            if not prefix or not machine or not pack or not fetcher:
                raise InternalError("corrupted slot data (slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

            batch, qps, list_key = batch_record.split('|')
            if not batch or not qps or not list_key:
                raise InternalError("corrupted batch record(%s=>%s) in %s" % (slot, batch, crawl_slot_key))

            ## double check the slot actually is downloading the batch
            ok = r.hdel(crawl_batch_key, batch_record)
            if not ok:
                raise InternalError("corrupted batch_record(%s) in table: %s" % (slot, crawl_batch_key))

            prefix, hostname, remains = batch.split(':', 2)
            if not prefix or not hostname or not remains:
                raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))
            host_key = "host:%s" % hostname

            score = pack * 100 + fetcher

            ##
            ## update the job store and schedule store
            ## 1. add slot back into idle slot list
            ## 2. add batch into host pending list
            ## 3. recover the host's QPS
            ## 4. remove slot from crawling slot table
            ##    crawling batch table is already clean up.
            ret = r.pipeline()                              \
                .zadd(idle_slot_key, slot, score)           \
                .lpush(list_key, batch)                     \
                .hincrby(host_key, 'qps', SITE_DEFAULT_QPS) \
                .hdel(crawl_slot_key, slot)                 \
                .execute()

            ## move the host out of DEADZONE if its qps is 0 or below.
            qps = int(r.hget(host_key, 'qps'))
            if qps <= SITE_DEFAULT_QPS:
                ## bring the host out of DEADZONE
                r.zincrby(host_list_key, host_key, 0 - SITE_DEADZONE_OFFSET)

            logger.debug("stop: from table %s: recovered slot(%s) and batch(%s)." % (crawl_slot_key, slot, batch))
        
        ## mark the scheduler stopped.
        r.set(schedule_state_key, SCHED_STATE_STOPPED)

        return

    ##
    ## prepare a batch to crawl
    ## 
    def getBatch(self, slot):
        ## check if scheduler is stopped.
        state = r.get(schedule_state_key)
        if not state or int(state) == SCHED_STATE_INIT: return (0, [SCHED_STATE_INIT])
        if int(state) == SCHED_STATE_STOPPED: return (0, [SCHED_STATE_STOPPED])

        slot = slot.strip().lower()
        logger.debug("getBatch(slot=%s)" % slot)

        ## report slot alive (update slot table with latest time)
        r.zadd(slot_table_key, slot, time())

        record = r.hget(schedule_slot_key, slot)
        if not record:
            logger.debug("getBatch(slot=%s): exit: no batch available." % slot)
            return (0, [])

        batch, qps, list_key = record.split('|')
        if not batch or not qps or not list_key:
            raise InternalError("corrupted batch record(%s=>%s) in %s" % (slot, batch, schedule_slot_key))

        urls = r.lrange(batch, 0, -1)
        count = len(urls)
        
        ##
        ## update the job store and schedule store
        ## 1. add batchId into crawling batch table
        ##    o add into crawling slot table
        ##    o add into crawling batch table
        ## 2. remove slot from scheduling slot table

        ret = r.pipeline()                          \
            .hset(crawl_slot_key, slot, record)     \
            .hset(crawl_batch_key, record, slot)    \
            .hdel(schedule_slot_key, slot)          \
            .execute()

        logger.debug("getBatch(slot=%s): exit: got a batch(%s) with % urls." % (slot, batch, count))
        return (int(qps), urls)


    def ackBatch(self, slot):
        ## check if scheduler is stopped.
        state = int(r.get(schedule_state_key))
        if not state or int(state) == SCHED_STATE_INIT: return 'init'
        if int(state) == SCHED_STATE_STOPPED: return 'stopped'

        slot = slot.strip().lower()
        logger.debug("ackBatch(slot=%s)" % slot)

        ## report slot alive (update slot table with latest time)
        r.zadd(slot_table_key, slot, time())

        ## get the batch the slot is downloading
        batch_record = r.hget(crawl_slot_key, slot)
        if not batch_record:
            logger.debug("ackBatch(slot=%s): exit: no batch is downloading on this slot." % slot)
            return 'slot'

        ## double check the slot actually is downloading the batch
        check_slot = r.hget(crawl_batch_key, batch_record)
        if not check_slot or slot != check_slot:
            raise InternalError("corrupted batch_record(%s) in table: %s!" % (batch_record, crawl_batch_key))

        batch, qps, list_key = batch_record.split('|')
        if not batch or not qps or not list_key:
            raise InternalError("corrupted batch record(%s=>%s) in %s" % (slot, batch, crawl_slot_key))

        prefix, hostname, remains = batch.split(':', 2)
        if not prefix or not hostname or not remains:
            raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))
        host_key = "host:%s" % hostname
        host_complete_list_key = "host:%s:completed" % hostname

        prefix, machine, pack, fetcher = slot.split(':')
        if not prefix or not hostname or not remains:
            raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

        score = pack * 100 + fetcher

        ## release the resource
        ##  1. add the batch into completed batch list.
        ##  2. remove the entry from both crawling:slots and crawling:batches list
        ##  3. add the slot into idle slot list
        ##  4. recover QPS for the host
        ret = r.pipeline()                              \
            .rpush(host_complete_list_key, batch)       \
            .hdel(crawl_slot_key, slot)                 \
            .hdel(crawl_batch_key, batch_record)        \
            .zadd(idle_slot_key, slot, score)           \
            .hincrby(host_key, 'qps', SITE_DEFAULT_QPS) \
            .execute()

        ## move the host out of DEADZONE if its qps is 0 or below.
        ## TODO: this operation either requires lock, or
        ##       correction in _schedule() to fix the host score in host_list.
        ##  
        qps = int(r.hget(host_key, 'qps'))
        if qps <= SITE_DEFAULT_QPS:
            ## bring the host out of DEADZONE
            r.zincrby(host_list_key, host_key, 0 - SITE_DEADZONE_OFFSET)

        logger.debug("ackBatch(slot=%s): exit: become idle: worked on batch(%s)." % (slot, batch))

        return 'ok'


    def _schedule(self):

        if not self.enabled: return

        ##
        ## try to grab the lock to do the scheduling
        ##
        now = time()
        lock_value = lock(lock_key, now)
        if not lock_value:
            return;

        ##
        ## check and remove dead slots.
        ##
        dead_slots = []
        slots = r.zrange(slot_table_key, 0, 200, withscores=True)
        if slots:
            for slot, score in slots:
                if now - score > SLOT_DELETE_INTERVAL:
                    dead_slots.append(slot)
                    logger.info("_schedule(): dead slot(%s): last seen: %s seconds before." % (slot, now - score))

        if dead_slots:
            self._delete_slots(dead_slots)
            logger.info("_schedule(): deleted %s dead slots." % len(dead_slots))

        ## go ahead schedule some batches upon idle slots
        ## we'd better complete this within a couple of secs
        slots = r.zrange(idle_slot_key, 0, -1)
        if not slots:
            unlock(lock_key, lock_value)
            logger.debug("_schedule(): exit: no idle slots.")
            return
        logger.debug("_schedule(): got %s idle slots to schedule upon" % len(slots))

        avail_slots = len(slots)
        while avail_slots > 0:
            slot_dict = {} ## reset slot dict.

            host_list = r.zrange(host_list_key, 0, 0, desc=False, withscores=True)
            if not host_list:
                unlock(lock_key, lock_value)
                logger.debug("_schedule(): exit: no host available for download.")
                return

            (host_key, score) = host_list[0]

            logger.debug("_schedule(): host from %s: host_key=%s, score=%s" % (host_list_key, host_key, score))

            ## check if we're done with  hosts
            if score >= SITE_DEADZONE_BORDER:
                unlock(lock_key, lock_value)
                logger.debug("_schedule(): exit: no job for any hosts: top host: host_key=%s, score=%s" % (host_key, score))
                return

            ## schedule the host
            host = r.hgetall(host_key)
            logger.debug("_schedule(): processing host=(%s: %s), detailed info: \n%s" % (host_key, score, host))

            qps = int(host['qps'])

            if qps <= 0:
                ## the host has no qps left, not in DEADZONE yet
                ## move the host into DEADZONE 
                r.zincrby(host_list_key, host_key, SITE_DEADZONE_OFFSET)
                logger.debug("_schedule(): host (%s: %s): no quota left, qps=%s, suspended." % (host_key, score, qps))
                continue

            ## get batches from the host's pending list
            high_list_key = "%s:pending:high" % host_key
            normal_list_key = "%s:pending:normal" % host_key

            num_batch = min(math.ceil(qps / SITE_DEFAULT_QPS), avail_slots)
            batches = r.lrange(high_list_key, 0, num_batch-1)
            num_high = len(batches)
            logger.info("_schedule(): get %s batches for host (%s: %s) from HIGH queue" % (num_high, host_key, score))
            if (num_high < num_batch):
                num_batch = num_batch - num_high
                batches = batches + r.lrange(normal_list_key, 0, num_batch-1)
                num_norm = len(batches) - num_high
                logger.info("_schedule(): get %s batches for host (%s: %s) from NORMAL queue" % (num_norm, host_key, score))

            if not batches:
                ## the host has no pending batch, not in DEADZONE yet
                ## move the host into DEADZONE
                r.zincrby(host_list_key, host_key, SITE_DEADZONE_OFFSET)
                logger.debug("_schedule(): host (%s: %s): no batches left, suspended" % (host_key, score))
                continue

            num = num_high + num_norm
            logger.debug("_schedule(): processing the batches(size: %s): detail: \n%s" % (num, batches))

            processed = 0
            ## schedule batches on idle slots
            for batch in batches:
                assert(qps > 0)

                ## calc the QPS for the batch
                if qps < SITE_DEFAULT_QPS:
                    batch_qps = qps
                else:
                    batch_qps = SITE_DEFAULT_QPS
                ## always subtract default value, so for none integral 
                ## qps we can recover them through add back the default
                ## value.
                qps = qps - SITE_DEFAULT_QPS

                if processed < num_high:
                    list_key = high_list_key
                else:
                    list_key = normal_list_key

                ## compose the batch record for the slot
                batch_record = batch + '|' + str(batch_qps) + '|' + list_key

                ## get an availble slot and assign a batch to it.
                slot = slots.pop(0)
                slot_dict[slot] = batch_record
                avail_slots = avail_slots - 1

                ## remove the idle slot from idle slot 
                r.zrem(idle_slot_key, slot)

                processed = processed + 1

            ## done with a host
            ## 1. add into the schedule slot table
            ## 2. remove batches from pending high priority list
            ## 3. remove batches from pending normal priority list
            ## 4. update host table for qps and pending_batches
            ## 5. adjust host score for scheduling
            assert (num == processed)
            host['pending_batches'] = int(host['pending_batches']) - processed
            host['qps'] = qps
            ret = r.pipeline()                                  \
                .hmset(schedule_slot_key, slot_dict)            \
                .ltrim(high_list_key, num_high, -1)             \
                .ltrim(normal_list_key, num - num_high, -1)     \
                .hmset(host_key, host)                          \
                .zadd(host_list_key, host_key, now)             \
                .execute()
            logger.debug("_schedule(): host(%s:%s): %s batches processed. host's new QPS=%s." % (host_key, score, processed, qps))

        unlock(lock_key, lock_value)

        logger.debug("_schedule(): exit: no more idle slots.")

    def _validate(self, **kw):
        if 'ip' not in kw or        \
                'external_ip' not in kw or \
                'name' not in kw or \
                'external_name' not in kw or \
                'num_pack' not in kw or \
                'num_slot' not in kw:
            raise MachineInvalidConfig("Invalid machine configuration!!!")

        mach_dict = kw;
        if 'enabled' not in mach_dict or \
                mach_dict['enabled'] != True:
            mach_dict['enabled'] = False

        return mach_dict

    ## add or update a machine info
    def addMachine(self, **kw):
        mach_dict = self._validate(**kw)
        ip = mach_dict['ip']
        mach_table_key = "machine:%s" % ip

        ##  update schedule store.
        ##  1. creat an entry in machine_tables
        ##  2. add into machine list
        r.pipeline()                                    \
                .hmset(mach_table_key, mach_dict)       \
                .sadd(mach_list_key, mach_table_key)    \
                .execute()

    ## info about a machine
    def machine(self, ip):
        mach_table_key = "machine:%s" % ip

        if not r.exists(mach_table_key):
            raise MachineDoesntExist("No such machine configured with IP = %s!!!" % ip)

        return r.hgetall(mach_table_key)

    def enableMachine(self, ip):
        mach_table_key = "machine:%s" % ip

        if not r.exists(mach_table_key):
            raise MachineDoesntExist("No such machine configured with IP = %s!!!" % ip)

        r.hset(mach_table_key, 'enabled', True)

    ## TODO: disable machine requires further work to stop current working slots!!!
    def disableMachine(self, ip):
        mach_table_key = "machine:%s" % ip

        if not r.exists(mach_table_key):
            raise MachineDoesntExist("No such machine configured with IP = %s!!!" % ip)

        r.hset(mach_table_key, 'enabled', False)

    def machineEnabled(self, ip):
        mach_table_key = "machine:%s" % ip

        if not r.exists(mach_table_key):
            raise MachineDoesntExist("No such machine configured with IP = %s!!!" % ip)

        return r.hget(mach_table_key, 'enabled')

    def _updateSlots(self, slot_list, idle=True):
        norm_slot_list = [] 
        machine_id = pack_id = 0
        for slot in slot_list:
            slot = slot.strip().lower()
            prefix, ip, pack, fetcher = slot.split(':')
            pack = int(pack)
            fetcher = int(fetcher)
            if (prefix != 'slot'):
                raise SlotInvalidConfig("Invalid slot Id: %s!!!" % slot)

            # reduce the roundtrip checking with redis store.
            if ip != machine_id or pack != pack_id:
                if not self.machineEnabled(ip):
                    return False

                mach_dict = self.machine(ip)

                ## validate pack index and fetcher index.
                if not (0 < pack <= mach_dict['num_pack']) or not (0 < fetcher <= mach_dict['num_slot']):
                    raise SlotInvalidConfig("Invalid slot Id: %s!!!" % slot)

                machine_id = ip
                pack_id = pack

            norm_slot_list.append(slot)

        ## update store in batch using pipeline
        pipe = r.pipeline()
        now = time()
        for slot in norm_slot_list:
            pipe.zadd(slot_table_key, slot, now)
            if idle:
                pipe.zadd(idle_slot_key, slot, pack * 100 + fetcher)
        pipe.execute()

        return True

    def addSlots(self, slot_list):
        return self._updateSlots(slot_list, idle=True)

    def reportSlots(self, slot_list):
        return self._updateSlots(slot_list, idle=False)

    ##
    ## delete slots from scheduler
    ##
    def deleteSlots(self, slot_list):
        ##
        ## try to grab the lock to do the scheduling
        ##
        now = time()
        lock_value = lock(lock_key, now)
        if not lock_value:
            return;

        self._delete_slots(slot_list)

        unlock(lock_key, lock_value)

    ##
    ## delete a list of slots from schedule
    ##  o might need to freeze all operation.
    ##  o check and rollback all crawling batches into host batch list.
    ##     o need to tell if a batch is from high or normal priority list.
    ##  o remove slot from slot table and idle slot table.
    ##
    def _delete_slots(self, slot_list):

        norm_slot_list = [] 
        machine_id = pack_id = 0
        for slot in slot_list:
            slot = slot.strip().lower()
            prefix, ip, pack, fetcher = slot.split(':')
            pack = int(pack)
            fetcher = int(fetcher)
            if (prefix != 'slot'):
                raise SlotInvalidConfig("Invalid slot Id: %s!!!" % slot)

            # reduce the roundtrip checking with redis store.
            if ip != machine_id or pack != pack_id:
                if not self.machineEnabled(ip):
                    return False

                mach_dict = self.machine(ip)

                ## validate pack index and fetcher index.
                if not (0 < pack <= mach_dict['num_pack']) or not (0 < fetcher <= mach_dict['num_slot']):
                    raise SlotInvalidConfig("Invalid slot Id: %s!!!" % slot)

                machine_id = ip
                pack_id = pack

            norm_slot_list.append(slot)

        pipe = r.pipeline()
        for slot in norm_slot_list:
            prefix, machine, pack, fetcher = slot.split(':')
            if not prefix or not machine or not pack or not fetcher:
                raise InternalError("corrupted slot data (slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

            record = r.hget(schedule_slot_key, slot)
            if record:
                batch, qps, list_key = record.split('|')
                if not batch or not qps or not list_key:
                    raise InternalError("corrupted scheduling record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))

                prefix, hostname, remains = batch.split(':', 2)
                if not prefix or not hostname or not remains:
                    raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))
                host_key = "host:%s" % hostname

                ## append redis commands into pipeline.
                ##
                ## update the job store and schedule store
                ## 1. add batch into host pending list
                ## 2. recover the host's QPS
                ## 3. remove slot from scheduling slot table
                pipe.lpush(list_key, batch)
                pipe.hincrby(host_key, 'qps', SITE_DEFAULT_QPS)
                pipe.hdel(schedule_slot_key, slot)

                ## move the host out of DEADZONE if its qps is 0 or below.
                qps = int(r.hget(host_key, 'qps'))
                if qps <= SITE_DEFAULT_QPS:
                    ## bring the host out of DEADZONE
                    pipe.zincrby(host_list_key, host_key, 0 - SITE_DEADZONE_OFFSET)
                logger.debug("deleteSlots: from table %s: recovered batch(%s) from slot(%s)." % (schedule_slot_key, batch, slot))
            else:
                batch_record = r.hget(crawl_slot_key, slot)
                if batch_record:
                    batch, qps, list_key = batch_record.split('|')
                    if not batch or not qps or not list_key:
                        raise InternalError("corrupted batch record(%s=>%s) in %s" % (slot, batch, crawl_slot_key))

                    ## double check the slot actually is downloading the batch
                    ok = r.hdel(crawl_batch_key, batch_record)
                    if not ok:
                        raise InternalError("corrupted batch_record(%s) in table: %s" % (slot, crawl_batch_key))

                    prefix, hostname, remains = batch.split(':', 2)
                    if not prefix or not hostname or not remains:
                        raise InternalError("corrupted batch record(slot:%s=>batch:%s) in %s" % (slot, batch, schedule_slot_key))
                    host_key = "host:%s" % hostname

                    ## append redis commands into pipeline.
                    ##
                    ## update the job store and schedule store
                    ## 1. add slot back into idle slot list
                    ## 2. add batch into host pending list
                    ## 3. recover the host's QPS
                    ## 4. remove slot from crawling slot table
                    ##    crawling batch table is already clean up.
                    pipe.lpush(list_key, batch)
                    pipe.hincrby(host_key, 'qps', SITE_DEFAULT_QPS)
                    pipe.hdel(crawl_slot_key, slot)

                    ## move the host out of DEADZONE if its qps is 0 or below.
                    qps = int(r.hget(host_key, 'qps'))
                    if qps <= SITE_DEFAULT_QPS:
                        ## bring the host out of DEADZONE
                        r.zincrby(host_list_key, host_key, 0 - SITE_DEADZONE_OFFSET)

                    logger.debug("deleteSlots: from table %s: recovered batch(%s) from slot(%s)." % (crawl_slot_key, batch, slot))

            pipe.zrem(slot_table_key, slot)
            pipe.zrem(idle_slot_key, slot)

        ## dispatch pipelined commands.
        pipe.execute()


if __name__ == "__main__":
    sched = Scheduler()

    while 1:
        sched._schedule()
        sleep(5)

