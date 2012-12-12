import logging
from time import time
import redis

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

r = redis.Redis()

PRIORITY_LIST = ("normal", "high")

JOB_BATCH_SIZE = 300

SITE_DEADZONE_OFFSET = 1200000000 ## move forward about 38 yrs
SITE_DEADZONE_BORDER = 2400000000

SITE_DEFAULT_QPS = 1

## 
## stores with fixed key name
## 
job_id_key = "jobs:nextJobId"
user_id_key = "jobs:nextUserId"
host_list_key = "host:list"

##
## check and resurrect the host from dead zone with newly added job.
##
def resurrect_host(r, host_table_key):
    with r.pipeline() as pipe:
        while 1:
            try:
                pipe.watch(host_list_key)
                score = pipe.zscore(host_list_key, host_table_key)
                if score >= SITE_DEADZONE_BORDER:
                    pipe.multi()
                    pipe.zincrby(host_list_key, host_table_key, 0 - SITE_DEADZONE_BORDER)
                    pipe.execute()
                break
            except redis.WatchError:
                continue

class Job(object):
    
    def __init__(self, priority, timestamp, name="default", email="default@example.com"):
        job_id = r.incr(job_id_key)

        self.id = job_id
        self.name = name.strip() + "_JOB_" + str(job_id)
        self.user = email.strip().lower()
        self.timestamp = timestamp


        priority = priority.strip().lower()
        ## validate priority and use internal value
        if priority not in PRIORITY_LIST:
            self.priority = "normal"
        else:
            self.priority = priority

        ## get or create user id
        uid = r.get('jobs:user:%s:uid' % self.user)
        if not uid:
            ## create a new user by email
            uid = r.incr(user_id_key)
            r.set("jobs:uid:%s:user" % uid, self.user)
            r.set("jobs:user:%s:uid" % self.user, self.user)

        ## add job into sorted set, score is jid.
        r.sadd("jobs:uid:%s:jobs" % uid, self.id)

        ##  create job (user:timestamp) => jid mapping
        r.set("jobs:user:%s:timestamp:%s:jid" % (self.user, self.timestamp), self.id)

        ## create job entry
        job_key = "jobs:jid:%s" % self.id
        ret = r.pipeline()  \
                .hset(job_key, 'name', self.name)   \
                .hset(job_key, 'uid', uid)          \
                .hset(job_key, 'priority', self.priority)       \
                .hset(job_key, 'timestamp', self.timestamp)   \
                .execute()


    def add(self, site, url_list, qps=SITE_DEFAULT_QPS, batch_size=JOB_BATCH_SIZE):
        host_table_key = "host:%s" % site

        if not r.exists(host_table_key):
            ## create a new host entry
            host_record = {
                    'name':site,
                    'total_qps':qps,
                    'qps':qps,
                    'total_urls':0,
                    'total_batches':0,
                    'pending_batches':0,
                    }

            ## 1.insert host into host table.
            ##
            ## 2.add the hostname into host list (sorted set)
            ##   * score=now, optimized to processing existing host first
            ##     r.zadd(host_list_key, now, site)
            ##   * score=SITE_DEADZONE_OFFSET, optimized to processing new host first
            ##     since the timestamp SITE_DEADZONE_OFFSET is the past
            r.pipeline()                                                    \
                .hmset(host_table_key, host_record)                         \
                .zadd(host_list_key, host_table_key, SITE_DEADZONE_OFFSET)  \
                .execute()

        host_pending_key = "host:%s:pending:%s" % (site, self.priority)

        job_batch_key = "jobs:jid:%s:batches" % self.id
        job_url_key = "jobs:jid:%s:urls" % self.id

        count = len(url_list)
        for i in range(0, count, batch_size):
            bid = r.incr("host:%s:nextBatchId" % site)

            batch_key = "host:%s:batch:%s" % (site, bid)

            batch_list = url_list[i:i+batch_size]
            batch_count = len(batch_list)

            ## add the batch record to DB.
            for url in batch_list:
                r.rpush(batch_key, url)

            ##
            ## 1. update site and job counters.
            ## 
            ## 2. update host pending list.
            ##
            ## 3. job batch list.
            ##
            ret = r.pipeline()                                          \
                    .hincrby(host_table_key, "total_urls", batch_count) \
                    .hincrby(host_table_key, "total_batches", 1)        \
                    .hincrby(host_table_key, "pending_batches", 1)      \
                    .incr(job_url_key, batch_count)                     \
                    .rpush(host_pending_key, batch_key)                 \
                    .rpush(job_batch_key, batch_key)                    \
                    .execute()

        ## restore the host for scheduling if it's been in deadzone.
        resurrect_host(r, host_table_key)

    ##
    ## note this routine is optional. The urls already submitted into
    ## seed store in the Job.add().
    ## this is just a place holder in case we want to use explicit
    ## commit for the store.
    ##
    ## it just do a simple marking here, it's basically a nop.
    ## we can safely comment out the redis update.
    ##
    def save(self):
        self.submitted = True
        job_key = "jobs:jid:%s" % self.id
        r.hset(job_key, "submitted", True)

    ##
    ## for maintanence only. only supports delete named (user
    ##
    def delete(self, uid, timestamp):
        pass

if __name__ == "__main__":

    job = Job("normal", time(), "test1", "default@example.com")

    job.add("www.bing.com", ["http://www.bing.com","http://www.msn.com/"])
    job.add("www.ttmeishi.com", ["http://www.ttmeishi.com",])
    job.add("www.facebook.com", ["http://www.facebook.com",])
    job.add("www.yelp.com", ["http://www.yelp.com",])
    job.add("www.mitbbs.com", ["http://www.mitbbs.com",])
    job.add("www.patch.com", ["http://www.patch.com",])
    job.add("www.amazon.com", ["http://www.amazon.com",])
    job.add("www.dajafa.com", ["http://www.dajafa.com",])

    job.save()

