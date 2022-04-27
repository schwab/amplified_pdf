import json
import math
import numpy as np
import random
import re
import redis
import time
import struct
import uuid

import numpy as np
from datetime import datetime
from redis.exceptions import ResponseError
from rejson import Client as RejClient, Path

from settings import CONFIG_DATA

LOGGER = object()
LOGGER.warning = lambda x : print(x)
LOGGER.info = lambda x : print(x)

def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()

def aquire_lock_with_timeout( conn, lockname, acquire_timeout=30, lock_timeout=30):
    """
    Create a cross process lock in redis cache with timeout.
    """
    ln = prepend_lockname(lockname)
    identifier = str(uuid.uuid4())
    lock_timeout = int(math.ceil(lock_timeout))
    end = time.time() + acquire_timeout
    lockname = 'lock:' + lockname
    while time.time() < end:
        if conn.setnx(ln, identifier):
            conn.expire(ln, lock_timeout)
            return identifier
        elif not conn.tll(ln):
            conn.expire(ln, lock_timeout)
        time.sleep(.001)
    return False

def clear_importer_lock(conn):
    """
    Clear an importer lock without regard to other processes (ie ABORT)
    """
    if not conn:
        conn = RedisConnection()
    ln = prepend_lockname("importer")
    conn.del_key(ln)


def release_lock(conn, lockname, identifier):
    """
    Release a cross process lock acquired in redis cache
    """
    pipe = conn.pipeline(True)
    ln = prepend_lockname(lockname)
    while True:
        try:
            pipe.watch(ln)
            if pipe.get(ln) == identifier:
                pipe.multi()
                pipe.delete(ln)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            LOGGER.info("Could not release lock %s, perhaps it's already been removed." % (ln))
            pass
        LOGGER.warning("The lock %s could not be found. It may have timedout or been deleted.")
    return False

def prepend_lockname(lockname):
    return "lock:" + lockname

def importer_lock(func):
    """
    Check if an importer lock already exists.  If so exit, otherwise allow the import to proceed.
    Lock is release when the import completes, raises an exception or after a timeout.
    """
    def wrapper(*args, **kwargs):
        rc = RedisConnection()
        lock_id = rc.get_by_key(prepend_lockname("importer"))
        LOGGER.info("Importer lock %s exists..." % (lock_id))
        if not lock_id:
            conn = rc.main
            lock_to = CONFIG_DATA["IMPORTER_LOCK_TIMEOUT"]
            lock_id = aquire_lock_with_timeout(conn, "importer",lock_timeout=lock_to)
            try:
                if lock_id:
                    LOGGER.info("Importer lock aquired %s:%s and will expire at %s ..." ,prepend_lockname("importer"), 
                        lock_id, 
                        datetime.fromtimestamp(time.time() + lock_to))
                    return func(*args, **kwargs)
                else:
                    LOGGER.info("Import was teminated because another process is alredy importing data.")
            except Exception as err:
                LOGGER.error("Exception occured during import: %s", err.__doc__, exc_info=True)
            finally:
                if release_lock(conn, "importer", lock_id):
                    LOGGER.info("%s:%s lock released" % (prepend_lockname("importer"), lock_id) )
                else:
                    conn.delete("importer")
                    LOGGER.warning("%s:%s lock release attempted, but it did not return True, unsafe delete performed instead.", prepend_lockname("importer"), lock_id)
        else:
            LOGGER.info("Importer process was teminated because another import is alredy in progress.")
            return False
    return wrapper

def clear_cache_hash_keys(func):
    def wrapper(*args, **kwargs):
        rc = RedisConnection()
        cnt_removed = 0
        clear_prefixes = ["hash_keys:*", "graphquery:*", "ml_cache*"]
        def remove_starts_with(prefix):
            existing_keys = rc.get_keys_starting_with(prefix)
            for k in existing_keys:
                rc.main.delete(k)
            return len(existing_keys)
        for p in clear_prefixes:
            cnt_removed += remove_starts_with(p)
        
        LOGGER.info(":redis_import_hashkeys_clear_before %s dropped %s keys "  %(func.__name__,cnt_removed))
        return func(*args, **kwargs)
    return wrapper

# Decorator with arguments
def suppress_redis_bgsave(redis_conn=None, connection_name=None):
    if redis_conn is None:
        redis_conn = RedisConnection().main
        connection_name = "redis_cache"
    def disable_bg_save():
        d_save = redis_conn.config_get("save")
        config_save = ""
        if not d_save  or not d_save == config_save:
            redis_conn.config_set("save", config_save)
        final_state = redis_conn.config_get("save")
        if final_state and not final_state == "":            
            LOGGER.info("Disabled bgsave for %s", connection_name)
        else:
            LOGGER.warning("Bgsave state was %s but should be disabled", final_state)
    
    def enable_bg_save():
        d_save = redis_conn.config_get("save")
        config_save = "3600 1 1800 10"
        if (d_save and not d_save.get("save","") == config_save) or not d_save: 
            config_save = "3600 1 1800 10"
            redis_conn.config_set("save", config_save)
            LOGGER.info("Enabled bgsave... save:%s for %s", redis_conn.config_get("save"), connection_name)
        
            final_state = redis_conn.config_get("save")
            if final_state and not final_state == "":            
                LOGGER.info("Enabled bgsave: %s", final_state)
            else:
                LOGGER.warning("Bgsave state was disabled but should be %s", config_save)
            # add a bgsave
            LOGGER.info("Running bgsave on %s" % connection_name)
            try:
                redis_conn.bgsave()
            except ResponseError:
                LOGGER.info("BGSave already in progress")

    def set_bg_save(func):
        def wrapper(*args, **kwargs):
            disable_bg_save()
            results = func(*args, **kwargs)
            enable_bg_save()
            return results
        return wrapper
    
    return set_bg_save

class RedisConnection(object):
    """
    A connection to the redis server.
    Writes only go to main. Reads from replica.
    """
    replica = None
    main = None
    config_data = None
    decode_responses = True

    @staticmethod
    def sanitize_json_key(key):
        # redis json keys cannot have characters other than those listed.
        # They can only start with letters, $, or _ characters. If the string
        # passed in doesn't start with one these, making the executive decision
        # to start it with an underscore, even if string starts with a letter.
        NON_VALID_JSON_KEY_VALUES_REGEX = re.compile("[^a-zA-z0-9_$]")
        # negative look ahead -- if the passed in key already starts with _ or $,
        # skip adding the default _
        NON_VALID_STARTS_WITH = re.compile("^(?![_$])")

        new_key = re.sub(NON_VALID_JSON_KEY_VALUES_REGEX, "__", key)
        if re.match(NON_VALID_STARTS_WITH, new_key):
            new_key = "_%s" % new_key
        return "%s" % new_key

    def __init__(self, decode_responses=True, main_uri=None, replica_uri=None, **kwargs):
        """
        Creates a connection to redis using "redis_servers" from config provider
        """
        if not "REDIS_REPLICAS" in CONFIG_DATA and not replica_uri:
            raise KeyError("REDIS_REPLICAS not found in config")
        if not "REDIS_MAIN_SERVER" in CONFIG_DATA and not main_uri:
            raise KeyError("REDIS_MAIN_SERVER not found in config")
        if not "REDIS_CACHE_PASSWORD" in CONFIG_DATA:
            raise KeyError("REDIS_CACHE_PASSWORD is not found in config")
        password = CONFIG_DATA["REDIS_CACHE_PASSWORD"]  if not CONFIG_DATA["REDIS_CACHE_PASSWORD"] == "" else None
        username = None
        if not password is None:
            username = "default"
        replicas = replica_uri or CONFIG_DATA["REDIS_REPLICAS"]
        main = main_uri or CONFIG_DATA["REDIS_MAIN_SERVER"][0]
        self.replica_parts = []
        self.main_parts = []
        if isinstance(replicas, list):
            ri = random.choice(range(0,len(replicas)))
            self.replica_parts = replicas[ri].split(':')
        elif replicas and isinstance(replicas, str):
            self.replica_parts = replicas.split(":")

        if isinstance(main, list):
            # if we have a list of main instances just take the first one provided
            main = main[0]
        if isinstance(main, str):
            self.main_parts = main.split(':')
            
        if self.replica_parts:
            self.replica = RejClient(
                        host=self.replica_parts[0], 
                        port=self.replica_parts[1],
                        username=username,
                        password=password,
                        decode_responses=decode_responses,  
                        **kwargs)
            if not self.replica:
                raise ConnectionError("unalbe to create replica connection to %s: %s" % (self.replica_parts[0], self.replica_parts[1]))
        else:
            raise ValueError("no valid redis replica uri found.")
        if self.main_parts:
            self.main = RejClient(
                    host=self.main_parts[0], 
                    port=self.main_parts[1], 
                    password=password,
                    username=username,
                    decode_responses=decode_responses, 
                    **kwargs)
            if not self.main:
                raise ConnectionError("unalbe to create main connection to %s: %s" % (self.main_parts[0], self.main_parts[1]))
        else:
            raise ValueError("no valid redis main uri found.")
            #LOGGER.info("Master server  %s:%s", main_parts[0], main_parts[1])
        self.decode_responses=decode_responses

    def add_list(self, key, values):
        """
        Create a list and add values or just append the values if the list already exists
        """
        for v in reversed(values):
            self.main.lpush(key, v)
        return True

    def add_to_set(self, set_name, value):
        """
        Add value to set.
        """
        return self.main.sadd(set_name, value)

    def add_values_to_set(self, set_name, values):
        """
        add multiple values to a set.
        """
        try:
            pipeline = self.main.pipeline(transaction=True)
            for v in values:
                pipeline.sadd(set_name, v)
            pipeline.execute()
            return True
        except Exception as err:
            LOGGER.error(err, err.__str__)

    def config_get(self, key):
        return self.main.config_get(key)

    def config_set(self, key, value):
        return self.main.config_set(key, value)
        
    def del_key(self, key):
        return self.main.delete(key)

    def del_keys_by_filter(self, filter=""):
        result = 0
        if filter:
            keys = self.get_keys(filter)
            if keys:
                for key in keys:
                    result += self.main.delete(key)
                    if result:
                        LOGGER.info("Dropped cache key: %s", key)
                    else:
                        LOGGER.info("Failed to drop key %s", key)
                return result

    def del_json_value(self, base, path=Path.rootPath()):
        return self.main.jsondel(base, path)

    def set_application_endpoint(self, name, value):
        """
        set the endpoint url for the endpoint name specified to redis
        """
        return self.set_hash_value_by_key("endpoints", name, value)

    def get_application_endpoint(self, name):
        """
        get the endpoint url for the endpoint name specified from redis
        """
        return self.get_hash_key_value("endpoints",name)

    def get_application_endpoint_names(self):
        """
        get the names of the endpoints specified in redis.
        """
        return self.get_keys_for_hash("endpoints")
    
    def get_set_members(self, name):
        return set(self.replica.smembers(name))

    def get_json_dump(self, key_name):
        json_string = self.replica.get(key_name)
        if json_string:
            return json.loads(json_string)
        return None

    def get_keys_starting_with(self, key_prefix):
        iter_keys = self.replica.scan_iter(key_prefix)
        return list(iter_keys)

    def get_in_set(self, set_name, value):
        """
        True if value exists set_name
        """
        return self.replica.sismember(set_name, value)

    def get_by_key(self, key, decode_string=True):
        """Get redis data by key"""
        result = self.replica.get(key)
        return result
        
    def get_json_obj_keys(self, base, path="."):
        """
        retrieve rejson object keys at the path or base key
        """
        if not path:
            path = Path.rootPath()
        return self.replica.jsonobjkeys(base, path)

    def get_json_value(self, base, path=None):
        """
        retrieve an rejson object from the base cache key or 
        from any node in the object using its x_path.
        """
        if not path:
            path=Path.rootPath()
        else:
            path = Path(path)
        return self.replica.jsonget(base, path)
       
    def get_keys_for_hash(self, hash_name):
        return self.replica.hkeys(hash_name)
    
    def get_hash_all(self, hash_name):
        return self.replica.hgetall(hash_name)

    def get_hash_key_exists(self, hash_name, key_name):
        return self.replica.hexists(hash_name, key_name)

    def get_hash_key_count(self, hash_name):
        return self.replica.hlen(hash_name)
        
    def get_hash(self, key_name):
        return self.replica.hgetall(key_name)

    def get_hash_key_value(self, hash_name, key_name):
        return self.replica.hget(hash_name, key_name)

    def get_keys(self, key_filter="*"):
        result = self.replica.keys(key_filter)
        return result

    def get_key_exists(self, key_filter="*"):
        keys = self.get_keys(key_filter)
        return True if keys else False

    def get_list(self, key):
        """
        Retrieve a list from the replica.
        """
        if self.key_exist(key):
            len = self.replica.llen(key)
            items = self.replica.lrange(key,0, len)
            return items
        return None

    def get_np_array(self, key):
        encoded = self.replica.get(key)
        h, w = struct.unpack(">II", encoded[:8])
        a = np.frombuffer(encoded, dtype=np.numeric, offset=8).reshape(h,w)
        return a

    def key_exist(self, key_name):
        """
        Check if key_name exists.
        """
        return self.replica.exists(key_name) 

    def pop_set(self, set_name, count=1):
        return self.main.spop(set_name, count)

    def remove_from_set(self, set_name, values):
        """
        Remove item from a redis set.
        """
        # return self.main.srem(set_name, values)
        return self.remove_values_from_set(set_name, values)
    
    def remove_values_from_set(self, set_name, values):
        try: 
            pipeline = self.main.pipeline(transaction=True)
            for v in values:
                pipeline.srem(set_name, v)
            pipeline.execute()
            return True
        except Exception as err:
            LOGGER.error(err, err.__doc__)
            raise err

    def remove_hash_value_by_key(self, hash_name, key):
        """
        Remove a item from a hash by it's key name
        """
        return self.main.hdel(hash_name, key)

    def save(self, bg=True):
        if bg:
            self.main.bgsave()
        else:
            self.main.save()
    
    def set_json_value(self, base, key, value):
        """
        Save the value under the key name (key) at the redis cache location (base)
        """
        #rej_client = RejClient(host=self.main_parts[0], port=self.main_parts[1], decode_responses=True)
        # check for and handle missing base ke
        existing_base = self.get_keys_starting_with(base)
        if not existing_base:
            self.main.jsonset(base, Path.rootPath(), {"created":time.time()})
        if not key:
            key = Path.rootPath()
        ret_val = self.main.jsonset(base, key, value)
        return ret_val

    def set_hash_values (self, key, d_values):
        """
        Set the dictionary values to a hash_key
        """
        return self.main.hmset(key, d_values)

    def set_hash_value_by_key(self, hash_name, key, value):
        """
        Set a hash object's key/value pair in redis
        """
        return self.main.hset(hash_name, key, value)
        
    def set_json_dump(self, key_name ,json_data, ex=None):
        json_string = json.dumps(json_data, default=np_encoder)
        if ex:
            return self.main.set(key_name, json_string, ex)
        else:
            return self.main.set(key_name, json_string)

    def set_pop(self, name, count):
        return self.main.spop(name, count)

    def set_intersect_keys(self, set_a, set_b):
        return self.replica.sinter(set_a, set_b)

    def set_diff_keys(self, set_a, set_b):
        return self.replica.sdiff(set_a, set_b)
    
    def set_string(self, key, value):
        return self.main.set(key, value)
    def get_string(self, key):
        return self.replica.get(key)

    def set_np_array(self, key,  np_array_numeric):
        """
        Convert a numpy numeric array to bytes and store in the key provided
        """
        h, w = np_array_numeric.shape
        shape = struct.pack(">II", h, w)
        encoded = shape + np_array_numeric.tobyes()
        self.main.set(key, encoded)
        return 1

    def x_ack(self, stream_name, group_name, l_ids):
        results = {}
        for id in l_ids:
            results[id] = self.main.xack(stream_name, group_name, id)
        return results

    def x_add(self, stream_name, d_values):
        if d_values is None:
            raise ValueError("No items specified to save to log.")
        return self.main.xadd(stream_name, d_values, )

    def x_len(self, stream_name):
        return self.replica.xlen(stream_name)

    def x_del(self, stream_name, id):
        return self.main.xdel(stream_name, id)

    def x_group_create(self, stream_name, group_name, mkstream=True):
        return self.main.xgroup_create(stream_name, group_name, id="$", mkstream=True)
            

    def x_group_delete(self, stream_name, group_name):

        if self.get_key_exists(stream_name):
            return self.main.xgroup_destroy(stream_name, group_name)

    def x_pending(self, stream_name, group_name):
        """
        Get a summary of the pending items for a consumer group

        """
        return self.replica.xpending(stream_name, group_name)
    
    def x_read(self, streams:dict, count=8, block=5000):
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        """
        return self.replica.xread(streams, count, block)

    def x_read_group(self, group_name, consumer_name, streams, count=None, block=None, noack=None, id=">"):
        """
        Read from a stream via a consumer group.
        group_name: name of the consumer group.
        consumer_name: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        """
        return self.main.xreadgroup(group_name, consumer_name, streams, count, block, noack, )

    def x_range(self, log_name, min_ts="-", max_ts='+', count_items=None):
        return self.replica.xrange(log_name, min_ts, max_ts, count_items)

    def x_rev_range(self, log_name, min_ts="+", max_ts='-', count_items=1):
        return self.replica.xrevrange(log_name, min_ts, max_ts, count_items)

    def zset_add_increment(self,name, key):
        self.main.zadd(name, {key:1}, incr=True)

    def zset_add_index(self, name, idx, value):
        self.main.zadd(name, {idx:value})

    def zset_remove(self, name, values):
        self.main.zrem(name, values)

    PRECISION = [1, 60, 300, 3600, 18000, 86400]       

    def update_counter(self, name, count=1, now=None):
        now = now or time.time()                          
        pipe = self.main.pipeline()                              
        for prec in self.PRECISION:                              
            pnow = int(now / prec) * prec                   
            hash = '%s:%s'%(prec, name)                     
            pipe.zadd('count:known:', {hash: 0})                    
            pipe.hincrby('count:' + hash, pnow, count)      
        pipe.execute()

def count(method):
    def counted(*args, **kw):
        result = method(*args, **kw)
        #rc = RedisConnection()
        #rc.update_counter("hits:%s" % method.__name__, 1)
        return result
    return counted

def timeit(method):
    def method_timer(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        runtime = round((te -ts)*1000,1)
        
        LOGGER.info("%s.%s took %s msec" % (method.__module__, method.__name__, int(runtime)))
        return result
    return method_timer
