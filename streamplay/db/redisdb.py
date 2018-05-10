import sys

import chainsync
import redis


def connect_to_db(hostname, portnumber, password=''):
    """ 
    Args:
        hostname (string) : IP addess of the machine where redis-server is running
        portnumber (integer) : Port number, default is 6379
        password (string) : Default password is ''

    Returns:
        redis.client.Redis instance --> referred to as 'r' in other places
    """

    try:
        r = redis.Redis(host=hostname,
                        port=portnumber,
                        password=password)
        r.ping()                        
    except redis.ConnectionError:
        print('ConnectionError: is the redis-server running?')
        sys.exit()
    return r 

def ingest_to_db(r, key_data):
    """ 
    Args:
        chainsync () :
        r (redis.client.Redis) : Connection instance to redis-server
        key (string)
        data (string)
   
    commit - stage

    """
    key, data = key_data
    r.rpush(key, data)


def pull_and_store(r, chainsync):
    """ 
    Args:
        r (redis.client.Redis) : Connection instance to redis-server
        chainsync () :

    Return:
        The specified block's information  
    """
    for key_data in chainsync.stream(['blocks', 'status'], mode='irreversible', batch_size=1):
        ingest_to_db(r, key_data)


if __name__ == "__main__":
    pass
