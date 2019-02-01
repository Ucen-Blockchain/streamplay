import ast
import configparser
import os
import sys

from steem.steem import Steemd
from steem.blockchain import Blockchain
from streamplay.db import redisdb
from streamplay.utils import read_config


def silence_stdout():
    """ Redirects stdout to devnull """
    sys.stdout = open(os.devnull, 'w')



def update_index(count):
    config = configparser.ConfigParser()
    config.read('config.ini')
    config.set('steem-blockchain', 'last_index', str(count))
    with open('config.ini', 'w+') as configfile:
        config.write(configfile)


def connect_to_redis(hostname, portnumber, password, last_index=0):
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber,
                        password=password,
                        last_index=last_index)

    # redis.StrictRedis(host='localhost', port=6379, db=0, password=None, socket_timeout=None, connection_pool=None, charset='utf-8', errors='strict', unix_socket_path=None)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


if __name__ == "__main__":
    endpoints, hostname, portnumber, password, last_index = read_config()
    r = connect_to_redis(hostname, portnumber, password, last_index)

    s = Steemd(nodes=endpoints)
    b = Blockchain(steemd_instance=s)

    num_of_rec_sync = r.pull_and_store_sync(b)
    update_index(last_index+num_of_rec_sync)
