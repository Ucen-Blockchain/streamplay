import ast
import configparser
import os
import sys

from chainsync import ChainSync
from chainsync.adapters.steemv2 import SteemV2Adapter
from streamplay.db import redisdb


def silence_stdout():
    """ Redirects stdout to devnull """
    sys.stdout = open(os.devnull, 'w')


def read_config():
    """ assuming all values are set properly, missing data etc can be handled
        later """
    config = configparser.ConfigParser()
    config.read('config.ini')
    endpoints = ast.literal_eval(config.get('chainsync', 'endpoints'))
    hostname = config.get('redis-server', 'hostname')
    portnumber = config.getint('redis-server', 'portnumber')
    return endpoints, hostname, portnumber


def connect_to_redis(hostname, portnumber, password):
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber,
                        password=password)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


if __name__ == "__main__":
    endpoints, hostname, portnumber = read_config()
    r = connect_to_redis(hostname, portnumber, password)
    adapter = SteemV2Adapter(endpoints=endpoints)
    chainsync = ChainSync(adapter)
    try:
        r.pull_and_store(chainsync)
    except KeyboardInterrupt:
        pass
