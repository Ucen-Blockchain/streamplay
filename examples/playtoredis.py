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
    """ assuming all values are set properly, missing data etc. can be handled later """
    config = configparser.ConfigParser()
    config.read('playtoredis_config.ini')
    endpoints = ast.literal_eval(config['DEFAULT']['endpoints'])
    hostname = config['redis-server']['hostname']
    portnumber = int(config['redis-server']['portnumber'])
    password = config['redis-server']['password']
    return endpoints, hostname, portnumber, password


if __name__ == "__main__":
    endpoints, hostname, portnumber, password = read_config()
    r = redisdb.connect_to_db(hostname=hostname,
                              portnumber=portnumber,
                              password=password)
    adapter = SteemV2Adapter(endpoints=endpoints)
    chainsync = ChainSync(adapter)
    redisdb.pull_and_store(r, chainsync)