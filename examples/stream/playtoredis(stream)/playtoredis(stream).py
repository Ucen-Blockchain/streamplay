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


def read_config():
    """ assuming all values are set properly, missing data etc can be handled
        later """
    config = configparser.ConfigParser()
    config.read('config.ini')
    endpoints = ast.literal_eval(config.get('ucen-python', 'endpoints'))
    hostname = config.get('redis-server', 'hostname')
    portnumber = config.getint('redis-server', 'portnumber')
    password = config.get('redis-server', 'password')
    return endpoints, hostname, portnumber, password


def connect_to_redis(hostname, portnumber, password=''):
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber,
                        password=password)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


if __name__ == "__main__":
    endpoints, hostname, portnumber, password = read_config()
    r = connect_to_redis(hostname, portnumber)

    s = Steemd(nodes=endpoints)
    b = Blockchain(steemd_instance=s)

    try:
        r.pull_and_store_stream(b)
    except:
        pass
