import ast
import configparser
import os
import sys

from steem.steem import Steemd
from steem.blockchain import Blockchain
from streamplay.db import redisdb


def silence_stdout():
    """ Redirects stdout to devnull """
    sys.stdout = open(os.devnull, 'w')


def read_config():
    """ assuming all values are set properly, missing data etc can be handled
        later """
    config = configparser.ConfigParser()
    config.read('config.ini')
    endpoints = ast.literal_eval(config.get('ucen-python', 'endpoints'))
    last_index = config.getint('ucen-python', 'last_index')
    hostname = config.get('redis-server', 'hostname')
    portnumber = config.getint('redis-server', 'portnumber')
    return endpoints, hostname, portnumber, last_index


def update_index(count):
    config = configparser.ConfigParser()
    config.read('config.ini')
    config.set('ucen-python', 'last_index', str(count))
    with open('config.ini', 'w+') as configfile:
        config.write(configfile)


def connect_to_redis(hostname, portnumber, password='', last_index=0):
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber,
                        password=password,
                        last_index=last_index)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


if __name__ == "__main__":
    endpoints, hostname, portnumber, last_index = read_config()
    r = connect_to_redis(hostname, portnumber, last_index)

    s = Steemd(nodes=endpoints)
    b = Blockchain(steemd_instance=s)

    num_of_rec_sync = r.pull_and_store_sync(b)
    update_index(last_index+num_of_rec_sync)
