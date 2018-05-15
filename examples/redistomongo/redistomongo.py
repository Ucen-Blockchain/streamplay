import ast
import configparser
import json

import pymongo
import redis
from streamplay.db import redisdb

""" discussion on db schema is to be done """

""" Could be improved:
    1. Password
 """


def store(fetch_return, m):
    block_data, status_data, total_count = fetch_return
    db = m.play
    if block_data:
        db.block.insert_many(block_data)
    if status_data:
        db.status.insert_many(status_data)
    update_index(total_count)


def update_index(total_count):
    config = configparser.ConfigParser()
    config.read('config.ini')
    config.set('mongodb', 'last_index', str(total_count))
    with open('config.ini', 'w+') as configfile:
        config.write(configfile)


def fetch(r):
    last_index = get_last_fetched_data_index()
    total_count = r.get_total_num_of_blocks()
    block_data = r.get_data('block', last_index[0], total_count[0] - 1)
    status_data = r.get_data('status', last_index[1], total_count[1] - 1)
    return block_data, status_data, total_count


def get_last_fetched_data_index():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return ast.literal_eval(config.get('mongodb', 'last_index'))


def read_config(section):
    config = configparser.ConfigParser()
    config.read('config.ini')
    hostname = config.get(section, 'hostname')
    portnumber = config.getint(section, 'portnumber')
    return hostname, portnumber


def connect_to_redis():
    hostname, portnumber = read_config('redis-server')
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


def connect_to_mongo():
    """ TODO
        1. Unavailability of mongo server
     """
    hostname, portnumber = read_config('mongodb')
    client = pymongo.MongoClient(host=hostname, port=portnumber)
    return client  # this is mongo connetion instance


def connect():
    return connect_to_redis(), connect_to_mongo()


if __name__ == "__main__":
    r, m = connect()
    f = fetch(r)
    store(f, m)
