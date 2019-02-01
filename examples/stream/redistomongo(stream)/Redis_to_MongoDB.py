import ast
import configparser
import json

import pymongo
import redis
from streamplay.db import redisdb
from streamplay.utils import read_config, read_config_section

""" discussion on db schema is to be done """

""" Could be improved:
    1. Password
 """


def store(fetch_return, m):
    data, total_count = fetch_return
    db = m.ucen
    if data:
        db.block.insert_many(data)
    update_index(total_count)


def update_index(total_count):
    config = configparser.ConfigParser()
    config.read('config.ini')
    config.set('mongodb', 'last_index', str(total_count))
    with open('config.ini', 'w+') as configfile:
        config.write(configfile)


def fetch(r):
    last_index = get_last_fetched_data_index()
    print(last_index)
    total_count = r.get_total_num_of_blocks()
    data = r.get_data(last_index, total_count - 1)
    return data, total_count


def get_last_fetched_data_index():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return ast.literal_eval(config.get('mongodb', 'last_index'))


def connect_to_redis():
    hostname, portnumber, password = read_config_section('redis-server')
    """ get a redisdb instance """
    r = redisdb.RedisDB(hostname=hostname,
                        portnumber=portnumber, password=password)
    """ connect to db """
    r.connect_to_db()
    return r  # this is RedisDB object


def connect_to_mongo():
    """ TODO
        1. Unavailability of mongo server
     """
    hostname, portnumber,password = read_config_section('mongodb')
    client = pymongo.MongoClient(host=hostname, port=portnumber)
    return client  # this is mongo connetion instance


def connect():
    return connect_to_redis(), connect_to_mongo()


if __name__ == "__main__":
    r, m = connect()
    f = fetch(r)
    store(f, m)
