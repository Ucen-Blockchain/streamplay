import json
import sys

from steem.blockchain import Blockchain
import redis

""" Could be improved:
    1. SSL encrytion
    2. Password
 """


class RedisDB:

    def __init__(self, hostname='localhost',
                 portnumber=6379,
                 password='',
                 last_index=0):
        """ Args:
                hostname (string): IP address of the machine where redis-server
                                    is running
                portnumber (integer): Port number, default is 6379
                password (string): Default password is ''
        """
        self.hostname = hostname
        self.portnumber = portnumber
        self.password = password
        self.last_index = last_index
        self.r = None

    def connect_to_db(self):
        """ Establishes connection with redis """
        r = redis.Redis(host=self.hostname,
                        port=self.portnumber,
                        password=self.password)
        try:
            r.ping()
        except redis.ConnectionError:
            sys.exit('ConnectionError: is the redis-server running?')
        self.r = r

    def ingest_to_db_stream(self, data):
        """ Args:
            data (string)
        """
        self.r.rpush('stream', json.dumps(data))

    def ingest_to_db_sync(self, data):
        """ Args:
            data (string)
        """
        self.r.rpush('sync', json.dumps(data))

    def pull_and_store_stream(self, b):
        """ Args:
                chainsync () :

            Return:
                The specified block's information """
        for data in b.stream_from(full_blocks=True):
            self.ingest_to_db_stream(data)

    def pull_and_store_sync(self, b):
        """ Args:
                chainsync () :

            Return:
                The specified block's information """
        records_synced = 0
        try:
            for data in b.stream_from(start_block=self.last_index,
                                      batch_operations=True,
                                      full_blocks=True):
                records_synced += 1
                self.ingest_to_db_sync(data)
        except:
            return records_synced  # number of records synced

    def get_total_num_of_blocks(self):
        return self.r.llen('stream')

    def get_data(self, start, end):
        """ have to model this better, then return """
        data = self.r.lrange('stream', start, end)
        for i in range(len(data)):
            data[i] = data[i].decode()
            data[i] = json.loads(data[i])
        return data
