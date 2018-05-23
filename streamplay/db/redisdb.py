import json
import sys

from steem.blockchain import Blockchain
import redis

""" Could be improved:
    1. SSL encrytion
    2. Password
 """


class RedisDB:

    def __init__(self, hostname='localhost', portnumber=6379, password=''):
        """ Args:
                hostname (string): IP address of the machine where redis-server
                                    is running
                portnumber (integer): Port number, default is 6379
                password (string): Default password is ''
        """
        self.hostname = hostname
        self.portnumber = portnumber
        self.password = password
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

    def ingest_to_db(self, data):
        """ Args:
            key_data (string)
        """
        self.r.rpush('stream', json.dumps(data))

    def pull_and_store(self, b):
        """ Args:
                chainsync () :

            Return:
                The specified block's information """
        for data in b.stream_from(full_blocks=True):
            self.ingest_to_db(data)

    def get_total_num_of_blocks(self):
        return self.r.llen('stream')

    def get_data(self, start, end):
        """ have to model this better, then return """
        data = self.r.lrange('stream', start, end)
        for i in range(len(data)):
            data[i] = data[i].decode()
            data[i] = json.loads(data[i])
        return data

