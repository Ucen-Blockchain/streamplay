import json
import sys

import chainsync
import redis


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

    def ingest_to_db(self, key_data):
        """ Args:
            key_data (string)
        """
        self.r.rpush(key_data[0], json.dumps(key_data[1]))

    def pull_and_store(self, chainsync):
        """ Args:
                chainsync () :

            Return:
                The specified block's information """
        for key_data in chainsync.stream(['blocks', 'status'],
                                         mode='irreversible',
                                         batch_size=1):
            self.ingest_to_db(key_data)

    def get_total_num_of_blocks(self):
        return self.r.llen('block'), self.r.llen('status')

    def get_data(self, datatype, start, end):
        """ have to model this better, then return """
        data = self.r.lrange(datatype, start, end)
        for i in range(len(data)):
            data[i] = data[i].decode()
            data[i] = json.loads(data[i])
        return data

