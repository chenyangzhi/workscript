import simplejson as json
from collections import namedtuple

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

ZkKafkaBroker = namedtuple('ZkKafkaBroker', ['host', 'port'])
ZkKafkaSpout = namedtuple('ZkKafkaSpout', ['id', 'partitions'])
ZkKafkaTopic = namedtuple('ZkKafkaTopic', ['topic', 'broker', 'num_partitions'])

class ZkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class ZkClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = KazooClient(hosts=':'.join([host, str(port)]))
        self.client.start()

    @classmethod
    def _zjoin(cls, e):
        return '/'.join(e)

    def get_children(self,path):
	return self.client.get_children(path)
    def get_node(self,path):
	return self.client.get(path)

    def brokers(self, broker_root='/brokers'):
        '''
        Returns a list of ZkKafkaBroker tuples, where each value is a
        ZkKafkaBroker.
        '''
        b = []
        id_root = self._zjoin([broker_root, 'ids'])

        #self.client.start()
        try:
            for c in self.client.get_children(id_root):
                n = self.client.get(self._zjoin([id_root, c]))[0]
                b.append(ZkKafkaBroker(c, n.split(':')[1], n.split(':')[2]))
        except NoNodeError:
            raise ZkError('Broker nodes do not exist in Zookeeper')
        self.client.stop()
        return b

    def topics(self, broker_root='/brokers'):
        '''
        Returns a list of ZkKafkaTopic tuples, where each tuple represents
        a topic being stored in a broker.
        '''
        topics = []
        t_root = self._zjoin([broker_root, 'topics'])

        self.client.start()
        try:
            for t in self.client.get_children(t_root):
                for b in self.client.get_children(self._zjoin([t_root, t])):
                    n = self.client.get(self._zjoin([t_root, t, b]))[0]
                    topics.append(ZkKafkaTopic._make([t, b, n]))
        except NoNodeError:
            raise ZkError('Topic nodes do not exist in Zookeeper')
        self.client.stop()
        return topics
    def stop(self):
        self.client.stop()

    def spouts(self, spout_root, topology):
        '''
        Returns a list of ZkKafkaSpout tuples, where each tuple represents
        a Storm Kafka Spout.
        '''
        s = []
	print "come into the spouts function!"
	print "spout_root = " + spout_root 
        self.client.start()
	print "spout_root = " + spout_root 
        try:
            for c in self.client.get_children(spout_root):
		print "c = " + c
		zp = self._zjoin([spout_root, c])
		print "zp  = " + zp
		jj = self.client.get(zp)[0]
		print "this is a tuple: %s" % (jj,)
                j = json.loads(jj)
                if j['topology']['name'] == topology:
                	s.append(ZkKafkaSpout._make([c, [j]]))
        except NoNodeError:
            raise ZkError('Kafka Spout nodes do not exist in Zookeeper')
        return tuple(s)
