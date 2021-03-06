# Takes lists of objects returned by the zkclient module, and
# consolidates the information for display.

import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('kafka.codec').addHandler(NullHandler())

import struct
import socket
from collections import namedtuple
from kafka.client import KafkaClient
from kafka.common import OffsetRequest

class ProcessorError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

PartitionState = namedtuple('PartitionState',
    [
         'broker',           # Broker host
         'topic',            # Topic on broker
         'partition',        # The partition
         'earliest',         # Earliest offset within partition on broker
         'latest',           # Current offset within partition on broker
         'depth',            # Depth of partition on broker.
         'spout',            # The Spout consuming this partition
         'current',          # Current offset for Spout
         'delta',             # Difference between latest and current
         'TopologyStatsAck',
         'TopologyStatsFailed',
         'SpoutsAck',
         'SpoutsFailed',
         'FirstBoltsAck',
         'FirstBoltsFailed',
         'FirstBoltsProcessLatency',
         'SecondBoltsAck',
         'SecondBoltsFailed',
         'SecondBoltsProcessLatency'
    ])
PartitionsSummary = namedtuple('PartitionsSummary',
    [
         'total_depth',      # Total queue depth.
         'total_delta',      # Total delta across all spout tasks.
         'num_partitions',   # Number of partitions.
         'num_brokers',      # Number of Kafka Brokers.
         'partitions'        # Tuple of PartitionStates
    ])

def process(spouts,json_data):
    '''
    Returns a named tuple of type PartitionsSummary.
    '''
    results = []
    total_depth = 0
    total_delta = 0
    brokers = []
    for s in spouts:
        for p in s.partitions:
            try:
                print "process function: broker host:" + p['broker']['host'] 
                k = KafkaClient(p['broker']['host'], str(p['broker']['port']))
            except socket.gaierror, e:
                raise ProcessorError('Failed to contact Kafka broker %s (%s)' %
                          (p['broker']['host'], str(e)))
                earliest_off = OffsetRequest(str(p['topic']), p['partition'], -2, 1)
                latest_off = OffsetRequest(str(p['topic']), p['partition'], -1, 1)
                earliest = k.send_offset_request([earliest_off])[0]
                latest = k.send_offset_request([latest_off])[0]
                current = p['offset']

                brokers.append(p['broker']['host'])
                total_depth = total_depth + (latest.offsets[0] - earliest.offsets[0])
                total_delta = total_delta + (latest.offsets[0] - current)

    results.append(PartitionState._make([
                    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]))
                    #p['broker']['host'],
                    #p['topic'],
                    #p['partition'],
                    #earliest.offsets[0],
                    #latest.offsets[0],
                    #latest.offsets[0] - earliest.offsets[0],
                    #s.id,
                    #current,
                    #latest.offsets[0] - current]),
                    #json_data['topologyStats'][2]['acked'],
                    #json_data['topologyStats'][2]['failed'],
                    #json_data['spouts'][0]['acked'],
                    #json_data['spouts'][0]['failed'],
                    #json_data['bolts'][0]['acked'],
                    #json_data['bolts'][0]['failed'],
                    #json_data['bolts'][0]['processLatency'],
                    #json_data['bolts'][1]['acked'],
                    #json_data['bolts'][1]['failed'],
                    #json_data['bolts'][1]['processLatency'])

    return PartitionsSummary(total_depth=total_depth,
                total_delta=total_delta,
                num_partitions=len(results),
                num_brokers=len(set(brokers)),
                partitions=tuple(results))
