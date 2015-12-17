#!/usr/bin/env python

import argparse
import sys
from prettytable import PrettyTable
import requests
import simplejson as json
import re, urllib
import smtplib

from zkclient import ZkClient, ZkError
from processor import process, ProcessorError

url = "http://10.10.10.169:8081/topology.html?id="
crawlUrl =  "http://10.10.10.169:8081"
sender = 'coco@zhangwenchao177.jx.diditaxi.com'
receivers = ['695611691@qq.com']
message = """From: From coco <coco@zhangwenchao177.jx.diditaxi.com>
To: To chenyangzhi <695611691@qq.com>
MIME-Version: 1.0
Content-type: text/html
Subject: SMTP HTML e-mail test

This is an e-mail message to be sent in HTML format
"""
htmlEnd = """
</table>
</body>
</html>
"""

def sizeof_fmt(num):
        for x in [' bytes','KB','MB','GB']:
                if num < 1024.0:
                        return "%3.1f%s" % (num, x)
        num /= 1024.0
        return "%3.1f%s" % (num, 'TB')

def null_fmt(num):
        return num
def strconcat(np):
    str1 = ""
    print "fsafs" 
    print type(str(np.SecondBoltsProcessLatency))

    str1 = str1 + \
          "<td>" + str(np.broker)                     +   "</td>"  + \
          "<td>" + str(np.topic)                      +   "</td>"  + \
          "<td>" + str(np.partition)                  +   "</td>"  + \
          "<td>" + str(np.earliest)                   +   "</td>"  + \
          "<td>" + str(np.latest)                     +   "</td>"  + \
          "<td>" + str(np.depth)                      +   "</td>"  + \
          "<td>" + str(np.spout)                      +   "</td>"  + \
          "<td>" + str(np.current)                    +   "</td>"  + \
          "<td>" + str(np.delta)                      +   "</td>"  + \
          "<td>" + str(np.TopologyStatsAck)           +   "</td>"  + \
          "<td>" + str(np.TopologyStatsFailed)        +   "</td>"  + \
          "<td>" + str(np.SpoutsAck)                  +   "</td>"  + \
          "<td>" + str(np.SpoutsFailed)               +   "</td>"  + \
          "<td>" + str(np.FirstBoltsAck)              +   "</td>"  + \
          "<td>" + str(np.FirstBoltsFailed)           +   "</td>"  + \
          "<td>" + str(np.FirstBoltsProcessLatency)   +   "</td>"  + \
          "<td>" + str(np.SecondBoltsAck)             +   "</td>"  + \
          "<td>" + str(np.SecondBoltsFailed)          +   "</td>"  + \
          "<td>" + str(np.SecondBoltsProcessLatency)  +   "</td>"  
    return str1

def mailmessage(summary):
    str = ""
    for p in summary.partitions:
        str = "<tr>"
        str = str + strconcat(p) + "</tr>"
    return str


def display(summary, friendly=False):
    if friendly:
        fmt = sizeof_fmt
    else:
        fmt = null_fmt

    table = PrettyTable(['Broker', 'Topic', 'Partition', 'Earliest', 'Latest',
            'Depth', 'Spout', 'Current', 'Delta'])
    table.align['broker'] = 'l'

    for p in summary.partitions:
        table.add_row([p.broker, p.topic, p.partition, p.earliest, p.latest,
               fmt(p.depth), p.spout, p.current, fmt(p.delta)])
    print table.get_string(sortby='Broker')
    print
    print 'Number of brokers:       %d' % summary.num_brokers
    print 'Number of partitions:    %d' % summary.num_partitions
    print 'Total broker depth:      %s' % fmt(summary.total_depth)
    print 'Total delta:             %s' % fmt(summary.total_delta)

def true_or_false_option(option):
    if option == None:
        return False
    else:
        return True

def read_args():
    parser = argparse.ArgumentParser(
                    description='Show complete state of Storm-Kafka consumers')
    parser.add_argument('--zserver', default='localhost',
                    help='Zookeeper host (default: localhost)')
    parser.add_argument('--zport', type=int, default=2181,
                    help='Zookeeper port (default: 2181)')
    parser.add_argument('--topology', type=str, required=True,
                    help='Storm Topology')
    parser.add_argument('--spoutroot', type=str, required=True,
                    help='Root path for Kafka Spout data in Zookeeper')
    parser.add_argument('--friendly', action='store_const', const=True,
                    help='Show friendlier data')
    parser.add_argument('--postjson', type=str,
                    help='endpoint to post json data to')
    return parser.parse_args()

def main():
    global message
    content = ""
    f = open("../resource/template.html")
    line = f.readline()
    while line:
        content = content + line
        line = f.readline()
    f.close()
    options = read_args()

    zc = ZkClient(options.zserver, options.zport)
    my_topology_list = zc.get_children("/")
    for single_topo in my_topology_list:
        api = crawlUrl + "/api/v1/topology/" + single_topo + "?sys=false"
        read = urllib.urlopen(api).read()
        storm_ui_data = json.loads(read)
        if not re.search("walle",single_topo):
            continue

        consumer_partition = "/" + single_topo + "/partition_0"
        print "consumer_partition" + consumer_partition
        tuple = zc.get_node(consumer_partition)[0]
        j = json.loads(tuple)
        toponame = j['topology']['name'] 
        try:
            zk_data = process(zc.spouts(consumer_partition, toponame),storm_ui_data)
        except ZkError, e:
            print 'Failed to access Zookeeper: %s' % str(e)
            return 1
        except ProcessorError, e:
            print 'Failed to process: %s' % str(e)
            return 1
        else:
            display(zk_data, true_or_false_option(options.friendly))
            print zk_data
            content = content + mailmessage(zk_data)
    zc.stop()
    message = message + content + htmlEnd
    print message 
    try:
         smtpObj = smtplib.SMTP('localhost')
         smtpObj.sendmail(sender, receivers, message)
         print "Successfully sent email"
    except SMTPException:
         print "Error: unable to send email"

if __name__ == '__main__':
    sys.exit(main())
