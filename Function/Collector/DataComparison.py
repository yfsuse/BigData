#! /usr/bin/env python
# --*-- coding:utf-8 --*--


__author__ = 'jeff'

"""
Kafka Data Log
Batch File
Hadoop File
Hive Record
Impala Record
"""

import os
import datetime
import socket
import fcntl
import struct



def get_ip_address(ifname = 'eth0'):
    """
    :param ifname: eth0
    :return: 10-1-10-10
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915, # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
        )[20:24]).replace('.', '-')


class Comparison(object):

    def __init__(self):
        self.today = self.getDate()
        self.localip = get_ip_address()
        self.tmpfile = 'data_{0}_{1}'.format(self.today, self.localip)

    def getDate(self):
        today = datetime.datetime.now()
        yesterday = today + datetime.timedelta(days=-1)
        return yesterday.strftime('%Y-%m-%d')

    def getCount(self):
        # get cmd
        kafkaCmd = 'zcat /data1/kafkaService/kafka.log.{0}.gz | grep -c success'.format(self.today)
        batchCmd = 'wc -l /data1/druidBatchData/ip-{0}.ec2.internaldata_{1}T*.json'.format(self.localip, self.today)
        hadoopCmd = 'wc -l /data/ymds_logs/yfnormalpf/ip-{0}.ec2.internal_{1}T*.completed'.format(self.localip, self.today)

        kafkaCount = os.popen(kafkaCmd).read().strip()
        batchCount = os.popen(batchCmd).read().split(' ')[-2]
        hadoopCount = os.popen(hadoopCmd).read().split(' ')[-2]

        with open(self.tmpfile, 'w') as f:
            f.writelines(kafkaCount + '\n')
            f.writelines(batchCount + '\n')
            f.writelines(hadoopCount + '\n')

if __name__ == '__main__':
    c = Comparison()
    c.getDate()