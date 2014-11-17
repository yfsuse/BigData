#! /usr/bin/env python
# --*-- coding:utf-8 --*--

__author__ = 'jeff.yu'

from random import choice
from datetime import date, timedelta
import time
import json
import urllib
import urllib2
import getpass
from Function.Query.YeahMobi.Cases.Producer import *

def combinationChoice(seq, count):
    """
    :param seq: [1,2,3,4,5,6,7,8,9,10]
    :param count: 3
    :return: [2,10,3]
    """
    choiceItem = []
    if not seq:
        return choiceItem
    loopCount = len(seq)
    for i in range(loopCount):
        selected = choice(seq)
        if selected not in choiceItem:
            choiceItem.append(selected)
        if len(choiceItem) == count:
            break
    return choiceItem

def datetime_timestamp():
    time_end = date.today()
    time_start = time_end + timedelta(days=-1)
    str_time_start, str_time_end = time_start.strftime('%Y-%m-%d'), time_end.strftime('%Y-%m-%d')
    return int(time.mktime(time.strptime(str_time_start, '%Y-%m-%d'))) + 28800, int(time.mktime(time.strptime(str_time_end, '%Y-%m-%d'))) + 28800

def queryConvert(jsonQuery):
    return str(jsonQuery)

def getHttpData(groupDimension):
    start, end = datetime_timestamp()
    report_param = '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"data_source":"ymds_druid_datasource","report_id":"report-id","pagination":{"size":500,"page":0}},"group":["%s"],"data":["click","conversion"],"filters":{"$and":{"datasource":{"$neq":"hasoffer"}}},"sort":[]}' % (start, end, groupDimension)
    post_data = {'report_param': report_param}
    encodeData = urllib.urlencode(post_data)
    post_url = 'http://resin-yeahmobi-214401877.us-east-1.elb.amazonaws.com:18080/report/report?'
    req = urllib2.Request(url = post_url, data = encodeData)
    try:
        res = urllib2.urlopen(req).read()
    except urllib2.HTTPError as e:
        return None
    return res

def getDimensionFile(queryType):
    return '/home/{0}/dataCenter/BigData/Function/Query/Config/{1}.json'.format(getpass.getuser(), queryType)

def JsonToStr(jsonReq):
    return str(jsonReq).replace("'", '"')

def getFilterValues(groupDimension, selectCount=1, queryType='YeahMobi'):
    dimensionResult = getHttpData(groupDimension)
    if not dimensionResult:
        if selectCount == 1:
            return ""
        else:
            return []
    try:
        dimensionList = json.loads(dimensionResult).get('data').get('data')
    except Exception as e:
        raise  e
    dataList = []

    print dimensionList


    for dataSturct in dimensionList[2:]:
        dataList.append(dataSturct[0])

    if selectCount == 1:
        return choice(dataList)
    else:
        return dataList[:selectCount]

if __name__ == '__main__':
    p = Producer('YeahMobi')
    p.getDimension()
    dimensions = p.dimensions
    for d in dimensions:
        print d, getFilterValues(d, 10)