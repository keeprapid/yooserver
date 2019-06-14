#!/usr/bin/python
# -*- coding: UTF-8 -*-
# filename:  httpgeproxy.py
# creator:   gabriel.liao
# datetime:  2013-6-17

"""http proxy server base gevent server"""
from gevent import monkey
monkey.patch_all()
# monkey.patch_os()
from gevent import socket
from gevent.server import StreamServer
from multiprocessing import Process
import logging
import logging.config
import threading
import uuid
import time
import sys
from Queue import Queue
import redis
import os
import json

try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser

logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logger = logging.getLogger('ronaldo')

class HttpProxyConsumer(threading.Thread):

    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created HttpProxyConsumer instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Ronaldo/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])


    def procdata(self, recvdata):
        recvbuf = json.loads(recvdata)
        if 'sockid' in recvbuf:
#            logger.debug(self._response_socket_dict)
            if recvbuf['sockid'] in self._response_socket_dict:
                respobj = self._response_socket_dict.pop(recvbuf['sockid'])
#                logger.debug(respobj)
                if respobj is not None and 'sock' in respobj:
                    respsockobj = respobj['sock']
#                    logger.debug("Recver callback [%s]" % (recvdata))
                    respsockobj.sendall('HTTP/1.1 200 OK\nContent-Type: application/json\n\n%s' % (recvdata))
                    respsockobj.shutdown(socket.SHUT_WR)
                    respsockobj.close()


    def run(self):
        queuename = "A:Queue:httpproxy"
        if self._config is not None and 'httpproxy' in self._config and self._config['httpproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['httpproxy'] and self._config['httpproxy']['Consumer_Queue_Name'] is not None:
                queuename = self._config['httpproxy']['Consumer_Queue_Name']

        listenkey = "%s:%s" % (queuename, os.getpid())
        logger.debug("HttpProxyConsumer::run listen key = %s" % (listenkey))
        while 1:
            try:
                recvdata = self._redis.brpop(listenkey)
#                logger.debug(recvdata)
                self.procdata(recvdata[1])

            except Exception as e:
                logger.error("PublishThread %s except raised : %s " % (e.__class__, e.args))
                time.sleep(1)


class PublishThread(threading.Thread):

    '''接收客户http请求内容，转换为封装AMPQ协议，采用Publish Api发送请求，
           并把socket对象和时间压入等待响应字典
       参数如下：
       httpclientsocketqueue:客户请求socket队列
       publisher:发送AMPQ消息对象
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
       recvbuflen:每次接收http请求内容长度，默认2048
    '''
    def __init__(self, httpclientsocketqueue, responsesocketdict, recvbuflen):  # 定义构造器
        logger.debug("Created PublishThread instance")
        threading.Thread.__init__(self)
        self._httpclientsocketqueue = httpclientsocketqueue
        self._response_socket_dict = responsesocketdict
        self._recvbuflen = recvbuflen
        fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Ronaldo/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])

    def run(self):
        queuename = "A:Queue:httpproxy"
        if self._config is not None and 'httpproxy' in self._config and self._config['httpproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['httpproxy'] and self._config['httpproxy']['Consumer_Queue_Name'] is not None:
                queuename = self._config['httpproxy']['Consumer_Queue_Name']

        selfqueuename = "%s:%s" % (queuename, os.getpid())
        logger.debug("PublishThread::run : %s" % (selfqueuename))
        while True:
            try:
                sockobj = self._httpclientsocketqueue.get()
                request_path = ""
                body = []
                p = HttpParser()
                seqid = uuid.uuid1()
                requestdict = dict()
                requestdict['sock'] = sockobj
                requestdatetime = time.strftime(
                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
                requestdict['requestdatetime'] = requestdatetime
                self._response_socket_dict[seqid.__str__()] = requestdict
                logger.debug("responsesocketdict len = %d", len(self._response_socket_dict))

                while True:
                    request = sockobj.recv(self._recvbuflen)
                    logger.warning("request  : %s" % (request))

                    recved = len(request)
                    logger.warning("recved   : %d" % (recved))

                    if(recved == 0):
                        logger.warning("socket is closed by peer")
                        sockobj.close()
                        break

                    nparsed = p.execute(request, recved)
                    logger.warning("nparsed  : %d" % (nparsed))
                    if nparsed != recved:
                        logger.warning("parse error")
                        sockobj.close()
                        break

                    if p.is_headers_complete():
                        request_headers = p.get_headers()
#                        for key in request_headers:
#                        logger.debug("headers complete %s" % (request_headers.__str__()))

#                        logger.warning("headers complete")

                    if p.is_partial_body():
                        body.append(p.recv_body())
#                        logger.warning("body  : %s" % (body))

                    if p.is_message_complete():
#                        logger.warning("message complete")
                        break

                content = "".join(body)

#                seqid = uuid.uuid1()

                routekey = ""
                servicepath = ""

                # 如果是/xxx格式认为是route key，如果是/xxx/yyy/zzz格式认为是dest service
                request_path = p.get_path()[1:]

                logger.warning('PublishThread request_path (%s), is routekey (%d)' % (request_path, request_path.find('/')))
#                logger.debug("content : %s" % (content))

                servicelist = os.listdir('./apps')

                if request_path.find('/') == -1 and len(request_path) and request_path in servicelist:

                    routekey = "A:Queue:%s" % request_path
                    if request_path in self._config:
                        routekey = self._config[request_path]['Consumer_Queue_Name']

                    if len(content) == 0:
                        content_json = dict()
                    else:
                        content_json = json.loads(content)

                    content_json['sockid'] = seqid.__str__()
                    content_json['from'] = selfqueuename
                    self._redis.lpush(routekey, json.dumps(content_json))
                else:
                    ret = dict()
                    ret['error_code'] = '40004'
                    sockobj.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(ret)))
                    sockobj.shutdown(socket.SHUT_WR)
                    sockobj.close()
                    continue


#                requestdict = dict()
#                requestdict['sock'] = sockobj
#                requestdatetime = time.strftime(
#                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
#                requestdict['requestdatetime'] = requestdatetime
#                self._response_socket_dict[seqid.__str__()] = requestdict

                # sockobj.sendall('HTTP/1.1 200 OK\n\nWelcome %s' % (
                #    seqid))
                # sockobj.close()

            except Exception as e:
                logger.error("PublishThread %s except raised : %s " % (
                    e.__class__, e.args))


class DogThread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
    '''
    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created DogThread instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        self._timeoutsecond = 300

    def _isoString2Time(self, s):
        '''
        convert a ISO format time to second
        from:2006-04-12 16:46:40 to:23123123
        把一个时间转化为秒
        '''
        ISOTIMEFORMAT = '%Y.%m.%d.%H.%M.%S'
        return time.strptime(s, ISOTIMEFORMAT)

    def calcPassedSecond(self, s1, s2):
        '''
        convert a ISO format time to second
        from:2006-04-12 16:46:40 to:23123123
        把一个时间转化为秒
        '''
        s1 = self._isoString2Time(s1)
        s2 = self._isoString2Time(s2)
        return time.mktime(s1) - time.mktime(s2)

    def run(self):
        logger.debug("DogThread::run")

        while True:
#            if 1:
            try:
#                now = time.strftime(
#                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
                now = time.time()


                sortedlist = sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['requestdatetime'], reverse = 0)
#                sortedlist = sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['requestdatetime'])
                for each in sortedlist:
                    key = each[0]
                    responseobj = each[1]
                    requestdatetime = responseobj['requestdatetime']
                    passedsecond = now - requestdatetime
                    if passedsecond > self._timeoutsecond:
                        tmpobj = self._response_socket_dict.pop(key)
                        sockobj = tmpobj['sock']
                        sockobj.sendall(
                            'HTTP/1.1 500 OK\n\Timeout %s' % (key))
#                        sockobj = responseobj['sock']
                        logger.debug("DogThread close timeout sock[%s]%r" %(key, sockobj))
                        sockobj.close()
#                        del(self._response_socket_dict[key])
                    else:
                        break
#                sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['requestdatetime'])
#
#                for key in self._response_socket_dict.keys():
#                    responseobj = self._response_socket_dict[key]
#                    requestdatetime = responseobj['requestdatetime']
#                    passedsecond = self.calcPassedSecond(now, requestdatetime)
#                    if passedsecond > self._timeoutsecond:
#                        del(self._response_socket_dict[key])
#                        sockobj = responseobj['sock']
#                        sockobj.sendall(
#                            'HTTP/1.1 500 OK\n\Timeout %s' % (key))
#                        sockobj.close()
#                    else:
#                        break

                time.sleep(0.1)


#        while True:
#            try:
#                now = time.strftime(
#                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
#
#                sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['requestdatetime'])
#
#                for key in self._response_socket_dict.keys():
#                    responseobj = self._response_socket_dict[key]
#                    requestdatetime = responseobj['requestdatetime']
#                    passedsecond = self.calcPassedSecond(now, requestdatetime)
#                    if passedsecond > self._timeoutsecond:
#                        del(self._response_socket_dict[key])
#                        sockobj = responseobj['sock']
#                        sockobj.sendall(
#                            'HTTP/1.1 500 OK\n\Timeout %s' % (key))
#                        sockobj.close()
#                    else:
#                        break
#
#                time.sleep(0.1)

            except Exception as e:
                logger.warning("DogThread %s except raised : %s " % (
                    e.__class__, e.args))
                time.sleep(0.1)



if __name__ == "__main__":

    httpclientsocketqueue = Queue()
    responsesocketdict = dict()
    fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()

    fileobj = open("/opt/Keeprapid/Ronaldo/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    _redis = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])
    recv_buf_len = 2048
    port = 8081

    queuename = "A:Queue:httpproxy"
    if _config is not None and 'httpproxy' in _config and _config['httpproxy'] is not None:
        if 'Consumer_Queue_Name' in _config['httpproxy'] and _config['httpproxy']['Consumer_Queue_Name'] is not None:
            queuename = _config['httpproxy']['Consumer_Queue_Name']

    selfqueuename = "%s:%s" % (queuename, os.getpid())
    def recvrawsocket2(sockobj, address):
        try:

            logger.error(sockobj)
            request_path = ""
            body = []
            p = HttpParser()
            seqid = uuid.uuid1()
            requestdict = dict()
            requestdict['sock'] = sockobj
    #                requestdatetime = time.strftime('%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
            requestdatetime = time.time()
            requestdict['requestdatetime'] = requestdatetime
            responsesocketdict[seqid.__str__()] = requestdict
            logger.debug("responsesocketdict len = %d", len(responsesocketdict))

            while True:
                request = sockobj.recv(recv_buf_len)
    #                    logger.warning("request  : %s" % (request))

                recved = len(request)
    #                    logger.warning("recved   : %d" % (recved))

                if(recved == 0):
                    logger.warning("socket is closed by peer %r" % (sockobj))
                    sockobj.close()
                    break

                nparsed = p.execute(request, recved)
                logger.warning("nparsed  : %d" % (nparsed))
                if nparsed != recved:
                    logger.warning("parse error")
                    sockobj.close()
                    break

                if p.is_headers_complete():
                    request_headers = p.get_headers()
    #                        for key in request_headers:
    #                        logger.debug("headers complete %s" % (request_headers.__str__()))

    #                        logger.warning("headers complete")

                if p.is_partial_body():
                    body.append(p.recv_body())
    #                        logger.warning("body  : %s" % (body))

                if p.is_message_complete():
    #                        logger.warning("message complete")
                    break

            content = "".join(body)

    #                seqid = uuid.uuid1()

            routekey = ""
            servicepath = ""

            # 如果是/xxx格式认为是route key，如果是/xxx/yyy/zzz格式认为是dest service
            request_path = p.get_path()[1:]

    #                logger.warning('PublishThread request_path (%s), is routekey (%d)' % (request_path, request_path.find('/')))
    #                logger.debug("content : %s" % (content))

            servicelist = os.listdir('./apps')

            if request_path.find('/') == -1 and len(request_path) and request_path in servicelist:

                routekey = "A:Queue:%s" % request_path
                if request_path in _config:
                    routekey = _config[request_path]['Consumer_Queue_Name']

                if len(content) == 0:
                    content_json = dict()
                else:
                    content_json = json.loads(content)

                content_json['sockid'] = seqid.__str__()
                content_json['from'] = selfqueuename
                _redis.lpush(routekey, json.dumps(content_json))
            else:
                ret = dict()
                ret['error_code'] = '40004'
                sockobj.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(ret)))
                sockobj.shutdown(socket.SHUT_WR)
                sockobj.close()


    #                requestdict = dict()
    #                requestdict['sock'] = sockobj
    #                requestdatetime = time.strftime(
    #                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
    #                requestdict['requestdatetime'] = requestdatetime
    #                responsesocketdict[seqid.__str__()] = requestdict

            # sockobj.sendall('HTTP/1.1 200 OK\n\nWelcome %s' % (
            #    seqid))
            # sockobj.close()

        except Exception as e:
            logger.error("recvrawsocket2 %s except raised : %s " % (e.__class__, e.args))

    def recvrawsocket(sockobj, address):
        '''
        接收客户http请求，将socket对象压入publish队列，由PublishThread处理
        '''
        httpclientsocketqueue.put(sockobj)

    recv_buf_len = 2048
    port = 8081

    dogAgent = DogThread(responsesocketdict)
    dogAgent.setDaemon(True)
    dogAgent.start()

    publishAgent = PublishThread(
        httpclientsocketqueue, responsesocketdict, recv_buf_len)
    publishAgent.setDaemon(True)
    publishAgent.start()

    response_count = 1
    for i in range(0,response_count):
        publishConsumer = HttpProxyConsumer(responsesocketdict)
        publishConsumer.setDaemon(True)
        publishConsumer.start()


    logger.error('Http gevent proxy serving on %d...' % (port))
    server = StreamServer(('', port), recvrawsocket2, backlog=100000)
    server.serve_forever()
