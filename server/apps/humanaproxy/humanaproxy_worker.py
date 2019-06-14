#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import sys
import subprocess
import os
import time
import datetime
import time
import threading
if '/opt/Keeprapid/Ronaldo/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Ronaldo/server/apps/common')
import workers

import json
import pymongo
import redis
import urllib
import requests
import logging
import logging.config
import uuid
from bson.objectid import ObjectId

logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class HumanaProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("HumanaProxy :running in __init__")
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self, moduleid)
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.thread_index = moduleid
        self.recv_queue_name = "W:Queue:HumanaProxy"
        if 'humanaproxy' in self._config:
            if 'Consumer_Queue_Name' in _config['humanaproxy']:
                self.recv_queue_name = _config['humanaproxy']['Consumer_Queue_Name']





#       fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/db.conf', 'r')
#       self._json_dbcfg = json.load(fileobj)
#       fileobj.close()

    def __str__(self):
        pass
        '''

        '''

    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        logr.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            logr.debug(recvbuf)
            msgdict = json.loads(recvbuf)
        except:
            logr.error("parse body error")
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logr.error("body lenght is zero")
            return
        if "from" not in msgdict:
            logr.error("no route in body")
            return
        msgfrom = msgdict['from']

        seqid = '0'
        if "seqid" in msgdict:
            seqid = msgdict['seqid']

        sockid = ''
        if 'sockid' in msgdict:
            sockid = msgdict['sockid']

        if "action_cmd" not in msgdict:
            logr.error("no action_cmd in msg")
            self._sendMessage(msgfrom, '{"from":%s,"error_code":"40000","seq_id":%s,"body":{},"sockid":%s)' % (self.recv_queue_name, seqid, sockid))
            return
        #构建回应消息结构
        action_cmd = msgdict['action_cmd']

        message_resp_dict = dict()
        message_resp_dict['from'] = self.recv_queue_name
        message_resp_dict['seq_id'] = seqid
        message_resp_dict['sockid'] = sockid
        message_resp_body = dict()
        message_resp_dict['body'] = message_resp_body
        
        self._proc_action(msgdict, message_resp_dict, message_resp_body)

        msg_resp = json.dumps(message_resp_dict)
        logr.debug(msg_resp)
        self._sendMessage(msgfrom, msg_resp)   

    def _proc_action(self, msg_in, msg_out_head, msg_out_body):
        '''action处理入口函数'''
        if 'action_cmd' not in msg_in or 'version' not in msg_in:
            logr.error("mandotry param error in action")
            msg_out_head['error_code'] = '40002'
            return
        action_cmd = msg_in['action_cmd']
        logr.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
        logr.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logr.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logr.debug('no action_body')

        if action_cmd == 'upload_data':
            self._proc_action_upload_data(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return

    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def run(self):
        logr.debug("Start HumanaProxy pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logr.debug("_proc_message cost %f" % (time.time()-t1))                    


    def _proc_action_upload_data(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'gps_upload', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : O
                        'vid'     : M
                        'long'     : M
                        'lat'   : M
                        'gpsinfo'    : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into _proc_action_upload_data action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('url' not in action_body) or  ('method' not in action_body) or  ('headers' not in action_body) or ('uploadbody' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['url'] is None or action_body['method'] is None or action_body['headers'] is None or action_body['uploadbody'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            vid = action_body['vid']
            url = action_body['url']
            method = action_body['method']
            headers = action_body['headers']
            headers["Content-Type"] = "application/json"
            uploadbody = action_body['uploadbody']
            uploadbody = json.dumps(uploadbody)
            logr.debug(uploadbody)
            logr.debug(headers)
            logr.debug(url)
            if method == 'POST':
                resp = requests.post(url,headers=headers,data=uploadbody)
            else:
                resp = requests.put(url,headers=headers,data=uploadbody)
            logr.debug(resp)
            logr.debug(resp.text)
            retdict['statusCode'] = resp.status_code
            if resp.status_code != 500:
                retdict['responeBody'] = json.loads(resp.text)
            else:
                retdict['responeBody'] = dict()
            retdict['error_code'] = '200'
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



if __name__ == "__main__":
    ''' parm1: moduleid,
    '''
    fileobj = open("/opt/Keeprapid/Ronaldo/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'humanaproxy' in _config and _config['humanaproxy'] is not None:
        if 'thread_count' in _config['humanaproxy'] and _config['humanaproxy']['thread_count'] is not None:
            thread_count = int(_config['humanaproxy']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = HumanaProxy(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
