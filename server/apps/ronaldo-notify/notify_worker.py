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
import redis
import json
import pymongo

import logging
import logging.config
import uuid
import random
import urllib

import smtplib
from email.mime.text import MIMEText
import hashlib
import socket

logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class NotifyCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("NotifyCenter :running in __init__")
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self, moduleid)
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
#        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']))
        self.db = self.mongoconn.notify
        self.notify_log = self.db.notify_log
        self.notify_template = self.db.notify_template
        self.thread_index = moduleid
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.recv_queue_name = "W:Queue:NotifyCenter"
        if 'ronaldo-notify' in self._config:
            if 'Consumer_Queue_Name' in _config['ronaldo-notify']:
                self.recv_queue_name = _config['ronaldo-notify']['Consumer_Queue_Name']

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

        if action_cmd == 'send_notify':
            self._proc_action_sendnotify(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return


    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def run(self):
        logr.debug("Start NotifyCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logr.debug("_proc_message cost %f" % (time.time()-t1))                    


    def _sendEmail(self, dest, content, email_from, email_subject):
        fs = open("/opt/Keeprapid/Ronaldo/server/conf/email.conf", "r")
        gwconfig = json.load(fs)
        fs.close()
        logr.debug(gwconfig)

        msg = MIMEText(content, _subtype=gwconfig['content_type'], _charset=gwconfig['content_charset'])
        msg['Subject'] = email_subject.encode(gwconfig['content_charset'])
        msg['From'] = email_from.encode(gwconfig['content_charset'])
        msg['To'] = dest
        try:
            #注意端口号是465，因为是SSL连接
            s = smtplib.SMTP()
            emailhost = gwconfig['gwip']
            logr.debug(emailhost)
            s.connect(emailhost)
            s.starttls()
            s.login(gwconfig['from'],gwconfig['password'])
#            print gwconfig['email']['email_from'].encode(gwconfig['email']['email_charset'])
            s.sendmail(email_from.encode(gwconfig['content_charset']), dest.split(';'), msg.as_string())
            time.sleep(self.SEND_EMAIL_INTERVAL)
            s.close()
            return '200',None
        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            return '400',None    


    def _proc_action_sendnotify(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'to'    : M
                        'content'     : M
                        'carrier'     : M
                        'notify_type'   : M
                        
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into _proc_action_sendnotify action_body:%s"%action_body)
#        try:
        if 1:
            
            if ('dest' not in action_body) or  ('content' not in action_body) or  ('carrier' not in action_body) or  ('notify_type' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['dest'] is None or action_body['content'] is None or action_body['carrier'] is None or action_body['notify_type'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            dest = urllib.unquote(action_body['dest'].encode('utf-8')).decode('utf-8')
            carrier = action_body['carrier']
            notify_type = action_body['notify_type']
            content = urllib.unquote(action_body['content'].encode('utf-8')).decode('utf-8')
            if carrier == 'email':
                if 'email_from' in action_body and action_body['email_from'] is not None:
                    email_from = action_body['email_from']
                else:
                    email_from = 'noreply@keeprapid.com'

                if 'email_subject' in action_body and action_body['email_subject'] is not None:
                    email_subject = action_body['email_subject']
                else:
                    email_subject = 'From keeprapid'

                self._sendEmail(dest, content, email_from, email_subject)

            logdict = dict()
            logdict['from'] = email_from
            logdict['to'] = dest
            logdict['notify_type'] = notify_type
            logdict['carrier'] = carrier
            logdict['content'] = content
            logdict['timestamp'] = datetime.datetime.now()
            self.notify_log.insert(logdict)
                
            retdict['error_code'] = self.ERRORCODE_OK
            return

#        except Exception as e:
#            logr.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

        


if __name__ == "__main__":
    ''' parm1: moduleid,
    '''
    fileobj = open("/opt/Keeprapid/Ronaldo/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'ronaldo-notify' in _config and _config['ronaldo-notify'] is not None:
        if 'thread_count' in _config['ronaldo-notify'] and _config['ronaldo-notify']['thread_count'] is not None:
            thread_count = int(_config['ronaldo-notify']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = NotifyCenter(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
