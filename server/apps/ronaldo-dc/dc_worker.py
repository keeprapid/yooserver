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
import logging
import logging.config
import uuid
from bson.objectid import ObjectId

logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class DataCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("DataCenter :running in __init__")
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self, moduleid)
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.gpsdb = self.mongoconn.gpsinfo
        self.gpsinfo = self.gpsdb.gpsinfo

#        self.memberconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.memberconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self.memberdb = self.memberconn.member
        self.memberinfo = self.memberdb.memberinfo

#        self.dcconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.dcconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self.dcdb = self.dcconn.datacenter
        self.dcinfo = self.dcdb.data

        self.thread_index = moduleid
        self.recv_queue_name = "W:Queue:DataCenter"
        if 'ronaldo-dc' in self._config:
            if 'Consumer_Queue_Name' in _config['ronaldo-dc']:
                self.recv_queue_name = _config['ronaldo-dc']['Consumer_Queue_Name']





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

        if action_cmd == 'gps_upload':
            self._proc_action_gpsupload(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'data_sync':
            self._proc_action_data_sync(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_sync_time':
            self._proc_action_query_sync_time(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return

    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def run(self):
        logr.debug("Start DataCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logr.debug("_proc_message cost %f" % (time.time()-t1))                    


    def _proc_action_gpsupload(self, version, action_body, retdict, retbody):
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
        logr.debug(" into _proc_action_gpsupload action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('long' not in action_body) or  ('lat' not in action_body) or  ('phone_id' not in action_body) or ('phone_os' not in action_body) or  ('phone_name' not in action_body) or ('app_version' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['long'] is None or action_body['lat'] is None or action_body['phone_id'] is None or action_body['phone_os'] is None or action_body['phone_name'] is None or action_body['phone_id'] is None or action_body['app_version'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = None
            if 'tid' in action_body and action_body['tid'] is not None:
                tid = action_body['tid']

            extinfo = None
            if 'extinfo' in action_body and action_body['extinfo'] is not None:
                extinfo = action_body['extinfo']

            vid = action_body['vid']
            long_value = float(action_body['long'])
            lat_value = float(action_body['lat'])
            phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            app_version = urllib.unquote(action_body['app_version'].encode('utf-8')).decode('utf-8')
            memberid = None
            if tid is not None:
                #更新用户数据中的long lat
                searchkey = self.KEY_TOKEN_NAME_ID % (action_body['tid'],'*','*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist):
                    tni = self._redis.hgetall(resultlist[0])
                    if tni is not None and '_id' in tni:
                        memberid = tni['_id']

                if memberid is not None:
                    memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
                    if memberinfo is not None:
                        self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'lastlong':long_value,'lastlat':lat_value}})


            insertgps = dict()
            if memberid is not None:
                insertgps['memberid'] = memberid
            if extinfo is not None:
                insertgps['extinfo'] = extinfo
            else:
                insertgps['except'] = dict()

            insertgps['long'] = long_value
            insertgps['lat'] = lat_value
            insertgps['phone_id'] = phone_id
            insertgps['phone_os'] = phone_os
            insertgps['phone_name'] = phone_name
            insertgps['vid'] = vid
            insertgps['timestamp'] = datetime.datetime.now()

            self.gpsinfo.insert(insertgps)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_query_sync_time(self, version, action_body, retdict, retbody):
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
        logr.debug(" into _proc_action_query_sync_time action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('uid' not in action_body) or  ('tid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['uid'] is None or action_body['tid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = None
            if 'tid' in action_body and action_body['tid'] is not None:
                tid = action_body['tid']

            vid = action_body['vid']
            tid = action_body['tid']
            uid = action_body['uid']
#            macid = action_body['mac_id']

            memberid = None
            searchkey = self.KEY_TOKEN_NAME_ID % (tid,'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tniinfo = self._redis.hgetall(resultlist[0])
            if tniinfo is None or '_id' not in tniinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            memberid = tniinfo['_id']

            memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            userlist = memberinfo.get('users')
            if userlist is None or len(userlist)==0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return

            isfind = False
            for userinfo in userlist:
                tmpuid = userinfo.get('uid')
                if tmpuid is None or tmpuid != uid:
                    continue
                else:
                    isfind = True
                    timestamp = userinfo.get('last_synctime')
                    if timestamp is None:
                        timestamp = 0
                    retbody['timestamp'] = timestamp
#                    macdict = userinfo.get('syncmacs')
#                    if macdict is None:
#                        tmpdict = dict()
#                        tmpmacdict = dict()
#                        tmpmacdict['timestamp'] = 0
#                        tmpdict[macid] = tmpmacdict
#                        userinfo['syncmacs'] = tmpdict
#                        self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'users':userlist}})
#                        retbody['timestamp'] = 0
#                    else:
#                        if macid in macdict:
#                            retbody['timestamp'] = macdict[macid]['timestamp']
#                        else:
#                            tmpmacdict = dict()
#                            tmpmacdict['timestamp'] = 0
#                            macdict[macid]= tmpmacdict
#                            self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'users':userlist}})
#                            retbody['timestamp'] = 0

            if isfind:
                retdict['error_code'] = '200'
            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_data_sync(self, version, action_body, retdict, retbody):
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
        logr.debug(" into _proc_action_data_sync action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('uid' not in action_body) or  ('tid' not in action_body) or  ('timestamp' not in action_body) or  ('data' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['uid'] is None or action_body['tid'] is None or action_body['timestamp'] is None or action_body['data'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = None
            if 'tid' in action_body and action_body['tid'] is not None:
                tid = action_body['tid']

            vid = action_body['vid']
            tid = action_body['tid']
            uid = action_body['uid']
            timestamp = int(action_body['timestamp'])
            data = action_body['data']

            memberid = None
            searchkey = self.KEY_TOKEN_NAME_ID % (tid,'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tniinfo = self._redis.hgetall(resultlist[0])
            if tniinfo is None or '_id' not in tniinfo:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            memberid = tniinfo['_id']

            memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            userlist = memberinfo.get('users')
            if userlist is None or len(userlist)==0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return

            isfind = False
            existdata = dict()
            existtimedict = dict()
            for userinfo in userlist:
                tmpuid = userinfo.get('uid')
                if tmpuid is None or tmpuid != uid:
                    continue
                else:
                    isfind = True
                    last_synctime = userinfo.get('last_synctime')
                    if last_synctime is None:
                        last_synctime = 0
                    userinfo['last_synctime'] = timestamp
                    self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'users':userlist}})

            if isfind:
                for key in data:
                    tmpmacid = key
                    macdata = data[key]
                    datalist = macdata.split(';')

                    maxtimestamp = 0
                    mintimestamp = 0
                    if len(datalist) > 0:
                        for datastr in datalist:
                            dataparam = datastr.split(',')
                            if len(dataparam)<5:
                                continue
                            datatimestamp = int(dataparam[4])
                            if mintimestamp == 0:
                                mintimestamp = datatimestamp
                            if maxtimestamp == 0:
                                maxtimestamp = datatimestamp

                            if datatimestamp > maxtimestamp:
                                maxtimestamp = datatimestamp
                            if datatimestamp < mintimestamp:
                                mintimestamp = datatimestamp
                    logr.debug("maxtimestamp = %d, mintimestamp = %d" % (maxtimestamp, mintimestamp))
                    existdata = self.dcinfo.find({'uid':uid, 'mac_id':tmpmacid,'timestamp':{'$gte':mintimestamp,'$lte':maxtimestamp}})
                    if existdata.count():
                        for obj in existdata:
                            existtimedict[obj['timestamp']] = obj

                    if len(datalist) > 0:
                        insertdatalist = list()
                        for datastr in datalist:
                            dataparam = datastr.split(',')
                            if len(dataparam)<5:
                                continue
                            datatimestamp = int(dataparam[4])
                            datatype = int(dataparam[0])
                            datacount = float(dataparam[2])
                            datastate = int(dataparam[3])
                            if datatimestamp in existtimedict:
#                                logr.debug(existtimedict)
#                                logr.debug(datatimestamp)
                                self.dcinfo.update({'uid':uid, '_id':existtimedict[datatimestamp]['_id']},{'$set':{'count':datacount,'state':datastate}})
                            else:
                                insertdata = dict()
                                insertdata['type'] = datatype
                                insertdata['mode'] = int(dataparam[1])
                                insertdata['count'] = datacount
                                insertdata['state'] = datastate
                                insertdata['timestamp'] = int(dataparam[4])
                                insertdata['mac_id'] = tmpmacid
                                insertdata['member_id'] = memberid
                                insertdata['uid'] = uid
                                insertdata['vid'] = vid
                                insertdatalist.append(insertdata)
                        if len(insertdatalist):
#                            logr.debug("insertdatalist====")
#                            logr.debug(insertdatalist)
                            self.dcinfo.insert(insertdatalist)

                retdict['error_code'] = '200'
            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
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
    if _config is not None and 'ronaldo-dc' in _config and _config['ronaldo-dc'] is not None:
        if 'thread_count' in _config['ronaldo-dc'] and _config['ronaldo-dc']['thread_count'] is not None:
            thread_count = int(_config['ronaldo-dc']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = DataCenter(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
