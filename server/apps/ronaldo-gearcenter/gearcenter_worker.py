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


class GearCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("GearCenter :running in __init__")
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self, moduleid)
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.gearcenter
        self.collection = self.db.gear_authinfo
        self.gearlog = self.db.gear_authlog

#        self.memberconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.memberconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self.memberdb = self.memberconn.member
        self.memberinfo = self.memberdb.memberinfo
        self.thread_index = moduleid
        self.recv_queue_name = "W:Queue:GearCenter"
        if 'ronaldo-gearcenter' in self._config:
            if 'Consumer_Queue_Name' in _config['ronaldo-gearcenter']:
                self.recv_queue_name = _config['ronaldo-gearcenter']['Consumer_Queue_Name']



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

        if action_cmd == 'gear_add':
            self._proc_action_gearadd(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_auth':
            self._proc_action_gearauth(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_bind':
            self._proc_action_gearbind(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_unbind':
            self._proc_action_gearunbind(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return


    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def run(self):
        logr.debug("Start GearCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logr.debug("_proc_message cost %f" % (time.time()-t1))            

    def _proc_action_gearadd(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'vid'    : M
                        'gear_type'     : M
                        'mac_id_start'     : M
                        'mac_id_end'   : O
                        'count'    : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into gear_add action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('gear_type' not in action_body) or  ('mac_id_start' not in action_body) or  ('count' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            key = self.KEY_MAXAUTHCOUNT % (action_body['vid'])
            maxauthcount = self._redis.get(key)
            if maxauthcount is None:
                maxauthcount = self._redis.get(self.KEY_COMMON_MAXAUTHCOUNT)

            if maxauthcount is None:
                maxauthcount = '2'
                self._redis.set(self.KEY_COMMON_MAXAUTHCOUNT, '2')

            maxauthcount = int(maxauthcount)

            used_macid = list()
            macstart = int(action_body['mac_id_start'].replace(':',''), 16)
            macend = int(action_body['mac_id_end'].replace(':',''), 16)

            if macstart>macend:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            count = 0
            if macstart == macend:
                macinfo = self.collection.find_one({'macid':macstart})
                if macinfo:
                    used_macid.append(hex(macstart).encode("UTF-8").upper())

                insertmac = dict({\
                    'macid':macstart,\
                    'vid':action_body['vid'],\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.AUTH_STATE_ENABLE,\
                    'maxauthcount':maxauthcount,\
                    'auths':list(),\
                    'owner':list()\
                    })
                self.collection.insert(insertmac)
                count += 1
            else:
                count = 0
            
                existimeilist = list()
                resultlist = self.collection.find({'macid':{'$gte':macstart,'$lte':macend}},fields=['macid'])
                for gearinfo in resultlist:
                    existimeilist.append(gearinfo['macid'])

                for macid in xrange(macstart,macend):
#                    macinfo = self.collection.find_one({'macid':macid})
#                    if macinfo:
#                        used_macid.append(hex(macid).encode("UTF-8").upper())
#                        continue
                    if macid in existimeilist:
                        used_macid.append(hex(macid).encode("UTF-8").upper())
                        count+=1
                        continue

                    insertmac = dict({\
                        'macid':macid,\
                        'vid':action_body['vid'],\
                        'createtime': datetime.datetime.now(),\
                        'activetime':None,\
                        'state': self.AUTH_STATE_ENABLE,\
                        'maxauthcount':maxauthcount,\
                        'auths':list(),\
                        'owner':list()\
                        })
                    self.collection.insert(insertmac)
                    count += 1


            retdict['error_code'] = '200'
            retbody['badmacid'] = ','.join(used_macid)
            retbody['gear_count'] = count
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_gearauth(self, version, action_body, retdict, retbody):
        logr.debug(" into _proc_action_gearauth action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('mac_id' not in action_body) or  ('phone_id' not in action_body) or  ('phone_os' not in action_body) or ('phone_name' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['mac_id'] is None or action_body['phone_id'] is None or action_body['phone_os'] is None or action_body['phone_name'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if 'device_name' in action_body and action_body['device_name'] is not None:
                device_name = urllib.unquote(action_body['device_name'].encode('utf-8')).decode('utf-8')
            else:
                device_name = ""

            phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']

            memberid = None
            if 'tid' in action_body and action_body['tid'] is not None:
                searchkey = self.KEY_TOKEN_NAME_ID % (action_body['tid'],'*','*')
                resultlist = self._redis.keys(searchkey)
                if len(resultlist):
                    tni = self._redis.hgetall(resultlist[0])
                    if tni is not None and '_id' in tni:
                        memberid = tni['_id']

            #查找macid记录
            #for test
            obj = open("/opt/Keeprapid/Ronaldo/server/conf/testauth.conf")
            testauthconf = json.load(obj)
            obj.close()
            logr.debug(testauthconf)
            a = testauthconf['testmacid'].split(',')
            if action_body['mac_id'] in a:
                retdict['error_code'] = testauthconf['errorcode']
                retbody['auth_flag'] = int(testauthconf['auth_flag'])
                retbody['is_owner'] = int(testauthconf['is_owner'])
                retbody['bind_username'] = testauthconf['bind_username']
                return


            macidstr = action_body['mac_id'].replace(':','')
            macid = int(macidstr, 16)

            #鑫瑞华创先不验证
            if vid == "000008001001":
                self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, 0, 'normal')
                retdict['error_code'] = '200'
                retbody['auth_flag'] = 0
                retbody['is_owner'] = 0
                retbody['bind_username'] = ''
                return


            macinfo = self.collection.find_one({'macid':macid})
            if macinfo is None:
                retdict['error_code'] = self.ERRORCODE_MACID_NOT_EXIST
                retbody['auth_flag'] = self.getAuthFlag(self.AUTH_SCENE_NOMACID, vid)
                self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, retbody['auth_flag'], self.AUTH_SCENE_NOMACID)
                return

            bind_membername = None
            if len(macinfo['owner']):
                bind_memberid = macinfo['owner'][0]
                bind_memberinfo = self.memberinfo.find_one({'_id':ObjectId(bind_memberid)})
                if bind_memberinfo:
                    bind_membername = urllib.quote(bind_memberinfo['username'].encode('utf-8'))

            #先判断是否绑定mac
            if memberid is not None:
                if len(macinfo['owner']) == 0:
                    self.collection.update({'macid':macid},{'$set':{'owner':[memberid]}})
                    self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, 0 , 'normal')
                    #更新用户信息
                    memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
                    if memberinfo is not None:
                        deviceinfo = dict()
                        deviceinfo['macid'] = macid
                        deviceinfo['alias'] = device_name
                        deviceinfo['timestamp'] = datetime.datetime.now()

                        if 'device' not in memberinfo or memberinfo['device'] is None:
                            deviceinfolist = list()
                        else:
                            deviceinfolist = memberinfo['device']

                        deviceinfolist.append(deviceinfo)
                        self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfolist}})

                    retdict['error_code'] = '200'
                    retbody['auth_flag'] = self.AUTH_FLAG_NORMAL
                    self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, 0, 'normal')
                    retbody['is_owner'] = 1
                    if bind_membername is not None:
                        retbody['bind_username'] = bind_membername

                else:
                    if memberid in macinfo['owner']:
                        retdict['error_code'] = '200'
                        retbody['auth_flag'] = self.AUTH_FLAG_NORMAL
                        retbody['is_owner'] = 1
                        if bind_membername is not None:
                            retbody['bind_username'] = bind_membername
                        self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, 0, 'normal')
                        #更新用户信息
                        memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
                        if memberinfo is not None:
                            deviceinfo = dict()
                            deviceinfo['macid'] = macid
                            deviceinfo['alias'] = device_name
                            deviceinfo['timestamp'] = datetime.datetime.now()
                            findflag = False
                            if 'device' not in memberinfo or memberinfo['device'] is None:
                                deviceinfolist = list()
                            else:
                                deviceinfolist = memberinfo['device']
                                for device in deviceinfolist:
                                    if device['macid'] == macid:
                                        findflag = True
                                        if device['alias'] is None or device['alias'] == "":
                                            device['alias'] = device_name
                                        break
                            if findflag is False:
                                deviceinfolist.append(deviceinfo)
                            
                            self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfolist}})
                    else:
                        retdict['error_code'] = self.ERRORCODE_MACID_HAS_OWNER
                        retbody['auth_flag'] = self.getAuthFlag(self.AUTH_SCENE_NOTOWNER, vid)
                        if bind_membername is not None:
                            retbody['bind_username'] = bind_membername
                        self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, retbody['auth_flag'], self.AUTH_SCENE_NOTOWNER)

                auths = macinfo['auths']
                findflag = False
                for auth in auths:
                    if auth['phone_id'] == phone_id:
                        findflag = True
                        break
                if findflag == False:
                    auths.append({'phone_id':phone_id,'phone_os':phone_os,'phone_name':phone_name})
                    self.collection.update({'macid':macid},{'$set':{'auths':auths}})
                return

            #再判断是否是多机连接
            auths = macinfo['auths']
            findflag = False
            for auth in auths:
                if auth['phone_id'] == phone_id:
                    findflag = True
                    break
            if findflag == False:
                auths.append({'phone_id':phone_id,'phone_os':phone_os,'phone_name':phone_name})
                self.collection.update({'macid':macid},{'$set':{'auths':auths}})

            if len(macinfo['owner']):
                retdict['error_code'] = self.ERRORCODE_MACID_HAS_OWNER
                retbody['auth_flag'] = self.getAuthFlag(self.AUTH_SCENE_NOTOWNER, vid)
                if bind_membername is not None:
                    retbody['bind_username'] = bind_membername
                self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, retbody['auth_flag'], self.AUTH_SCENE_NOTOWNER)
                return

            if len(auths) > macinfo['maxauthcount']:
                retdict['error_code'] = self.ERRORCODE_MACID_OUT_MAXCOUNT
                retbody['auth_flag'] = self.getAuthFlag(self.AUTH_SCENE_TOOMANYDEVICE, vid)
                if bind_membername is not None:
                    retbody['bind_username'] = bind_membername
                self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, retbody['auth_flag'], self.AUTH_SCENE_TOOMANYDEVICE)
                return



            retdict['error_code'] = '200'
            retbody['auth_flag'] = self.AUTH_FLAG_NORMAL
            if bind_membername is not None:
                retbody['bind_username'] = bind_membername
            self.add_authlog(macid, vid, phone_id, phone_name, phone_os, memberid, 0, 'normal')
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def getAuthFlag(self, Scene, vid):
        searchkey = self.AUTH_LOGIC_VID % (Scene, vid)
        resultlist = self._redis.keys(searchkey)
        logr.debug("search vid authlogic %s" % searchkey)
        logr.debug("resultlist = %r" % resultlist)
        if len(resultlist):
            return int(self._redis.get(resultlist[0]))
        else:
            searchkey = self.AUTH_LOGIC_COMMON % (Scene)
            resultlist = self._redis.keys(searchkey)
            logr.debug("search common authlogic %s" % searchkey)
            logr.debug("resultlist = %r" % resultlist)
            if len(resultlist):
                return int(self._redis.get(resultlist[0]))
            else:
                return 0

 
    def add_authlog(self, macid,vid,phone_id,phone_name,phone_os,memberid,authflag,reason):
        #添加鉴权记录
        mid = memberid
        if memberid is None:
            mid = ''
        insertlog = dict({\
            'macid':macid,\
            'vid':vid,\
            'timestamp': datetime.datetime.now(),\
            'phone_id':phone_id,\
            'phone_name': phone_name,\
            'phone_os':phone_os,\
            'memberid':mid,\
            'authflag':authflag,\
            'reason':reason,\
            'params':list()\
            })
        self.gearlog.insert(insertlog)


    def _proc_action_gearunbind(self, version, action_body, retdict, retbody):
        logr.debug(" into _proc_action_gearunbind action_body:%s"%action_body)
        try:
            
            if ('vid' not in action_body) or  ('mac_id' not in action_body) or  ('tid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['vid'] is None or action_body['mac_id'] is None or action_body['tid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            memberid = None
            searchkey = self.KEY_TOKEN_NAME_ID % (action_body['tid'],'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                tni = self._redis.hgetall(resultlist[0])
                if tni is not None and '_id' in tni:
                    memberid = tni['_id']

            #查找macid记录
            macidstr = action_body['mac_id'].replace(':','')
            macid = int(macidstr, 16)
            macinfo = self.collection.find_one({'macid':macid})
            if macinfo is None:
                retdict['error_code'] = self.ERRORCODE_MACID_NOT_EXIST
                return
            
            #先判断是否绑定mac
            if memberid is not None:
                if memberid in macinfo['owner']:
                    memberlist = macinfo['owner']
                    memberlist.pop(memberlist.index(memberid))
                    self.collection.update({'macid':macid},{'$set':{'owner':memberlist}})

                memberinfo = self.memberinfo.find_one({'_id':ObjectId(memberid)})
                if memberinfo is not None:
                    if 'device' not in memberinfo or memberinfo['device'] is None:
                        deviceinfolist = list()
                    else:
                        deviceinfolist = memberinfo['device']                            
                        for device in deviceinfolist:
                            if 'macid' in device and device['macid'] == macid:
                                deviceinfolist.pop(deviceinfolist.index(device))

                    self.memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfolist}})


                retdict['error_code'] = '200'


            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS


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
    if _config is not None and 'ronaldo-gearcenter' in _config and _config['ronaldo-gearcenter'] is not None:
        if 'thread_count' in _config['ronaldo-gearcenter'] and _config['ronaldo-gearcenter']['thread_count'] is not None:
            thread_count = int(_config['ronaldo-gearcenter']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = GearCenter(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
