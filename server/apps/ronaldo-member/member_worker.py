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

import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
from bson.objectid import ObjectId


logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class MemberCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("MemberCenter :running in __init__")
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self, moduleid)
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.member
        self.collect_memberinfo = self.db.memberinfo
        self.collect_memberlog = self.db.memberlog
#        self.notifyconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.notifyconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['user'],self._json_dbcfg['passwd'],self._json_dbcfg['host'],self._json_dbcfg['port']))
        self.notifydb = self.notifyconn.notify
        self.notify_template = self.notifydb.notify_template
        self.thread_index = moduleid
        self.recv_queue_name = "W:Queue:Member"
        if 'ronaldo-member' in self._config:
            if 'Consumer_Queue_Name' in _config['ronaldo-member']:
                self.recv_queue_name = _config['ronaldo-member']['Consumer_Queue_Name']



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

        if action_cmd == 'register':
            self._proc_action_register(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'register_login':
            self._proc_action_register_login(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update':
            self._proc_action_memberupdate(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'login':
            self._proc_action_login(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'logout':
            self._proc_action_logout(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_info':
            self._proc_action_memberinfo(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'change_password':
            self._proc_action_changepassword(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'user_add':
            self._proc_action_useradd(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'user_update':
            self._proc_action_userupdate(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'user_del':
            self._proc_action_userdel(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'user_headimg_upload':
            self._proc_action_userheadimgupload(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'getback_password':
            self._proc_action_getbackpassword(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'device_alias':
            self._proc_action_devicealias(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_device_info':
            self._proc_action_memberdeviceinfo(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'update_alarm':
            self._proc_action_update_alarm(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'update_valadic_state':
            self._proc_action_update_valadic_state(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = self.ERRORCODE_UNKOWN_CMD

        return

    def run(self):
        logr.debug("Start MemberCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logr.debug("_proc_message cost %f" % (time.time()-t1))                    


    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()

    def calcpassword(self, password, verifycode):
        m0 = hashlib.md5(verifycode)
        logr.debug("m0 = %s" % m0.hexdigest())
        m1 = hashlib.md5(password + m0.hexdigest())
    #        print m1.hexdigest()
        logr.debug("m1 = %s" % m1.hexdigest())
        md5password = m1.hexdigest()
        return md5password

    def generator_tokenid(self, userid, timestr, verifycode):
        m0 = hashlib.md5(verifycode)
    #        print m0.hexdigest()
        m1 = hashlib.md5("%s%s%s" % (userid,timestr,m0.hexdigest()))
    #        print m1.hexdigest()
        token = m1.hexdigest()
        return token

    def redisdelete(self, argslist):
        logr.debug('%s' % ('","'.join(argslist)))
        ret = eval('self._redis.delete("%s")'%('","'.join(argslist)))
        logr.debug('delete ret = %d' % (ret))

    def _proc_action_register(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into _proc_action_register action_body:%s"%action_body)
        try:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid'];
               
            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            username = username.lower()
            username = username.replace(' ','')
            if self.collect_memberinfo.find_one({'username': username}):
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                return
            

            nickname = username
            if 'nickname' in action_body and action_body['nickname'] is not None:
                nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')

            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']
            else:
                email = username

            if 'mobile' in action_body and action_body['mobile'] is not None:
                mobile = action_body['mobile']
            else:
                mobile = ''

            if 'source' in action_body and action_body['source'] is not None:
                source = action_body['source']
            else:
                source = self.USER_SOURCE_ORIGIN


            insertmember = dict({\
                'username':username,\
                'nickname':nickname,\
                'password':self.calcpassword(action_body['pwd'],self.MEMBER_PASSWORD_VERIFY_CODE),\
                'vid':vid,\
                'source':source,\
                'createtime': datetime.datetime.now(),\
                'lastlogintime':None,\
                'lastlong':None,\
                'lastlat':None,\
                'state': 0,\
                'mobile':mobile,\
                'email':email,\
                'friends':list(),\
                'users':list(),\
                'device':list()\
                })
            logr.debug(insertmember)
            self.collect_memberinfo.insert(insertmember)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_register_login(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into _proc_action_register_login action_body:%s"%action_body)
        try:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            vid = action_body['vid'];

            source = self.USER_SOURCE_ORIGIN
            if 'source' in action_body and action_body['source'] is not None:
                source = action_body['source']

            email = ""
            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            username = username.lower()
            username = username.replace(' ','')
            if self.collect_memberinfo.find_one({'username': username}):
                if source == self.USER_SOURCE_ORIGIN:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                    return
                if email != "":
                    self.collect_memberinfo.update({'username': username},{"$set":{'email':email}})
            else:
                insertmember = dict({\
                    'username':username,\
                    'nickname':username,\
                    'password':self.calcpassword(action_body['pwd'],self.MEMBER_PASSWORD_VERIFY_CODE),\
                    'vid':vid,\
                    'source':self.USER_SOURCE_ORIGIN,\
                    'createtime': datetime.datetime.now(),\
                    'lastlogintime':None,\
                    'lastlong':None,\
                    'lastlat':None,\
                    'state': 0,\
                    'mobile':'',\
                    'email':email,\
                    'friends':list(),\
                    'users':list(),\
                    'device':list()\
                    })            
                for key in action_body:
                    if key in ['username','pwd','vid']:
                        continue
                    elif key in ['nickname']:
                        nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')
                        insertmember[key] = nickname
                    else:
                        insertmember[key] = action_body[key]

                
                logr.debug(insertmember)
                self.collect_memberinfo.insert(insertmember)

            tokenid = None
            memberid = ''

            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username,'*')
            logr.debug("key = %s" % searchkey)
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % resultlist)
            if len(resultlist):
                redis_memberinfo = self._redis.hgetall(resultlist[0])
                if redis_memberinfo is None:
                    #redis中没有用户信息，就从mongo中读取
                    member = self.collect_memberinfo.find_one({'username': username})
                    if member is None:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                        return
                    #产生tokenid
                    memberid = member['_id'].__str__()
                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                    #写入redis
                    key = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                    self._redis.hset(key,"_id", memberid)
                    self._redis.hset(key,"username", member['username'])
                    self._redis.hset(key,"tid", tokenid)
                    self._redis.hset(key,"password", member['password'])
                    #删除无用的key
                    self._redis.delete(resultlist[0])

                else:
                    memberid = redis_memberinfo['_id']
                    tokenid = redis_memberinfo['tid']
                    password = redis_memberinfo['password']
            else:
                member = self.collect_memberinfo.find_one({'username': username})
                if member is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                #产生tokenid
                memberid = member['_id'].__str__()
                tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                #写入redis
                key = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                self._redis.hset(key,"_id", memberid)
                self._redis.hset(key,"username", member['username'])
                self._redis.hset(key,"tid", tokenid)
                self._redis.hset(key,"password", member['password'])


            #更新上次登陆时间
            self.collect_memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'lastlogintime':datetime.datetime.now()}})


            if 'phone_name' in action_body and action_body['phone_name'] is not None:
                phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            else:
                phone_name = ''
            if 'phone_os' in action_body and action_body['phone_os'] is not None:
                phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            else:
                phone_os =  ''
            if 'app_version' in action_body and action_body['app_version'] is not None:
                app_version = action_body['app_version']
            else:
                app_version =  ''
            if 'phone_id' in action_body and action_body['phone_id'] is not None:
                phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            else:
                phone_id =  ''
            #添加登陆记录
            insertlog = dict({\
                'mid':memberid,\
                'tid':tokenid,\
                'vid':vid,\
                'phone_id': phone_id,\
                'phone_name': phone_name,\
                'phone_os': phone_os,\
                'app_version': app_version,\
                'timestamp': datetime.datetime.now()\
                })
            logr.debug(insertlog)
            self.collect_memberlog.insert(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['tid'] = tokenid

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_login(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                        'phone_name':O
                        'phone_os':O
                        'app_version':O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
        '''
        logr.debug(" into _proc_action_login action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            username = username.lower()
            username = username.replace(' ','')
            vid = action_body['vid'];
            #在redis中查找用户登陆信息
            tokenid = None
            memberid = ''

            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username,'*')
            logr.debug("key = %s" % searchkey)
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % resultlist)
            if len(resultlist):
                redis_memberinfo = self._redis.hgetall(resultlist[0])
                if redis_memberinfo is None:
                    #redis中没有用户信息，就从mongo中读取
                    member = self.collect_memberinfo.find_one({'username': username})
                    if member is None:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                        return
                    #比较密码
                    if action_body['pwd'] != member['password']:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
                    #产生tokenid
                    memberid = member['_id'].__str__()
                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                    #写入redis
                    key = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                    self._redis.hset(key,"_id", memberid)
                    self._redis.hset(key,"username", member['username'])
                    self._redis.hset(key,"tid", tokenid)
                    self._redis.hset(key,"password", member['password'])
                    #删除无用的key
                    self._redis.delete(resultlist[0])

                else:
                    memberid = redis_memberinfo['_id']
                    tokenid = redis_memberinfo['tid']
                    password = redis_memberinfo['password']
                    #比较密码
                    if action_body['pwd'] != password:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
            else:
                member = self.collect_memberinfo.find_one({'username': username})
                if member is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                #比较密码
                if action_body['pwd'] != member['password']:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                    return
                #产生tokenid
                memberid = member['_id'].__str__()
                tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                #写入redis
                key = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
                self._redis.hset(key,"_id", memberid)
                self._redis.hset(key,"username", member['username'])
                self._redis.hset(key,"tid", tokenid)
                self._redis.hset(key,"password", member['password'])


            #更新上次登陆时间
            self.collect_memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'lastlogintime':datetime.datetime.now()}})


            if 'phone_name' in action_body and action_body['phone_name'] is not None:
                phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            else:
                phone_name = ''
            if 'phone_os' in action_body and action_body['phone_os'] is not None:
                phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            else:
                phone_os =  ''
            if 'app_version' in action_body and action_body['app_version'] is not None:
                app_version = action_body['app_version']
            else:
                app_version =  ''
            if 'phone_id' in action_body and action_body['phone_id'] is not None:
                phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            else:
                phone_id =  ''
            #添加登陆记录
            insertlog = dict({\
                'mid':memberid,\
                'tid':tokenid,\
                'vid':vid,\
                'phone_id': phone_id,\
                'phone_name': phone_name,\
                'phone_os': phone_os,\
                'app_version': app_version,\
                'timestamp': datetime.datetime.now()\
                })
            logr.debug(insertlog)
            self.collect_memberlog.insert(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['tid'] = tokenid
            retbody['memberid'] = memberid

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_logout(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_logout action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_memberinfo(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'uid'   : O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_memberinfo action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = None
            tid = action_body['tid']

            if 'uid' in action_body and action_body['uid'] is not None:
                uid = action_body['uid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return

                source = self.USER_SOURCE_ORIGIN
                if 'source' in memberinfo:
                    source = memberinfo.get('source')

                retbody['source'] = source
                retbody['email'] = memberinfo['email']
                retbody['mobile'] = memberinfo['mobile']
                retbody['nickname'] = urllib.quote(memberinfo['nickname'].encode('utf-8'))
                retbody['vid'] = memberinfo['vid']
                retbody['memberid'] = memberinfo['_id'].__str__()
                retbody['state'] = memberinfo['state']
                userinfolist = list()
                deviceinfolist = list()
                retbody['userinfo'] = userinfolist
#                retbody['deviceinfo'] = deviceinfolist
#
#                if 'device' not in memberinfo or memberinfo['device'] is None:
#                    self.collect_memberinfo.update({'_id':memberid},{'$set':{'device':list()}})
#                else:
#                    for device in memberinfo['device']:
#                        deviceinfo = dict()
#                        deviceinfo['mac_id'] = device['macid']
#                        deviceinfo['alias'] = urllib.quote(device['alias'].encode('utf-8'))
#                        deviceinfolist.append(deviceinfo)


                findflag = True
                if uid is not None:
                    findflag = False

                for user in memberinfo['users']:
                    if uid:
                        if 'uid' in user and user['uid'] == uid:
                            findflag = True
                            userinfo = dict()
                            for key in user:
                                if key in ['name']:
                                    userinfo[key] = urllib.quote(user[key].encode('utf-8'))
                                elif key in ['alarms']:
                                    #获得闹钟信息
                                    userinfo['alarmlist'] = list()
                                    for mackey in user[key]:
                                        macid_alarms = user[key][mackey]
                                        for typeidkey in macid_alarms:
                                            dbalarminfo = macid_alarms[typeidkey]
                                            alarminfo = dict()
                                            for paramkey in dbalarminfo:
                                                if paramkey in ['name']:
                                                    alarminfo[paramkey] = urllib.quote(dbalarminfo[paramkey].encode('utf-8'))
                                                else:
                                                    alarminfo[paramkey] = dbalarminfo[paramkey]
                                            userinfo['alarmlist'].append(alarminfo)

                                else:
                                    userinfo[key] = user[key]
                            if 'gear_type' in user and user['gear_type'] is not None:
                                userinfo['gear_type'] = user['gear_type']
                            else:
                                userinfo['gear_type'] = '001'
                            if 'gear_subtype' in user and user['gear_subtype'] is not None:
                                userinfo['gear_subtype'] = user['gear_subtype']
                            else:
                                userinfo['gear_subtype'] = ' '


#                            if 'uid' in user and user['uid'] is not None:
#                                userinfo['uid'] = user['uid']
#                            if 'name' in user and user['name'] is not None:
#                                userinfo['name'] = urllib.quote(user['name'].encode('utf-8'))
#                            if 'gender' in user and user['gender'] is not None:
#                                userinfo['gender'] = user['gender']
#                            if 'headimg' in user and user['headimg'] is not None:
#                                userinfo['img_url'] = user['headimg']
#                            if 'headimg_fmt' in user and user['headimg_fmt'] is not None:
#                                userinfo['img_format'] = user['headimg_fmt']
#                            if 'height' in user and user['height'] is not None:
#                                userinfo['height'] = user['height']
#                            if 'weight' in user and user['weight'] is not None:
#                                userinfo['weight'] = user['weight']
#                            if 'bloodtype' in user and user['bloodtype'] is not None:
#                                userinfo['bloodtype'] = user['bloodtype']
#                            if 'stride' in user and user['stride'] is not None:
#                                userinfo['stride'] = user['stride']
#                            if 'birth' in user and user['birth'] is not None:
#                                userinfo['birth'] = user['birth']


                            userinfolist.append(userinfo)
                            break
                    else:
                        userinfo = dict()
                        for key in user:
                            if key in ['name']:
                                userinfo[key] = urllib.quote(user[key].encode('utf-8'))
                            elif key in ['alarms']:
                                #获得闹钟信息
                                userinfo['alarmlist'] = list()
                                for mackey in user[key]:
                                    macid_alarms = user[key][mackey]
                                    for typeidkey in macid_alarms:
                                        dbalarminfo = macid_alarms[typeidkey]
                                        alarminfo = dict()
                                        for paramkey in dbalarminfo:
                                            if paramkey in ['name']:
                                                alarminfo[paramkey] = urllib.quote(dbalarminfo[paramkey].encode('utf-8'))
                                            else:
                                                alarminfo[paramkey] = dbalarminfo[paramkey]
                                        userinfo['alarmlist'].append(alarminfo)

                            else:
                                userinfo[key] = user[key]
                        if 'gear_type' in user and user['gear_type'] is not None:
                            userinfo['gear_type'] = user['gear_type']
                        else:
                            userinfo['gear_type'] = '001'
                        if 'gear_subtype' in user and user['gear_subtype'] is not None:
                            userinfo['gear_subtype'] = user['gear_subtype']
                        else:
                            userinfo['gear_subtype'] = ' '

                        userinfolist.append(userinfo)

                if findflag:
                    retdict['error_code'] = '200'
                else:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_memberupdate(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_memberupdate action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']
            email = None
            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']
            mobile = None
            if 'mobile' in action_body and action_body['mobile'] is not None:
                mobile = action_body['mobile']
            nickname = None
            if 'nickname' in action_body and action_body['nickname'] is not None:
                nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')


            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                modifydict = dict()
                if email is not None:
                    modifydict['email'] = email
                if mobile is not None:
                    modifydict['mobile'] = mobile
                if nickname is not None:
                    modifydict['nickname'] = nickname

                self.collect_memberinfo.update({'_id':memberid},{'$set':modifydict})

                retdict['error_code'] = '200'
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_changepassword(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'change_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'old_password'   : M
                        'new_password'  : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_changepassword action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('old_password' not in action_body) or  ('new_password' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['old_password'] is None or action_body['new_password'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            old_password = action_body['old_password']
            new_password = action_body['new_password']
            tid = action_body['tid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                if 'password' not in memberinfo or old_password != memberinfo['password']:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                    return

                self.collect_memberinfo.update({'_id':memberid},{'$set':{'password':new_password}})
                self._redis.hset(key,'password',new_password)
                
                retdict['error_code'] = '200'
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_useradd(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'user_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'name'   : O
                        'gender'  : O
                        'height' : O
                        'weight' : O
                        'bloodtype': O
                        'stride':O
                        'birth':O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_useradd action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('uid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['uid'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            tid = action_body['tid']
            userdict = dict()
            for key in action_body:
                if key in ['tid','vid','uid']:
                    continue
                elif key in ['name']:
                    userdict['name'] = urllib.unquote(action_body['name'].encode('utf-8')).decode('utf-8')
                else:
                    userdict[key] = action_body[key]

            userdict['uid'] = uid
            if 'gear_type' not in userdict:
                userdict['gear_type'] ='001'

            if 'gear_subtype' not in userdict:
                userdict['gear_subtype'] = ''

            userdict['img_version'] = 0
            

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                if memberinfo['users'] is None or len(memberinfo['users']) == 0:
                    userlist = list()
                    userlist.append(userdict)
                    self.collect_memberinfo.update({'_id':memberid},{'$set':{'users':userlist}})
                    retdict['error_code'] = self.ERRORCODE_OK
                    return

                userlist = memberinfo['users']
                for user in userlist:
                    if user['uid'] == uid:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_USER_ALREADY_EXIST
                        return

                userlist.append(userdict)
                self.collect_memberinfo.update({'_id':memberid},{'$set':{'users':userlist}})
                retdict['error_code'] = '200'
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_userupdate(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'user_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'uid'   :M
                        'name'   : O
                        'gender'  : O
                        'height' : O
                        'weight' : O
                        'bloodtype': O
                        'stride':O
                        'birth':O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_userupdate action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('uid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['uid'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            tid = action_body['tid']
            userdict = dict()
            userdict['uid'] = uid
            for key in action_body:
                if key in ['tid','vid','uid']:
                    continue
                elif key in ['name']:
                    userdict['name'] = urllib.unquote(action_body['name'].encode('utf-8')).decode('utf-8')
                else:
                    userdict[key] = action_body[key]
            if 0:
                if 'name' in action_body and action_body['name'] is not None:
                    userdict['name'] = urllib.unquote(action_body['name'].encode('utf-8')).decode('utf-8')
                if 'gender' in action_body and action_body['gender'] is not None:
                    userdict['gender'] =action_body['gender']
                if 'height' in action_body and action_body['height'] is not None:
                    userdict['height'] =action_body['height']
                if 'weight' in action_body and action_body['weight'] is not None:
                    userdict['weight'] =action_body['weight']
                if 'bloodtype' in action_body and action_body['bloodtype'] is not None:
                    userdict['bloodtype'] =action_body['bloodtype']
                if 'stride' in action_body and action_body['stride'] is not None:
                    userdict['stride'] =action_body['stride']
                if 'birth' in action_body and action_body['birth'] is not None:
                    userdict['birth'] =action_body['birth']
                if 'gear_subtype' in action_body and action_body['gear_subtype'] is not None:
                    userdict['gear_subtype'] =action_body['gear_subtype']
                if 'goal_steps' in action_body and action_body['goal_steps'] is not None:
                    userdict['goal_steps'] =action_body['goal_steps']
                if 'goal_cal' in action_body and action_body['goal_cal'] is not None:
                    userdict['goal_cal'] =action_body['goal_cal']
                if 'goal_dis' in action_body and action_body['goal_dis'] is not None:
                    userdict['goal_dis'] =action_body['goal_dis']
                if 'goal_act' in action_body and action_body['goal_act'] is not None:
                    userdict['goal_act'] =action_body['goal_act']
                if 'goal_slp' in action_body and action_body['goal_slp'] is not None:
                    userdict['goal_slp'] =action_body['goal_slp']
                if 'yoo_goal' in action_body and action_body['yoo_goal'] is not None:
                    userdict['yoo_goal'] =action_body['yoo_goal']
                if 'yoo_challenge' in action_body and action_body['yoo_challenge'] is not None:
                    userdict['yoo_challenge'] =action_body['yoo_challenge']
                if 'yoo_challengeid' in action_body and action_body['yoo_challengeid'] is not None:
                    userdict['yoo_challengeid'] =action_body['yoo_challengeid']
                if 'unit' in action_body and action_body['unit'] is not None:
                    userdict['unit'] =action_body['unit']

            logr.debug(userdict)
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                if memberinfo['users'] is None or len(memberinfo['users']) == 0:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                    return

                userlist = memberinfo['users']
                for user in userlist:
                    if user['uid'] == uid:
                        for key in userdict:
                            user[key] = userdict[key]
                        self.collect_memberinfo.update({'_id':memberid},{'$set':{'users':userlist}})
                        retdict['error_code'] = self.ERRORCODE_OK
                        return
                
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_userdel(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'user_del', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'uid'   : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_userdel action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('uid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['uid'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            tid = action_body['tid']
            userdict = dict()
            userdict['uid'] = uid
            if 'name' in action_body and action_body['name'] is not None:
                userdict['name'] = urllib.unquote(action_body['name'].encode('utf-8')).decode('utf-8')
            if 'gender' in action_body and action_body['gender'] is not None:
                userdict['gender'] =action_body['gender']
            if 'height' in action_body and action_body['height'] is not None:
                userdict['height'] =action_body['height']
            if 'weight' in action_body and action_body['weight'] is not None:
                userdict['weight'] =action_body['weight']
            if 'bloodtype' in action_body and action_body['bloodtype'] is not None:
                userdict['bloodtype'] =action_body['bloodtype']
            if 'stride' in action_body and action_body['stride'] is not None:
                userdict['stride'] =action_body['stride']
            if 'birth' in action_body and action_body['birth'] is not None:
                userdict['birth'] =action_body['birth']


            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                if memberinfo['users'] is None or len(memberinfo['users']) == 0:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                    return

                userlist = memberinfo['users']
                for user in userlist:
                    if user['uid'] == uid:
                        index = userlist.index(user)
                        userlist.pop(index)
                        self.collect_memberinfo.update({'_id':memberid},{'$set':{'users':userlist}})
                        retdict['error_code'] = self.ERRORCODE_OK
                        return
                
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_userheadimgupload(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'user_headimg_upload', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'uid'   :M
                        'img'   : O
                        'format'  : O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                        img_version
                   }
                }
        '''
        logr.debug(" into _proc_action_userheadimgupload action_body")
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('uid' not in action_body) or  ('img' not in action_body) or ('format' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['uid'] is None  or action_body['img'] is None or action_body['format'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            tid = action_body['tid']
            vid = action_body['vid']
            format = action_body['format']
            img = action_body['img']


            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                if memberinfo['users'] is None or len(memberinfo['users']) == 0:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                    return

                userlist = memberinfo['users']
                findflag = False
                for user in userlist:
                    if user['uid'] == uid:
                        #找到user，处理上传的头像
                        filedir = "%s%s" % (self.FILEDIR_IMG_HEAD, vid)
                        filename = "%s.%s" % (uid, format)
                        imgurl = "%s/%s/%s" % (self._config['imageurl'],vid,filename)
                        logr.debug(filedir)
                        logr.debug(filename)
                        logr.debug(imgurl)

                        if os.path.isdir(filedir):
                            pass
                        else:
                            os.mkdir(filedir)

                        filename = "%s/%s" % (filedir, filename)
                        fs = open(filename, 'wb')
                        fs.write(base64.b64decode(img))
                        fs.flush()
                        fs.close()

                        user['headimg'] = imgurl
                        user['headimg_fmt'] = format
                        self.collect_memberinfo.update({'_id':memberid},{'$set':{'users':userlist}})
                        retdict['error_code'] = self.ERRORCODE_OK
                        retbody['img_url'] = imgurl
                        return
                
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_getbackpassword(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'getback_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_getbackpassword action_body")
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            lang = 'eng'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            username = username.lower()
            username = username.replace(' ','')
            vid = action_body['vid']
            memberinfo = self.collect_memberinfo.find_one({'username':username})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if 'email' in memberinfo and memberinfo['email'] is not None and memberinfo['email'] != '':
                email = memberinfo['email']
            else:
                email = memberinfo['username']

            email = email.lower()
            newpassword = "%d" % random.randint(100000,999999)
            md5password = self.calcpassword(newpassword, self.MEMBER_PASSWORD_VERIFY_CODE)
            logr.debug("newpassword = %s, md5password = %s" % (newpassword,md5password))
            #找对应的语言模板
            if vid == '00000F001002':
                searchdict = dict({'type':self.NOTIFY_TYPE_GETBACKPASSWORD_YOO, 'carrier':'email'})
            else:
                searchdict = dict({'type':self.NOTIFY_TYPE_GETBACKPASSWORD, 'lang':lang, 'carrier':'email'})
            logr.debug(searchdict)
            templateinfo = self.notify_template.find_one(searchdict)
            if templateinfo is None:
                #找默认的
                searchdict['lang'] = 'eng'
                logr.debug(searchdict)
                templateinfo = self.notify_template.find_one(searchdict)
                if templateinfo is None:
                    retdict['error_code'] = self.ERRORCODE_DB_ERROR
                    return


            sendcontent = templateinfo['content'] % (username, newpassword)

            to = "W:Queue:Notify"
            if 'ronaldo-notify' in self._config and self._config['ronaldo-notify'] is not None:
                to = self._config['ronaldo-notify']['Consumer_Queue_Name']

            body = dict()
            body['dest'] = email
            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
            body['carrier'] = 'email'
            body['notify_type'] = self.NOTIFY_TYPE_GETBACKPASSWORD_YOO
            body['email_from'] = templateinfo['email_from']
            body['email_subject'] = templateinfo['email_subject']
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'send_notify'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''          
            self._sendMessage(self._config['ronaldo-notify']['Consumer_Queue_Name'], json.dumps(action))      

            #修改密码
            self.collect_memberinfo.update({'username':username},{'$set':{'password':md5password}})
            #删除redis的cache数据
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



    def _proc_action_devicealias(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'getback_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logr.debug(" into _proc_action_devicealais action_body %s" % action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('mac_id' not in action_body) or  ('alias' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['mac_id'] is None or action_body['alias'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']
            macidstr = action_body['mac_id'].replace(':','')
            macid = int(macidstr, 16)
            alias = urllib.unquote(action_body['alias'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return

#                deviceinfolist = memberinfo['device']
                #如果是None 说明无绑定设备
                if 'device' not in memberinfo or memberinfo['device'] is None:
                    self.collect_memberinfo.update({'_id':memberid},{'$set':{'device':list()}})
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_BIND_DEVICE
                    return
                deviceinfolist = memberinfo['device']
                
                for device in deviceinfolist:
                    if device['macid'] == macid:
                        device['alias'] = alias
                        self.collect_memberinfo.update({'_id':memberid},{'$set':{'device':deviceinfolist}})
                        retdict['error_code'] = self.ERRORCODE_OK
                        return
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_BIND_DEVICE
                return
            else:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return


        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_memberdeviceinfo(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_device_info', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                    device_count
                    device_info{
                      mac_id,
                      alias,
                      timestamp
                    }
                   }
                }
        '''
        logr.debug(" into _proc_action_memberdeviceinfo action_body %s" % action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = None
            tid = action_body['tid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return

                deviceinfolist = list()
                retbody['deviceinfo'] = deviceinfolist


                if 'device' not in memberinfo or memberinfo['device'] is None:
                    self.collect_memberinfo.update({'_id':memberid},{'$set':{'device':list()}})
                    retbody['device_count'] = 0
                else:
                    retbody['device_count'] = len(memberinfo['device'])
                    for device in memberinfo['device']:
                        deviceinfo = dict()
                        deviceinfo['mac_id'] = device['macid']
                        deviceinfo['alias'] = urllib.quote(device['alias'].encode('utf-8'))
                        if 'timestamp' in device and device['timestamp'] is not None:
                            deviceinfo['timestamp'] = device['timestamp'].__str__()
                        else:
                            deviceinfo['timestamp'] = datetime.datetime.now().__str__()
                            
                        
                        deviceinfolist.append(deviceinfo)

                retdict['error_code'] = self.ERRORCODE_OK
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_update_alarm(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_device_info', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                    device_count
                    device_info{
                      mac_id,
                      alias,
                      timestamp
                    }
                   }
                }
        '''
        logr.debug(" into _proc_action_update_alarm action_body %s" % action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('uid' not in action_body) or  ('mac_id' not in action_body) or ('type' not in action_body) or  ('alarm_id' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['uid'] is None or action_body['mac_id'] is None or action_body['type'] is None or action_body['alarm_id'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            macid = action_body['mac_id']
            vid = action_body['vid']
            tid = action_body['tid']
            alarm_type = action_body['type']
            alarm_id = action_body['alarm_id']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logr.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                key = resultlist[0]
                logr.debug(key)
                tni = self._redis.hgetall(key)
                if tni is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return

                memberid = ObjectId(tni['_id'])
                memberinfo = self.collect_memberinfo.find_one({'_id':memberid})
                if memberinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return

                userlist = memberinfo.get('users')
                if userlist is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                    return

                userinfo = None
                for user in userlist:
                    if 'uid' in user and user['uid'] == uid:
                        userinfo = user
                        break

                if userinfo is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                    return


                alarms = userinfo.get('alarms')
                if alarms is None:
                    alarms = dict()
                    userinfo['alarms'] = alarms

                macid_alarms = alarms.get(macid)
                if macid_alarms is None:
                    macid_alarms = dict()
                    alarms[macid] = macid_alarms

                alarmkey = "%d:%d" % (alarm_type,alarm_id)
                alarminfo = macid_alarms.get(alarmkey)
                if alarminfo is None:
                    alarminfo = dict()
                    macid_alarms[alarmkey] = alarminfo

                for key in action_body:
                    if key in ['tid','vid']:
                        continue
                    elif key in ['name']:
                        alarminfo[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    else:
                        alarminfo[key] = action_body[key]

                self.collect_memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'users':userlist}})
                logr.debug(userlist)
                retdict['error_code'] = self.ERRORCODE_OK
                return
            else:

                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            return

        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_update_valadic_state(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_device_info', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                    device_count
                    device_info{
                      mac_id,
                      alias,
                      timestamp
                    }
                   }
                }
        '''
        logr.debug(" into _proc_action_update_valadic_state action_body %s" % action_body)
        try:
#        if 1:
            
            if ('vid' not in action_body) or ('uid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['vid'] is None or action_body['uid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            uid = action_body['uid']
            vid = action_body['vid']
            updatedict = dict()
            updatedict.update(action_body)
            updatedict.pop('uid')
            updatedict.pop('vid')
            if 'tid' in updatedict:
                updatedict.pop('tid')

            memberinfo = self.collect_memberinfo.find_one({'users':{'$elemMatch':{'uid':uid}}})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            userlist = memberinfo.get('users')
            if userlist is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return

            userinfo = None
            for user in userlist:
                if 'uid' in user and user['uid'] == uid:
                    userinfo = user
                    userinfo.update(updatedict)
                    #userinfo['validic_authrozie'] = validic_authrozie
                    break

            if userinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_USER
                return

            self.collect_memberinfo.update({'_id':memberinfo['_id']},{'$set':{'users':userlist}})

            retdict['error_code'] = self.ERRORCODE_OK
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
    if _config is not None and 'ronaldo-member' in _config and _config['ronaldo-member'] is not None:
        if 'thread_count' in _config['ronaldo-member'] and _config['ronaldo-member']['thread_count'] is not None:
            thread_count = int(_config['ronaldo-member']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = MemberCenter(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)

