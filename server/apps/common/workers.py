#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  asset_main.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo wokers类的基类
import json

import logging
#import time
import logging.config
logging.config.fileConfig("/opt/Keeprapid/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')





class WorkerBase():

    ERROR_RSP_UNKOWN_COMMAND = '{"seq_id":"123456","body":{},"error_code":"40000"}'
    FILEDIR_IMG_HEAD = '/mnt/www/html/ronaldo/image/'
    #errorcode define
    ERRORCODE_OK = "200"
    ERRORCODE_UNKOWN_CMD = "40000"
    ERRORCODE_SERVER_ABNORMAL = "40001"
    ERRORCODE_CMD_HAS_INVALID_PARAM = '40002'
    ERRORCODE_DB_ERROR = '40003'
    ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST = '41001'
    ERRORCODE_MEMBER_PASSWORD_INVALID = '41002'
    ERRORCODE_MEMBER_NOT_EXIST = '41003'
    ERRORCODE_MEMBER_TOKEN_OOS = '41004'
    ERRORCODE_MEMBER_USER_ALREADY_EXIST = '41005'
    ERRORCODE_MEMBER_NO_USER = '41006'
    ERRORCODE_MEMBER_NO_BIND_DEVICE = '41007'

    ERRORCODE_MACID_NOT_EXIST = '42001'
    ERRORCODE_MACID_OUT_MAXCOUNT = '42002'
    ERRORCODE_MACID_HAS_OWNER = '42003'
    ERRORCODE_MACID_STATE_OOS = '42004'
    ERRORCODE_MEMBER_NOT_ONWER = '42005'
    #auth state
    AUTH_STATE_ENABLE = 1
    AUTH_STATE_DISABLE = 0
    #auth flag
    AUTH_FLAG_NORMAL = 0
    AUTH_FLAG_QUIT = 1
    AUTH_FLAG_BAN_CONNECT = 2
    AUTH_FLAG_BAN_FUNCTION = 3
    #auth Scene
    AUTH_LOGIC_COMMON = "R:authlogic:%s"
    AUTH_LOGIC_VID = "R:authlogic:%s:%s*"
    AUTH_SCENE_NOMACID = "badmacid"
    AUTH_SCENE_NOTOWNER = "isnotowner"
    AUTH_SCENE_TOOMANYDEVICE = "toomany"

    #member_state
    MEMBER_STATE_INIT_ACTIVE = 0
    MEMBER_STATE_EMAIL_COMFIRM = 1

    MEMBER_PASSWORD_VERIFY_CODE = "abcdef"

    #notify type
    NOTIFY_TYPE_GETBACKPASSWORD = 'GetBackPassword'
    NOTIFY_TYPE_GETBACKPASSWORD_YOO = 'GetBackPassword_yoo'
    #redis
    #maxauthcount key
    KEY_MAXAUTHCOUNT = "R:%s:maxauthcount"
    KEY_COMMON_MAXAUTHCOUNT = "R:maxauthcount"
    KEY_TOKEN_NAME_ID = "R:tni:%s:%s:%s"

    #email
    SEND_EMAIL_INTERVAL = 2

    USER_SOURCE_ORIGIN = "origin"
    USER_SOURCE_FACEBOOK = "facebook"

    #gearsubtype for yoo
    GEAR_SUBTYPE_YOO = "yoo"
    GEAR_SUBTYPE_YOO2 = "yoo2"
    GEAR_SUBTYPE_YOOSA = "yoosa"
    GEAR_SUBTYPE_YOOC = "yooc"

    def __init__(self, moduleid):
        try:
            logr.debug('WorkerBase::__init__')
            self._moduleid = moduleid
            fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/db.conf', 'r')
            self._json_dbcfg = json.load(fileobj)
            fileobj.close()

            fileobj = open('/opt/Keeprapid/Ronaldo/server/conf/config.conf', 'r')
            self._config = json.load(fileobj)
            fileobj.close()
            self._redis = None
            
        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))

    def redisdelete(self, argslist):
        logger.debug('%s' % ('","'.join(argslist)))
        ret = eval('self._redis.delete("%s")'%('","'.join(argslist)))
        logger.debug('delete ret = %d' % (ret))

    def _sendMessage(self, to, body):
        #发送消息到routkey，没有返回reply_to,单向消息
#        logger.debug(to +':'+body)
        if to is None or to == '' or body is None or body == '':
            return

        self._redis.lpush(to, body)


    def _getvalueinlistdict(self, request_key, condition_key, condition_value, d):
        for k in d:
            if request_key not in k or condition_key not in k:
                continue
            if k[condition_key] == condition_value:
                return k[request_key]

        return None

    def _setvalueinlistdict(self, request_key, request_value, condition_key, condition_value, d):
        for k in d:
            if request_key not in k or condition_key not in k:
                continue
            if k[condition_key] == condition_value:
                k[request_key] = request_value
                return True

        return False

    def _getobjinlistdict(self, condition_key, condition_value, d):
        for k in d:
            if condition_key not in k:
                continue
            if k[condition_key] == condition_value:
                return k

        return None
        
    def _findvalueinlistdict(self, key, value, d):
        for k in d:
            if key not in k:
                continue
            if k[key] == value:
                return True

        return False

    def _makeclientbodys(self, action_name, action_version, paramsdict=dict(), invoke_id=None, categroy=None, user_agent=None):
        
        actiondict = dict({'body':paramsdict})
#        if invoke_id is None:
#            clientdict['id'] = invoke_id
#        else:
#            clientdict['id'] = self._ident
#
#        if categroy is None:
#            clientdict['categroy'] = 'SERVER'
#        else:
#            clientdict['categroy'] = categroy
#
#        if user_agent is None:
#            clientdict['user_agent'] = 'python'
#        else:
#            clientdict['user_agent'] = categroy

        actiondict['action_cmd'] = action_name
        actiondict['version'] = action_version
        actiondict['seq_id'] = str(random.randint(1,100000000))
#        logr.debug(actiondict)

        return json.dumps(actiondict)

    def _getvaluefromactionresponse(self, request_key, ret, content):
#        if 'status'in ret and ret['status'] == '200':
        try:
            if content is not None:
                contentxml = xmltodict.parse(content)
                return contentxml.get('server').get('action').get(request_key)
        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            return "UNKOWN"


    def _sendhttpaction(self, module_key, action_name, action_version, action_paramdict):
        bodys = self._makeclientbodys(action_name, action_version, action_paramdict)
        obj = sendhttppost.SendHttpPostRequest()
        ret,content = obj.sendhttprequest(module_key,bodys)
        errorcode = self._getvaluefromactionresponse('errorcode', ret, content)
        return errorcode, ret, content

    def _sendhttpaction2(self, ip, port, module_key, action_name, action_version, action_paramdict):
        bodys = self._makeclientbodys(action_name, action_version, action_paramdict)
        obj = sendhttppost.SendHttpPostRequest()
        ret,content = obj.sendhttprequest2(ip, port, module_key,bodys)
        errorcode = self._getvaluefromactionresponse('errorcode', ret, content)
        return errorcode, ret, content



    def printplus(self, obj):
        # Dict
        logr.debug("===============begin dump============== ")
        if isinstance(obj, dict):
            for k, v in sorted(obj.items()):
                logr.debug(u'{0}: {1}'.format(k, v))

        # List or tuple            
        elif isinstance(obj, list) or isinstance(obj, tuple):
            for x in obj:
                logr.debug(x)

        # Other
        else:
            logr.debug(obj)
        logr.debug("===============end dump============== ")
    #///////////////////for Ronaldo only////////////////////////////////
 

