#!/usr/bin/env python
# -*-coding:utf8-*-
__author__ = 'Kairong'
#!/usr/bin/env python
# -*-coding:utf8-*-
__author__ = 'Kairong'
#引入额外库
import pymongo
import re
import logging
import ConfigParser
import socket
from time import time
import json
import requests

class MonMongo(object):
    def __init__(self, section):
        self.section = section
        self.timeout = 3


    def process_section(self):
        '''处理指定section，如为单机模式，则判断端口是否存活。
        如为副本，则随机选择一个机器，登陆后获取各节点状态
         '''
        logging.info('%s is processing' % (self.section,))
        mon_type = conf.get(self.section, 'type')
        logging.debug('%s type is %s' % (self.section, mon_type))
        self.machine_list = conf.get(self.section, 'mach_list')
        logging.debug('%s have machie list %s' % (self.section, self.machine_list))
        if mon_type == 'single':
            self.process_machine_list(mtype = 'single')
        else:
            self.process_machine_list()


    def process_machine_list(self, mtype = 'repl'):
        self.split_machine_list = self.machine_list.split(';')
        for mach_port in self.split_machine_list:
            host, port = mach_port.split(':')
            #根据返回值上传，1为正常，0为异常
            if self.mon_socket(host, port):
                if mtype == 'repl':
                    self.mon_repl(host, port)
                    break
                else:
                    push_data.push_data(host, port, 1)
            else:
                push_data.push_data( host, port, 0)

    def mon_socket(self, host, port):
        '''使用socket编程中的流处理，去探测端口是否存活,存活返回True,失败返回False'''
        logging.debug('mon level is %s' % (loglevel,))
        logging.info('%s:%s will be monitord' % (host, port))
        port = int(port)
        try:
            sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sc.settimeout(self.timeout)
            logging.info('connect to %s:%d' % (host, port))
            sc.connect((host, port))
            logging.info('%s:%d is online' % (host, port))
            return True
        except Exception, e:
            logging.error(e)
            logging.ERROR('%s:%d is offline' % (host, str(port)))
            return False

    def mon_repl(self, host, port):
        '''
            监控repl集群,并时刻把拓扑结构回传给conf文件
        '''
        try:
            #初始化mongo链接，并获取副本状态
            logging.info('connect repl start')
            conn  = pymongo.MongoReplicaSetClient("%s:%s" % (host, port))
            repl_status = conn.admin.command("replSetGetStatus")
            ReplSetName =  repl_status['set']
            logging.info('ReplSetName is %s' %(ReplSetName))
            #解析所有成员状态
            new_machine_list = ''
            for member in  repl_status['members']:
                member_host, member_port = member['name'].split(':')
                if re.search(r"(reachable|healthy|RECOVERING)",member['stateStr']):
                    #logging.error("%s:%s is offline" % (member_host, member_port))
                    push_data.push_data(member_host, member_port, 0)
                else:
                    #logging.info("%s:%s is normal" % (member_host, member_port))
                    push_data.push_data(member_host, member_port, 1)
                new_machine_list += member['name'] + ";"
            new_machine_list = new_machine_list.strip(';')
            conf.set(self.section, 'mach_list', new_machine_list)
            conf.write(open(config_file, 'w'))
        except Exception, e:
            #截获链接异常
            logging.critical(e)
        finally:
            conn.close()
            logging.info('connect repl %s end' % (ReplSetName,))

#函数区块
class push_data(object):
    def __init__(self):
        self.Endpoint = socket.gethostname()
        self.result = []
        self.ts = int(time())
        self.metric = 'mon_mongo'
        self.push_path = conf.get('global', 'push_path')

    def push_data(self, host, port, value=1):
        logging.info("push data to falcon %s:%s value is %d" % (host, port, value))
        self.result.append(
            {
            "endpoint" : self.Endpoint,
            "metric": self.metric,
            "timestamp" : self.ts,
            "step": 60,
            "value": value,
            "counterType": 'GAUGE',
            "tags": "host=%s,port=%s" % (host, port),
            }
        )

    def get(self):
        pass

    def out(self):
        return json.dumps(self.result)

    def push(self):
        '''push 数据至falcon'''
        r = requests.post("http://%s/v1/push" % (self.push_path,) , data =  self.out())



def conf_parser(filename='./config.conf'):
    '''拆分配置文件，并返回配置文件中的句柄
    '''
    #conf = ConfigParser.ConfigParser()
    conf = ConfigParser.RawConfigParser()
    try:
        global cfgopen
        cfgopen = open(filename, 'rw')
        conf.readfp(cfgopen)
    except Exception,e:
        #此处使用print的原因是因为conf已经加载不成功了。
        print e
    return conf


#变量声明区域
##获取配置文件中的关于日志以及日志级别的情况
config_file = "./config.conf"
conf= conf_parser(config_file)

formatter = '%(asctime)s %(levelname)-8s %(message)s'
log_level = {'debug': logging.DEBUG,
             'info': logging.INFO,
             'warn': logging.WARN,
             'error': logging.ERROR
             }
logpath = conf.get('global', 'logpath')
loglevel = conf.get('global', 'loglevel')
logging.basicConfig(level=log_level[loglevel],filename=logpath,filemode='a',format=formatter)
##获取需要监控的sections列表,需要移除global
_mon_list = conf.sections()
_mon_list.remove('global')
mon_list  = _mon_list
push_data = push_data()


##配置文件解析
def main():
    logging.info('===start mon===')
    for section in mon_list:
        mon = MonMongo(section)
        mon.process_section()
    logging.info('===end mon===')
    push_data.push()

if __name__ == "__main__":
    main()
