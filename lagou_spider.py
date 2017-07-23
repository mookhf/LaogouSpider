# -*- coding: utf-8 -*-
"""
@author: xfire
"""

import urllib2
import sqlite3
import threading
from bs4 import BeautifulSoup
import requests

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

lock = threading.Lock()

class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  
        
        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)
    
    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   
      
    def conn_close(self,conn=None):  
        conn.close()  
    
    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)  
            self.lock.release()  
            return rs  
        return connection  
    
    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            return -1
        except Exception, e:
            print e
            return -2
        return 0
    
    @conn_trans
    def select(self,command='',conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists


def pos_spider(db, url=''):

    print ("try : "+url)

    headers = {
        "Host": "www.lagou.com",
        "Connection": "keep-alive",
        "Origin": "https://www.lagou.com",
        "X-Anit-Forge-Code": "0",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-Anit-Forge-Token": "None",
        "Referer": url,
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
        "Cookie": 'user_trace_token=20170719151155-8a5cad0a-9917-4e4c-84b4-c0e9461ee85a; LGUID=20170719151156-8cb7b568-6c51-11e7-a5ed-525400f775ce; index_location_city=%E5%85%A8%E5%9B%BD; X_HTTP_TOKEN=4ba012e71aa8215542a6a7d94ac1e08d; TG-TRACK-CODE=search_code; SEARCH_ID=3d2ebb7be9d14de68a52ef1cf484aa7a; JSESSIONID=ABAAABAAAFCAAEG479C734847DBEB7096FF0F9A2C39B91C; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2Fgongsi%2F; _gid=GA1.2.1812397793.1500732776; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1500448330,1500732776; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1500742336; _ga=GA1.2.653489074.1500448329; LGSID=20170723005129-02258089-6efe-11e7-b25e-5254005c3644; LGRID=20170723005153-1045f52c-6efe-11e7-9a38-525400f775ce'
    }

    response = requests.get(url,headers=headers)

    if response.status_code != 200:
        print "get page error %d" % response.status_code
        return {}

    soup = BeautifulSoup(response.text)

    job = {}
    job_soup = soup.find('dd',{'class':'job_bt'})
    if job_soup:
        job_bt = job_soup.find('div')
        if job_bt:
            job['pos_desc'] = job_bt.text.strip()
        else:
            return job
    else :
        print 'get job detail failed'
        return job

    #os.system('rm tmp.html')

    return job

def get_page_job_list(city,page):

    refurl = u"https://www.lagou.com/jobs/list_%E4%BA%A7%E5%93%81%E7%BB%8F%E7%90%86?px=default&city="+urllib2.quote(str(city))

    headers = {
        "Host": "www.lagou.com",
        "Connection" : "keep-alive",
        "Origin":"https://www.lagou.com",
        "X-Anit-Forge-Code":"0",
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
        "Content-Type" : "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "X-Requested-With":"XMLHttpRequest",
        "X-Anit-Forge-Token": "None",
        "Referer":refurl,
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language":"zh-CN,zh;q=0.8,en;q=0.6",
        "Cookie":'user_trace_token=20170719151155-8a5cad0a-9917-4e4c-84b4-c0e9461ee85a; LGUID=20170719151156-8cb7b568-6c51-11e7-a5ed-525400f775ce; index_location_city=%E5%85%A8%E5%9B%BD; X_HTTP_TOKEN=4ba012e71aa8215542a6a7d94ac1e08d; TG-TRACK-CODE=search_code; SEARCH_ID=3d2ebb7be9d14de68a52ef1cf484aa7a; JSESSIONID=ABAAABAAAFCAAEG479C734847DBEB7096FF0F9A2C39B91C; PRE_UTM=; PRE_HOST=; PRE_SITE=; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2Fgongsi%2F; _gid=GA1.2.1812397793.1500732776; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1500448330,1500732776; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1500742336; _ga=GA1.2.653489074.1500448329; LGSID=20170723005129-02258089-6efe-11e7-b25e-5254005c3644; LGRID=20170723005153-1045f52c-6efe-11e7-9a38-525400f775ce'
    }

    base_url = u'http://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false&px=default&city='+urllib2.quote(str(city))

    data = {'first': 'true', 'pn': page, 'kd':u'产品经理'}
    req = requests.post(base_url, headers=headers, params=data)
    json = req.json()
    print json
    if not json['success']:
        print "get job list for city %s at page %d failed" % (city,page)
        return []
    list_con = json['content']['positionResult']['result']
    info_list = []
    for i in list_con:
        job = {}
        job['pos_name'] = i['positionName']
        job['pos_city'] = city
        job['pos_url'] = "https://www.lagou.com/jobs/%d.html" % i['positionId']
        job['pos_salary'] = i['salary']
        job['pos_edu'] = i['education']
        job['pos_work_age'] = i['workYear']
        job['com_name'] = i['companyFullName']
        job['com_scale'] = i['companySize']
        job['com_stage'] = i['financeStage']
        info_list.append(job)

    return info_list

def get_city_job_list(city):
    page = 1
    info_result = []
    while page < 31:
        info = get_page_job_list(city, page)
        info_result += info
        page += 1

    print "get %d jobs for city %s" % (len(info_result), city)

    return info_result


def do_spider(db,city):
    jobs = get_city_job_list(city)
    for job in jobs:
        job_detail = pos_spider(db,job['pos_url'])
        if job_detail.has_key('pos_desc'):
            job['pos_desc'] = job_detail['pos_desc']
        else:
            job['pos_desc'] = ''

        sqlCommand = gen_insert_command(job)
        result = db.execute(sqlCommand,1)

        #time.sleep(2)


    print jobs

def gen_insert_command(info_dict):
    command = (r"insert or replace into pos values(:pos_url,:pos_city,:pos_name,:pos_salary,"
               r":pos_desc,:pos_work_age,:pos_edu,:com_name,:com_scale,:com_stage)",info_dict)
    return command

if __name__=="__main__":

    command = "create table if not exists pos (" \
              "pos_url TEXT," \
              "pos_city TEXT," \
              "pos_name TEXT ," \
              "pos_salary TEXT ," \
              "pos_desc TEXT," \
              "pos_work_age TEXT," \
              "pos_edu TEXT , " \
              "com_name TEXT," \
              "com_scale TEXT," \
              "com_stage," \
              "primary key (pos_url))"

    db = SQLiteWraper('pos.sqlite', command)

    do_spider(db,u'北京')
    do_spider(db,u'上海')
    do_spider(db,u'广州')