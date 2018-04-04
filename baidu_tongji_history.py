#! /usr/bin/env python
# -*- coding:utf-8 -*-
#====#====#====#====
# __author__ = "blackang"
#FileName: *.py
#Version:1.0.0
#====#====#====#====
import os
import sys
import logging
import requests
import time
import datetime
import json
from get_cookie import *
from function import *
import cx_Oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
reload(sys)
sys.setdefaultencoding('utf-8')

proxies = {
    "http": "http://tj_user_5_2:111111@60.28.110.66:8899",
}
db_oracle_str = 'behavior/iSo1gO2HoMe@192.168.245.31:7783/behaviorlog1'
conn = cx_Oracle.connect(db_oracle_str)
cur = conn.cursor()

class BaiduCrawler:
    def __init__(self):
        self.bd_ip = 338
        self.bd_pv = 339
        self.bd_uv = 340
        self.id = '20745093' #hc_360 id
        # self.siteId = '10835307'
        self.host = 'tongji.baidu.com'
        self.origin = 'https://tongji.baidu.com'
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36'
        self.cookie = ''
        self.url = 'https://tongji.baidu.com/web/'+self.id+'/ajax/post'


    ## 解析mip下趋势分析每个小时的数据
    def flow_trend_hour(self, siteId, st, et, yesterday):

        headers = {}
        headers['User-Agent'] = self.user_agent
        headers['Referer'] = 'https://tongji.baidu.com/web/'+self.id+'/trend/time?siteId='+ siteId
        headers['Cookie'] = self.cookie
        headers['Host'] = self.host
        headers['Origin'] = self.origin
        headers['X-Requested-With'] = 'XMLHttpRequest'
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        headers['Connection'] = 'keep-alive'
        headers['Accept'] = 'text/plain, */*; q=0.01'

        data = {}
        data['siteId'] = siteId
        data['clientDevice'] = 'all'
        data['indicators'] = 'pv_count,visitor_count,ip_count,bounce_ratio,avg_visit_time'
        data['order'] = 'simple_date_title,desc'
        data['offset'] = '0'
        data['target'] = '-1'
        data['gran'] = '6'
        data['flag'] = 'today'
        data['pageSize'] = '24'
        data['reportId'] = '3'
        data['method'] = 'trend/time/a'
        data['queryId'] = ''
        data['st'] = st
        data['et'] = et

        r = None
        while True:
            try:
                r = requests.post(self.url, headers=headers, data=data, allow_redirects=False, timeout=120)
                time.sleep(1)
                break
            except requests.Timeout:
                logging.warning("[HISTORY] requests post timeout! url=%s data=%s", self.url, str(data))
                continue
            except Exception, e:
                logging.error("[HISTORY] requests post failed! url=%s data=%s", self.url, str(data))

        if r.status_code != requests.codes.ok:
            logging.error("[HISTORY] url parse failed! code=%d url=%s jump_url=%s", r.status_code, self.url, r.headers.get('location', ''))
            return

        if r.text:
            try:
                body = json.loads(r.text)
                ## items[0]....items[1]
                items = body['data']['items']
                records = items[1]
                for index, item in enumerate(items[0]):
                    time_h= item[0]
                    hour = time_h.split('-')[0].split(':')[0]
                    pv = records[index][0]
                    uv = records[index][1]
                    ip = records[index][2]

                    results_hours= {'yesterday': yesterday, 'hour': hour, 'time_h':time_h, 'pv': pv, 'uv':uv, 'ip': ip}
                    logging.info('\n[HISTORY_HOURS] results_hours:%s \n', results_hours)
                    crawler.insert_oracle_hours(results_hours, yesterday, hour)

                # if 'data' in body:
                #     data = body['data']
                #     if 'pageSum' in data:
                #         pv = data['pageSum'][0][0]
                #         uv = data['pageSum'][0][1]
                #         ip = data['pageSum'][0][2]
                #         results_days = {'yesterday': yesterday, 'pv': pv, 'uv':uv, 'ip': ip}
                #         logging.info('\n[HISTORY_DAYS] results_days:%s \n', results_days)
                #         crawler.insert_oracle_days(results_days, yesterday)
            except Exception, e:
                logging.warning('[HISTORY] get items error! body=%s\n errmsg=%s ', r.text, str(e).decode('gb18030'))
                return


    def insert_oracle_hours(self, results_items, yesterday, hour):
        irsl_date_h = ''.join(yesterday.split('-')) + hour
        try:
            sql = "insert into SJPT_REALTIME_STATIC_HOUR(id, data_type, data_count, irsl_date_h) \
                    values(sjpt_realtime_seq.nextval, :1, :2, :3)"
            insert_items = []
            if 'ip' in results_items:
                insert_items.append([self.bd_ip, results_items['ip'], irsl_date_h])
            if 'pv' in results_items:
                insert_items.append([self.bd_pv, results_items['pv'], irsl_date_h])
            if 'uv' in results_items:
                insert_items.append([self.bd_uv, results_items['uv'], irsl_date_h])

            cur.executemany(sql, insert_items)
            conn.commit()

            logging.info('[HISTORY_HOURS] insert_hour succeed %s %s %s', yesterday, irsl_date_h, results_items)
        except Exception, e:
            logging.warning('[HISTORY_HOURS] insert_hour error! %s %s', sql, str(e))

    def insert_oracle_days(self, results_items, yesterday):
        irsl_date = ''.join(yesterday.split('-'))
        try:
            sql = "insert into SJPT_REALTIME_STATIC_DAY(id, data_type, data_count, irsl_date) \
                    values(sjpt_realtime_day_seq.nextval, :1, :2, :3)"
            insert_items = []
            if 'ip' in results_items:
                insert_items.append([self.bd_ip, results_items['ip'], irsl_date])
            if 'pv' in results_items:
                insert_items.append([self.bd_pv, results_items['pv'], irsl_date])
            if 'uv' in results_items:
                insert_items.append([self.bd_uv, results_items['uv'], irsl_date])

            cur.executemany(sql, insert_items)
            conn.commit()
            logging.info('[HISTORY_DAYS] insert_days succeed %s %s %s', yesterday, irsl_date, results_items)
        except Exception, e:
            logging.warning('[HISTORY_DAYS] insert_days error! %s %s', sql, str(e))

if __name__ == '__main__':

    dirname, filename = os.path.split(os.path.realpath(__file__))
    log_file = dirname + '/logs/' + filename.replace('.py', '.log')
    logInit(log_file, logging.INFO, True)


    logging.info('[HISTORY] start fetch baidu tongji date ')

    crawler = BaiduCrawler()

    crawler.cookie, browser = get_baidu_tongji_cookie("spiderhc360", "HCsearch2014", '18210607604')
    time.sleep(2)
    ## range(27-55): 2018-02-01->2018-02-28; range(55,86): 2018-01-01->2018-01-31
    # for index in range(0, 29):
    #     today = datetime.date.today()
    #     ago = datetime.timedelta(days=index+1)
    #     yesterday = today - ago
    #     yesterday = yesterday.strftime('%Y-%m-%d')
    #     today_z = time.strptime(yesterday,'%Y-%m-%d')
    #     st = et = str(int(time.mktime(today_z) * 1000))

    # today tmp hour
    today = time.strftime('%Y-%m-%d')
    today_z = time.strptime(today,'%Y-%m-%d')
    st = et = str(int(time.mktime(today_z) * 1000))
    results_items = crawler.flow_trend_hour('10835307', st, et, today)
    logging.info('[HISTORY] end fetch baidu tongji date')

    browser.quit()

