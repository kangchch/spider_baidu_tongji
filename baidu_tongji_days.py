# -*- coding: utf-8 -*-
import os
import sys
import logging
import requests
import time
import datetime
import json
import copy
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
        self.host = 'tongji.baidu.com'
        self.origin = 'https://tongji.baidu.com'
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36'
        self.cookie = ''
        self.url = 'https://tongji.baidu.com/web/'+self.id+'/ajax/post'


    ## 解析mip下趋势分析的数据
    def flow_trend_day(self, siteId, st, et, yesterday):

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
        # while True:
        try:
            r = requests.post(self.url, headers=headers, data=data, allow_redirects=False, timeout=120)
            time.sleep(1)
        except requests.Timeout:
            logging.warning("[DAY] requests post timeout! url=%s data=%s", self.url, str(data))
            return
        except Exception, e:
            logging.error("[DAY] requests post failed! url=%s data=%s", self.url, str(data))

        if r.status_code != requests.codes.ok:
            logging.error("[DAY] url parse failed! code=%d url=%s jump_url=%s", r.status_code, self.url, r.headers.get('location', ''))
            return

        if r.text:
            try:
                body = json.loads(r.text)
                records = body['data']
                ## 前一天总量
                records_sum = records['pageSum']
                pv = records_sum[0][0]
                uv = records_sum[0][1]
                ip = records_sum[0][2]

                results_days = {'pv': pv, 'uv':uv, 'ip': ip}

                irsl_date = ''.join(yesterday.split('-'))
                try:
                    sql_day = "insert into SJPT_REALTIME_STATIC_DAY(id, data_type, data_count, irsl_date) \
                            values(sjpt_realtime_day_seq.nextval, :1, :2, :3)"
                    insert_items = []
                    if 'ip' in results_days:
                        insert_items.append([338, results_days['ip'], irsl_date])
                    if 'pv' in results_days:
                        insert_items.append([339, results_days['pv'], irsl_date])
                    if 'uv' in results_days:
                        insert_items.append([340, results_days['uv'], irsl_date])

                    cur.executemany(sql_day, insert_items)
                    conn.commit()
                    logging.info('[DAY] insert_day succeed %s %s', yesterday, results_days)
                except Exception, e:
                    logging.warning('[DAY] insert_day error! %s %s', sql_day, str(e))

                ## 前一天 23:00-23:59
                records_hour = records['items'][1]
                pv = records_hour[0][0]
                uv = records_hour[0][1]
                ip = records_hour[0][2]
                results_items = {'pv': pv, 'uv':uv, 'ip': ip}

                time_now = datetime.datetime.now()
                one_hour = datetime.timedelta(hours=23)
                irsl_d = time_now + one_hour
                irsl_date_h = ''.join(yesterday.split('-')) + str(irsl_d.hour)
                try:
                    sql_hour = "insert into SJPT_REALTIME_STATIC_HOUR(id, data_type, data_count, irsl_date_h) \
                            values(sjpt_realtime_seq.nextval, :1, :2, :3)"
                    insert_items = []
                    if 'ip' in results_items:
                        insert_items.append([338, results_items['ip'], irsl_date_h])
                    if 'pv' in results_items:
                        insert_items.append([339, results_items['pv'], irsl_date_h])
                    if 'uv' in results_items:
                        insert_items.append([340, results_items['uv'], irsl_date_h])

                    cur.executemany(sql_hour, insert_items)
                    conn.commit()
                    logging.info('[DAY] insert_hour succeed %s %s %s', yesterday, irsl_date_h, results_items)
                except Exception, e:
                    logging.warning('[DAY] insert_hour error! %s %s', sql_hour, str(e))

            except Exception, e:
                logging.warning('[DAY] get items error! body=%s\n errmsg=%s ', r.text, str(e).decode('gb18030'))
                return


if __name__ == '__main__':

    dirname = os.path.split(os.path.abspath(sys.argv[0]))[0]
    log_file = dirname + '/logs/baidu_tongji_days.log'
    logInit(log_file, logging.INFO, True, 0)

    logging.info('[DAY] start fetch baidu tongji date ')

    crawler = BaiduCrawler()

    while True:
        if time.strftime('%H:%M:%S') == '00:20:00':
            crawler.cookie, browser = get_baidu_tongji_cookie("spiderhc360", "HCsearch2014", '18210607604')
            today = datetime.date.today()
            one = datetime.timedelta(days=1)
            yesterday = today - one
            yesterday = yesterday.strftime('%Y-%m-%d')
            today_z = time.strptime(yesterday,'%Y-%m-%d')
            st = et = str(int(time.mktime(today_z) * 1000))
            crawler.flow_trend_day('10835307', st, et, yesterday)
            logging.info('[DAY] end fetch baidu tongji date')
            browser.quit()
            continue
        else:
            continue
