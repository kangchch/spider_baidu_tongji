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
# db_oracle_str = 'behavior/iSo1gO2HoMe@192.168.245.31:7783/behaviorlog1'
# conn = cx_Oracle.connect(db_oracle_str)
# cur = conn.cursor()

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
    def flow_trend_hour(self, siteId, st, et):

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
            logging.warning("[HOUR] requests post timeout! url=%s data=%s", self.url, str(data))
            return
        except Exception, e:
            logging.error("[HOUR] requests post failed! url=%s data=%s", self.url, str(data))

        if r.status_code != requests.codes.ok:
            logging.error("[HOUR] url parse failed! code=%d url=%s jump_url=%s", r.status_code, self.url, r.headers.get('location', ''))
            return

        if r.text:
            try:
                body = json.loads(r.text)
                ## items[0]....items[1]
                records = body['data']['items'][1]
                pv = records[1][0]
                uv = records[1][1]
                ip = records[1][2]

                results_item = {'pv': pv, 'uv':uv, 'ip': ip}
                # logging.info('\n[HOUR] results_item:%s \n', results_item)
                return results_item

            except Exception, e:
                logging.warning('[HOUR] get items error! body=%s\n errmsg=%s ', r.text, str(e).decode('gb18030'))
                return


    def insert_oracle_hours(self, results_items, today, cur):
        time_now = datetime.datetime.now()
        one_hour = datetime.timedelta(hours=1)
        irsl_h = time_now - one_hour
        foward_hour = str(irsl_h.hour)
        if len(foward_hour) == 1:
            foward_hour = '0' + foward_hour
        irsl_date_h = ''.join(today.split('-')) + foward_hour
        try:
            sql = "insert into SJPT_REALTIME_STATIC_HOUR(id, data_type, data_count, irsl_date_h) \
                    values(sjpt_realtime_seq.nextval, :1, :2, :3)"
            insert_items = []
            if 'ip' in results_items:
                insert_items.append([338, results_items['ip'], irsl_date_h])
            if 'pv' in results_items:
                insert_items.append([339, results_items['pv'], irsl_date_h])
            if 'uv' in results_items:
                insert_items.append([340, results_items['uv'], irsl_date_h])

            cur.executemany(sql, insert_items)
            conn.commit()
            logging.info('[HOUR] insert_hour succeed %s %s %s', today, irsl_date_h, results_items)
        except Exception, e:
            logging.warning('[HOUR] insert_hour error! %s %s', sql, str(e))


if __name__ == '__main__':
    
    dirname = os.path.split(os.path.abspath(sys.argv[0]))[0]
    log_file = dirname + '/logs/baidu_tongji_hours.log'
    logInit(log_file, logging.INFO, True, 0)
    
    logging.info('[HOUR] start fetch baidu tongji date ')

    crawler = BaiduCrawler()

    while True:
        db_oracle_str = 'behavior/iSo1gO2HoMe@192.168.245.31:7783/behaviorlog1'
        conn = cx_Oracle.connect(db_oracle_str)
        cur = conn.cursor()
        if time.strftime('%H:%M:%S') == '00:20:00':
            break
        elif time.strftime('%M:%S') == '20:00':
            crawler.cookie, browser = get_baidu_tongji_cookie("spiderhc360", "HCsearch2014", '18210607604')
            time.sleep(2)
            today = time.strftime('%Y-%m-%d')
            today_z = time.strptime(today,'%Y-%m-%d')
            st = et = str(int(time.mktime(today_z) * 1000))
            results_items = crawler.flow_trend_hour('10835307', st, et)
            crawler.insert_oracle_hours(results_items, today, cur)
            logging.info('[HOUR] end fetch baidu tongji date')
            conn.close()
            cur.close()
            browser.quit()
        else:
            conn.close()
            cur.close()
            continue
