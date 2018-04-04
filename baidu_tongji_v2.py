# -*- coding: utf-8 -*-
import os
import sys
import logging
import requests
import time
import datetime
import json
import copy
from get_cookie import get_baidu_tongji_cookie
from function import *
import uuid

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
reload(sys)
sys.setdefaultencoding('utf-8')

proxies = {
  "http": "http://tj_user_5_2:111111@60.28.110.66:8899",
}

class BaiduCrawler:
    def __init__(self):
        ## interface_url 抓取完数据后需要调用的接口
        self.interface_url = 'https://logrecords.hc360.com/logrecordservice/logrecordget'
        # self.interface_url = 'http://log.org.hc360.com/logrecordservice/logrecordget'#test_ip
        self.id = '20745093' #hc_360 id
        self.host = 'tongji.baidu.com'
        self.origin = 'https://tongji.baidu.com'
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36'
        self.cookie = ''
        self.url = 'https://tongji.baidu.com/web/'+self.id+'/ajax/post'

        self.call_headers = {
            'User-Agent': self.user_agent,
            # 'Referer': self.Referer,
        }
    ## 解析4个域的实时访客信息
    def realtime_visitor_and_call_interface(self, siteId, offset, pageSize):

        headers = {}
        headers['User-Agent'] = self.user_agent
        headers['Referer'] = 'https://tongji.baidu.com/web/'+self.id+'/trend/latest?siteId='+siteId
        headers['Cookie'] = self.cookie
        headers['Host'] = self.host
        headers['Origin'] = self.origin
        headers['X-Requested-With'] = 'XMLHttpRequest'
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        headers['Connection'] = 'keep-alive'
        headers['Accept'] = 'text/plain, */*; q=0.01'

        data = {}
        data['siteId'] = siteId
        data['order'] = 'start_time,desc'
        data['offset'] = str(offset)    # 页数 0-> 第一页，20-> 第二页，，，，以此类推
        data['pageSize'] = pageSize        #每页显示条数 默认是20条, 最大100条
        data['tab'] = 'visit'
        data['timeSpan'] = '14'
        data['isPromotion'] = '0'
        data['indicators'] = 'start_time,area,source,access_page,searchword,visitorId,ip,visit_time,visit_pages'
        data['reportId'] = '4'
        data['method'] = 'trend/latest/a'
        data['queryId'] = ''

        r = None
        # while True:
        try:
            r = requests.post(self.url, headers=headers, data=data, allow_redirects=False, timeout=120)
            time.sleep(1)
            # break
        except requests.Timeout:
            logging.warning("requests post timeout! url=%s data=%s", self.url, str(data))
            return
            # continue
        except Exception, e:
            logging.error("requests post failed! url=%s data=%s", self.url, str(data))

        if r.status_code != requests.codes.ok:
            logging.error("url parse failed! code=%d url=%s jump_url=%s", r.status_code, self.url, r.headers.get('location', ''))
            return

        if r.text:
            try:
                body = json.loads(r.text)
                ## items[0]....items[1]
                items = body['data']['items']
                records = items[1]
                results_item = {}
                for index, item in enumerate(items[0]):
                    try:
                        if 'resolution' in item[0]['detail']:
                            sx_sy = item[0]['detail']['resolution']
                            sx = sx_sy.split('x')[0]
                            sy = sx_sy.split('x')[1]
                        else:
                            sx = sy = ''
                    except Exception,e:
                        logging.error('list index of range [%s]', str(e))
                        sx = sy = ''
                    pt = '0'
                    cs = 'UTF-8'
                    ps = '0'
                    vi = '_'.join(['bd', records[index][6]])#访客标识
                    ft = records[index][0].replace('/', '-')#time
                    ai = str(uuid.uuid1()).replace('-','')
                    aci = ai
                    bi = ai
                    pi = ai
                    ot = '1'
                    visitIp = records[index][5] if records[index][5] else ' '
                    call_referer = records[index][4] if records[index][4] else ' '#用户的当前地址
                    pu = records[index][2]['url']
                    if pu == '--' or pu == None:
                        pu = ''

                    self.call_headers['Referer'] = call_referer
                    results_item = {'sx': sx,'sy': sy,'pt': pt,'cs': cs,'ps': ps,'vi': vi,
                        'ft': ft,'ai': ai,'aci': aci,'bi': bi,'pi': pi,'ot': ot,'visitIp': visitIp, 'pu': pu
                    }
                    # logging.info('sx:%s\n sy:%s\n pt:%s\n cs:%s\n ps:%s\n vi:%s\n ft:%s\n ai:%s\n aci:%s\n bi:%s\n pi:%s\n ot:%s\n visitIp:%s\n',sx, sy, pt, cs, ps, vi, ft, ai, aci, bi, pi, ot, visitIp)

                    ## call interface
                    try:
                        call = requests.get(self.interface_url, params = results_item, headers = self.call_headers, verify=False)
                        if call.status_code != 200:
                            logging.error('call.status_code:%s\n', call.status_code)
                            continue
                        logging.info('call interface method (get) ok\n, siteId:%s status:%s  offset:%s  index:%s\n, data=%s\n', siteId, call.status_code, offset, index, results_item)
                    except Exception, e:
                        logging.error('call interface failed [], [%s]', str(e))

            except Exception, e:
                logging.warning('get items error! body=%s\n errmsg=%s ', r.text, str(e).decode('gb18030'))
                return


if __name__ == '__main__':

    dirname, filename = os.path.split(os.path.realpath(__file__))
    log_file = dirname + '/logs/' + filename.replace('.py', '.log')
    logInit(log_file, logging.INFO, True)

    logging.info('start fetch baidu tongji date ')


    crawler = BaiduCrawler()

    crawler.cookie = get_baidu_tongji_cookie("spiderhc360", "HCsearch2014", '18210607604')
    time.sleep(5)
    logging.info('*****************************')

    siteIDs = (
        #{'siteid': '5933403'},#hc360
        #{'siteid': '5971229'},#m_hc360
        {'siteid': '6603065'},#js_hc360
        {'siteid': '10835307'}#mip_hc360
    )
    while True:
        for siteid in siteIDs:
            for page in range(0, 2000, 100):
                crawler.realtime_visitor_and_call_interface(siteid['siteid'], page, 50)
        time.sleep(10)

    logging.info('#############################')

