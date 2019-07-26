# -*- coding: utf-8 -*-
import requests
import re, csv
from datetime import datetime, timedelta, date
import datetime
import threading
from time import time, sleep
from lxml import html
from traceback import format_exc
import sys
import argparse
import xml.etree.ElementTree as ET 
from lxml import etree
import calendar
from unidecode import unidecode

try:
    import queue
    from urllib.parse import urlparse
    q = queue.Queue()
except ImportError:
    from Queue import Queue
    from urlparse import urlparse
    q = Queue()


total_cnt = 0
blogs = set()
domain_list = []
first = ''
last = ''
start_time = None
# external_domain_limit = 500
external_domain_limit = 500
blog_limit = 4000
thread_limit_numbers = 20

headers = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
}

f = open('blog_urls.csv', 'w')
csv_writer = csv.writer(f)

def parse(url):
    global total_cnt
    global first
    global last
    global blogs

    print ('parse function start', url)
    root_url = url
    try:
        r = requests.get(url, headers=headers, allow_redirects=True)
        sub_sitexml_list = re.findall("<loc>(.*?)</loc>", r.text)

        if len(sub_sitexml_list) > 0:
            if  '.xml' in sub_sitexml_list[0]:
                parse_threads = []
                print('------------ Start Thread ------------', len(sub_sitexml_list))
                while len(sub_sitexml_list) > 0:
                    
                    if len(parse_threads) < thread_limit_numbers:
                        sub_url = sub_sitexml_list.pop(0)
                        # print('----- THREAD ----', len(parse_threads), check_sub_xml(sub_url))
                        if check_sub_xml(sub_url):
                            t = threading.Thread(target=parse, args=(sub_url.replace('&amp;','&').replace('<![CDATA[','').replace(']]>',''),))
                            t.daemon = True
                            parse_threads.append(t)
                            t.start()

                    for thread in parse_threads:
                        if not thread.is_alive():
                            thread.join()
                            parse_threads.remove(thread)
                            print("Remain parse_threads -> {}".format(len(parse_threads)))
            else:
                re_date_list = re.findall("<lastmod>(.*?)</lastmod>", r.text)
                html_ = unidecode(r.text.encode('utf-8').decode('utf-8'))
                root = ET.fromstring(html_)
                sub_sitexml_list = []
                re_date_list = []

                for row in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
                    try:
                        url = row.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                    except:
                        url = ''
                    try:
                        lastmod = row.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text
                    except:
                        lastmod = ''

                    sub_sitexml_list.append(url)
                    re_date_list.append(lastmod)

                # print("loc : ",len(sub_sitexml_list))
                # print("lastmod : ",len(re_date_list))

                for idx, sub_url in enumerate(sub_sitexml_list):
                    # print('--------------')
                    # print(sub_url)
                    # print(re_date_list[idx])
                    if '/blog' in sub_url or 'blog.' in sub_url:
                        try:
                            lastmod = datetime.datetime.strptime(re_date_list[idx].split('T')[0], '%Y-%m-%d')
                            if lastmod >= first and lastmod <= last:
                                print ('lastmod',lastmod,'blog url:', sub_url)
                                total_cnt += 1
                                blogs.add(sub_url.replace('&amp;','&'))
                            # csv_writer.writerow([root_url, sub_url, lastmod])
                            # f.flush()
                        except:
                            pass
    except:
        pass
        # print (format_exc())

    print ('parse function end', url)

def check_sub_xml(sub_url):
    block_list = ['tag', 'attachment', 'category', 'author', 'page']

    parsed_uri = urlparse(sub_url)
    ch_domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    s_url = sub_url.split(ch_domain)[-1]
    for b in block_list:
        if b in s_url.lower():
            return False

    return True

domain_cnt = 0

def main():
    global total_cnt
    global blogs
    global first
    global last
    global domain_list
    global start_time

    start_time = datetime.datetime.now().replace(microsecond=0)
    today = date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    d = calendar.monthrange(lastMonth.year, lastMonth.month)
    first_s = lastMonth.replace(day=1)
    last_s = lastMonth.replace(day=d[1])

    first = datetime.datetime.strptime(str(first_s), '%Y-%m-%d')
    last = datetime.datetime.strptime(str(last_s), '%Y-%m-%d')

    try:
        print('=========================')
        with open('domain_list.csv') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                domain = ''.join(row).strip(' /') + '/'
                parsed_uri = urlparse(domain)
                domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)                       
                q.put(domain)
                domain_list.append(domain)
    except:
        print('Please check the Input file. File name must "domain_list.csv".')

    f1 = open('good_domain_{}.csv'.format(start_time), 'w')
    f2 = open('bad_domain_{}.csv'.format(start_time), 'w')
    csv_writer1 = csv.writer(f1)
    csv_writer2 = csv.writer(f2)

    good_domain_cnt = 0
    bad_domain_cnt = 0

    xml_urls = ['sitemap.xml', 'sitemap_index.xml', 'blog/sitemap_index.xml', 'sitemap_blog.xml', 'blog/sitemap.xml', 'sitemap-core.xml', 'emea/sitemap-core.xml']
    while not q.empty():
        total_cnt = 0
        blogs = set()
        url = q.get()
        print ("Start URL: ", url)

        for xml_url in xml_urls:
            sitemap_xml = url + xml_url
            parse(sitemap_xml)
            
        print ("==================>" , url,  total_cnt)
        if total_cnt >= 8:
            csv_writer1.writerow([url, total_cnt])
            f1.flush()                
            good_domain_cnt += 1

        else:           
            csv_writer2.writerow([url, total_cnt])
            f2.flush()
            bad_domain_cnt += 1

        sleep(1)
        delta_time = datetime.datetime.now().replace(microsecond=0) - start_time
        print ("-----------------------------------------------------------------")
        print('===== >> time : {}, total: {}, good : {}, bad : {}, rest : {} << ====='.format(delta_time, len(domain_list)-1, good_domain_cnt, bad_domain_cnt, q.qsize()))
        print ("-----------------------------------------------------------------")

if __name__ == '__main__':
    main()

# https://vooozer.com