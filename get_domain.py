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
external_domain_limit = 1000
blog_limit = 4000
thread_limit_numbers = 20

headers = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
}

f = open('domain_list.csv', 'w')
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
                        print('----- THREAD ----', len(parse_threads), check_sub_xml(sub_url))
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

def find_domain(url):
    global domain_list, blogs, start_time, domain_cnt

    delta_time = datetime.datetime.now().replace(microsecond=0)-start_time
    print ('time = {}, blogs len = {}, external domains = {}, find = {}, blog url = {}'.format(delta_time, len(blogs), len(domain_list), domain_cnt, url))

    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        t = html.fromstring(r.text)
        all_links = t.xpath('//a[contains(@href, "blog")]/@href')
        result = []
        for _url in all_links:
            parsed_uri = urlparse(_url)
            _domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            if not _domain in domain_list and 'http' in _domain and '///' not in _domain:
                if 'www.' in _domain:
                    _domain_w = _domain.replace('www.', '')
                    if _domain_w not in domain_list:
                        _domain_h = _domain.replace('http://', 'https://').replace('https://', 'http://')
                        if _domain_h not in domain_list and len(_domain_h) < 100:
                            domain_list.append(_domain)
                            q.put(_domain)
                            print ('add domain', _domain)
                            domain_cnt += 1
                            csv_writer.writerow([_domain])
                            f.flush()
                else:
                    _domain_n = _domain.replace('http://', 'http://www.').replace('https://', 'https://www.')
                    if _domain_n not in domain_list:
                        _domain_h = _domain.replace('http://', 'https://').replace('https://', 'http://')
                        if _domain_h not in domain_list and len(_domain_h) < 100:
                            domain_list.append(_domain)
                            q.put(_domain)
                            print ('add domain', _domain)
                            domain_cnt += 1
                            csv_writer.writerow([_domain])
                            f.flush()

    except:
        # print (format_exc())
        pass
    # print ('find domain function end', url)


def add_domain():
    global blogs, domain_list, domain_cnt
    domain_cnt = 0
    blog_cnt = 0
    domain_threads = []

    temp = []
    for bItem in blogs:
        if bItem not in temp:
            temp.append(bItem)

    blogs = temp

    while len(blogs) > 0:
        blog_url = blogs.pop()
        while threading.active_count() > thread_limit_numbers:
            sleep(1)
        
        for tt in domain_threads:
            if not tt.is_alive():
                tt.join()
                domain_threads.remove(tt)

        t = threading.Thread(target=find_domain, args=(blog_url,))
        t.daemon = True
        domain_threads.append(t)
        t.start()
        # sleep(1)

        if (len(blogs) == 0) or (blog_cnt > blog_limit):
            print('------- EXTERNAL BLOG ----------')
            break

        if domain_cnt >= external_domain_limit:
            print('------- EXTERNAL DOMAIN ----------')
            break
        blog_cnt += 1

    # for t in domain_threads:
    #     t.join()
    # print ('add domain function end')

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

    print('+++++++++++++++++++++++++')
    sys_ver = sys.version_info
    if sys_ver.major == 2:
        domain = raw_input("Please input the Domain URL (e.g. https://example.com/): ")
    else:
        domain = input("Please input the Domain URL (e.g. https://example.com/): ")

    if 'http' not in domain:
        print('Domain URL should be contained a protocol (http:// or https://).')
        return

    domain = domain.strip(' /') + '/'
    parsed_uri = urlparse(domain)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)        
    q.put(domain)
    domain_list.append(domain)

    flag = True
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
        if total_cnt >= 8 or flag:
            add_domain()
            flag = False

        sleep(1)

if __name__ == '__main__':
    main()

# https://vooozer.com