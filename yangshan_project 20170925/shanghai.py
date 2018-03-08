# coding:utf-8


import lxml
import urllib2
import re
import time

from lxml.html import fromstring, tostring
from bs4 import BeautifulSoup
from selenium import webdriver

####################################################

# xpath爬取网页数据，中文解码


def _callback(matches):
    id = matches.group(1)
    try:
        return unichr(int(id))
    except:
        return id


def decode_unicode_references(data):
    return re.sub("&#(\d+)(;|(?=\s))", _callback, data)

#########################################################

# 动态爬虫，爬取上海气象报告

def shanghai_report(html_url):

    driver = webdriver.Chrome("/home/qiu/Downloads/chromedriver")
    # driver.set_page_load_timeout(60)
    driver.get(html_url)
    driver.implicitly_wait(30)
    time.sleep(2)

    publish_time = driver.find_element_by_xpath('/html/body/div[1]/div[4]/div[2]/div[1]/div[2]/ul/li[1]').text
    report = driver.find_element_by_xpath("/html/body/div[1]/div[4]/div[2]/div[1]/div[2]/ul/li[2]/pre").text

    report_str = ''
    report_list = report.split('\n')
    try:
        cj_index = report_list.index(u"    今天和明天上海市和长江口区天气预报：")
        cj_report = report_list[cj_index] + '\n' + report_list[cj_index + 1]
        cj_report = cj_report.split(' ')
        for x in cj_report:
            if x:
                x = x.split('\n')
                for xx in x:
                    report_str = report_str + xx
    except:
        print '进入半夜'

    try:
        cj_index = report_list.index(u"    今天夜里和明天上海市和长江口区天气预报：")
        cj_report = report_list[cj_index] + '\n' + report_list[cj_index + 1]
        cj_report = cj_report.split(' ')
        for x in cj_report:
            if x:
                x = x.split('\n')
                for xx in x:
                    report_str = report_str + xx
    except:
        print '进入白天'

    driver.quit()

    return publish_time, report_str


#########################################################

# 爬取上海天气预警

def shanghai_disaster(html_url):
    url = html_url
    headers = {'User-Agent': 'Mozilla/5.0 (X11; U; Linux i686)Gecko/20071127 Firefox/2.0.0.11'}

    ###########################################################################################

    # req, response用于获得网页源代码

    req = urllib2.Request(url, headers=headers)
    response = urllib2.urlopen(req).read()

    ###########################################################################################

    # soup以另一种格式存储网页源代码

    soup = BeautifulSoup(response, 'html.parser').decode('utf-8')
    root = lxml.etree.HTML(soup)
    disaster_xpath = root.xpath('/html/body/div[1]/div[4]/div[2]/div[1]/div[2]/div')

    disaster = ''
    try:
        disaster = decode_unicode_references(tostring(disaster_xpath[0]))
        pattern_time = re.compile(u'<li class="txt img">(.*?)</li>', re.S)
        pattern_disaster = re.compile(u'<li class="txt">(.*?)</li>', re.S)

        items_time = re.findall(pattern_time, disaster)
        items_disaster = re.findall(pattern_disaster, disaster)

        for x in items_disaster:
            disaster_str = disaster_str + x
        return items_time[0], disaster_str
    except:
        return '', ''



if __name__ == '__main__':
    # try:
    #     sh_disaster = shanghai_disaster("http://www.smb.gov.cn/sh/tqyb/zhyj/index.html")
    #     print sh_disaster
    #     print '预警来源:http://www.smb.gov.cn/sh/tqyb/zhyj/index.html'
    # except:
    #     print '当前暂时无预警'

    x = shanghai_report("http://www.smb.gov.cn/sh/tqyb/qxbg/index.html")
    print x
    # disaster_time, disaster_str = shanghai_report("http://www.smb.gov.cn/sh/tqyb/zhyj/index.html")
    # print disaster_time, disaster_str