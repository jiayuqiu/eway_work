# coding:utf8 #

import lxml
import urllib2
import pandas as pd
import time
import re

from lxml.html import fromstring, tostring
from bs4 import BeautifulSoup

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

####################################################

# Xpath解析网页结构，获取发布时间

def get_publish_time(html_xpath, html_root):
    temp_path = html_root.xpath(html_xpath)
    temp = decode_unicode_references(tostring(temp_path[0])) \
            .split('<div class="title_right">')[1] \
            .split('</div>')[0].split('\n')[1]
    temp = temp.split(' ')
    temp = temp[len(temp) - 1].encode('utf-8')
    return temp


# Xpath解析网页结构，爬取温度与风力数据


def get_html_tmp_wind(html_xpath, html_root):
    temp_path = html_root.xpath(html_xpath)
    temp = decode_unicode_references(tostring(temp_path[0])) \
            .split('<td rowspan="2">')[1] \
            .split('</td>')[0].split('\n')[1]
    temp = temp.split(' ')
    temp = temp[len(temp) - 1].encode('utf-8')
    return temp


# Xpath解析网页结构，爬取预报时间数据

def get_html_time1(html_xpath, html_root):
    temp_path = html_root.xpath(html_xpath)
    temp = decode_unicode_references(tostring(temp_path[0])) \
        .split('<td style="background: #e2f3ff">')[1] \
        .split('</td>')[0].split('\n')[1]
    temp = temp.split(' ')
    temp = temp[len(temp) - 1].encode('utf-8')
    return temp


def get_html_time2(html_xpath, html_root):
    temp_path = html_root.xpath(html_xpath)
    temp = decode_unicode_references(tostring(temp_path[0])) \
        .split('<td style="background: #eff8ff">')[1] \
        .split('</td>')[0].split('\n')[1]
    temp = temp.split(' ')
    temp = temp[len(temp) - 1].encode('utf-8')
    return temp

# 获取天气预报


def get_html_wea(html_xpath, html_root):
    temp_path = html_root.xpath(html_xpath)
    temp = decode_unicode_references(tostring(temp_path[0])) \
        .split('<td style=" width:120px;">')[1] \
        .split('</td>')[0].split('\n')[1]
    temp = temp.split(' ')
    temp = temp[len(temp) - 1].encode('utf-8')
    return temp

#######################################################


def html_weather_predict(html_url):

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

    ###################################################################################################################

    # 今日天气数据爬取

    # 获取预报发布时间

    get_time_path = ".//*[@id='ContentPlaceHolder1_forecastContent']/div[4]/div/div[2]"
    publish_time = get_publish_time(html_xpath=get_time_path, html_root=root)

    # 气温数据

    tdy_tmp_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[2]/td[4]'
    tdy_tmp = get_html_tmp_wind(tdy_tmp_path, root)

    # 风力数据

    tdy_wind_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[2]/td[5]'
    tdy_wind = get_html_tmp_wind(tdy_wind_path, root)

    # 预报时间

    tdy_time1_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[2]/td[1]'
    tdy_time1 = get_html_time1(tdy_time1_path, root)

    # 天气预报

    tdy_wea1_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[2]/td[3]'
    tdy_wea1 = get_html_wea(tdy_wea1_path, root)

    tdy_time2_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[3]/td[1]'
    tdy_time2 = get_html_time2(tdy_time2_path, root)

    tdy_wea2_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[3]/td[3]'
    tdy_wea2 = get_html_wea(tdy_wea2_path, root)

    ##################################################################################################################

    # 明日天气数据爬取

    tmr_tmp_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[4]/td[4]'
    tmr_tmp = get_html_tmp_wind(tmr_tmp_path, root)

    tmr_wind_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[4]/td[5]'
    tmr_wind = get_html_tmp_wind(tmr_wind_path, root)

    tmr_time1_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[4]/td[1]'
    tmr_time1 = get_html_time1(tmr_time1_path, root)

    tmr_wea1_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[4]/td[3]'
    tmr_wea1 = get_html_wea(tmr_wea1_path, root)

    tmr_time2_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[5]/td[1]'
    tmr_time2 = get_html_time2(tmr_time2_path, root)

    tmr_wea2_path = './/*[@id="ContentPlaceHolder1_forecastContent"]/div[4]/table/tr[5]/td[3]'
    tmr_wea2 = get_html_wea(tmr_wea2_path, root)

    # convert to dataframe

    predict_time        = [tdy_time1, tdy_time2, tmr_time1, tmr_time2]
    predict_weather     = [tdy_wea1, tdy_wea2, tmr_wea1, tmr_wea2]
    predict_temperature = [tdy_tmp, tdy_tmp, tmr_tmp, tmr_tmp]
    predict_wind        = [tdy_wind, tdy_wind, tmr_wind, tmr_wind]
    weather_predict_df  = pd.DataFrame({'get_time'     : time.strftime('%Y-%m-%d-%H:%M', time.localtime(time.time())),
                                        'Ptime'        : predict_time,
                                        'Pweather'     : predict_weather,
                                        'Ptemperature' : predict_temperature,
                                        'Pwind'        : predict_wind})
    return weather_predict_df, publish_time


def getStrFromDF(df):
    timeStr = str(df['get_time'][0])
    dayStr_td = str(df['Ptime'][0]) + str(df['Pweather'][0]) + str(df['Pwind'][0])
    eveStr_td = str(df['Ptime'][1]) + str(df['Pweather'][1]) + str(df['Pwind'][1])
    dayStr_tmr = str(df['Ptime'][2]) + str(df['Pweather'][2]) + str(df['Pwind'][2])
    eveStr_tmr = str(df['Ptime'][3]) + str(df['Pweather'][3]) + str(df['Pwind'][3])

    weaStr = dayStr_td + '\n' + eveStr_td + '\n' + dayStr_tmr + '\n' + eveStr_tmr
    return weaStr


def html_disaster_predict(html_url):
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
    ###################################################################################################################

    disaster_str = ''
    try:
        d_xpath = root.xpath('//*[@id="ContentPlaceHolder1_WarningContent"]')
        d_topic_xpath = root.xpath('//*[@id="ContentPlaceHolder1_lblTitle"]')
        d_time_xpath = root.xpath('//*[@id="ContentPlaceHolder1_WarningContent"]/div')
        d_topic = decode_unicode_references(tostring(d_topic_xpath[0])) \
                    .split('<span id="ContentPlaceHolder1_lblTitle">')[1] \
                    .split('</span>')[0]
        disaster = decode_unicode_references(tostring(d_xpath[0])) \
                    .split('<div class="warningContent" id="ContentPlaceHolder1_WarningContent">')[1] \
                    .split('<br>')[0]
        d_time = decode_unicode_references(tostring(d_time_xpath[0])) \
                    .split('<div style=" text-align:right; float:right">')[1] \
                    .split('</div>')[0]
        disaster_str = d_topic + ':' + disaster
    except:
        disaster_str = ''
        d_time = ''


    # disaster_path = root.xpath('.//*[@id="tip_waring"]')
    # disaster = decode_unicode_references(tostring(disaster_path[0])) \
    #             .split('<div id="tip_waring">')[1] \
    #             .split('</div>')[0] \
    #             .split('\n')[1]
    #
    # disaster = disaster.encode('utf-8', 'ignore')

    return d_time, disaster_str

##########################################################

# 获取舟山沿海数据
def get_sea_weather(html_url):
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

    # 获取全市天气
    all_city_xpath = ".//*[@id='cphContent_lblForContent']"
    temp_path = root.xpath(all_city_xpath)
    tmp_soup = decode_unicode_references(tostring(temp_path[0]))
    tmpStr_list = decode_unicode_references(tostring(temp_path[0])).split('<br>')

    pattern_time = re.compile(u'舟山市气象台(.*?)发布', re.S)
    items_time = re.findall(pattern_time, tmp_soup)

    tmpStr = []
    for x in tmpStr_list:
        x = x.split(' ')
        for y in x:
            if y:
                tmpStr.append(y)
    sea_weather_index = tmpStr.index(u"舟山沿海海面：\n")

    sea_weather_str = tmpStr[sea_weather_index] + tmpStr[sea_weather_index + 2] + \
                      tmpStr[sea_weather_index + 4] + tmpStr[sea_weather_index + 6]

    out_str = ''
    tmp_list = sea_weather_str.split('\n')

    for x in tmp_list:
        if x:
            out_str = out_str + x

    return items_time[0], out_str



if __name__ == "__main__":

    a, b = get_sea_weather('http://www.zs121.com.cn/TourismWebsite/City/CityWeather.aspx')
    print a, "\n", b
    # 获得天气预报信息
    # weather_predict_df, publish_time = html_weather_predict('http://www.zs121.com.cn/weatherforecast/ShortTimeForecast.aspx')
    # print weather_predict_df
    #
    # # 获得预警信息
    #
    # weaStr = getStrFromDF(weather_predict_df)
    # print publish_time + '\n' + weaStr
    # print '天气预报来源：%s' % 'http://www.zs121.com.cn/weatherforecast/ShortTimeForecast.aspx'

    # weather_predict_df.to_csv('/home/qiu/文档/weather_predict_data.csv', index=False)
    # predict_disaster = html_disaster_predict('http://www.zs121.com.cn/Warning/warninginfo.aspx')
    # print predict_disaster
    # print "天气预警来源：%s" % 'http://www.zs121.com.cn/Warning/warninginfo.aspx'
    # print 'done!'

    # 开始获取舟山沿海数据

    # sea_weather = get_sea_weather('http://www.zs121.com.cn/TourismWebsite/City/CityWeather.aspx')
