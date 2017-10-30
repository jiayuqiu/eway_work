# coding:utf-8

import pandas as pd
import numpy as np
import time
import urllib.request
import json
import lxml
import re
import pymysql

from lxml.html import tostring
from bs4 import BeautifulSoup
from base_func import isInRange


####################################################
# xpath爬取网页数据，中文解码
def _callback(matches):
    id = matches.group(1)
    try:
        return chr(int(id))
    except:
        return id


def decode_unicode_references(data):
    return re.sub("&#(\d+)(;|(?=\s))", _callback, str(data))


class Weather(object):
    def __init__(self):
        self.shanghai_url = "http://www.smb.gov.cn/sh/tqyb/qxbg/index.html"
        self.zhoushan_url = "http://www.zs121.com.cn/TourismWebsite/City/CityWeather.aspx"
        self.northPort_url = "http://115.231.126.81/Forecast/PopupContent?stationNum=58472&interval=24&_="
        self.northPort_pub_time_url = "http://115.231.126.81/Forecast/"
        self.headers = {'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                      r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
                        'Referer': r'http://www.lagou.com/zhaopin/Python/?labelWords=label',
                        'Connection': 'keep-alive'}

    # ------------------------------------------------------------------------------------
    def north_port_pub_time(self):
        """
        获取北部港区预报更新时间
        :return: 返回date和clock
        """
        req = urllib.request.Request(self.northPort_pub_time_url, headers=self.headers)
        page_source = urllib.request.urlopen(req).read().decode()
        bs_page_source = BeautifulSoup(page_source, "html.parser").decode()
        root_page = lxml.etree.HTML(bs_page_source)

        # 通过xpath获取天气预报更新时间
        pub_time_xpath = '//*[@id="minPopupInnerContent43"]'
        root_pub_time_list = root_page.xpath(pub_time_xpath)
        pub_time_string = decode_unicode_references(tostring(root_pub_time_list[0]))
        pub_time_pattern = re.compile('sub3="(.*?)"', re.S)
        pub_time_elements_list = re.findall(pub_time_pattern, pub_time_string)
        pub_time = pub_time_elements_list[0]
        time_split_pattern = re.compile(r'(.*?)年(.*?)月(.*?)日(.*?)时(.*?)分', re.S)
        time_split_list = re.findall(time_split_pattern, pub_time)[0]
        pub_date_string = str(time_split_list[0]) + "-" + str(time_split_list[1]) + "-" + str(time_split_list[2])
        pub_clock_string = str(time_split_list[3]) + ":" + str(time_split_list[4]) + ":" + "00"
        return pub_date_string, pub_clock_string

    def north_port(self):
        """
        获取北部港区风力、能见度数据
        :return: 返回日期，时间，风力，能见度数据，类型：data frame
        """
        now_timestamp = int(time.time() * 1000)
        url = self.northPort_url + "%s" % now_timestamp

        req = urllib.request.Request(url, headers=self.headers)
        page_source_json = urllib.request.urlopen(req).read().decode('utf-8')
        page_source = json.loads(page_source_json)['html']
        bs_page = BeautifulSoup(page_source, 'html.parser').decode('utf-8')
        root_page = lxml.etree.HTML(bs_page)

        # 初始化日期，时间，风力，能见度列表
        all_date_list = []
        all_clock_list = []
        all_wind_list = []
        all_njd_list = []

        # xpath获取时间数据
        time_xpath = '//*[@style="min-width:60px;"]'
        root_time_list = root_page.xpath(time_xpath)
        for time_element in root_time_list:
            time_str = decode_unicode_references(tostring(time_element)).split(r'<td style="min-width:60px;">\n    ')[1] \
                .split(r'\n   </td>')[0]
            time_date = int(time_str.split('日')[0])
            time_clock = int(time_str.split('日')[1].split('时')[0])
            all_date_list.append(time_date)
            all_clock_list.append(time_clock)

        # xpath获取风力数据
        wind_xpath = '//table/tbody/tr'
        root_wind_list = root_page.xpath(wind_xpath)
        wind_str = decode_unicode_references(tostring(root_wind_list[0]))
        wind_pattern = re.compile(u'<span style="background:.*?">'
                                  u'(.*?)</span>', re.S)
        wind_elements_list = re.findall(wind_pattern, wind_str)
        for wind_element in wind_elements_list:
            wind_level = (wind_element.split(r'\n     ')[1]).split(r'\n    ')[0]
            all_wind_list.append(int(wind_level))

        # xpath获取能见度数据
        njd_xpath = '//table/tbody/tr[5]/td'
        root_njd_list = root_page.xpath(njd_xpath)
        root_njd_list = root_njd_list[1:]
        njd_pattern_warning = re.compile(r'<span .*?>(.*?)</span>', re.S)
        njd_pattern_no_warning = re.compile(r'<span>(.*?)</span>', re.S)
        for njd_element in root_njd_list:
            njd_value_warning = re.findall(njd_pattern_warning, decode_unicode_references(tostring(njd_element)))
            njd_value_no_warning = re.findall(njd_pattern_no_warning, decode_unicode_references(tostring(njd_element)))
            if len(njd_value_warning) > 0:
                njd_value = (njd_value_warning[0].split(r'\n     ')[1]).split(r'\n    ')[0]
            elif len(njd_value_no_warning) > 0:
                njd_value = (njd_value_no_warning[0].split(r'\n     ')[1]).split(r'\n    ')[0]
            all_njd_list.append(int(njd_value))
        # njd_pattern = re.compile(r'<span .*?>(.*?)</span>', re.S)
        # njd_elements_list = re.findall(njd_pattern, njd_str)
        # for njd_element in njd_elements_list:
        #     if (not '能见度' in njd_element) & (not '(单位:m)' in njd_element):
        #         njd_dst = (njd_element.split(r'\n     ')[1]).split(r'\n    ')[0]
        #         all_njd_list.append(int(njd_dst))
        weather_predick_df = pd.DataFrame(columns=['date', 'clock', 'wind', 'njd'])
        weather_predick_df['date'] = all_date_list
        weather_predick_df['clock'] = all_clock_list
        weather_predick_df['wind'] = all_wind_list
        weather_predick_df['njd'] = all_njd_list
        now_time_index = weather_predick_df[(weather_predick_df['date'] == int(time.strftime('%d'))) &
                                            (weather_predick_df['clock'] == int(time.strftime('%H')))].index.tolist()

        # 获取北部港区预报更新时间
        # pub_date, pub_clock = self.north_port_pub_time()
        pub_time = json.loads(page_source_json)['publishTime']
        pub_date = pub_time.split(' ')[0]
        pub_clock = pub_time.split(' ')[1] + ":00"

        # 找到当前时间后的天气预报
        if len(now_time_index) > 0:
            weather_predick_df = weather_predick_df.iloc[now_time_index[0]:, :]
        else:
            weather_predick_df = weather_predick_df.iloc[-1, :]

        # np.max(weather_predick_df['wind']), np.min(weather_predick_df['njd'])
        north_port_report_string = "北部港区24小时内，最大风力达到%s级，最小能见度达到%s米" % \
                                   (np.max(weather_predick_df['wind']), np.min(weather_predick_df['njd']))

        return pub_date, pub_clock, np.max(weather_predick_df['wind']), np.min(weather_predick_df['njd']), \
               north_port_report_string

    # ---------------------------------------------------------------------------------------
    def zhoushan_pub_time(self, re_pattern, pub_time_string):
        """
        获取舟山天气预报的发布时间
        :param re_pattern: 获取时间的正则表达式
        :param pub_time_string: 原始字符串
        :return: date和clock
        """
        time_element_list = re.findall(re_pattern, pub_time_string)[0]
        pub_date = str(time_element_list[0]) + "-" + str(time_element_list[1]) + "-" + str(time_element_list[2])
        pub_clock = str(time_element_list[3]) + ":" + "00" + ":" + "00"
        return pub_date, pub_clock

    def zhoushan(self):
        """
        舟山气象报告天气爬虫
        :return: 发布时间，舟山气象报告
        """
        req = urllib.request.Request(self.zhoushan_url, headers=self.headers)
        page_source = urllib.request.urlopen(req).read().decode()
        bs_page_source = BeautifulSoup(page_source, 'html.parser').decode('utf-8')
        root_page_source = lxml.etree.HTML(bs_page_source)

        # 获取全市天气
        weather_report_xpath_string = ".//*[@id='cphContent_lblForContent']"
        weather_report_element_list = root_page_source.xpath(weather_report_xpath_string)
        report_string = decode_unicode_references(tostring(weather_report_element_list[0])).replace(' ', '')
        report_split_list = decode_unicode_references(report_string).split('<br>')
        sea_weather_index = report_split_list.index(u"\\n舟山沿海海面：\\n")

        sea_weather_str = report_split_list[sea_weather_index] + report_split_list[sea_weather_index + 2] + \
                          report_split_list[sea_weather_index + 4] + report_split_list[sea_weather_index + 6]
        sea_weather_str = sea_weather_str.replace(r'\n', '')

        # 获取天气预报更新时间
        time_element_pattern = re.compile(r'舟山市气象台(.*?)年(.*?)月(.*?)日(.*?)点钟发布', re.S)
        pub_date, pub_clock = self.zhoushan_pub_time(time_element_pattern, report_string)
        return pub_date, pub_clock, sea_weather_str, "舟山"

    # -----------------------------------------------------------------------------------
    def shanghai_pub_time(self, pub_time_string):
        """
        获取上海气象报告更新时间
        :param pub_time_string: 上海气象报告更新时间字符串，类型：string
        :return: date,clock
        """
        pub_time_string = pub_time_string.replace(r'\n', '')
        pub_time_string = pub_time_string.replace(' ', '')
        time_split_pattern = re.compile(r'日期:(.*?)年(.*?)月(.*?)日(.*?):(.*?)</li>', re.S)
        time_split_list = re.findall(time_split_pattern, pub_time_string)[0]
        pub_date_string = str(time_split_list[0]) + "-" + str(time_split_list[1]) + "-" + str(time_split_list[2])
        pub_clock_string = str(time_split_list[3]) + ":" + str(time_split_list[4]) + ":" + "00"
        return pub_date_string, pub_clock_string


    def cj_report(self, report_list):
        """
        获取长江口天气预报
        :param report_list: 按回车分割后的上海气象报告
        :return: 长江口天气预报，类型：stirng
        """
        cj_report_str = ""
        try:
            cj_index = report_list.index(u"今天和明天上海市和长江口区天气预报：")
            cj_report = report_list[cj_index] + '\n' + report_list[cj_index + 1]
            cj_report = cj_report.split(' ')
            for x in cj_report:
                if x:
                    x = x.split('\n')
                    for xx in x:
                        cj_report_str = cj_report_str + xx
        except:
            pass

        try:
            cj_index = report_list.index(u"今天夜里和明天上海市和长江口区天气预报：")
            cj_report = report_list[cj_index] + '\n' + report_list[cj_index + 1]
            cj_report = cj_report.split(' ')
            for x in cj_report:
                if x:
                    x = x.split('\n')
                    for xx in x:
                        cj_report_str = cj_report_str + xx
        except:
            pass
        return cj_report_str

    def shanghai(self):
        """
        获取上海气象报告
        :return: 更新时间，气象报告
        """
        req = urllib.request.Request(self.shanghai_url, headers=self.headers)
        page_source = urllib.request.urlopen(req).read().decode()
        bs_page_source = BeautifulSoup(page_source, 'html.parser').decode('utf-8')
        root_page_source = lxml.etree.HTML(bs_page_source)

        weather_report_xpath_string = "/html/body/div[2]/div[4]/div[2]/div[1]/div[2]/ul/li[2]/pre"
        weather_report_element_list = root_page_source.xpath(weather_report_xpath_string)
        weather_report_element_str = decode_unicode_references(tostring(weather_report_element_list[0]))
        report_string = weather_report_element_str.replace(' ', '').replace('\\r', '')
        report_list = report_string.split('\\n')

        # 获取长江口天气预报
        cj_report_str = self.cj_report(report_list)

        # 获取上海气象报告更新时间
        pub_time_xpath = '/html/body/div[2]/div[4]/div[2]/div[1]/div[2]/ul/li[1]'
        pub_time_element_list = root_page_source.xpath(pub_time_xpath)
        pub_time_string = decode_unicode_references(tostring(pub_time_element_list[0]))
        pub_date, pub_clock = self.shanghai_pub_time(pub_time_string)
        return pub_date, pub_clock, cj_report_str, "上海"

    # --------------------------------------------------------------------
    def get_newest_report(self, src_loc):
        """
        从数据库中获取气象报告
        :param src_loc: 数据源，舟山、上海、北部港区
        :return: 气象报告，类型：string
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()
        weather_select_sql = "SELECT * from weather_report ORDER BY wr_id DESC limit 50"
        cur.execute(weather_select_sql)
        weather_report_tuple = cur.fetchall()
        weather_report_list = list(weather_report_tuple)
        weather_report_df = pd.DataFrame(weather_report_list)
        weather_report_df.columns = ["wz_id", "pub_date", "pub_clock", "report", "njd", "src_loc", "src_url"]
        src_loc_weather_df = weather_report_df[(weather_report_df['src_loc'] == src_loc)]
        newest_weather_df = src_loc_weather_df[src_loc_weather_df['wz_id'] == np.max(src_loc_weather_df['wz_id'])]
        conn.commit()
        cur.close()
        conn.close()
        if len(newest_weather_df) > 0:
            newest_pub_date = newest_weather_df.iloc[0, 1]
            newest_pub_clock = newest_weather_df.iloc[0, 2]
            newest_report = newest_weather_df.iloc[0, 3]
            return str(newest_pub_date), str(newest_pub_clock), str(newest_report)
        else:
            return "-1", "-1", "-1"

    def report_mysql(self):
        """
        将获取到的气象报告、气象预警入库
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        # 获取上海、舟山、北部港区气象报告
        zhoushan_weather = self.zhoushan()
        shanghai_weather = self.shanghai()
        north_port_weather = self.north_port()

        # 从气象报告中获取风力、能见度预警
        zhoushan_newest_pub_date, zhoushan_newest_pub_clock, zhoushan_newest_report = self.get_newest_report('舟山')
        zhoushan_max_avg_wind, zhoushan_max_zf_wind = self.max_wind(zhoushan_weather[2], "舟山")
        shanghai_newest_pub_date, shanghai_newest_pub_clock, shanghai_newest_report = self.get_newest_report('上海')
        shanghai_max_avg_wind, shanghai_max_zf_wind = self.max_wind(shanghai_weather[2], "上海")
        north_port_newest_pub_date, north_port_newest_pub_clock, north_port_newest_report = self.get_newest_report('北部港区')
        north_port_max_avg_wind, north_port_max_zf_wind = self.max_wind(north_port_weather[4], "北部港区")

        # 获取最大平均风力、阵风风力
        max_avg_wind = np.max([zhoushan_max_avg_wind, shanghai_max_avg_wind, north_port_max_avg_wind])
        max_zf_wind = np.max([zhoushan_max_zf_wind, shanghai_max_zf_wind, north_port_max_zf_wind])

        min_njd = north_port_weather[3]
        wind_warning_level, njd_warning_level = self.get_warning(max_avg_wind, max_zf_wind, min_njd)

        zhoushan_change_bool, shanghai_change_bool, north_port_change_bool = False, False, False

        # 判断舟山气象报告是否变化
        if (zhoushan_newest_pub_date != zhoushan_weather[0]) | (zhoushan_newest_pub_clock != zhoushan_weather[1]):
            zhoushan_change_bool = True
            zs_report = '舟山气象台' + zhoushan_weather[0] + ' ' + zhoushan_weather[1] + '发布: ' + zhoushan_weather[2]
            zhoushan_insert_sql = "INSERT INTO weather_report(pubDate, pubClock, report, njd, srcLoc) " \
                                  "VALUES('%s', '%s', '%s', '%d', '%s')" % \
                                  (zhoushan_weather[0], zhoushan_weather[1], zs_report,
                                   north_port_weather[3], "舟山")
            cur.execute(zhoushan_insert_sql)
            print("舟山数据插入成功")

        # 判断上海气象是否变化
        if (shanghai_newest_pub_date != shanghai_weather[0]) | (shanghai_newest_pub_clock != shanghai_weather[1]):
            shanghai_change_bool = True
            sh_report = '上海气象台' + shanghai_weather[0] + ' ' + shanghai_weather[1] + '发布: ' + shanghai_weather[2]
            shanghai_insert_sql = "INSERT INTO weather_report(pubDate, pubClock, report, njd, srcLoc) " \
                                  "VALUES('%s', '%s', '%s', '%d', '%s')" % \
                                  (shanghai_weather[0], shanghai_weather[1], sh_report,
                                   north_port_weather[3], "上海")
            cur.execute(shanghai_insert_sql)
            print("上海数据插入成功")

        # 判断北部港区气象是否变化
        if (north_port_newest_pub_date != north_port_weather[0]) | (north_port_newest_pub_clock !=
                                                                        north_port_weather[1]):
            north_port_change_bool = True
            np_report = '北部港区' + north_port_weather[0] + ' ' + north_port_weather[1] + '发布: ' + north_port_weather[4]
            north_port_insert_sql = "INSERT INTO weather_report(pubDate, pubClock, report, njd, srcLoc) " \
                                    "VALUES('%s', '%s', '%s', '%d', '%s')" % \
                                    (north_port_weather[0], north_port_weather[1], np_report,
                                     north_port_weather[3], "北部港区")
            cur.execute(north_port_insert_sql)
            print("北部港区数据插入成功")

        if zhoushan_change_bool | shanghai_change_bool | north_port_change_bool:
            print("气象有变化，计算时间:%s" % time.strftime('%Y-%m-%d %H:%M:%S'))
            print("------------------------------------")
            pub_date = time.strftime("%Y-%m-%d")
            pub_clock = time.strftime("%H:%M:%S")
            pub_time = time.strftime("%Y-%m-%d %H:%M:%S")
            warning_insert_sql = """
                                 INSERT INTO weather_conf(pub_date, pub_clock, max_avg_wind, max_zf_wind, 
                                 min_njd, suggest_warn, suggest_njd_warn, pub_time) VALUE ('%s', '%s', '%d', '%d', 
                                 '%d', '%d', '%d', '%s')
                                 """ % (pub_date, pub_clock, max_avg_wind, max_zf_wind, min_njd, wind_warning_level,
                                        njd_warning_level, pub_time)
            cur.execute(warning_insert_sql)
        else:
            print("气象无变化，计算时间:%s" % time.strftime('%Y-%m-%d %H:%M:%S'))
            print("------------------------------------")

        conn.commit()
        cur.close()
        conn.close()

    # ---------------------------------------------------------------------------
    def get_wind_data(self, report_):
        """
        从气象报告中获取风力数据
        :param report_: 气象报告，类型：string
        :return: 平均风力等级，阵风风力等级
        """
        pattern = re.compile(u'今天.*?阵风(.*?)级', re.S)
        items_zf = re.findall(pattern, report_)

        pattern_to = re.compile(u'今天.*?到(.*?)级', re.S)
        pattern_east = re.compile(u'今天.*?东风(.*?)级', re.S)
        pattern_south = re.compile(u'今天.*?南风(.*?)级', re.S)
        pattern_west = re.compile(u'今天.*?西风(.*?)级', re.S)
        pattern_north = re.compile(u'今天.*?北风(.*?)级', re.S)

        items_to = re.findall(pattern_to, report_)
        for x in items_to:
            for xx in x:
                if xx.isdigit():
                    del items_to[items_to.index(x)]
                    break
        items_E = re.findall(pattern_east, report_)
        items_S = re.findall(pattern_south, report_)
        items_W = re.findall(pattern_west, report_)
        items_N = re.findall(pattern_north, report_)

        items_avg = items_E + items_S + items_W + items_N + items_to

        return items_avg, items_zf

    def get_wind_from_list(self, wind_data):
        """
        从风力数据列表中获取风力数值
        :param wind_data: 从气象报告中获取到的风力数据，类型：list
        :return: 返回最大风力数值，类型：int
        """
        wind_list = []
        for aw in wind_data:
            aw_list = aw.split('-')
            if len(aw_list) == 1:
                tmpStr = aw_list[0]
                for char in tmpStr:
                    if char.isdigit():
                        wind_list.append(int(char))
            else:
                wind_list.append(int(aw_list[1]))
        if len(wind_list) > 0:
            return np.max(wind_list)
        else:
            return 0

    def get_wind_njd_north_port_report(self, report):
        """
        从北部港区的气象报告中获取最大风力与最小能见度数据
        :param report: 北部港区气象报告，类型：string
        :return: 最大风力，最小能见度，类型：int
        """
        re_pattern = re.compile("最大风力达到(.*?)级，最小能见度达到(.*?)米")
        north_port_wind_njd = re.findall(re_pattern, report)[0]
        return int(north_port_wind_njd[0]), int(north_port_wind_njd[1])

    def get_warning(self, max_avg_wind, max_zf_wind, min_njd):
        """
        根据风力与能见度数据，获取对应的预警等级
        :param max_avg_wind: 最大平均风力，类型：int
        :param max_zf_wind: 最大阵风风力，类型：int
        :param min_njd: 最小能见度，类型：int
        :return: 风力预警与能见度预警等级，类型：int
        """
        # 从数据库中找出预警参数表格
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()
        paramter_sql = """
                       SELECT * FROM weather_parameter
                       """
        cur.execute(paramter_sql)
        paramter_df = pd.DataFrame(list(cur.fetchall()))
        paramter_df.columns = ['wp_id', 'warn_level', 'min_avg_wind', 'max_avg_wind', 'min_zf_wind',
                               'max_zf_wind', 'min_njd', 'max_njd', 'param_type', 'njd_time']
        wind_paramter_df = paramter_df[paramter_df['param_type'] == 1].loc[:, ['warn_level', 'min_avg_wind',
                                                                               'max_avg_wind', 'min_zf_wind',
                                                                               'max_zf_wind']]
        njd_paramter_df = paramter_df[paramter_df['param_type'] == 2].loc[:, ['warn_level', 'min_njd', 'max_njd']]

        # 获取风力预警等级
        wind_paramter_array = np.array(wind_paramter_df)
        avg_wind_warning_level = 0
        for wind_row in wind_paramter_array:
            if isInRange(max_avg_wind, wind_row[1], wind_row[2]):
                avg_wind_warning_level = wind_row[0]

        zf_wind_warning_level = 0
        for wind_row in wind_paramter_array:
            if isInRange(max_zf_wind, wind_row[3], wind_row[4]):
                zf_wind_warning_level = wind_row[0]

        # 获取能见度预警等级
        njd_paramter_array = np.array(njd_paramter_df)
        njd_warning_level = 0
        for njd_row in njd_paramter_array:
            if isInRange(min_njd, njd_row[1], njd_row[2]):
                njd_warning_level = njd_row[0]
        return int(np.max([avg_wind_warning_level, zf_wind_warning_level])), int(njd_warning_level)

    def max_wind(self, report, src_loc):
        """
        从气象报告中获取天气预警信息
        :param report: 气象报告数据
        :return:
        """
        # 获取气象报告中的最大风力数据
        if (src_loc == "舟山") | (src_loc == "上海"):
            avg_wind_list, zf_wind_list = self.get_wind_data(report)
            max_avg_wind = self.get_wind_from_list(avg_wind_list)
            if len(zf_wind_list) > 0:
                max_zf_wind = self.get_wind_from_list(zf_wind_list)
            else:
                max_zf_wind = 0
            return max_avg_wind, max_zf_wind

        # 获取北部港区气象报告
        if src_loc == "北部港区":
            max_avg_wind, min_njd = self.get_wind_njd_north_port_report(report)
            max_zf_wind = 0
            return max_avg_wind, max_zf_wind

        # zhoushan_newest_data = self.get_newest_report('舟山')
        # zhoushan_avg_wind_list, zhoushan_zf_wind_list = self.get_wind_data(zhoushan_newest_data[2])
        # max_zhoushan_avg_wind = self.get_wind_from_list(zhoushan_avg_wind_list)
        # max_zhoushan_zf_wind = self.get_wind_from_list(zhoushan_zf_wind_list)
        #
        # # 获取上海气象报告中的最大风力数据
        # shanghai_newest_data = self.get_newest_report('上海')
        # shanghai_avg_wind_list, shanghai_zf_wind_list = self.get_wind_data(shanghai_newest_data[2])
        # max_shanghai_avg_wind = self.get_wind_from_list(shanghai_avg_wind_list)
        # max_shanghai_zf_wind = self.get_wind_from_list(shanghai_zf_wind_list)
        #
        # # 获取北部港区天气预报中的最大风力与最小能见度数据
        # north_port_newest_data = self.get_newest_report('北部港区')
        # max_north_port_avg_wind, min_njd = self.get_wind_njd_north_port_report(north_port_newest_data[2])
        #
        # max_avg_wind = np.max([max_zhoushan_avg_wind, max_shanghai_avg_wind, max_north_port_avg_wind])
        # max_zf_wind = np.max([max_zhoushan_zf_wind, max_shanghai_zf_wind])
        #
        # wind_warning_level, njd_warning_level = self.get_warning(max_avg_wind, max_zf_wind, min_njd)
        # print(wind_warning_level, njd_warning_level)

if __name__ == "__main__":
    # -------------------------------------------------------
    # 天气报告
    while True:
        weather = Weather()
        weather.report_mysql()
        time.sleep(1800)
