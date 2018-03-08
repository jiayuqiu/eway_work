# coding:utf-8

import re
import pandas as pd
import numpy as np
import MySQLdb


###############################################

# 判断一个数值时候在一个区间内，左右都为闭区间，返回T或F


def isInRange(num, min, max):
    if(num >= min and num <= max):
        return True
    else:
        return False

###############################################

# 从列表中获取风力等级，返回一个列表，元素为元祖


def get_wind_from_list(tmp_list):
    wind_list = []
    for aw in tmp_list:
        aw_list = aw.split('-')
        if(len(aw_list) == 1):
            tmpStr = aw_list[0]
            for char in tmpStr:
                if char.isdigit():
                    wind_list.append(int(char))
        else:
            wind_list.append(int(aw_list[1]))

    return wind_list

# 根据能见度数据，判断能见度预警等级


def get_njd_warning(tmp_njd):
    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        # print '启动成功'
    except:
        print "启动失败"

    para_get = """
        SELECT * FROM WARN_PAR
        """

    cur.execute(para_get)
    tmp = cur.fetchall()

    blue_min_njd = tmp[0][5]
    blue_max_njd = tmp[0][6]
    yellow_min_njd = tmp[1][5]
    yellow_max_njd = tmp[1][6]-1
    orange_min_njd = tmp[2][5]
    orange_max_njd = tmp[2][6]-1
    red_min_njd = tmp[3][5]
    red_max_njd = tmp[3][6]-1

    njd_level = 0

    if(isInRange(tmp_njd, blue_min_njd, blue_max_njd)):
        njd_level = njd_level + 1
    if (isInRange(tmp_njd, yellow_min_njd, yellow_max_njd)):
        njd_level = njd_level + 2
    if (isInRange(tmp_njd, orange_min_njd, orange_max_njd)):
        njd_level = njd_level + 3
    if (isInRange(tmp_njd, red_min_njd, red_max_njd)):
        njd_level = njd_level + 4

    return njd_level


# 根据风力数据列表，判断预警等级


def get_warning(tmp_list_avg, tmp_list_zf):

    # 从数据库中找出预警参数表格

    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        # print '启动成功'
    except:
        print "启动失败"

    para_get = """
               SELECT * FROM WARN_PAR
               """
    cur.execute(para_get)
    tmp = cur.fetchall()

    blue_avg_min = tmp[0][1]+1
    blue_avg_max = tmp[0][2]
    blue_zf_min = tmp[0][3]
    # print "blue_zf_min = ", blue_zf_min
    blue_zf_max = tmp[0][4]
    # print "blue_zf_max = ", blue_zf_max

    yellow_avg_min = tmp[1][1]
    yellow_avg_max = tmp[1][2]
    yellow_zf_min = tmp[1][3]
    # print "yellow_zf_min = ", yellow_zf_min
    yellow_zf_max = tmp[1][4]
    # print "yellow_zf_max = ", yellow_zf_max

    orange_avg_min = tmp[2][1]
    orange_avg_max = tmp[2][2]
    orange_zf_min = tmp[2][3]
    orange_zf_max = tmp[2][4]

    red_avg_min = tmp[3][1]
    red_zf_min = tmp[3][3]

    # 判断平均风速与阵风风速数列内是否有数据

    if(len(tmp_list_avg) != 0):
        tmp_avg = int(max(tmp_list_avg))
    else:
        tmp_avg = 0

    if(len(tmp_list_zf) != 0):
        tmp_zf = int(max(tmp_list_zf))
    else:
        tmp_zf = 0

    warning_level = 0


    # 判断预警等级

    if(isInRange(tmp_avg, blue_avg_min, blue_avg_max) and
       isInRange(tmp_zf, blue_zf_min, blue_zf_max)):
        warning_level = 1
    elif(isInRange(tmp_avg, blue_avg_min, blue_avg_max) or
       isInRange(tmp_zf, blue_zf_min, blue_zf_max)):
        warning_level = 1

    if (isInRange(tmp_avg, yellow_avg_min, yellow_avg_max) and
            isInRange(tmp_zf, yellow_zf_min, yellow_zf_max)):
        warning_level = 2
    elif(isInRange(tmp_avg, yellow_avg_min, yellow_avg_max) or
            isInRange(tmp_zf, yellow_zf_min, yellow_zf_max)):
        warning_level = 2

    if (isInRange(tmp_avg, orange_avg_min, orange_avg_max) and
            isInRange(tmp_zf, orange_zf_min, orange_zf_max)):
        warning_level = 3
    elif(isInRange(tmp_avg, orange_avg_min, orange_avg_max) or
            isInRange(tmp_zf, orange_zf_min, orange_zf_max)):
        warning_level = 3

    if ((tmp_avg >= red_avg_min) or \
              (tmp_zf >= red_zf_min)):
        warning_level = 4

    return warning_level

# 从文字报告字段中，取出风力数值数据


def get_wind_data(report_):
    pattern = re.compile(u'阵风(.*?)级', re.S)
    items_zf = re.findall(pattern, report_)

    pattern_to = re.compile(u'到(.*?)级', re.S)
    pattern_east = re.compile(u'东风(.*?)级', re.S)
    pattern_south = re.compile(u'南风(.*?)级', re.S)
    pattern_west = re.compile(u'西风(.*?)级', re.S)
    pattern_north = re.compile(u'北风(.*?)级', re.S)

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

############################################################

# 针对舟山气象报告中的风力数据获取预警数据
# 1.首先获取今天与明天的舟山气象报告
# 2.获取到气象报告后，利用正则表达将风力等级数据抽取出来
# 3.在获取到风力等级后，判断风力预警


def zs_cut_report(report_):
    reg_today = u'今天(.*?)。'
    pattern_today = re.compile(reg_today, re.S)
    items_today = re.findall(pattern_today, report_)
    if(len(items_today) != 0):
        report_today = items_today[0]
        items_avg_today, items_zf_today = get_wind_data(report_today)

        zf_today = get_wind_from_list(items_zf_today)
        avg_today = get_wind_from_list(items_avg_today)
    else:
        zf_today = [0]
        avg_today = [0]

    reg_tmr = u'明天(.*?)。'
    pattern_tmr = re.compile(reg_tmr, re.S)
    items_tmr = re.findall(pattern_tmr, report_)
    if(len(items_tmr) != 0):
        report_tmr = items_tmr[0]
        items_avg_tmr, items_zf_tmr = get_wind_data(report_tmr)

        zf_tmr    = get_wind_from_list(items_zf_tmr)
        avg_tmr   = get_wind_from_list(items_avg_tmr)
    else:
        zf_tmr = [0]
        avg_tmr = [0]

    if (len(zf_today) == 0):
        zf_today = [0]
    if (len(avg_today) == 0):
        avg_today = [0]
    if (len(zf_tmr) == 0):
        zf_tmr = [0]
    if (len(avg_tmr) == 0):
        avg_tmr = [0]


    return zf_today, avg_today, zf_tmr, avg_tmr


def zs_wind_warning(zf_report):
    zs_zf_today, zs_avg_today, zs_zf_tmr, zs_avg_tmr = \
        zs_cut_report(zf_report)

    if (max(zs_zf_today) != 0 or max(zs_avg_today) != 0):
        zs_today_warning_level = get_warning(zs_avg_today, zs_zf_today)
    else:
        zs_today_warning_level = -1

    if (max(zs_zf_tmr) != 0 or max(zs_avg_tmr) != 0):
        zs_tmr_warning_level = get_warning(zs_avg_tmr, zs_zf_tmr)
    else:
        zs_tmr_warning_level = -1

    # zs_warning_today, zs_warning_tmr = zs_wind_warning(zs_zf_today, zs_avg_today,
    #                                                    zs_zf_tmr, zs_avg_tmr)



    return zs_today_warning_level, zs_tmr_warning_level

############################################################

# 针对上海气象报告中的风力数据获取预警数据
# 1.获取到气象报告后，利用正则表达将风力等级数据抽取出来
# 2.在获取到风力等级后，判断风力预警
# 返回值：今日上海风力预警等级 0：无预警;1：蓝色;2：黄色;3：橙色;4：红色


def get_sh_warning(sh_report):
    sh_avg_wind_list, sh_zf_wind_list = get_wind_data(sh_report)
    sh_avg_wind = get_wind_from_list(sh_avg_wind_list)
    sh_zf_wind = get_wind_from_list(sh_zf_wind_list)

    warning = get_warning(sh_avg_wind, sh_zf_wind)

    return warning


############################################################
if __name__ == '__main__':
    sh_url = u'http://www.smb.gov.cn/sh/tqyb/qxbg/index.html' + '\n'
    zs_url = u'http://www.zs121.com.cn/TourismWebsite/City/CityWeather.aspx' + '\n'

    file = open('/home/qiu/文档/tst/report_20161110.csv')
    for line in file:
        tmp_list = unicode(line, 'utf-8').split('\n')[0].split(';')
        if(tmp_list[4] == u"shanghai"):
            print tmp_list[2]
            warning = get_sh_warning(tmp_list[2])
            print warning

        if (tmp_list[4] == u"zhoushan"):
            print tmp_list[2]

            zs_warning_today, zs_warning_tmr = zs_wind_warning(tmp_list[2])

            print zs_warning_today
        #     print "Today, " + zs_warning_today
        #     print "Tomorrow, " + zs_warning_tmr

    file.close()
