# coding:utf-8

import time
import os
import re
import sys

from shanghai import shanghai_report
from zsqxj import get_sea_weather as zhoushan_report
from new_weather_source import get_data as get_njd

from input_reports import input_reports, input_weather_data, input_portWeather

from northPort import ysCrawler

from get_warning_main import get_wind_from_list, get_wind_data, \
    zs_cut_report, zs_wind_warning, get_warning, get_njd_warning

reload(sys)
sys.setdefaultencoding('utf8')

######################################################

# 将预警信息输出到文件中


def file_save(outStr, out_path):

    if not os.path.exists(out_path):
        print '创建文件，并放入数据'
        fileout = open(out_path, 'w')
        fileout.write(outStr.encode('utf-8'))
        fileout.close()
    else:
        print '文件存在，在文件最后追加内容'
        fileout = open(out_path, 'a')
        fileout.write(outStr.encode('utf-8'))
        fileout.close()

####################################################

# 找出气象报告发布时间中的，年、月、日、点钟数据


def sh_publish_time(sh_time_str):
    pattern = re.compile(u"日期:(.*?)年(.*?)月(.*?)日(.*?):", re.S)
    items = re.findall(pattern, sh_time_str)

    year, month, day = items[0][0], items[0][1], items[0][2]
    date = year+month+day
    clock = u""
    for x in items[0][3].split(' '):
        if x:
            clock = clock + x

    return date, clock


def zs_publish_time(zs_time_str):
    pattern = re.compile(u"(.*?)年(.*?)月(.*?)日(.*?)点钟", re.S)
    items = re.findall(pattern, zs_time_str)

    year, month, day, clock = items[0][0], items[0][1], items[0][2], items[0][3]

    if(len(day) == 1):
        day = u"0" + day
    if(len(month) == 1):
        month = u"0" + month

    date = year + month + day
    clock = clock
    return date, clock


if __name__ == '__main__':
    sh_url = u'http://www.smb.gov.cn/sh/tqyb/qxbg/index.html'
    zs_url = u'http://www.zs121.com.cn/TourismWebsite/City/CityWeather.aspx'
    # njd_rul = u"http://61.152.145.62/shMMshare/"
    date = int(time.strftime('%Y%m%d'))
    tmp_date = 0

    sh_report = ''
    sh_publish_report = ''
    zs_report = ''
    zs_publish_report = ''

    while True:
        print time.strftime('%H:%M:%S')
        now_clock_int = (int(time.strftime('%H'))/3)*3 + 2

        if(now_clock_int < 10):
            now_clock = u"0" + str(now_clock_int).encode("utf-8") + u"时"
        else:
            now_clock = str(now_clock_int).encode("utf-8") + u"时"

        tmp_publish_time_sh, tmp_sh_report = shanghai_report(sh_url)
        tmp_publish_time_zs, tmp_zs_report = zhoushan_report(zs_url)

        # 获取北部港区天气数据
        ysCrawler = ysCrawler()
        now_wind, now_njd = ysCrawler.ysPortCrawler()
        now_njd_warn = get_njd_warning(now_njd)

        # time_list_three_hour, wind_three_hour, njd_three_hour, \
        # time_list_five_date, time_list_five_day, wind_five_day = get_njd(njd_rul)
        #
        # njd_three_hour = [int(x) for x in njd_three_hour]
        # index = time_list_three_hour.index(now_clock)
        # now_njd = min(njd_three_hour[index:])
        # now_njd_warn = get_njd_warning(now_njd)
        # max_wind = max(wind_three_hour[index:])
        # now_wind = max_wind
        #
        # print "now_njd = %d" % now_njd
        # print "now_njd_warn = %d" % now_njd_warn

        # print wind_three_hour
        # print njd_three_hour

        if((tmp_sh_report == sh_report) and (tmp_zs_report == zs_report) and
               (tmp_publish_time_sh == sh_publish_report) and
               (tmp_publish_time_zs == zs_publish_report)):
            pass
        else:
            print 'report has changed!'
            if(tmp_sh_report != sh_report or sh_publish_report != tmp_publish_time_sh)\
                    or (len(sh_report) == 0):

                # 获取气象报告数据
                sh_report = tmp_sh_report
                sh_publish_report = tmp_publish_time_sh
                sh_putlish_date, sh_publish_clock = sh_publish_time(sh_publish_report)

                sh_report_all = sh_putlish_date + ';' + sh_publish_clock \
                                + ';' + tmp_sh_report + ';' + str(now_njd) + ';' +\
                                sh_url + ';' + "shanghai" + '\n'
                print tmp_sh_report

                # 获取上海气象报告的预警信息，风力信息

                sh_avg_wind_list, sh_zf_wind_list = get_wind_data(sh_report)
                sh_avg_wind = get_wind_from_list(sh_avg_wind_list)
                sh_zf_wind = get_wind_from_list(sh_zf_wind_list)
                sh_warning = get_warning(sh_avg_wind, sh_zf_wind)

                print "now_njd = %s" % now_njd
                print "sh_avg_wind, sh_zf_wind", sh_avg_wind, sh_zf_wind
                print sh_warning

                # 将最大平均风力，最大阵风锋利，最小能见度等天气信息传入数据库表格weatherData
                shInPutPubDate = sh_putlish_date
                shInPutPubClock = sh_publish_clock
                shSrcLoc = u"shanghai"

                if(len(sh_avg_wind) != 0):
                    shInPutMaxAvgWind = max(sh_avg_wind)
                else:
                    shInPutMaxAvgWind = 0

                if (len(sh_zf_wind) != 0):
                    shInPutMaxZfWind = max(sh_zf_wind)
                else:
                    shInPutMaxZfWind = 0

                shMinNJD = now_njd
                shInPutSuggestWarn = sh_warning

                # fileout_path = "/home/qiu/文档/tst/report_%s.csv" % int(time.strftime('%Y%m%d'))
                # file_save(outStr=sh_report_all, out_path=fileout_path)

                # --------------------------------------------------------------
                # 输入数据到数据库
                # input_reports(sh_report_all.encode('utf-8'))
                # input_weather_data(pubDatei=shInPutPubDate, pubClocki=shInPutPubClock,
                #                    srcLoci=shSrcLoc, maxAvgWindi=shInPutMaxAvgWind,
                #                    maxZfWindi=shInPutMaxZfWind, njdi=shMinNJD,
                #                    suggestWarni=sh_warning, suggestionNJDWarni=now_njd_warn)

            if (tmp_zs_report != zs_report or zs_publish_report != tmp_publish_time_zs)\
                    or (len(zs_report) == 0):
                zs_report = tmp_zs_report
                zs_publish_report = tmp_publish_time_zs
                zs_putlish_date, zs_publish_clock = zs_publish_time(zs_publish_report)

                tmp_str = zs_putlish_date + ';' + zs_publish_clock + ';' + \
                                tmp_zs_report + ';' + zs_url + ';' + "zhoushan" + '\n'

                zs_zf_today, zs_avg_today, zs_zf_tmr, zs_avg_tmr = \
                    zs_cut_report(tmp_str)

                zs_warning_today, zs_warning_tmr = zs_wind_warning(tmp_str)

                print "zs_report = %s" % zs_report
                print "zs_avg_today, zs_zf_today", zs_avg_today, zs_zf_today
                print "zs_warning_today = %s" % zs_warning_today
                print "now_njd = %s" % now_njd

                zs_report_all = zs_putlish_date + ';' + zs_publish_clock + ';' + \
                                tmp_zs_report + ';' + str(now_njd) + ';' + zs_url + ';' \
                                + "zhoushan" + '\n'

                # 将最大平均风力，最大阵风锋利，最小能见度等天气信息传入数据库表格weatherData

                zsInPutPubDate = zs_putlish_date
                zsInPutPubClock = zs_publish_clock
                zsSrcLoc = u"zhoushan"

                if (len(zs_avg_today) != 0):
                    zsInPutMaxAvgWind = max(zs_avg_today)
                else:
                    zsInPutMaxAvgWind = 0

                if (len(zs_zf_today) != 0):
                    zsInPutMaxZfWind = max(zs_zf_today)
                else:
                    zsInPutMaxZfWind = 0

                zsMinNJD = now_njd
                zsInPutSuggestWarn = zs_warning_today

                # fileout_path = "/home/qiu/文档/tst/report_%s.csv" % int(time.strftime('%Y%m%d'))
                # file_save(outStr=zs_report_all.encode("utf-8"), out_path=fileout_path)
                # # --------------------------------------------------------------------
                # # 输入数据到数据库
                # input_reports(zs_report_all.encode('utf-8'))
                # input_weather_data(pubDatei=zsInPutPubDate, pubClocki=zsInPutPubClock,
                #                    srcLoci=zsSrcLoc, maxAvgWindi=zsInPutMaxAvgWind,
                #                    maxZfWindi=zsInPutMaxZfWind, njdi=zsMinNJD,
                #                    suggestWarni=zsInPutSuggestWarn, suggestionNJDWarni=now_njd_warn)
                # inputTime = time.strftime("%Y%m%d-%H:%M")
                # input_portWeather(inputTime=inputTime, wind=now_wind, njd=now_njd)

        print "sleeping..."
        time.sleep(7200)
