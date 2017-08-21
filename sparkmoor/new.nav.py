# coding:utf-8

import numpy as np
import pandas as pd
import math
import time

from pyspark import SparkContext
from pyspark import SparkConf
from moor2017 import moor

# 获取航程
# 输入参数：lonList--经度列表；latList--纬度列表
# 返回值：航行距离，单位：千米
def getSailDst(lonList, latList):
    sailDst = 0
    lonLen = len(lonList)
    for index in range(lonLen - 1):
        tmpDst = getDist(lon1=lonList[index], lat1=latList[index],
                         lon2=lonList[index + 1], lat2=latList[index + 1])
        sailDst = sailDst + tmpDst
    return sailDst

#########################################################
# 格式转换类
class format_convert(object):
    # 获取areaID
    def areaID(self, longitude, latitude, grade=0.1):
        longLen = 360 / grade
        longBase = (longLen - 1) / 2
        laBase = (180 / grade - 1) / 2

        if (longitude < 0):
            longArea = longBase - math.floor(abs(longitude / grade))
        else:
            longArea = longBase + math.ceil(longitude / grade)

        if (latitude < 0):
            laArea = laBase + math.ceil(abs(latitude / grade))
        else:
            laArea = laBase - math.floor(latitude / grade)

        area_ID = longArea + (laArea * longLen)
        return int(area_ID)

    # 博贸转三阶段表
    def bm_to_thr(self, line):
        if (not "value" in line):
            n_spl = line.split('\n')[0]
            description_spt = n_spl.split('\t')
            mmsi_time = description_spt[1].split(' ')
            detail_data = description_spt[3].split('@')

            unique_ID = str(mmsi_time[0])
            acq_time = str(mmsi_time[1])
            target_type = "0"
            data_supplier = "246"
            data_source = "0"
            status = detail_data[0]
            longitude = float(detail_data[4])
            latitude = float(detail_data[5])
            areaID = self.areaID(longitude, latitude)
            speed = detail_data[2]
            conversion = "0.514444"
            cog = detail_data[6]
            true_head = str(int(detail_data[7]) * 100)
            power = ""
            ext = ""
            extend = detail_data[1] + "&" + detail_data[3] + "&&&&"

            outStr = unique_ID + "," + acq_time + "," + target_type + "," + data_supplier + "," + \
                     data_source + "," + status + "," + \
                     str(int(longitude * 1000000.)) + "," + str(int(latitude * 1000000.)) + "," + \
                     str(areaID) + "," + speed + "," + conversion + "," + cog + "," + \
                     true_head + "," + power + "," + ext + "," + extend

            return outStr
        else:
            pass

    # 从博贸的ais中找出700的船舶并转换为三阶段表格
    def bm_to_thr_700(self, line):
        if (not "value" in line):
            n_spl = line.split('\n')[0]
            description_spt = n_spl.split('\t')
            mmsi_time = description_spt[1].split(' ')
            detail_data = description_spt[3].split('@')

            unique_ID = str(int(mmsi_time[0]))
            acq_time = str(mmsi_time[1])
            target_type = "0"
            data_supplier = "246"
            data_source = "0"
            status = detail_data[0]
            longitude = float(detail_data[4])
            latitude = float(detail_data[5])
            areaID = self.areaID(longitude, latitude)
            speed = detail_data[2]
            conversion = "0.514444"
            cog = detail_data[6]
            true_head = detail_data[7]
            power = ""
            ext = ""
            extend = detail_data[1] + "&" + detail_data[3] + "&&&&"
            shiptype = int(detail_data[-1])

            if (isInRange(shiptype, 70., 89.)):
                outStr = unique_ID + "," + acq_time + "," + target_type + "," + data_supplier + "," + \
                         data_source + "," + status + "," + \
                         str(int(longitude * 1000000.)) + "," + str(int(latitude * 1000000.)) + "," + \
                         str(areaID) + "," + speed + "," + conversion + "," + cog + "," + \
                         true_head + "," + power + "," + ext + "," + extend

                return outStr
            else:
                pass
        else:
            pass

    # 船讯网添加data_supplier字段
    def cx_to_thr(self, line):
        if (not "longitude" in line):
            detail_data = line.split("\n")[0].split(",")
            outStr = str(int(detail_data[0])) + "," + detail_data[1] + "," + detail_data[2] + "," + "248" + "," + \
                     "0" + "," + detail_data[4] + "," + \
                     detail_data[5] + "," + detail_data[6] + "," + \
                     detail_data[7] + "," + detail_data[8] + "," + detail_data[9] + "," + detail_data[10] + "," + \
                     detail_data[11] + "," + detail_data[12] + "," + detail_data[13] + "," + detail_data[14]
            return outStr
        else:
            pass

##########################################################
# 判断范围
def isInRange(num, min, max):
    if (num >= min and num <= max):
        return True
    else:
        return False

#########################################################
# 将原始三阶段表格式ais数据，转换为停泊事件需要的输入格式
# 将每行ais数据进行格式转换
def convertAIS(ais):
    try:
        ais_s = ais.split(',')
        unique_id        = int(ais_s[0])                   # 船舶mmsi
        acquisition_time = int(ais_s[1])                   # utc时间
        status           = int(ais_s[5])                   # 船舶航行状态
        longitude        = float(ais_s[6]) / 1000000.0     # 原始数据为0.000001°，转换为1°
        latitude         = float(ais_s[7]) / 1000000.0
        zone_id          = int(ais_s[8])                   # 0.1°为一个格子
        sog              = int(ais_s[9])                   # 单位：毫米/秒，直接保留即可
        cog              = int(ais_s[11])                  # 单位：0.01°，直接保留即可
        true_head        = int(ais_s[12])                  # 单位：0.01°，直接保留即可
        rot              = int(ais_s[15].split('&')[0])    # 单位：0.01°，直接保留即可
        # 类型为list，[unique_id, acquisition_time, ...]
        ais_convert = [unique_id, acquisition_time, longitude, latitude, status,
                       zone_id, sog, cog, true_head, rot]
        # ais_convert = [unique_id, acquisition_time, longitude, latitude]
        return ais_convert
    except Exception as e:
        print(ais)
        print("error is : %s" % e)

#########################################################
# 获得地球两点间距离
EARTH_RADIUS = 6378.137  # 地球半径，单位千米

def getRadian(x):
    return x * math.pi / 180.0

def getDist(lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                  math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000
    return dst

###########################################################
# 判断停泊事件，字段不完整
def get_nav_spark(grouped_ship, PREISION = 1000000.0, D_DST = 282842.712474619, D_SPEED = 100.0):
    group_list = list(grouped_ship[1])
    group_num = len(group_list)
    AIS_array = list(group_list)
    each_ship = [[float(x) for x in inner] for inner in AIS_array]
    each_ship.sort(key=lambda y: y[1])

    nav_event = []

    if (group_num > 1):
        startIndex = 0
        while (startIndex < (group_num - 1)):  # find left, in 0 to n-2
            endIndex = startIndex
            maxLon = each_ship[startIndex][2]  # initialize longitude and latitude
            maxLat = each_ship[startIndex][3]
            minLon = each_ship[startIndex][2]
            minLat = each_ship[startIndex][3]
            while (endIndex < (group_num - 1)):
                # get average speed of endIndex and endIndex + 1
                try:
                    avgSpeed = (PREISION *
                                getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],each_ship[endIndex + 1][3])/
                                (each_ship[endIndex + 1][1] - each_ship[endIndex][1]))
                except:
                    endIndex = endIndex + 1
                    avgSpeed = (PREISION *
                                getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],
                                        each_ship[endIndex + 1][3]) /
                                (each_ship[endIndex + 1][1] - each_ship[endIndex][1]))

                if (avgSpeed <= D_SPEED):  # satisfy speed factor

                    if maxLon < each_ship[endIndex + 1][2]:  # find max and min of longitude and latitude
                        maxLon = each_ship[endIndex + 1][2]
                    if maxLat < each_ship[endIndex + 1][3]:
                        maxLat = each_ship[endIndex + 1][3]
                    if minLon > each_ship[endIndex + 1][2]:
                        minLon = each_ship[endIndex + 1][2]
                    if minLat > each_ship[endIndex + 1][3]:
                        minLat = each_ship[endIndex + 1][3]

                    maxDst = PREISION * getDist(maxLon, maxLat, minLon, minLat)
                    if (maxDst <= D_DST):  # satisfy distance factor
                        endIndex = endIndex + 1
                        if (endIndex == (group_num - 1)):
                            temp_nav =  [
                                        [
                                            each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                            each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                            each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                            (endIndex - startIndex + 1)
                                        ]
                                        ]

                            nav_event = nav_event + temp_nav
                            startIndex = endIndex
                            break
                    else:  # do not satisfy distance factor
                        if (endIndex > startIndex):
                            temp_nav =  [
                                        [
                                            each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                            each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                            each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                            (endIndex - startIndex + 1)
                                        ]
                                        ]

                            nav_event = nav_event + temp_nav
                            startIndex = endIndex
                            break
                        else:
                            startIndex = endIndex + 1
                            break

                else:  # do not satisfy speed factor
                    if (endIndex > startIndex):
                        temp_nav =  [
                                    [
                                        each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                        each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                        each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                        (endIndex - startIndex + 1)
                                    ]
                                    ]

                        nav_event = nav_event + temp_nav
                        startIndex = endIndex
                        break
                    else:
                        startIndex = endIndex + 1
                        break

    # print nav_event
    # print 'nav_event_num = ', len(nav_event)
    if(len(nav_event)!=0):
        try:
            events = list(nav_event)
            num = len(nav_event)
            # print('eventsNum = ', num)
            i = 0
            temp_events = ''
            while(i < num):
                if(i == (num-1)):
                    temp = str(events[i][0]) + ',' + str(events[i][1]) + ',' + str(events[i][2]) + ',' + str(
                        events[i][3]) + ',' + str(events[i][4]) + ',' + str(events[i][5]) + ',' + str(
                        events[i][6]) + ',' + str(events[i][7])
                    temp_events = temp_events + temp
                else:
                    temp = str(events[i][0]) + ',' + str(events[i][1]) + ',' + str(events[i][2]) + ',' + str(
                        events[i][3]) + ',' + str(events[i][4]) + ',' + str(events[i][5]) + ',' + str(
                        events[i][6]) + ',' + str(events[i][7]) + '\n'
                    temp_events = temp_events + temp
                i = i + 1
            return temp_events
        except:
            print('err events')

###########################################################
# D_DST: mm; PREISION: 1 - km, 1000 - m, 1000000 - mm;
# set: input ais format
# column 0: mmsi; column 1: acquisition_time; column 2: longitude;
# column 3: latitude; column 4: area_id; column 5: speed;
# column 6: cog; column 7: true_head; column 8: rot;
# 判断停泊事件
def new_nav_spark(grouped_ship, PREISION = 1000000.0, D_DST = 282842.712474619, D_SPEED = 100.0):
    # 获取一条船舶的AIS数据
    MMSI = list(grouped_ship[0])
    each_ship = list(grouped_ship[1])
    group_num = len(each_ship)
    each_ship = [[float(x) for x in inner] for inner in each_ship]
    each_ship.sort(key=lambda y: y[1])
    # 初始化该船舶的停泊事件
    nav_event = []
    # 判断船舶AIS数据的条数
    if (group_num > 1):  # 若大于1条，则可能存在停泊事件。找出停泊事件
        # 初始化停泊时间窗口的左窗口
        startIndex = 0
        # 初始化上一条停泊事件的结束时间与索引
        pre_time = 0
        pre_endIndex = 0
        # 判断停泊时间窗口开启，startIndex为窗口左端
        while (startIndex < (group_num - 1)):
            endIndex = startIndex
            # 初始化最大最小经纬度
            maxLon = each_ship[startIndex][2]
            maxLat = each_ship[startIndex][3]
            minLon = each_ship[startIndex][2]
            minLat = each_ship[startIndex][3]
            while (endIndex < (group_num - 1)):
                # get average speed of endIndex and endIndex + 1
                try:
                    avgSpeed = (PREISION * getDist(each_ship[endIndex][2], each_ship[endIndex][3],
                                        each_ship[endIndex + 1][2], each_ship[endIndex + 1][3]))/\
                               (each_ship[endIndex + 1][1] - each_ship[endIndex][1])
                except:
                    if(endIndex == (group_num - 2)):
                        startIndex = endIndex + 1
                        break
                    else:
                        endIndex = endIndex + 1
                        avgSpeed = (PREISION *
                                    getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],
                                            each_ship[endIndex + 1][3]) /
                                    (each_ship[endIndex + 1][1] - each_ship[endIndex][1]))

                if (avgSpeed <= D_SPEED):  # satisfy speed factor
                    # find max and min of longitude and latitude
                    if maxLon < each_ship[endIndex + 1][2]:
                        maxLon = each_ship[endIndex + 1][2]
                    if maxLat < each_ship[endIndex + 1][3]:
                        maxLat = each_ship[endIndex + 1][3]
                    if minLon > each_ship[endIndex + 1][2]:
                        minLon = each_ship[endIndex + 1][2]
                    if minLat > each_ship[endIndex + 1][3]:
                        minLat = each_ship[endIndex + 1][3]
                    # 获得最大最小经纬度形成的最大距离
                    maxDst = PREISION * getDist(maxLon, maxLat, minLon, minLat)
                    if (maxDst <= D_DST):  # satisfy distance factor
                        #################################
                        # get nav elements
                        endIndex = endIndex + 1

                        if (endIndex == (group_num - 1)):
                            begin_time = each_ship[startIndex][1]  # time of starting nav point
                            end_time = each_ship[endIndex][1]  # time of ending nav point

                            if (startIndex != 0):
                                apart_time = begin_time - pre_time  # get apart_time of now_nav and pre_nav
                            else:
                                apart_time = 0  # get apart_time of now_nav and pre_nav

                            mmsi = each_ship[endIndex][0]                    # ship mmsi
                            begin_lon = each_ship[startIndex][2] * PREISION  # longitude of starting nav point
                            begin_lat = each_ship[startIndex][3] * PREISION  # latitude of starting nav point
                            begin_hdg = each_ship[startIndex][8]             # true_head of starting nav point
                            begin_sog = each_ship[startIndex][6]             # sog of starting nav point
                            begin_cog = each_ship[startIndex][7]             # cog of starting nav point
                            end_lon = each_ship[endIndex][2] * PREISION      # longitude of ending nav point
                            end_lat = each_ship[endIndex][3] * PREISION      # latitude of ending nav point
                            end_hdg = each_ship[endIndex][8]                 # true_head of ending nav point
                            end_sog = each_ship[endIndex][6]                 # sog of ending nav point
                            end_cog = each_ship[endIndex][7]                 # cog of ending nav point
                            point_num = endIndex - startIndex + 1            # ais data nums between nav
                            zone_id = each_ship[endIndex][5]                 # zone_id of ending nav point
                            navistate = each_ship[startIndex + 1][4]         # status of start+1 nav point

                            if (point_num == 2):
                                avg_lon = (begin_lon + end_lon) / 2.0        # average longitude of nav
                                avg_lat = (begin_lat + end_lat) / 2.0        # average latitude of nav
                                avg_hdgMcog = abs(((begin_hdg - begin_cog) + (end_hdg - end_cog)) / 2.0)
                                avg_sog = (begin_sog + end_sog) / 2.0

                                var_hdg = np.var([begin_hdg, end_hdg])
                                var_cog = np.var([begin_cog, end_cog])
                                var_sog = np.var([begin_sog, end_sog])
                                var_rot = np.var([each_ship[startIndex][9], each_ship[endIndex][9]])

                                max_sog = max([begin_sog, end_sog])
                                maxSog_cog = [begin_cog, end_cog][np.argmax([begin_sog, end_sog])]
                                max_rot = max([each_ship[startIndex][9], each_ship[endIndex][9]])

                            elif (point_num > 2):
                                tmp_avg_lon, tmp_avg_lat, tmp_avg_hdgMcog, tmp_avg_sog = [], [], [], []
                                tmp_var_hdg, tmp_var_cog, tmp_var_sog, tmp_var_rot = [], [], [], []

                                for index in range(startIndex + 1, endIndex):
                                    tmp_avg_lon.append(each_ship[index][2])
                                    tmp_avg_lat.append(each_ship[index][3])
                                    tmp_avg_hdgMcog.append(abs(each_ship[index][8] - each_ship[index][7]))
                                    tmp_avg_sog.append(each_ship[index][6])

                                    tmp_var_hdg.append(each_ship[index][8])
                                    tmp_var_cog.append(each_ship[index][7])
                                    tmp_var_sog.append(each_ship[index][6])
                                    tmp_var_rot.append(each_ship[index][9])

                                avg_lon = np.mean(tmp_avg_lon)
                                avg_lat = np.mean(tmp_avg_lat)
                                avg_hdgMcog = np.mean(tmp_avg_hdgMcog)
                                avg_sog = np.mean(tmp_avg_sog)

                                var_hdg = np.var(tmp_var_hdg)
                                var_cog = np.var(tmp_var_cog)
                                var_sog = np.var(tmp_var_sog)
                                var_rot = np.var(tmp_var_rot)

                                max_sog = max(tmp_var_sog)
                                maxSog_cog = tmp_var_cog[np.argmax(tmp_var_sog)]
                                max_rot = max(tmp_var_rot)
                            pre_time = end_time      # reset pretime
                            pre_endIndex = endIndex  # reset pre_endIndex
                            #########################################################

                            temp_nav = [
                                        mmsi,
                                        begin_time, end_time, apart_time,
                                        begin_lon, begin_lat, begin_hdg, begin_sog, begin_cog,
                                        end_lon, end_lat, end_hdg, end_sog, end_cog,
                                        point_num,
                                        avg_lon, avg_lat, var_hdg, var_cog, avg_hdgMcog,
                                        avg_sog, var_sog, max_sog, maxSog_cog,
                                        max_rot, var_rot, zone_id, navistate
                                       ]
                            nav_event.append(temp_nav)
                            startIndex = endIndex
                            break
                    else:  # do not satisfy distance factor
                        if (endIndex > startIndex):

                            #################################
                            # get nav elements
                            begin_time = each_ship[startIndex][1]  # time of starting nav point
                            end_time = each_ship[endIndex][1]  # time of ending nav point

                            if (startIndex != 0):
                                apart_time = begin_time - pre_time  # get apart_time of now_nav and pre_nav
                            else:
                                apart_time = 0  # get apart_time of now_nav and pre_nav

                            mmsi = each_ship[endIndex][0]  # ship mmsi

                            begin_lon = each_ship[startIndex][2] * PREISION  # longitude of starting nav point
                            begin_lat = each_ship[startIndex][3] * PREISION  # latitude of starting nav point
                            begin_hdg = each_ship[startIndex][8]  # true_head of starting nav point
                            begin_sog = each_ship[startIndex][6]  # sog of starting nav point
                            begin_cog = each_ship[startIndex][7]  # cog of starting nav point
                            end_lon = each_ship[endIndex][2] * PREISION  # longitude of ending nav point
                            end_lat = each_ship[endIndex][3] * PREISION  # latitude of ending nav point
                            end_hdg = each_ship[endIndex][8]  # true_head of ending nav point
                            end_sog = each_ship[endIndex][6]  # sog of ending nav point
                            end_cog = each_ship[endIndex][7]  # cog of ending nav point
                            point_num = endIndex - startIndex + 1  # ais data nums between nav
                            zone_id = each_ship[endIndex][5]  # zone_id of ending nav point
                            navistate = each_ship[startIndex + 1][4]  # status of start+1 nav point

                            if (point_num == 2):
                                avg_lon = (begin_lon + end_lon) / 2.0  # average longitude of nav
                                avg_lat = (begin_lat + end_lat) / 2.0  # average latitude of nav
                                avg_hdgMcog = abs(((begin_hdg - begin_cog) + (end_hdg - end_cog)) / 2.0)
                                avg_sog = (begin_sog + end_sog) / 2.0

                                var_hdg = np.var([begin_hdg, end_hdg])
                                var_cog = np.var([begin_cog, end_cog])
                                var_sog = np.var([begin_sog, end_sog])
                                var_rot = np.var([each_ship[startIndex][9], each_ship[endIndex][9]])

                                max_sog = max([begin_sog, end_sog])
                                maxSog_cog = [begin_cog, end_cog][np.argmax([begin_sog, end_sog])]
                                max_rot = max([each_ship[startIndex][9], each_ship[endIndex][9]])

                            elif (point_num > 2):
                                tmp_avg_lon, tmp_avg_lat, tmp_avg_hdgMcog, tmp_avg_sog = [], [], [], []
                                tmp_var_hdg, tmp_var_cog, tmp_var_sog, tmp_var_rot = [], [], [], []

                                for index in range(startIndex + 1, endIndex):
                                    tmp_avg_lon.append(each_ship[index][2])
                                    tmp_avg_lat.append(each_ship[index][3])
                                    tmp_avg_hdgMcog.append(abs(each_ship[index][8] - each_ship[index][7]))
                                    tmp_avg_sog.append(each_ship[index][6])

                                    tmp_var_hdg.append(each_ship[index][8])
                                    tmp_var_cog.append(each_ship[index][7])
                                    tmp_var_sog.append(each_ship[index][6])
                                    tmp_var_rot.append(each_ship[index][9])

                                avg_lon = np.mean(tmp_avg_lon)
                                avg_lat = np.mean(tmp_avg_lat)
                                avg_hdgMcog = np.mean(tmp_avg_hdgMcog)
                                avg_sog = np.mean(tmp_avg_sog)

                                var_hdg = np.var(tmp_var_hdg)
                                var_cog = np.var(tmp_var_cog)
                                var_sog = np.var(tmp_var_sog)
                                var_rot = np.var(tmp_var_rot)

                                max_sog = max(tmp_var_sog)
                                # print("tmp_var_cog = ", tmp_var_cog)
                                maxSog_cog = tmp_var_cog[np.argmax(tmp_var_sog)]
                                max_rot = max(tmp_var_rot)
                            pre_time = end_time  # reset pretime
                            #########################################################
                            temp_nav = [
                                        mmsi,
                                        begin_time, end_time, apart_time,
                                        begin_lon, begin_lat, begin_hdg, begin_sog, begin_cog,
                                        end_lon, end_lat, end_hdg, end_sog, end_cog,
                                        point_num,
                                        avg_lon, avg_lat, var_hdg, var_cog, avg_hdgMcog,
                                        avg_sog, var_sog, max_sog, maxSog_cog,
                                        max_rot, var_rot, zone_id, navistate
                                       ]
                            nav_event.append(temp_nav)
                            startIndex = endIndex
                            break
                        else:
                            startIndex = endIndex + 1
                            break
                else:  # do not satisfy speed factor
                    if (endIndex > startIndex):
                        #################################
                        # get nav elements
                        begin_time = each_ship[startIndex][1]  # time of starting nav point
                        end_time = each_ship[endIndex][1]  # time of ending nav point

                        if (startIndex != 0):
                            apart_time = begin_time - pre_time  # get apart_time of now_nav and pre_nav
                        else:
                            apart_time = 0  # get apart_time of now_nav and pre_nav

                        mmsi = each_ship[endIndex][0]  # ship mmsi

                        begin_lon = each_ship[startIndex][2] * PREISION  # longitude of starting nav point
                        begin_lat = each_ship[startIndex][3] * PREISION  # latitude of starting nav point
                        begin_hdg = each_ship[startIndex][8]  # true_head of starting nav point
                        begin_sog = each_ship[startIndex][6]  # sog of starting nav point
                        begin_cog = each_ship[startIndex][7]  # cog of starting nav point
                        end_lon = each_ship[endIndex][2] * PREISION  # longitude of ending nav point
                        end_lat = each_ship[endIndex][3] * PREISION  # latitude of ending nav point
                        end_hdg = each_ship[endIndex][8]  # true_head of ending nav point
                        end_sog = each_ship[endIndex][6]  # sog of ending nav point
                        end_cog = each_ship[endIndex][7]  # cog of ending nav point
                        point_num = endIndex - startIndex + 1  # ais data nums between nav
                        zone_id = each_ship[endIndex][5]  # zone_id of ending nav point
                        navistate = each_ship[startIndex + 1][4]  # status of start+1 nav point

                        if (point_num == 2):
                            avg_lon = (begin_lon + end_lon) / 2.0  # average longitude of nav
                            avg_lat = (begin_lat + end_lat) / 2.0  # average latitude of nav
                            avg_hdgMcog = abs(((begin_hdg - begin_cog) + (end_hdg - end_cog)) / 2.0)
                            avg_sog = (begin_sog + end_sog) / 2.0

                            var_hdg = np.var([begin_hdg, end_hdg])
                            var_cog = np.var([begin_cog, end_cog])
                            var_sog = np.var([begin_sog, end_sog])
                            var_rot = np.var([each_ship[startIndex][9], each_ship[endIndex][9]])

                            max_sog = max([begin_sog, end_sog])
                            maxSog_cog = [begin_cog, end_cog][np.argmax([begin_sog, end_sog])]
                            max_rot = max([each_ship[startIndex][9], each_ship[endIndex][9]])

                        elif (point_num > 2):
                            tmp_avg_lon, tmp_avg_lat, tmp_avg_hdgMcog, tmp_avg_sog = [], [], [], []
                            tmp_var_hdg, tmp_var_cog, tmp_var_sog, tmp_var_rot = [], [], [], []

                            for index in range(startIndex + 1, endIndex):
                                tmp_avg_lon.append(each_ship[index][2])
                                tmp_avg_lat.append(each_ship[index][3])
                                tmp_avg_hdgMcog.append(abs(each_ship[index][8] - each_ship[index][7]))
                                tmp_avg_sog.append(each_ship[index][6])

                                tmp_var_hdg.append(each_ship[index][8])
                                tmp_var_cog.append(each_ship[index][7])
                                tmp_var_sog.append(each_ship[index][6])
                                tmp_var_rot.append(each_ship[index][9])

                            avg_lon = np.mean(tmp_avg_lon)
                            avg_lat = np.mean(tmp_avg_lat)
                            avg_hdgMcog = np.mean(tmp_avg_hdgMcog)
                            avg_sog = np.mean(tmp_avg_sog)

                            var_hdg = np.var(tmp_var_hdg)
                            var_cog = np.var(tmp_var_cog)
                            var_sog = np.var(tmp_var_sog)
                            var_rot = np.var(tmp_var_rot)

                            max_sog = max(tmp_var_sog)
                            # print("tmp_var_cog = ", tmp_var_cog)
                            maxSog_cog = tmp_var_cog[np.argmax(tmp_var_sog)]
                            max_rot = max(tmp_var_rot)
                        pre_time = end_time  # reset pretime
                        ##########################################################
                        temp_nav = [
                                    mmsi,
                                    begin_time, end_time, apart_time,
                                    begin_lon, begin_lat, begin_hdg, begin_sog, begin_cog,
                                    end_lon, end_lat, end_hdg, end_sog, end_cog,
                                    point_num,
                                    avg_lon, avg_lat, var_hdg, var_cog, avg_hdgMcog,
                                    avg_sog, var_sog, max_sog, maxSog_cog,
                                    max_rot, var_rot, zone_id, navistate
                                   ]
                        nav_event.append(temp_nav)
                        startIndex = endIndex
                        break
                    else:
                        startIndex = endIndex + 1
                        break

    if(len(nav_event)!=0):
        try:
            events = nav_event
            row_num = len(nav_event)  # 获取停泊事件的行数
            col_num = len(nav_event[0])  # 获取停泊事件的列数
            temp_events = ""  # 初始化该船舶的所有停泊事件
            for i in range(0, row_num):  # 循环停泊事件列表的每一列
                if(i == (row_num-1)):
                    for j in range(0, col_num):  # 循环获取每一列的数据
                        if(j != (col_num-1)):  # 若不是最后一列，用","分割
                            temp = str(events[i][j]) + ","
                        else:  # 若是最后一行的最后一列，只需要加入数据即可
                            temp = str(events[i][j])
                        # print("temp = ", temp)
                        temp_events = temp_events + temp
                else:
                    for j in range(0, col_num):  # 循环获取每一列的数据
                        if (j != (col_num - 1)):  # 若不是最后一列，用","分割
                            temp = str(events[i][j]) + ","
                        else:  # 若不是最后一行的最后一列，只需要加入"\n"
                            temp = str(events[i][j]) + "\n"
                        # print("temp = ", temp)
                        temp_events = temp_events + temp

            return temp_events

        except Exception as e:
            print('err events')
            print('There is error when outputing nav, error is : %s' % e)

########################################################

if __name__ == "__main__":
    # MASTER_HOME = "spark://192.168.1.121:7077"
    MASTER_HOME = "local[*]"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("new.nav.qiu")
    # conf.set("spark.cores.max", "24")
    # conf.set("spark.executor.memory", "6g")

    sc = SparkContext(conf=conf)
    fc = format_convert()
    mr = moor()
    # 读取码头数据
    # 获取"多边形港口"信息
    portNameList = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                    "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                    "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                    "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08",
                    "rotterdam04", "newjersey03", "newyork02", "busan03", "singapore03",
                    "hongkong03"]
    polyPortDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/Asia_anchores.csv")
    polyPortDF = polyPortDF[polyPortDF["anchores_id"].isin(portNameList)]
    polyPortDF.columns = ["longitude", "latitude", "portName"]

    # 获取"点港口"信息
    pointPortDF = pd.read_csv("/home/qiu/Documents/sparkTestResult/allPortTest/"
                              "part-00000", header=None, error_bad_lines=False)
    pointPortDF.columns = ["portName", "portLon", "portLat", "portAreaID", "closePortAreaID"]
    # pointPortDF = pointPortDF.iloc[:, [30, 26, 1]]
    # pointPortDF.columns = ["longitude", "latitude", "portName"]

    # 读取静态数据
    staticDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/staticData2016/"
                           "static_ships_20160901")
    staticDF.columns = ["shipid", "time", "shiptype", "length", "width", "left",
                        "trail", "imo", "name", "callsign", "draught", "destination", "eta"]

    # 读取ais数据，并转换为停泊事件模型需要的输入格式
    # testRDD = sc.textFile("hdfs://192.168.1.204:9000/qiu/ais/ships_20150901.csv")
    # shipsAISRDD = sc.textFile("hdfs://192.168.1.121:9000/qiu/201409.tsv")\
    #                 .map(lambda line: fc.bm_to_thr(line))\
    #                 .filter(lambda line: line != None)\
    #                 .map(lambda line: convertAIS(line)) \
    #                 .filter(lambda line: line != None)
    shipsAISRDD = sc.textFile("/home/qiu/Documents/data/ships_20160901.csv")\
                    .map(lambda line: fc.cx_to_thr(line))\
                    .filter(lambda line: line != None)\
                    .map(lambda line: line.split(","))
    startTime = time.time()
    navsRDD = shipsAISRDD.groupBy(lambda v: v[0])\
                         .map(lambda group: mr.moorShip(shipAIS=group, staticDF=staticDF)) \
                         .filter(lambda group: group!=None)\
                         .saveAsTextFile("/home/qiu/Documents/sparkTestResult/portTest")
    endTime = time.time()
    print("spark uses %f" % (endTime - startTime))

    # moorPortRDD = navsRDD.map(lambda group: mr.moorPort(moorRDD=group, pointPortDF=pointPortDF,
    #                                                     polyPortDF=polyPortDF))\
    #                      .saveAsTextFile("/home/qiu/Documents/sparkTestResult/moorPortTest")
    # print navsRDD[:5]
    print("hellow world")
    sc.stop()
