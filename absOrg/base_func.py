#coding:utf-8

import math
from pandas import Series, DataFrame
import numpy as np
import pandas as pd

###############################################
# 注意请在最后加上斜杠
def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

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
##########################################################
# 判断点是否在给定多边形内，返回T或F
def point_poly(pointLon, pointLat, polygon):
    polygon = np.array(polygon)
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while (i < cor):
        if ((((polygon[i, 1] < pointLat) & (polygon[j, 1] >= pointLat))
                 | ((polygon[j, 1] < pointLat) & (polygon[i, 1] >= pointLat)))
                & ((polygon[i, 0] <= pointLon) | (polygon[j, 0] <= pointLon))):
            a = (polygon[i, 0] +
                 (pointLat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if (a < pointLon):
                inside = not inside
        j = i
        i = i + 1

    return inside

# 判断点是否在给定多边形内，返回港口名称
def point_poly_anchor(pointLon, pointLat, polygon):
    polygon = np.array(polygon)
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while (i < cor):
        if ((((polygon[i, 1] < pointLat) & (polygon[j, 1] >= pointLat))
                 | ((polygon[j, 1] < pointLat) & (polygon[i, 1] >= pointLat)))
                & ((polygon[i, 0] <= pointLon) | (polygon[j, 0] <= pointLon))):
            a = (polygon[i, 0] +
                 (pointLat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if (a < pointLon):
                inside = not inside
        j = i
        i = i + 1

    if(inside):
        return polygon[1][2]
    else:
        return None

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

            unique_ID = str(int(mmsi_time[0]))
            acq_time = str(int(mmsi_time[1]))
            target_type = "0"
            data_supplier = "246"
            data_source = "0"
            status = detail_data[0]
            longitude = float(detail_data[4])
            latitude = float(detail_data[5])
            areaID = self.areaID(longitude, latitude)
            speed = detail_data[2]
            conversion = "0.514444"
            cog = str(int(detail_data[6]) * 10)
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

###########################################################
# 判断停泊事件
def get_nav(grouped_ship, PREISION = 1000000.0, D_DST = 282842.712474619, D_SPEED = 100.0):
    nav_event = []  # initialize nav_events
    for group in grouped_ship:
        group_list = list(group)
        group_num = group_list[1]['unique_ID'].count()

        # mmsi-0 time-1 target_type-2 data_supplier-3 data_source-4 status -5 longitude-6 latitude -7 area_id-8
        # speed-9 conversion-10 cog-11 true_head-12 power-13 ext-14 extend-15
        each_ship = np.array(group_list[1].iloc[0:group_num])

        if (group_num > 1):
            # print group_num
            print each_ship[0][0]
            startIndex = 0
            endIndex = 0
            while (startIndex < (group_num - 1)):  # find left, in 0 to n-2
                endIndex = startIndex
                maxLon = each_ship[startIndex][2]  # initialize longitude and latitude
                maxLat = each_ship[startIndex][3]
                minLon = each_ship[startIndex][2]
                minLat = each_ship[startIndex][3]
                while (endIndex < (group_num - 1)):
                    # get average speed of endIndex and endIndex + 1
                    avgSpeed = (
                    PREISION * getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],
                                       each_ship[endIndex + 1][3])
                    / (each_ship[endIndex + 1][1] - each_ship[endIndex][1]))

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
                                temp_nav = [
                                            each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                            each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                            each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                            (endIndex - startIndex + 1)
                                            ]

                                nav_event.append(temp_nav)
                                startIndex = endIndex
                                break
                        else:  # do not satisfy distance factor
                            if (endIndex > startIndex):
                                temp_nav = [
                                            each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                            each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                            each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                            (endIndex - startIndex + 1)
                                            ]

                                nav_event.append(temp_nav)
                                startIndex = endIndex
                                break
                            else:
                                startIndex = endIndex + 1
                                break

                    else:  # do not satisfy speed factor
                        if (endIndex > startIndex):

                            temp_nav = [
                                        each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                        each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                        each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                        (endIndex - startIndex + 1)
                                        ]

                            nav_event.append(temp_nav)
                            startIndex = endIndex
                            break
                        else:
                            startIndex = endIndex + 1
                            break

    return nav_event