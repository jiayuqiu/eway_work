#coding:utf-8

import math
# import numpy as np
# import pandas as pd

def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

#############################################################################
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

##########################################################
# 判断范围，区间左闭右闭
def isInRange(num, min, max):
    if (num >= min and num <= max):
        return True
    else:
        return False

##########################
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
        if "value" not in line:
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

    # 船讯网添加data_supplier字段、将sog字段单位转换为0.1节
    def cx_to_thr(self, line):
        if "longitude" not in line:
            detail_data = line.split("\n")[0].split(",")
            outStr = str(int(detail_data[0])) + "," + detail_data[1] + "," + detail_data[2] + "," + "248" + "," + \
                     "0" + "," + detail_data[4] + "," + detail_data[5] + "," + detail_data[6] + "," + \
                     detail_data[7] + "," + str(int(detail_data[8]) * 0.001 * 0.514444 * 10) + "," + \
                     detail_data[9] + "," + detail_data[10] + "," + \
                     detail_data[11] + "," + detail_data[12] + "," + detail_data[13] + "," + detail_data[14]
            return outStr
        else:
            pass

#########################################################
# 获得地球两点间距离
EARTH_RADIUS = 6378.137  # 地球半径，单位千米

def getRadian(x):
    return x * math.pi / 180.0

def getDist(lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
    lon1, lat1 = float(lon1), float(lat1)
    lon2, lat2 = float(lon2), float(lat2)
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                  math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000

    return dst

# 根据地球两点间距离求得平均速度
def getAvgSpeed(dst, detaTime):
    if(detaTime == 0):
        avgSpeed = dst / (detaTime + 1)
    else:
        avgSpeed = dst / detaTime
    return avgSpeed

##########################################################
# 获取areaID
def areaID(longitude, latitude, grade=0.1):
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
##########################################################
# 判断点是否在给定多边形内
def point_poly(pointLon, pointLat, polygon):
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

