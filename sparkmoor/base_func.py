#coding:utf-8

import math
# import numpy as np
# import pandas as pd

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
# 判断范围
def isInRange(num, min, max):
    if (num >= min and num <= max):
        return True
    else:
        return False

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
        try:
            return int(area_ID)
        except Exception as e:
            print(e)
            print(longitude, latitude)

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
            true_head = detail_data[7]
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
            true_head = detail_data[7]
            power = ""
            ext = ""
            extend = detail_data[1] + "&" + detail_data[3] + "&&&&"
            shiptype = int(detail_data[-1])

            if(isInRange(shiptype, 70., 79.)):
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
###########################################################
# D_DST: mm; PREISION: 1 - km, 1000 - m, 1000000 - mm;
# 判断停泊事件
def get_nav_spark(grouped_ship, PREISION = 1000000.0, D_DST = 282842.712474619, D_SPEED = 100.0):
    group_list = list(grouped_ship[1])
    group_num = len(group_list)
    AIS_array = list(group_list)
    each_ship = [[float(x) for x in inner] for inner in AIS_array]
    each_ship.sort(key=lambda y: y[1])

    nav_event = []
    if (group_num > 1):
        # print group_num
        print(each_ship[0][0])
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
                avgSpeed = (PREISION *
                            getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],each_ship[endIndex + 1][3])/
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
            print('eventsNum = ', num)
            i = 0
            temp_events = ''
            while(i < num):
                if(i == (num-1)):
                    temp = str(events[i][0]) +','+ str(events[i][1])+','+str(events[i][2]) +','+ str(events[i][3])+','+str(events[i][4]) +','+ str(events[i][5])+','+str(events[i][6]) +','+ str(events[i][7])
                    temp_events = temp_events + temp
                else:
                    temp = str(events[i][0]) +','+ str(events[i][1])+','+str(events[i][2]) +','+ str(events[i][3])+','+str(events[i][4]) +','+ str(events[i][5])+','+str(events[i][6]) +','+ str(events[i][7]) +'\n'
                    temp_events = temp_events + temp

                i=i+1

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
    AIS_array = list(grouped_ship[1])
    group_num = len(AIS_array)
    each_ship = [[float(x) for x in inner] for inner in AIS_array]
    each_ship.sort(key=lambda y: y[1])

    temp_nav = []
    # print 'group_num = ', group_num
    nav_event = []
    if (group_num > 1):
        # print group_num
        print(each_ship[0][0])
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
                avgSpeed = (PREISION *
                            getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2], each_ship[endIndex + 1][3])/
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
                            temp_nav = [
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
                            temp_nav = [
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
                        temp_nav = [
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
            print('eventsNum = ', num)
            i = 0
            temp_events = ''
            while(i < num):
                if(i == (num-1)):
                    temp = str(events[i][0]) +','+ str(events[i][1])+','+str(events[i][2]) +','+ str(events[i][3])+','+str(events[i][4]) +','+ str(events[i][5])+','+str(events[i][6]) +','+ str(events[i][7])
                    temp_events = temp_events + temp
                else:
                    temp = str(events[i][0]) +','+ str(events[i][1])+','+str(events[i][2]) +','+ str(events[i][3])+','+str(events[i][4]) +','+ str(events[i][5])+','+str(events[i][6]) +','+ str(events[i][7]) +'\n'
                    temp_events = temp_events + temp

                i=i+1

            return temp_events
        except:
            print('err events')
