#coding:utf-8

import math
import numpy as np
import pandas as pd
import pymysql


def coor_cog(lon1, lat1, lon2, lat2):
    """
    根据AIS数据两点，得到cog
    :param lon1:
    :param lat1:
    :param lon2:
    :param lat2:
    :return:
    """
    import math
    deta_lon = lon2 - lon1
    deta_lat = lat2 - lat1
    if (deta_lon >= 0.) & (deta_lat <= 0.):
        return 90 - (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
    elif (deta_lon >= 0.) & (deta_lat >= 0.):
        return 90 + (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
    elif (deta_lon <= 0.) & (deta_lat >= 0.):
        return 270 - (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
    elif (deta_lon <= 0.) & (deta_lat <= 0.):
        return 270 + (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))


def get_ship_static_mysql():
    print('loading ship static data...')
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    # 获取我们自己的船舶静态数据
    select_sql_eway = """
                             SELECT * FROM ship_static_data_eway
                             """
    cur.execute(select_sql_eway)
    ship_static_eway_array = np.array(list(cur.fetchall()))

    # 逐行去除英文船名中的空格字符
    for index in range(len(ship_static_eway_array)):
        if ship_static_eway_array[index, 11]:
            ship_static_eway_array[index, 11] = ship_static_eway_array[index, 11].replace(' ', '')

    ship_static_eway_df = pd.DataFrame(ship_static_eway_array)
    ship_static_eway_df.columns = ['mmsi', 'create_time', 'ship_type', 'imo', 'callsign',
                                   'ship_length', 'ship_width', 'pos_type', 'eta', 'draught', 'destination',
                                   'ship_name', 'standard_ship_english_name']

    # 获取洋山的船舶档案
    select_sql = """
                         SELECT * FROM ship_static_data
                         """
    cur.execute(select_sql)
    ship_static_array = np.array(list(cur.fetchall()))

    # 逐行去除英文船名中的空格字符
    for index in range(len(ship_static_array)):
        if ship_static_array[index, 4]:
            ship_static_array[index, 4] = ship_static_array[index, 4].replace(' ', '')
    ship_static_df = pd.DataFrame(ship_static_array)
    ship_static_df.columns = ['ssd_id', 'mmsi', 'imo', 'ship_chinese_name', 'ship_english_name', 'ship_callsign',
                              'sea_or_river', 'flag', 'sail_area', 'ship_port', 'ship_type', 'tonnage', 'dwt',
                              'monitor_rate', 'length', 'width', 'wind_resistance_level', 'height_above_water',
                              'standard_ship_english_name']
    ship_static_df = ship_static_df[~ship_static_df['mmsi'].isnull()]

    # 合并两个静态数据
    ship_static_merge = pd.merge(ship_static_eway_df, ship_static_df, how='outer',
                                 left_on='ship_name', right_on='ship_english_name').fillna(0)
    cur.close()
    conn.close()
    print('finish loading...')
    return ship_static_merge


def tb_pop_mysql(create_time, warn_type, warn_text, if_pop):
    """
    将预警数据导入表tb_warning_pop
    :param create_time: 预警创建时间，类型：string
    :param warn_type: 预警类型，类型：string
    :param warn_text: 预警内容，类型：string
    :param if_pop: 0 - 未弹窗
    :return:
    """
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    # 数据库插入语句
    insert_sql = """
                 INSERT INTO tb_warning_pop(create_time, warn_type, warn_text, if_pop) VALUE ('%s', '%s', '%s', '%d')
                 """ % (create_time, warn_type, warn_text, if_pop)
    cur.execute(insert_sql)
    conn.commit()
    cur.close()
    conn.close()


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

###################################################################################
def get_area(point0, point1, point2):
    """
    利用三点，求出三角形所对面积
    :param point0: 点0，类型list
    :param point1: 点1，类型list
    :param point2: 点2，类型list
    :return: 叉乘值，类型float
    """
    s = point0[0] * point1[1] + point2[0] * point0[1] + point1[0] * point2[1] - \
        point2[0] * point1[1] - point0[0] * point2[1] - point1[0] * point0[1]
    return s


def is_line_cross(str1, end1, str2, end2):
    """
    判断两条线段是否相交
    :param str1: 起始点1，类型list
    :param end1: 终止点1，类型list
    :param str2: 起始点2，类型list
    :param end2: 终止点2，类型list
    :return: T/F
    """
    s1 = get_area(str1, end1, str2)
    s2 = get_area(str1, end1, end2)
    s3 = get_area(str2, end2, str1)
    s4 = get_area(str2, end2, end1)
    if (s1 * s2 <= 0) & (s3 * s4 <= 0):
        return True
    else:
        return False

