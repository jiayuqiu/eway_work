# coding:utf-8

import numpy as np
import math
import pandas as pd

from pyspark import SparkContext
from pyspark import SparkConf

##########################################################
# 判断范围
def isInRange(num, min, max):
    if(num >= min and num <= max):
        return True
    else:
        return False

##########################################################
# 转换并找出需要的mmsi博贸的ais数据
def convert_bm_ais(ais):
    if(not "value" in ais):
        n_spl = ais.split('\n')[0]
        description_spt = n_spl.split('\t')
        mmsi_time = description_spt[1].split(' ')
        detail_data = description_spt[3].split('@')

        mmsi = str(mmsi_time[0])
        acq_time = str(mmsi_time[1])
        longitude = str(detail_data[4])
        latitude = str(detail_data[5])

        if (isInRange(float(longitude), 121.8212, 122.1495) and isInRange(float(latitude), 40.3966, 40.2066)):
            outStr = mmsi + "," + acq_time + "," + longitude + "," + latitude
            return outStr
        else:
            pass
    else:
        pass

##########################################################
# 转换并找出需要的mmsi博贸的ais数据
def find_ais(ais, ship_mmsi):
    each_ship = list(ais[1])
    length = len(each_ship)

    if(int(each_ship[0][0]) in ship_mmsi):
        outStr = ""
        for index in range(length):
            if(index != (length - 1)):
                tmpStr = str(each_ship[index][0]) + "," + str(each_ship[index][1]) + "," +\
                         str(each_ship[index][2]) + "," + str(each_ship[index][3]) + "\n"
                outStr = outStr + tmpStr
            else:
                tmpStr = str(each_ship[index][0]) + "," + str(each_ship[index][1]) + "," + \
                         str(each_ship[index][2]) + "," + str(each_ship[index][3])
                outStr = outStr + tmpStr
        return outStr
    else:
        pass

def find_ais2(ais, ship_mmsi):
    if(int(ais[0]) in ship_mmsi):
        print(int(ais[0]))
        tmpStr = str(ais[0]) + "," + str(ais[1]) + "," + \
                 str(ais[2]) + "," + str(ais[3])
        return tmpStr
    else:
        pass

def find_area_ais(ais, max_lon, min_lon, max_lat, min_lat):
    if((isInRange(num = float(ais[2]), min = min_lon, max = max_lon)) &
        isInRange(num = float(ais[3]), min = min_lat, max = max_lat)):
        tmpStr = str(ais[0]) + "," + str(ais[1]) + "," + \
                 str(ais[2]) + "," + str(ais[3])
        return tmpStr
    else:
        pass

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

#################################################################
# 根据小谢要求优化水域内停泊时间
def ais_poly(ais, poly):
    try:
        ais = [[float(x) for x in inner] for inner in ais[1]]
        ais.sort(key=lambda y: y[1])
        inside_stage_index = []
        tmp_index = []

        pre_point_bool = point_poly(ais[0][2], ais[0][3], poly)

        for i in range(1, len(ais) - 1):
            # print poly
            i_point_bool = pre_point_bool
            ip1_point_bool = point_poly(ais[i + 1][2], ais[i + 1][3], poly)

            if (i_point_bool or ip1_point_bool):
                tmp_index.append(i)
                tmp_index.append(i + 1)
                if (i == (len(ais) - 2)):
                    if (len(tmp_index) != 0):
                        inside_stage_index.append(tmp_index)
                        tmp_index = []
            elif (not i_point_bool or ip1_point_bool):
                if (len(tmp_index) != 0):
                    inside_stage_index.append(tmp_index)
                    tmp_index = []
            pre_point_bool = ip1_point_bool

        outStr = ""
        new_nav_num = len(inside_stage_index)
        i = 0
        for x in inside_stage_index:
            if(i != (new_nav_num - 2)):
                max_index = x[-1]
                min_index = x[0]

                anchor = poly[0][2]
                unique_id = ais[0][0]
                begin_time = ais[min_index][1]
                end_time = ais[max_index][1]
                interval = end_time - begin_time

                tmp_str = str(anchor) + "," + str(unique_id) + "," + str(begin_time) + "," + \
                          str(end_time) + "," + str(interval) + "\n"
                outStr = outStr + tmp_str
            else:
                max_index = x[-1]
                min_index = x[0]

                anchor = poly[0][2]
                unique_id = ais[0][0]
                begin_time = ais[min_index][1]
                end_time = ais[max_index][1]
                interval = end_time - begin_time

                tmp_str = str(anchor) + "," + str(unique_id) + "," + str(begin_time) + "," + \
                          str(end_time) + "," + str(interval)
                outStr = outStr + tmp_str
            i += 1
        return outStr
    except Exception as e:
        print(e)

if __name__ == "__main__":
    MASTER_HOME = "spark://192.168.1.121:7077"
    # MASTER_HOME = "local[*]"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("new.nav.find.error.mmsi.qiu")
    conf.set("spark.cores.max", "8")
    conf.set("spark.executor.memory", "2g")

    sc = SparkContext(conf=conf)

    # static = pd.read_csv("/home/qiu/Documents/unique_static_2016.csv")
    # ship700_pd = static[(static["shiptype"] >= 70) & (static["shiptype"] <= 79)]
    # ship700 = [int(x) for x in ship700_pd["mmsi"]]
    # print ship700
    # raw_input("=================")

    mmsi_list = [413700170, 413972950]
    print("finding 700ships......")
    # aisRDD_700 = sc.textFile("/home/qiu/xaa").map(lambda line: convert_bm_ais(line))\
    #                .filter(lambda line: line!=None)\
    #                .map(lambda line: line.split(","))\
    #                .map(lambda line: find_ais2(line, ship700)) \
    #                .filter(lambda group: group != None)

    aisRDD_700 = sc.textFile("hdfs://192.168.1.121:9000/qiu/xaa")\
                   .map(lambda line: line.split(","))\
                   .map(lambda line: find_area_ais(ais=line, max_lon=122.1495, min_lon=121.8212,
                                                   max_lat=40.3966, min_lat=40.2066))\
                   .filter(lambda line: line!=None)

    aisRDD_700.coalesce(1).saveAsTextFile("hdfs://192.168.1.121:9000/ais/yingkou_ais")
    # print(aisRDD_700.take(5))
    sc.stop()
    print("done!")

