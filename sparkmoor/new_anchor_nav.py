# coding:utf-8

import numpy as np
import math
import sys
import pandas as pd

from pyspark import SparkContext
from pyspark import SparkConf

# sys.setrecursionlimit(1000000)

##########################################################
# 判断范围
def isInRange(num, min, max):
    if(num >= min and num <= max):
        return True
    else:
        return False

##########################################################
# 转换博贸的ais数据
def convert_ais(ais):
    if(not "value" in ais):
        n_spl = ais.split('\n')[0]
        description_spt = n_spl.split('\t')
        mmsi_time = description_spt[1].split(' ')
        detail_data = description_spt[3].split('@')

        mmsi = str(mmsi_time[0])
        acq_time = str(mmsi_time[1])
        longitude = str(detail_data[4])
        latitude = str(detail_data[5])
        shiptype = int(detail_data[-1])

        if (isInRange(float(longitude), 106.0, 126.0) and isInRange(float(latitude), 17.5, 41.0) and
                shiptype == 70):
            outStr = mmsi + "," + acq_time + "," + longitude + "," + latitude
            return outStr
        else:
            pass
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
        inside_stage_index = []
        tmp_index = []

        ais = [[float(x) for x in inner] for inner in ais]
        ais.sort(key=lambda y: y[1])
        pre_point_bool = point_poly(ais[0][2], ais[0][3], poly)
        # print len(ais)
        if (len(ais) > 2):
            for i in range(1, len(ais) - 1):
                # print poly
                i_point_bool = pre_point_bool
                ip1_point_bool = point_poly(ais[i + 1][2], ais[i + 1][3], poly)

                if (i_point_bool or ip1_point_bool):
                    if (i == 1):
                        tmp_index.append(i - 1)
                    tmp_index.append(i)
                    if (i == (len(ais) - 2)):
                        if (len(tmp_index) != 0):
                            tmp_index.append(i + 1)
                            inside_stage_index.append(tmp_index)
                            tmp_index = []
                elif (not i_point_bool or ip1_point_bool):
                    if (len(tmp_index) != 0):
                        tmp_index.append(i)
                        inside_stage_index.append(tmp_index)
                        tmp_index = []
                pre_point_bool = ip1_point_bool
        elif (len(ais) == 2):
            if(pre_point_bool):
                tmp_index.append(0)
                tmp_index.append(1)
                inside_stage_index.append(tmp_index)
                tmp_index = []

        outStr = ""
        # print inside_stage_index
        for index, x in enumerate(inside_stage_index):
            max_index = x[-1]
            min_index = x[0]

            anchor = poly[0][2]
            unique_id = ais[0][0]
            begin_time = ais[min_index][1]
            end_time = ais[max_index][1]
            interval = end_time - begin_time

            length = len(inside_stage_index)
            if(index == (length - 1)):
                tmp_str = str(anchor) + "," + str(unique_id) + "," + str(begin_time) + "," + \
                          str(end_time) + "," + str(interval)
            else:
                tmp_str = str(anchor) + "," + str(unique_id) + "," + str(begin_time) + "," + \
                          str(end_time) + "," + str(interval) + "\n"

            outStr = outStr + tmp_str

        return outStr
    except Exception as e:
        print(e)

##########################################################
# 减少map的停泊事件判断
def ais_Gpoly(ais, poly_list):
    each_ship = list(ais[1])
    # polyDF = pd.DataFrame(polyDF)
    # grouped_anchor = polyDF.groupby("anchores_id")
    # print poly_list
    polyDF = pd.DataFrame(poly_list)
    polyDF.columns = ["longitude", "latitude", "anchores_id"]
    # print polyDF
    grouped_anchor = polyDF.groupby("anchores_id")
    outList = []
    for group_anchor in grouped_anchor:
        group_list = list(group_anchor)
        group_num = len(group_list[1])

        water_list = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                      "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                      "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                      "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08"]

        # mmsi-0 time-1 target_type-2 data_supplier-3 data_source-4 status -5 longitude-6 latitude -7 area_id-8
        # speed-9 conversion-10 cog-11 true_head-12 power-13 ext-14 extend-15
        each_anchor = np.array(group_list[1].iloc[0:group_num])
        if(each_anchor[1,2] in water_list):
            tmpStr = ais_poly(ais=each_ship, poly=each_anchor)
            # print "tmpStr = ", tmpStr
            if(tmpStr):
                outList.append(tmpStr)
            else:
                pass

    length = len(outList)
    # print outList
    outStr = ""
    for index in range(length):
        if(index == (length - 1)):
            outStr = outStr + str(outList[index])
        else:
            outStr = outStr + str(outList[index]) + "\n"

    return outStr


##########################################################

if __name__ == "__main__":
    # MASTER_HOME = "spark://marathon-lb.marathon.mesos:17077"
    MASTER_HOME = "local"
    # MASTER_HOME = "spark://192.168.1.121:7077"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("new.nav.qiu.water.201609")
    # conf.set("spark.cores.max", "8")
    # conf.set("spark.executor.memory", "16g")

    sc = SparkContext(conf=conf)

    print("reading anchores ....")
    # anchor = pd.read_csv("/home/qiu/Documents/Asia_anchores.csv")  # 获得码头坐标信息
    anchor = open("/home/qiu/Documents/Asia_anchores.csv", "r")
    anchor_list = []
    for line in anchor:
        if(not "longitude" in line):
            longitude = float(line.split("\n")[0].split(",")[0])
            latitude = float(line.split("\n")[0].split(",")[1])
            anchor_id = line.split("\n")[0].split(",")[2]
            anchor_list.append([longitude, latitude, anchor_id])
    anchor.close()

    print("reading ais ....")
    # aisRDD = sc.textFile("hdfs://192.168.1.121:9000/qiu/xaa")\
    #            .map(lambda line: convert_ais(line))\
    #            .filter(lambda line: line!=None)\
    #            .map(lambda line: line.split(","))
    #
    # print aisRDD.take(5)
    aisRDD = sc.textFile("hdfs://192.168.1.121:9000/qiu/xaa")
    convertRDD = aisRDD.map(lambda line: convert_ais(line))\
                       .filter(lambda line: line!=None)\
                       .map(lambda line: line.split(","))

    anchorNavRDD = convertRDD.groupBy(lambda v: v[0])\
                         .map(lambda group: ais_Gpoly(group, anchor_list))\
                         .filter(lambda group: group != "")

    # anchorNavRDD.coalesce(1).saveAsTextFile("hdfs://192.168.1.121:9000/qiu/201606_yingkou_nav_3")
    anchorNavRDD.coalesce(1).saveAsTextFile("/home/qiu/Documents/qiu/new.nav.qiu.tst6")

    print("done")
    sc.stop()
