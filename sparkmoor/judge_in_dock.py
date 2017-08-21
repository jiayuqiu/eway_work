# coding:utf-8

import pandas as pd
import numpy as np

from pyspark import SparkContext
from pyspark import SparkConf

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
        if(not each_anchor[1,2] in water_list):
            # print "tmpStr = ", tmpStr
            for line in each_ship:
                if(point_poly(float(line[15])/1000000., float(line[16])/1000000., each_anchor)):
                    tmpList = [each_anchor[1, 2], int(line[0]), int(line[1]), int(line[2]),
                               int(line[2]) - int(line[1])]
                    outList.append(tmpList)

    length = len(outList)
    # print outList
    outStr = ""
    for index in range(length):
        if(index == (length - 1)):
            outStr = outStr + str(outList[index])
        else:
            outStr = outStr + str(outList[index]) + "\n"
    # outStr = outStr.split("[")[1].split("]")[0]
    return outStr

if __name__ == "__main__":
    MASTER_HOME = "spark://192.168.1.121:7077"
    # MASTER_HOME = "local[*]"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("new.nav.qiu")
    conf.set("spark.cores.max", "24")
    conf.set("spark.executor.memory", "6g")

    sc = SparkContext(conf=conf)

    print("reading anchores ....")
    # anchor = pd.read_csv("/home/qiu/Documents/Asia_anchores.csv")  # 获得码头坐标信息
    anchor = open("/home/qiu/Documents/Asia_anchores.csv", "r")
    anchor_list = []
    for line in anchor:
        if (not "longitude" in line):
            longitude = float(line.split("\n")[0].split(",")[0])
            latitude = float(line.split("\n")[0].split(",")[1])
            anchor_id = line.split("\n")[0].split(",")[2]
            anchor_list.append([longitude, latitude, anchor_id])
    anchor.close()

    # 读取ais数据，并转换为停泊事件模型需要的输入格式
    shipsAISRDD = sc.textFile("hdfs://192.168.1.121:9000/qiu/201609_allShips_anchor_nav_20170303.2")\
                    .map(lambda line: line.split(","))\
                    .groupBy(lambda v: v[0])\
                    .map(lambda group: ais_Gpoly(group, anchor_list))\
                    .filter(lambda group: group != "")

    shipsAISRDD.coalesce(1).saveAsTextFile("hdfs://192.168.1.121:9000/qiu/201609_bm_allShips_dock_nav_20170306")
    print(shipsAISRDD.take(5))
    print("hello world!")
