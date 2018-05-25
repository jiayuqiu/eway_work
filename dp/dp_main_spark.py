# coding:utf-8

import math
import time
import sys
import numpy as np
import pandas as pd

from pyspark import SparkContext
from pyspark import SparkConf
# from compress import compress
# from base_func import format_convert

##########################################################################
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
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    try:
        dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                      math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    except:
        print(lon1, lat1, lon2, lat2)

    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000

    return dst
##########################################################
# 获得三维空间中点到点的距离,precision:1000000-mm;1000-m;1-km,ed
# W_time为当前阶段的时间权重
def getDist_3d(lon1, lat1, time1, lon2, lat2, time2, W_time, precision = 1000000.):
    dst_2d = getDist(lon1, lat1, lon2, lat2) * precision
    dst_3d = math.sqrt(dst_2d ** 2 + (W_time * (time2 - time1)) ** 2)
    return dst_3d

##########################################################
# 获得向量内既函数段，向量1点乘向量2,ed
def NeiJi(lon1, lat1, time1, lon2, lat2, time2):
    return (lon1 * lon2 + lat1 * lat2 + time1 * time2)

# 获得向量外积函数段，向量1叉乘向量2,ed
def WaiJi(lon1, lat1, time1, lon2, lat2, time2):
    wj_lon  = lat1  * time2 - lat2  * time1
    wj_lat  = time1 * lon2  - time2 * lon1
    wj_time = lon1  * lat2  - lon2  * lat1
    return [wj_lon, wj_lat, wj_time]

##########################################################
# 获取三维空间中，起点与终点连线与经纬度平面所成的夹角的正弦值
def cosVector(x,y):
    if(len(x)!=len(y)):
        print('error input,x and y is not in the same space')
        return
    result1=0.0
    result2=0.0
    result3=0.0
    for i in range(len(x)):
        result1+=x[i]*y[i]   #sum(X*Y)
        result2+=x[i]**2     #sum(X*X)
        result3+=y[i]**2     #sum(Y*Y)
    if ((result2 * result3) ** 0.5 == 0):
        return 0.
    else:
        return (result1 / ((result2 * result3) ** 0.5))  # 结果显示

##########################################################
# 判断点是否在给定多边形内，返回T或F
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

# 将list转换为一个大字符串输出
# 输入参数：nav_event -- 停泊事件list
def getNavStr(input_list):
    output_str_list = list()
    input_str_list = [[str(x) for x in ele] for ele in input_list]
    for ele in input_str_list:
        ele[6] = str(int(float(ele[6]) * 1000000))
        ele[7] = str(int(float(ele[7]) * 1000000))
        output_str_list.append(','.join(ele) + '\n')
    output_str = ''.join(list(set(output_str_list)))
    return output_str[:-1]


#------------------------------------------------------------------------------------------
# 压缩算法类
class compress(object):
    def __init__(self, maxDst = 0.05):
        # 设置1°经度方向上的距离，单位：公里
        self.lonDst = 111.319
        # 设置距离精度
        self.accuracy = 1000000.
        # 设置最小平均速度
        self.minSpeed = 0.0001 * (1. / self.lonDst)
        # 设置经纬度改变阈值
        self.extDegree = float(maxDst) * (1. / self.lonDst)
        # 设置距离阈值
        self.extDst = float(maxDst)

    ########################################################################
    # 对AIS数据按速度进行分段
    def __speed_point(self, each_data, speed=300.):
        list_x = [0, len(each_data)]
        if len(each_data) > 2:
            for i in range(1, len(each_data) - 1):
                if each_data[i][1] - each_data[i - 1][1] == 0:
                    v1 = (getDist(each_data[i - 1][6], each_data[i - 1][7], each_data[i][6],
                                  each_data[i][7]) * self.accuracy) / (each_data[i][1] - each_data[i - 1][1] + 1)
                else:
                    v1 = (getDist(each_data[i - 1][6], each_data[i - 1][7], each_data[i][6],
                                  each_data[i][7]) * self.accuracy) / (each_data[i][1] - each_data[i - 1][1])
                if each_data[i + 1][1] - each_data[i][1] == 0:
                    v2 = (getDist(each_data[i][6], each_data[i][7], each_data[i + 1][6],
                                  each_data[i + 1][7]) * self.accuracy) / (each_data[i + 1][1] - each_data[i][1] + 1)
                else:
                    v2 = (getDist(each_data[i][6], each_data[i][7], each_data[i + 1][6],
                                  each_data[i + 1][7]) * self.accuracy) / (each_data[i + 1][1] - each_data[i][1])
                if ((v1 < speed) & (v2 > speed)) | ((v1 > speed) & (v2 < speed)):
                    list_x.append(i)
            list_x.sort()
        else:
            pass
        return list_x

    # # 对分组后的数据调用压缩算法
    # def compressMain(self, input_data):
    #     compressed_str = []
    #     for group in input_data:
    #         # 将grouped数据转换为list进行调用，最终转换为数组array
    #         group_list = list(group)
    #         group_num = len(group_list[1])
    #         each_data = np.array(group_list[1])
    #         for line in each_data:
    #             line[6] = line[6] / 1000000.
    #             line[7] = line[7] / 1000000.
    #         list_x = self.__speed_point(each_data)
    #         i = 0
    #         while (i < len(list_x) - 1):
    #             a = list_x[i]
    #             b = list_x[i + 1]
    #             eachship = each_data[a:b, :]
    #             i = i + 1
    #             ais_length = len(eachship)
    #             # 初始化压缩后的数据，加入起点与终点
    #             output_data = []
    #             if ais_length > 1:
    #                 output_data.append(eachship[0])
    #                 output_data.append(eachship[-1])
    #                 # 调用压缩算法
    #                 output_data = self.quickDPOPT(oneStage=eachship, outData=output_data)
    #                 # print output_data
    #                 compressed_str.extend(output_data)
    #             else:
    #                 output_data.append(eachship[0])
    #                 compressed_str.extend(output_data)
    #     return compressed_str

    # spark分组后的数据调用qdp压缩算法
    def spark_qdp_main(self, sparkGroupData):
        import sys
        sys.setrecursionlimit(1000000)
        compressedStr = []
        groupList = list(sparkGroupData[1])
        groupList.sort(key=lambda v: v[1])
        for lineAIS in groupList:
            lineAIS[1] = int(lineAIS[1])
            lineAIS[6] = float(lineAIS[6]) / 1000000.
            lineAIS[7] = float(lineAIS[7]) / 1000000.
        list_x = self.__speed_point(groupList)
        i = 0
        while (i < len(list_x) - 1):
            a = list_x[i]
            b = list_x[i + 1]
            eachship = groupList[a:b]
            i = i + 1
            ais_length = len(groupList)
            # 初始化压缩后的数据，加入起点与终点
            output_data = []
            if ais_length > 1:
                output_data.append(eachship[0])
                output_data.append(eachship[-1])
                # 调用压缩算法
                output_data = self.quickDPOPT(oneStage=eachship, outData=output_data)
                # print output_data
                compressedStr.extend(output_data)
            else:
                output_data.append(eachship[0])
                compressedStr.extend(output_data)
        compressedStr = getNavStr(compressedStr)
        return compressedStr

    ####################################################################################################

    # 获得所有点的在平面M上的投影
    # 输入参数：oneStage -- 一条船一段AIS数据；col0 : mmsi, col1 : time, col2 : lon, col3 : lat
    # vectorSE -- 首尾两点形成的向量
    def __projection(self, oneStage, vectorSE, W, W2):
        # 初始化投影轨迹点列表
        prejectionPointsList = []
        # 循环每行，获取投影
        for point in oneStage:
            # 获取当前点的坐标信息
            pointTime = point[1] * W
            pointLon = point[6] * W2
            pointLat = point[7]
            # 获取对应的投影点坐标
            tmpWaiji = WaiJi(pointLon, pointLat, pointTime, vectorSE[0], vectorSE[1], vectorSE[2])
            prejectionPointsList.append(tmpWaiji)
        return prejectionPointsList

    # 通过投影点找到状态驻点，并找到中间点到首尾两点连线的最大值
    # 输入参数：projectionPoints -- 投影点的集合
    def __getBlockPoint(self, projectionPoints):
        # 初始化最大距离与对应索引
        maxDst = 0
        maxDstIndex = 0
        # 获取投影点的长度
        pointsLen = len(projectionPoints)
        # 特殊处理：第二天判断时，不存在上一条向量的反向量，初始化为0向量
        # preVector = [projectionPoints[0][0] - projectionPoints[1][0],
        #              projectionPoints[0][1] - projectionPoints[1][1],
        #              projectionPoints[0][2] - projectionPoints[1][2]]
        preVector = [0, 0, 0]
        # 从第二条循环至倒数第二条，找到中间点到首尾两点连线的最大距离
        for index in range(1, (pointsLen - 1)):
            # 求出在平面上，点i与点i+1形成的向量
            vectorIPlusLon = projectionPoints[index + 1][0] - projectionPoints[index][0]
            vectorIPlusLat = projectionPoints[index + 1][1] - projectionPoints[index][1]
            vectorIPlusTime = projectionPoints[index + 1][2] - projectionPoints[index][2]
            # 求出在平面上，点s与点i形成的向量
            vectorSILon = projectionPoints[index][0] - projectionPoints[0][0]
            vectorSILat = projectionPoints[index][1] - projectionPoints[0][1]
            vectorSITime = projectionPoints[index][2] - projectionPoints[0][2]
            # 求出向量i,i+1与向量si的内积
            neijiPlus = NeiJi(vectorIPlusLon, vectorIPlusLat, vectorIPlusTime,
                              vectorSILon, vectorSILat, vectorSITime)
            # 求出向量i,i-1与向量si的内积
            neijiMius = NeiJi(preVector[0], preVector[1], preVector[2],
                              vectorSILon, vectorSILat, vectorSITime)
            # 判断点i是否为状态驻点
            if (neijiMius <= 0) & (neijiPlus < 0):  # 若点i为状态驻点
                # 求出向量si在平面M上的长度
                vectorSILength = vectorSILon ** 2 + vectorSILat ** 2 + vectorSITime ** 2
                # 判断是否比暂存最大距离大
                if vectorSILength > maxDst:  # 若比最大距离大，重新复制最大距离与索引
                    maxDst = vectorSILength
                    maxDstIndex = index
                else:  # 若比最大距离小，不进行操作
                    pass
            # 最后得到向量Iplus的反向量
            preVector = [-vectorIPlusLon, -vectorIPlusLat, -vectorIPlusTime]
        return maxDst, maxDstIndex

    # 快速DP算法
    # 输入参数：oneStage -- 一条船一段AIS数据；col0 : mmsi, col1 : time, col2 : lon, col3 : lat
    # outData -- 输出的AIS数据
    def quickDPOPT(self, oneStage, outData):
        # 判断AIS数据是否需要压缩
        if len(oneStage) > 2:  # 若轨迹点大于2条，进行压缩
            # 经度在维度方向上的权重
            W2 = math.cos(((oneStage[-1][7] + oneStage[0][7]) * (math.pi / 180.)) / 2)
            # 获取收尾两点的空间坐标
            strTime = oneStage[0][1]
            strLon = oneStage[0][6]
            strLat = oneStage[0][7]
            endTime = oneStage[-1][1]
            endLon = oneStage[-1][6]
            endLat = oneStage[-1][7]
            # 得到在经纬度平面上起点与终点之间的经纬度距离的平方，单位：1°
            detaDegree = (endLon * W2 - strLon * W2) ** 2 + (endLat - strLat) ** 2
            # 获得时间轴在三维坐标系中权重，W可理解为速度，W2可理解为维度对经度的权重转换
            W = math.sqrt(detaDegree) / (endTime - strTime)
            if endTime - strTime == 0:
                endTime += 1
            if W < self.minSpeed:
                W = self.minSpeed
            strTime, endTime = strTime * W, endTime * W
            # 获得起点与终点形成的向量，记作向量l,ed
            vectorL_time = endTime - strTime
            vectorL_lon = endLon - strLon
            vectorL_lat = endLat - strLat
            vectorL = [vectorL_lon, vectorL_lat, vectorL_time]
            # 获取所有点到平面M的投影
            projectionPoints = self.__projection(oneStage=oneStage, vectorSE=vectorL, W=W, W2=W2)
            # 通过投影点找到分割点
            maxDst, blockIndex = self.__getBlockPoint(projectionPoints)
            # 判断是否存在分割点
            # print("maxDst = %f mm" % ((maxDst ** 0.5) * (math.pi/180.) * 111.319 * self.accuracy))
            cos = cosVector([strLon - endLon, strLat - endLat, strTime - endTime],
                            [strLon - endLon, strLat - endLat, 0])
            sin = math.sqrt(1 - cos ** 2)
            if maxDst > (((self.extDegree * sin) ** 2) *
                         (vectorL_lon ** 2 + vectorL_lat ** 2 + vectorL_time ** 2)):  # 若存在分割点
                # 保存分割点
                outData.append(oneStage[blockIndex])
                # 分段
                self.quickDPOPT(oneStage[0:(blockIndex + 1)], outData)
                self.quickDPOPT(oneStage[blockIndex:], outData)
        return outData

if __name__ == "__main__":
    # MASTER_HOME = "spark://192.168.10.100:7077"
    # MASTER_HOME = "local[20]"
    conf = SparkConf()
    # conf.setMaster(MASTER_HOME)
    # conf.setAppName("dp_201611")
    # conf.set("spark.cores.max", "20")
    # conf.set("spark.executor.memory", "200g")
    # conf.set("spark.local.dir", "/mnt/sdc")

    # 判断参数是否输入完全
    if len(sys.argv) < 5:
        print("参数不足")
        exit(1)

    sc = SparkContext(conf=conf)
    fc = format_convert()
    compress = compress(maxDst=sys.argv[1])

    # 转换数据格式为三阶段表格式
    if sys.argv[3] == "bm":
        orgAisRDD = sc.textFile(sys.argv[2])\
                      .map(lambda line: fc.bm_to_thr(line)) \
                      .filter(lambda line: line != None) \
                      .map(lambda line: line.split(","))
    elif sys.argv[3] == "cx":
        orgAisRDD = sc.textFile(sys.argv[2])\
                      .map(lambda line: fc.cx_to_thr(line)) \
                      .filter(lambda line: line != None) \
                      .map(lambda line: line.split(","))
    elif sys.argv[3] == "thr":
        orgAisRDD = sc.textFile(sys.argv[2])\
                      .map(lambda line: line.split(","))

    # 调用压缩算法
    compressAisRDD = orgAisRDD.groupBy(lambda aisListRDD: aisListRDD[0]) \
                              .map(lambda group: compress.spark_qdp_main(group))\
                              .filter(lambda group: group!=None)\
                              .repartition(1)\
                              .saveAsTextFile(sys.argv[4])
    # compressAisRDD.take(5)
    sc.stop()
    print("done!")
