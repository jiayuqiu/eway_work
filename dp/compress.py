# coding:utf-8

import math
import time
import numpy as np
import pandas as pd
from base_func import getDist, getDist_3d, WaiJi, NeiJi, cosVector
from DST import check_deviation

# 压缩算法类
class compress(object):
    def __init__(self, maxDst = 0.05):
        # 设置1°经度方向上的距离，单位：公里
        self.lonDst = 111.319
        # 设置距离精度
        self.accuracy = 1000000.
        # 设置最小平均速度
        self.minSpeed = 0.0001 * (1 / self.lonDst)
        # 设置经纬度改变阈值
        self.extDegree = maxDst * (1 / self.lonDst)
        # 设置距离阈值
        self.extDst = maxDst

    ########################################################################
    # 对AIS数据按速度进行分段
    def __speed_point(self, each_data, speed=300.):
        list_x = [0, len(each_data)]
        if len(each_data) > 2:
            for i in range(1, len(each_data) - 1):
                v1 = (getDist(each_data[i - 1][2], each_data[i - 1][3], each_data[i][2], \
                              each_data[i][3]) * self.accuracy) / (each_data[i][1] - each_data[i - 1][1])
                v2 = (getDist(each_data[i][2], each_data[i][3], each_data[i + 1][2], \
                              each_data[i + 1][3]) * self.accuracy) / (each_data[i + 1][1] - each_data[i][1])
                if (v1 < speed and v2 > speed) | (v1 > speed and v2 < speed):
                    list_x.append(i)
                    list_x.sort()
        else:
            pass
        return list_x

    # 对分组后的数据调用压缩算法
    def compressMain(self, input_data):
        compressed_str = []
        for group in input_data:
            # 将grouped数据转换为list进行调用，最终转换为数组array
            group_list = list(group)
            group_num = len(group_list[1])
            each_data = np.array(group_list[1])
            list_x = self.__speed_point(each_data)
            i = 0
            while (i < len(list_x) - 1):
                a = list_x[i]
                b = list_x[i + 1]
                eachship = each_data[a:b, :]
                i = i + 1
                ais_length = len(eachship)
                # 初始化压缩后的数据，加入起点与终点
                output_data = []
                if ais_length > 1:
                    output_data.append(eachship[0])
                    output_data.append(eachship[-1])
                    # 调用压缩算法
                    output_data = self.quickDPOPT(oneStage=eachship, outData=output_data)
                    # print output_data
                    compressed_str.extend(output_data)
                else:
                    output_data.append(eachship[0])
                    compressed_str.extend(output_data)
        return compressed_str
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
            pointLon = point[2] * W2
            pointLat = point[3]
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
            W2 = math.cos(((oneStage[-1][3] + oneStage[0][3]) * (math.pi / 180.)) / 2)
            # 获取收尾两点的空间坐标
            strTime = oneStage[0][1]
            strLon = oneStage[0][2]
            strLat = oneStage[0][3]
            endTime = oneStage[-1][1]
            endLon = oneStage[-1][2]
            endLat = oneStage[-1][3]
            # 得到在经纬度平面上起点与终点之间的经纬度距离的平方，单位：1°
            detaDegree = (endLon * W2 - strLon * W2) ** 2 + (endLat - strLat) ** 2
            # 获得时间轴在三维坐标系中权重，W可理解为速度，W2可理解为维度对经度的权重转换
            W = math.sqrt(detaDegree) / (endTime - strTime)
            if endTime - startTime == 0:
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
            # 求出实际地球两点间距离
            str_time = oneStage[0][1]
            str_lon = oneStage[0][2]
            str_lat = oneStage[0][3]
            end_time = oneStage[- 1][1]
            end_lon = oneStage[- 1][2]
            end_lat = oneStage[- 1][3]
            dst_str2end_2d = (self.accuracy * getDist(str_lon, str_lat, end_lon, end_lat))
            Wreal = dst_str2end_2d / (end_time - str_time)
            dst_str2end_3d = getDist_3d(str_lon, str_lat, str_time, end_lon, end_lat, end_time, Wreal)
            dst_i2s_3d = getDist_3d(oneStage[blockIndex][2], oneStage[blockIndex][3], oneStage[blockIndex][1],
                                    str_lon, str_lat, str_time, Wreal)
            dst_i2e_3d = getDist_3d(oneStage[blockIndex][2], oneStage[blockIndex][3], oneStage[blockIndex][1],
                                    end_lon, end_lat, end_time, Wreal)
            pi = (dst_i2s_3d + dst_i2e_3d + dst_str2end_3d) / 2.0  # 获得半周长
            areaI = (pi * abs(pi - dst_i2s_3d) * abs(pi - dst_i2e_3d) * abs(pi - dst_str2end_3d)) ** 0.5
            dst_i2se = (2 * areaI) / dst_str2end_3d
            print("real dst = %f" % dst_i2se)
            cos = cosVector([strLon - endLon, strLat - endLat, strTime - endTime],
                            [strLon - endLon, strLat - endLat, 0])
            sin = math.sqrt(1 - cos ** 2)
            print("maxDst = %f" % (((maxDst / (vectorL_lon ** 2 + vectorL_lat ** 2 + vectorL_time ** 2)) ** 0.5) *
                  int(111.319 * self.accuracy)))
            raw_input("=============================")
            if maxDst > (((self.extDegree * sin) ** 2) *
                         (vectorL_lon ** 2 + vectorL_lat ** 2 + vectorL_time ** 2)):  # 若存在分割点
                # 保存分割点
                outData.append(oneStage[blockIndex])
                # 分段
                self.quickDPOPT(oneStage[0:(blockIndex + 1)], outData)
                self.quickDPOPT(oneStage[blockIndex:], outData)
        return outData

if __name__ == "__main__":
    MMSIList = [413376760]
    maxDst = 0.20
    cmp = compress(maxDst=maxDst)
    data = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    data = data.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    # data = data[data["unique_ID"].isin(MMSIList)]
    # data = data[(data["latitude"] < 36.5 * 1000000.) & (data["acquisition_time"] <= 1472691018)]
    # data.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    data["longitude"] = data["longitude"] / 1000000.0
    data["latitude"] = data["latitude"] / 1000000.0
    dataGDF = data.groupby("unique_ID")
    startTime = time.time()
    print("start compressing...")
    compressedData = cmp.compressMain(dataGDF)
    endTime = time.time()
    compressed_data_DF = pd.DataFrame(compressedData)
    compressed_data_DF.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    compressed_data_DF = compressed_data_DF.sort_values(by=["unique_ID", "acquisition_time"])
    # compressed_data_DF.to_csv("/home/qiu/Documents/data/DP-DATA/QDPOPT/"
    #                           "201609_chinaCoast_qdpopt_%sm.csv" % int(maxDst * 1000), index=None)
    print("use time %s seconds" % (endTime - startTime))
    print("origin length is %s" % len(data))
    print("compress length is %s" % len(compressed_data_DF))

