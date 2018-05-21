# coding:utf-8

import numpy as np
import pandas as pd
import math

from pyspark import SparkContext
from pyspark import SparkConf
from base_func import getDist, format_convert, isInRange

import sys
reload(sys)
sys.setdefaultencoding('utf8')

# 求得每个珊格与其他所有珊格的距离，找到与当前珊格小于25公里的珊格
def gridDst(coorTuple, grade, dst = 25.):
    coorList = list(coorTuple)
    lon = coorList[0] + (grade / 2.)
    lat = coorList[1] + (grade / 2.)
    # 当前坐标所在的areaID
    areaID = fc.areaID(longitude=lon, latitude=lat, grade=grade)
    print("lon = %f" % lon)
    # 形成地球上的每个珊格的中心的坐标
    lonList = [(i * grade) for i in range(int(-180/grade), int(180/grade))]
    latList = [(i * grade) for i in range(int(-90/grade), int(90/grade))]
    # 求得该areaID到其他所有珊格的距离，找出小于25公里的进行记录
    gridList = []  # 初始化输出的珊格编号列表
    for gridLat in latList:
        # 获得珊格的中心经度
        gridLat = gridLat + (grade / 2.)
        for gridLon in lonList:
            # 获得珊格的中心维度
            gridLon = gridLon + (grade / 2.)
            # 获取当前areaID与当前珊格的距离
            tmpGridDst = getDist(lon1=lon, lat1=lat, lon2=gridLon, lat2=gridLat)
            # 判断当前areaID与当前珊格的距离是否小于25公里
            if(tmpGridDst < dst):  # 若小于25公里，记录此珊格的areaID
                # 记录此珊格的areaID
                gridList.append(fc.areaID(longitude=gridLon, latitude=gridLat))
            else:  # 若大于25公里，不做处理
                pass
    gridStr = ""
    for grid in gridList:
        gridStr = gridStr + str(grid) + "*"
    outStr = str(areaID) + "," + gridStr
    return outStr

#################################################################
# 优化求得附近的areaID程序
# 获取当前区域内的最大最小经纬
def getAreaCenter(lon, lat, grade):
    lon = lon / grade
    lat = lat / grade
    # 判断经度是正数还是负数
    if (lon < 0):  # 若是负数获取当前区域内的经度极值
        areaMinLon = math.ceil(lon)
        areaMaxLon = math.floor(lon)
    else:  # 若是整数获取当前区域内的经度极值
        areaMinLon = math.floor(lon)
        areaMaxLon = math.ceil(lon)
    # 判断纬度，方法同经度
    if (lat < 0):
        areaMinLat = math.ceil(lat)
        areaMaxLat = math.floor(lat)
    else:
        areaMinLat = math.floor(lat)
        areaMaxLat = math.ceil(lat)
    # 获取区域的中心经纬度
    areaCenterLon = ((areaMinLon + areaMaxLon) / 2.) * grade
    areaCenterLat = ((areaMinLat + areaMaxLat) / 2.) * grade
    return areaCenterLon, areaCenterLat

# 获取给定半径与单位距离的整数关系
def getMoveNum(unionDst, dst):
    n = int(dst / unionDst) + 1
    return n

# 根据经纬度方向上的移动格数获取附近的经纬度
def getCoorList(areaCenterLon, areaCenterLat, lonMoveNum, latMoveNum, grade):
    import itertools
    # 初始化形成的经纬度列表
    lonList = []
    latList = []
    coorList = []
    # 获取经度列表
    for nLon in range((lonMoveNum + 1)):
        # 获取附近的栅格中心经度
        tmpLonBigger = areaCenterLon + nLon * grade
        tmpLonSmaller = areaCenterLon - nLon * grade
        # 若数值变化后跨越了东西半球，进行处理
        if(tmpLonBigger > 180.):  # 若由东向西
            tmpLonBigger = tmpLonBigger - 360.
        if(tmpLonSmaller < -180.):  # 若由西向东
            tmpLonSmaller = tmpLonSmaller + 360.
        lonList.append(tmpLonBigger)
        lonList.append(tmpLonSmaller)

    # 获取纬度列表
    for nLat in range((latMoveNum + 1)):
        # 获取附近栅格的纬度里
        tmpLatBigger = areaCenterLat + nLat * grade
        tmpLatSmaller = areaCenterLat - nLat * grade
        if(tmpLatBigger < 90.):
            latList.append(tmpLatBigger)
        if(tmpLatSmaller > -90.):
            latList.append(tmpLatSmaller)
    # 求出经度列表与纬度列表形成的笛卡尔积
    for coor in itertools.product(lonList, latList):
        # 把笛卡尔积内的每个元素放入列表记录
        coorList.append(coor)
    # 输出存放所有坐标的列表
    return coorList


# 获取临近的栅格对应的中心经纬度
def getCloseArea(areaCenterLon, areaCenterLat, dst, grade):
    # 初始化当前区域的中心经纬度
    tmpAreaCenterLon = areaCenterLon
    tmpAreaCenterLat = areaCenterLat

    # 求出当前栅格在纬度方向上的单位距离
    if ((tmpAreaCenterLon + grade) > 180.):
        # 将经度转换为负值
        tmpAreaCenterLon = -180. + (grade / 2.0)
    else:
        tmpAreaCenterLon += grade
    lonDst = getDist(lon1=areaCenterLon, lat1=areaCenterLat,
                     lon2=tmpAreaCenterLon, lat2=areaCenterLat)
    # 求出当前栅格在经度方向上的单位距离
    tmpAreaCenterLat += grade
    latDst = getDist(lon1=areaCenterLon, lat1=areaCenterLat,
                     lon2=areaCenterLon, lat2=tmpAreaCenterLat)

    # 获取经纬度需要移动的格数
    lonMoveNum = getMoveNum(unionDst=lonDst, dst=dst)
    latMoveNum = getMoveNum(unionDst=latDst, dst=dst)
    # 获取附近的所有栅格的中心坐标点
    closeCoorList = getCoorList(areaCenterLon=areaCenterLon, areaCenterLat=areaCenterLat,
                                lonMoveNum=lonMoveNum, latMoveNum=latMoveNum, grade=grade)
    # 获取附近所有栅格的ID
    closeAreaIDList = []
    for coor in closeCoorList:
        coorList = list(coor)
        tmpLon = coorList[0]
        tmpLat = coorList[1]
        tmpAreaID = fc.areaID(tmpLon, tmpLat, grade=grade)
        closeAreaIDList.append(tmpAreaID)
    return closeAreaIDList


# 求得当前区域临近的areaID
def gridDstOpt(coorTuple, grade, dst=25.):
    coorList = list(coorTuple)
    lon = float(coorList[0])
    lat = float(coorList[1])
    if len(coorList[2]) == 2:
        portName = str(coorList[2][0])
        portID = str(coorList[2][1]).replace(",", ";")

        # 获取当前区域内的经纬度极值
        areaCenterLon, areaCenterLat = getAreaCenter(lon, lat, grade)
        # 当前坐标所在的areaID
        areaID = fc.areaID(longitude=lon, latitude=lat, grade=grade)
        # 找到以25公里为半径，在半径范围内的栅格的ID
        closeAreaIDList = getCloseArea(areaCenterLon=areaCenterLon, areaCenterLat=areaCenterLat,
                                       dst=dst, grade=grade)
        gridStr = ""
        for grid in closeAreaIDList:
            gridStr = gridStr + str(grid) + "*"
        outStr = portName + "," + portID + "," + str(lon) + "," + str(lat) + "," + str(areaID) + "," + gridStr
        return outStr
    elif len(coorList[2]) == 3:
        portName = str(coorList[2][0])
        portID = str(coorList[2][1]).replace(",", ";")
        berth_id = str(coorList[2][2]).replace(",", ";")

        # 获取当前区域内的经纬度极值
        areaCenterLon, areaCenterLat = getAreaCenter(lon, lat, grade)
        # 当前坐标所在的areaID
        areaID = fc.areaID(longitude=lon, latitude=lat, grade=grade)
        # 找到以25公里为半径，在半径范围内的栅格的ID
        closeAreaIDList = getCloseArea(areaCenterLon=areaCenterLon, areaCenterLat=areaCenterLat,
                                       dst=dst, grade=grade)
        gridStr = ""
        for grid in set(closeAreaIDList):
            gridStr = gridStr + str(grid) + "*"

        outStr = portName + "," + portID + "," + berth_id + "," + str(lon) + "," + str(lat) + "," + \
                 str(areaID) + "," + gridStr
        return outStr
    elif len(coorList[2]) == 4:
        portName = str(coorList[2][0])
        portID = str(coorList[2][1]).replace(",", ";")
        berth_id = str(coorList[2][2]).replace(",", ";")
        terminal_id = str(coorList[2][3]).replace(",", ";")

        # 获取当前区域内的经纬度极值
        areaCenterLon, areaCenterLat = getAreaCenter(lon, lat, grade)
        # 当前坐标所在的areaID
        areaID = fc.areaID(longitude=lon, latitude=lat, grade=grade)
        # 找到以25公里为半径，在半径范围内的栅格的ID
        closeAreaIDList = getCloseArea(areaCenterLon=areaCenterLon, areaCenterLat=areaCenterLat,
                                       dst=dst, grade=grade)
        gridStr = ""
        for grid in set(closeAreaIDList):
            gridStr = gridStr + str(grid) + "*"

        outStr = portName + "," + portID + "," + berth_id + "," + terminal_id + "," + str(lon) + "," + str(lat) + "," +\
                 str(areaID) + "," + gridStr
        return outStr


if __name__ == "__main__":
    MASTER_HOME = "local[*]"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("new.nav.qiu")
    # spark启动
    sc = SparkContext(conf=conf)

    # 实例化格式转换类
    fc = format_convert()

    # 设置地球珊格化的最小精度
    grade = 0.1
    lonList = [(i * grade) for i in range(int(-180/grade), int(180/grade))]
    latList = [(i * grade) for i in range(int(-90/grade), int(90/grade))]

    lonListRDD = sc.parallelize(lonList)
    latListRDD = sc.parallelize(latList)

    # 获取ABS给出的港口数据
    portDF = pd.read_excel("/Users/qiujiayu/PycharmProjects/eway_work/sparkmoor/data/tblPortTerminal.xlsx")
    portDF = portDF.loc[:, ["Terminal_ID", "Terminal_Name", "Dec_Latitude", "Dec_Longitude", "Port_ID"]]
    portDF = portDF[~(portDF["Dec_Latitude"].isnull() | portDF["Dec_Longitude"].isnull())]
    print(portDF.head())
    portList = []
    # 循环每个港口数据，元祖(portLon, portLat, portName)
    for index, value in portDF.iterrows():
        tmp_name = str(value["Terminal_Name"]).replace(",", ";")
        portList.append((value["Dec_Longitude"], value["Dec_Latitude"], [tmp_name, value["Port_ID"], value["Terminal_ID"]]))

    print(portList[:4])
    areaRDD = sc.parallelize(portList)
    areaRDD.map(lambda x: gridDstOpt(coorTuple=x, grade=grade))\
           .repartition(1)\
           .saveAsTextFile("./data/terminal_areaID")
    # spark关闭
    sc.stop()
