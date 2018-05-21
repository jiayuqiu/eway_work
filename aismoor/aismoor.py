# coding:utf-8

import numpy as np
import pandas as pd
import math

from base_func import getDist
from base_func import format_convert
from base_func import point_poly
from avg_speed import steadyAvgSpeed

from pyspark import SparkContext
from pyspark import SparkConf

##################################################
# 栅格化函数
# 求得每个珊格与其他所有珊格的距离，找到与当前珊格小于25公里的珊格
fc = format_convert()
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

##################################################
# 20170516停泊事件模型类
class moor(object):
    def __init__(self):
        # 实例化格式转换类
        self.fc = format_convert()
        # 实例化平均航速类
        self.steadyAvgSpeed = steadyAvgSpeed()
        # 初始化距离精度 1 -- km; 1000 -- m; 1000000 -- mm;
        self.preision = 1000000.
        # 初始化停泊事件最大位移，单位：毫米。数值200米 * sqrt(2) * 1000
        self.D_DST = 282842.2725
        # 初始化停泊事件最大低速点，单位：毫米/秒
        self.D_SPEED = 100.
        # 初始化判断点港口与停泊事件之间的位置关系距离阈值，单位：千米
        self.moorDst = 25.
        # 初始化判断合并停泊事件条件
        self.mergeDst = 100000.  # 距离阈值，单位：毫米
        self.mergeTime = 30 * 60  # 时间阈值，单位：秒

    # 对三阶段AIS数据求出指定索引范围内的航程
    # 参数输入：shipAISList -- AIS数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引
    def __getSailDst(self, shipAISList, startIndex, endIndex):
        sailDst = 0
        # 获取指定索引内的AIS数据与条数
        tmpShipAISList = shipAISList[startIndex:(endIndex + 1)]
        # 初始化需求航程的经纬度列表
        lonList = []
        latList = []
        # 循环获得经纬度信息
        for line in tmpShipAISList:
            lonList.append(line[6])
            latList.append(line[7])
        # 求出航程
        for index in range((endIndex - startIndex)):
            tmpDst = getDist(lon1=lonList[index], lat1=latList[index],
                             lon2=lonList[index + 1], lat2=latList[index + 1])
            sailDst = sailDst + tmpDst
        # 返回航程，单位：千米
        return sailDst

    # 对三阶段AIS数据形成停泊事件的数据格式
    # 参数输入：shipAISList -- AIS数据；staticDF -- 静态数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引；lastEndIndex -- 上次停泊结束的索引；
    def __convertMoorResult(self, shipAISList, staticDF, startIndex, endIndex, lastEndIndex):
        # 获取停泊事件的输出数据
        shipAISList = [[float(x) for x in y] for y in shipAISList]
        shipAISList = np.array(shipAISList)
        begin_time = shipAISList[startIndex][1]   # time of starting nav point
        end_time = shipAISList[endIndex][1]       # time of ending nav point
        # 判断是否存在上一停泊事件
        if(lastEndIndex != 0):  # 若存在上次停泊事件
            # 上一停泊事件结束时间
            last_time = shipAISList[lastEndIndex][1]
            # 获取两个停泊事件之间的间隔时间
            apart_time = begin_time - last_time
            # 获取上一停泊结束时间至当前停泊开始时间的静态数据
            staticDF = staticDF[(staticDF["shipid"] == shipAISList[endIndex][0]) &
                                (staticDF["time"] >= last_time) &
                                (staticDF["time"] <= begin_time)]
            # 获取该航段内出现过的吃水深度个数
            draughtList = list(set(staticDF.iloc[:, 10]))
            draughtLen  = len(draughtList)
            # 若有且仅有一次吃水深度更新
            if draughtLen >= 2:
                draught = draughtList[-1]
            else:
                draught = None
        else:  # 若不存在上次停泊事件
            apart_time  = None
            draught     = None
        mmsi      = shipAISList[endIndex][0]                    # 船舶MMSI
        begin_lon = shipAISList[startIndex][6] * self.preision  # longitude of starting nav point
        begin_lat = shipAISList[startIndex][7] * self.preision  # latitude of starting nav point
        begin_hdg = shipAISList[startIndex][12]                 # true_head of starting nav point
        begin_sog = shipAISList[startIndex][9]                  # sog of starting nav point
        begin_cog = shipAISList[startIndex][11]                 # cog of starting nav point
        end_lon   = shipAISList[endIndex][6] * self.preision    # longitude of ending nav point
        end_lat   = shipAISList[endIndex][7] * self.preision    # latitude of ending nav point
        end_hdg   = shipAISList[endIndex][12]                   # true_head of ending nav point
        end_sog   = shipAISList[endIndex][9]                    # sog of ending nav point
        end_cog   = shipAISList[endIndex][11]                   # cog of ending nav point
        point_num = endIndex - startIndex + 1                   # ais data nums between nav
        avg_lon = np.mean(shipAISList[startIndex:(endIndex + 1), 6].astype(float)) * self.preision
        avg_lat = np.mean(shipAISList[startIndex:(endIndex + 1), 7].astype(float)) * self.preision
        zone_id   = shipAISList[endIndex][8]                    # zone_id of ending nav point
        navistate = shipAISList[startIndex + 1][5]              # status of start+1 nav point

        sailArray = np.array(shipAISList[lastEndIndex:(startIndex + 1)])
        avgSpeed = self.steadyAvgSpeed.shipSteadySpeedThr(sailArray)  # 获取平均速度
        # 判断该停泊事件包含几条AIS数据
        if(point_num == 2):  # 若该停泊事件只由2条AIS数据组成
            # 获取输出数据
            avg_lon = (begin_lon + end_lon) / 2.0
            avg_lat = (begin_lat + end_lat) / 2.0
            avg_hdgMcog = abs(((begin_hdg - begin_cog) + (end_hdg - end_cog)) / 2.0)
            avg_sog = (begin_sog + end_sog) / 2.0
            var_hdg = np.var([begin_hdg, end_hdg])
            var_cog = np.var([begin_cog, end_cog])
            var_sog = np.var([begin_sog, end_sog])
            var_rot = np.var([shipAISList[startIndex][9], shipAISList[endIndex][9]])
            max_sog = max([begin_sog, end_sog])
            maxSog_cog = [begin_cog, end_cog][np.argmax([begin_sog, end_sog])]
            max_rot = max([shipAISList[startIndex][9], shipAISList[endIndex][9]])
        else:
            # 获取输出数据
            tmp_avg_lon, tmp_avg_lat, tmp_avg_hdgMcog, tmp_avg_sog = [], [], [], []
            tmp_var_hdg, tmp_var_cog, tmp_var_sog, tmp_var_rot = [], [], [], []
            for index in range(startIndex, endIndex + 1):
                tmp_avg_lon.append(shipAISList[index][6] * self.preision)
                tmp_avg_lat.append(shipAISList[index][7] * self.preision)
                tmp_avg_hdgMcog.append(abs(shipAISList[index][12] - shipAISList[index][11]))
                tmp_avg_sog.append(shipAISList[index][9])
                tmp_var_hdg.append(shipAISList[index][12])
                tmp_var_cog.append(shipAISList[index][11])
                tmp_var_sog.append(shipAISList[index][9])
                tmp_var_rot.append(shipAISList[index][15])
            # 求出平均值、方差
            avg_lon = np.mean(tmp_avg_lon)
            avg_lat = np.mean(tmp_avg_lat)
            avg_hdgMcog = np.mean(tmp_avg_hdgMcog)
            avg_sog = np.mean(tmp_avg_sog)
            var_hdg = np.var(tmp_var_hdg)
            var_cog = np.var(tmp_var_cog)
            var_sog = np.var(tmp_var_sog)
            var_rot = np.var(tmp_var_rot)
            max_sog = max(tmp_var_sog)
            maxSog_cog = tmp_var_cog[np.argmax(tmp_var_sog)]
            max_rot = max(tmp_var_rot)

        return [mmsi, begin_time, end_time, apart_time,
                begin_lon, begin_lat, begin_hdg, begin_sog, begin_cog,
                end_lon, end_lat, end_hdg, end_sog, end_cog,
                point_num, avg_lon, avg_lat, var_hdg, var_cog, avg_hdgMcog,
                avg_sog, var_sog, max_sog, maxSog_cog,
                max_rot, var_rot, draught, avgSpeed, zone_id, navistate]

    # 将停泊事件list转换为一个大字符串输出
    # 输入参数：nav_event -- 停泊事件list
    def __getNavStr(self, input_list):
        if len(input_list) != 0:
            output_str_list = list()
            input_str_list = [[str(x) for x in ele] for ele in input_list]
            for ele in input_str_list:
                # ele[6] = str(int(float(ele[6]) * 1000000))
                # ele[7] = str(int(float(ele[7]) * 1000000))
                output_str_list.append(','.join(ele) + '\n')
            output_str = ''.join(list(set(output_str_list)))
            return output_str[:-1]
        else:
            pass

    # 判断当前停泊事件与暂存停泊事件是否需要合并
    # 参数输入：shipAISList -- AIS数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引；lastEndIndex -- 上次停泊结束的索引；lastStartIndex -- 上次停泊开始的索引；
    def __mergeMoor(self, shipAISList, startIndex, lastEndIndex):
        # 获取上一停泊事件结束时的经纬度
        preTime = int(shipAISList[lastEndIndex][1])
        preLon  = float(shipAISList[lastEndIndex][6])
        preLat  = float(shipAISList[lastEndIndex][7])
        # 获取当前停泊事件开始时的经纬度
        nowTime = int(shipAISList[startIndex][1])
        nowLon  = float(shipAISList[startIndex][6])
        nowLat  = float(shipAISList[startIndex][7])

        # 获取停泊事件之间的间隔距离与间隔时间
        apartDst  = getDist(lon1=preLon, lat1=preLat, lon2=nowLon, lat2=nowLat) * self.preision
        apartTime = nowTime - preTime
        # 判断是否满足合并条件
        if((apartDst <= self.mergeDst) | (apartTime <= self.mergeTime)):  # 需要合并
            # 返回值True
            mergeBool = True
        else:  # 不需要合并
            # 返回值False
            mergeBool = False
        return mergeBool

    # 将cogroupData分割出动态数据与静态数据
    # 输入参数: cogroupData -- AIS动态数据与静态数据的合并数据，键值对
    def __splitCogroup(self, cogroupData):
        # 获取主键，MMSI
        keyName = cogroupData[0]
        # 获取建值，动态数据与静态数据的合并数据
        valueList = list(cogroupData[1])
        # 初始化动态数据、静态数据的存储列表
        aisList = []
        staticList = []
        # 获取动态、静态数据
        for value in valueList:
            if value:
                valueList = list(value)
                for x in valueList:
                    if len(list(x)) == 16:
                        aisList.append(list(x))
                    elif len(list(x)) == 13:
                        staticList.append(list(x))
        return keyName, aisList, staticList

    # 根据地球两点间距离求得平均速度
    def getAvgSpeed(self, dst, detaTime):
        if (detaTime == 0):
            avgSpeed = dst / (detaTime + 1)
        else:
            avgSpeed = dst / detaTime
        return avgSpeed

    # 获取停泊事件程序段
    # 输入参数：shipAIS -- sparkRDD分组后的每个元祖
    def moorShipGroup(self, shipAIS, staticDF):
        # 将分组后的AIS数据转换为list
        groupList = list(shipAIS)
        MMSI = groupList[0]  # 船舶MMSI
        # print MMSI
        # shipAISList = list(groupList[1])  # 船舶AIS数据
        # shipAISList = np.array(groupList[1])  # 船舶AIS数据
        # 将AIS数据中的str转为整型或浮点型
        shipAISList = []
        for lineAIS in list(groupList[1]):
            lineAIS[0] = int(lineAIS[0])
            lineAIS[1] = int(lineAIS[1])
            lineAIS[6] = float(lineAIS[6]) / 1000000.
            lineAIS[7] = float(lineAIS[7]) / 1000000.
            lineAIS[9] = float(lineAIS[9])
            lineAIS[11] = float(lineAIS[11])
            lineAIS[12] = float(lineAIS[12])
            lineAIS[15] = float(lineAIS[15].split("&")[0])
            shipAISList.append([int(lineAIS[0]), int(lineAIS[1]), 2, 3, 4, 5, float(lineAIS[6]),
                                float(lineAIS[7]), 8, 9, 10, 11, 12, 13, 14, 15])
        shipAISList.sort(key=lambda v: v[1])
        shipAISList = np.array(shipAISList)
        # for x in shipAISList:
        #     print x
        #     print "-----------------------------------"
        # 初始化该船舶形成的最终停泊事件列表，暂存停泊事件索引
        tmpNavBool = False  # 判断是否存在暂存停泊事件
        tmpNavStartIndex = 0
        tmpNavEndIndex = 0
        nav_event = []
        # 获取船舶AIS数据的条数
        aisLen = len(shipAISList)
        # 判断AIS数据是否仅存在一条
        if (aisLen <= 1):  # 若AIS数据只有1条，无法形成停泊事件
            pass
        else:  # 若AIS数据大于1条，找出停泊事件
            # 初始化停泊时间窗口的左窗口
            startIndex = 0
            # 初始化上一条停泊事件的时间与索引
            pre_startIndex = 0
            pre_endIndex = 0
            # 判断停泊时间窗口开启，startIndex为窗口左端
            # startIndex从AIS数据的第一条开始循环，循环制倒数第二条
            while (startIndex < (aisLen - 1)):
                # 初始化窗口右端
                endIndex = startIndex
                # 初始化最大最小经纬度
                maxLon = shipAISList[startIndex][6]
                maxLat = shipAISList[startIndex][7]
                minLon = shipAISList[startIndex][6]
                minLat = shipAISList[startIndex][7]
                # 判断窗口右端是否需要移动
                while (endIndex < (aisLen - 1)):
                    # 获取endIndex 与 endIndex + 1的平均速度
                    tmpDst = getDist(lon1=shipAISList[endIndex][6], lat1=shipAISList[endIndex][7],
                                     lon2=shipAISList[endIndex + 1][6], lat2=shipAISList[endIndex + 1][7])
                    tmpDetaTime = shipAISList[endIndex + 1][1] - shipAISList[endIndex][1]
                    avgSpeed = self.getAvgSpeed(tmpDst, tmpDetaTime)
                    # 判断平均速度条件是否满足停泊事件的最大低速条件
                    if (avgSpeed < self.D_SPEED):  # 若满足停泊事件的低速阈值条件
                        # 找出次停泊范围内的经纬度极值
                        if maxLon < shipAISList[endIndex + 1][6]:
                            maxLon = shipAISList[endIndex + 1][6]
                        if maxLat < shipAISList[endIndex + 1][7]:
                            maxLat = shipAISList[endIndex + 1][7]
                        if minLon > shipAISList[endIndex + 1][6]:
                            minLon = shipAISList[endIndex + 1][6]
                        if minLat > shipAISList[endIndex + 1][7]:
                            minLat = shipAISList[endIndex + 1][7]
                        # 获取此范围内生成的最大距离
                        maxDst = self.preision * getDist(maxLon, maxLat, minLon, minLat)
                        # 判断是否满足停泊事件的距离阈值条件
                        if (maxDst < self.D_DST):  # 满足距离阈值条件
                            # 满足距离、速度条件，输出数据
                            # 窗口右端向右移动
                            endIndex = endIndex + 1
                            # 特殊处理部分：最后一条仍为停泊事件
                            if (endIndex == (aisLen - 1)):  # 若停泊条件且endIndex为最后一条
                                # 判断是否存在暂存停泊事件
                                if (tmpNavBool):  # 若存在暂存停泊事件
                                    # 判断暂存停泊事件与该停泊事件是否需要合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if (mergeBool):  # 若需要进行合并
                                        # 输出停泊事件，暂存停泊开始至当前停泊结束
                                        outList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=endIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(outList)
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        pre_endIndex = tmpNavEndIndex
                                        # 输出当前停泊事件
                                        outList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=tmpNavEndIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        nav_event.append(outList)
                                    # 清空暂存停泊事件
                                    tmpNavBool = False
                                else:  # 若不存在暂存停泊事件
                                    pass
                                startIndex = endIndex
                                break
                        else:  # 不满足距离阈值条件
                            if (endIndex > startIndex):  # 若已有停泊事件生成
                                # 判断是否存在暂存停泊事件
                                if (tmpNavBool):  # 若存在暂存停泊事件
                                    # 判断是否需要进行合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if (mergeBool):  # 若需要进行合并
                                        tmpNavEndIndex = endIndex
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        pre_endIndex = tmpNavEndIndex
                                        tmpNavStartIndex = startIndex
                                        tmpNavEndIndex = endIndex
                                else:  # 若不存在暂存停泊事件
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                                    tmpNavBool = True
                                startIndex = endIndex
                                break
                            else:  # 若没有生成停泊事件
                                startIndex = endIndex + 1
                                break
                    else:  # 若不满足停泊事件低速条件
                        if (endIndex > startIndex):  # 若已有停泊事件生成
                            # 判断是否存在暂存停泊事件
                            if (tmpNavBool):  # 若存在暂存停泊事件
                                # 判断是否需要进行合并
                                mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                             startIndex=startIndex,
                                                             lastEndIndex=tmpNavEndIndex)
                                if (mergeBool):  # 若需要进行合并
                                    tmpNavEndIndex = endIndex
                                else:  # 若不需要进行合并
                                    # 输出暂存停泊事件
                                    tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                          staticDF=staticDF,
                                                                          startIndex=tmpNavStartIndex,
                                                                          endIndex=tmpNavEndIndex,
                                                                          lastEndIndex=pre_endIndex)
                                    nav_event.append(tmpOutList)
                                    pre_endIndex = tmpNavEndIndex
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                            else:  # 若不存在暂存停泊事件
                                tmpNavStartIndex = startIndex
                                tmpNavEndIndex = endIndex
                                tmpNavBool = True
                            startIndex = endIndex
                            break
                        else:  # 若没有产生过停泊事件，即又窗口没有产生过，左窗口向右移动一行
                            startIndex = endIndex + 1
                            break
                # 特殊处理：当右端窗口达到倒数第二条，判断是否存在暂存停泊事件需要输出
                if (endIndex == (aisLen - 2)):
                    # 判断是否存在暂存停泊事件
                    if (tmpNavBool):  # 若存在暂存停泊事件
                        # 输出暂存停泊事件
                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                              staticDF=staticDF,
                                                              startIndex=tmpNavStartIndex,
                                                              endIndex=tmpNavEndIndex,
                                                              lastEndIndex=pre_endIndex)
                        nav_event.append(tmpOutList)
                        tmpNavBool = False
                    else:  # 若不存在暂存停泊事件
                        pass
                    startIndex = endIndex + 1
        moorStr = self.__getNavStr(nav_event)
        return moorStr

    # 获取停泊事件程序段
    # 输入参数：cogroupData -- AIS动态数据与静态数据的合并数据，键值对
    def moorShipCogroup(self, cogroupData):
        # 获取AIS动态数据、静态数据
        mmsi, shipAISList, staticDF = self.__splitCogroup(cogroupData)
        print(mmsi)
        # 将分组后的AIS数据转换为list
        # groupList = list(shipAIS)
        # shipAISList = shipAISList  # 船舶AIS数据
        staticDF = pd.DataFrame(staticDF, columns=["shipid", "time", "shiptype", "length", "width", "left",
                            "trail", "imo", "name", "callsign", "draught", "destination", "eta"])
        staticDF["shipid"] = staticDF["shipid"].astype(int)
        staticDF["time"] = staticDF["time"].astype(int)
        # shipAISList = np.array(groupList[1])  # 船舶AIS数据
        # 将AIS数据中的str转为整型或浮点型
        for lineAIS in shipAISList:
            lineAIS[0]  = int(lineAIS[0])
            lineAIS[1]  = int(lineAIS[1])
            lineAIS[6]  = float(lineAIS[6]) / 1000000.
            lineAIS[7]  = float(lineAIS[7]) / 1000000.
            lineAIS[9]  = float(lineAIS[9])
            lineAIS[11] = int(lineAIS[11])
            lineAIS[12] = int(lineAIS[12])
            lineAIS[15] = int(lineAIS[15].split("&")[0])
        # shipAISList = shipAISList.sort(key=lambda v: v[1])
        shipAISArray = np.array(shipAISList)
        # 初始化该船舶形成的最终停泊事件列表，暂存停泊事件索引
        tmpNavBool = False  # 判断是否存在暂存停泊事件
        tmpNavStartIndex = 0
        tmpNavEndIndex = 0
        nav_event = []
        # 获取船舶AIS数据的条数
        aisLen = len(shipAISList)
        # 判断AIS数据是否仅存在一条
        if(aisLen <= 1):  # 若AIS数据只有1条，无法形成停泊事件
            pass
        else:  # 若AIS数据大于1条，找出停泊事件
            # 初始化停泊时间窗口的左窗口
            startIndex = 0
            # 初始化上一条停泊事件的时间与索引
            pre_startIndex = 0
            pre_endIndex = 0
            # 判断停泊时间窗口开启，startIndex为窗口左端
            # startIndex从AIS数据的第一条开始循环，循环制倒数第二条
            while (startIndex < (aisLen - 1)):
                # 初始化窗口右端
                endIndex = startIndex
                # 初始化最大最小经纬度
                maxLon = shipAISList[startIndex][6]
                maxLat = shipAISList[startIndex][7]
                minLon = shipAISList[startIndex][6]
                minLat = shipAISList[startIndex][7]
                # 判断窗口右端是否需要移动
                while(endIndex < (aisLen - 1)):
                    # 获取endIndex 与 endIndex + 1的平均速度
                    tmpDst = getDist(lon1=shipAISList[endIndex][6], lat1=shipAISList[endIndex][7],
                                     lon2=shipAISList[endIndex + 1][6], lat2=shipAISList[endIndex + 1][7])
                    tmpDetaTime = shipAISList[endIndex + 1][1] - shipAISList[endIndex][1]
                    avgSpeed = self.getAvgSpeed(tmpDst, tmpDetaTime)
                    # 判断平均速度条件是否满足停泊事件的最大低速条件
                    if(avgSpeed < self.D_SPEED):  # 若满足停泊事件的低速阈值条件
                        # 找出次停泊范围内的经纬度极值
                        if maxLon < shipAISList[endIndex + 1][6]:
                            maxLon = shipAISList[endIndex + 1][6]
                        if maxLat < shipAISList[endIndex + 1][7]:
                            maxLat = shipAISList[endIndex + 1][7]
                        if minLon > shipAISList[endIndex + 1][6]:
                            minLon = shipAISList[endIndex + 1][6]
                        if minLat > shipAISList[endIndex + 1][7]:
                            minLat = shipAISList[endIndex + 1][7]
                        # 获取此范围内生成的最大距离
                        maxDst = self.preision * getDist(maxLon, maxLat, minLon, minLat)
                        # 判断是否满足停泊事件的距离阈值条件
                        if(maxDst < self.D_DST):  # 满足距离阈值条件
                            # 满足距离、速度条件，输出数据
                            # 窗口右端向右移动
                            endIndex = endIndex + 1
                            # 特殊处理部分：最后一条仍为停泊事件
                            if(endIndex == (aisLen - 1)):  # 若停泊条件且endIndex为最后一条
                                # 判断是否存在暂存停泊事件
                                if tmpNavBool:  # 若存在暂存停泊事件
                                    # 判断暂存停泊事件与该停泊事件是否需要合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISArray,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if mergeBool:  # 若需要进行合并
                                        # 输出停泊事件，暂存停泊开始至当前停泊结束
                                        outList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=endIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(outList)
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        pre_endIndex = tmpNavEndIndex
                                        # 输出当前停泊事件
                                        outList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=tmpNavEndIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        nav_event.append(outList)
                                    # 清空暂存停泊事件
                                    tmpNavBool = False
                                else:  # 若不存在暂存停泊事件
                                    pass
                                startIndex = endIndex
                                break
                        else:  # 不满足距离阈值条件
                            if endIndex > startIndex:  # 若已有停泊事件生成
                                # 判断是否存在暂存停泊事件
                                if tmpNavBool:  # 若存在暂存停泊事件
                                    # 判断是否需要进行合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISArray,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if mergeBool:  # 若需要进行合并
                                        tmpNavEndIndex = endIndex
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        pre_endIndex = tmpNavEndIndex
                                        tmpNavStartIndex = startIndex
                                        tmpNavEndIndex = endIndex
                                else:  # 若不存在暂存停泊事件
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                                    tmpNavBool = True
                                startIndex = endIndex
                                break
                            else:  # 若没有生成停泊事件
                                startIndex = endIndex + 1
                                break
                    else:  # 若不满足停泊事件低速条件
                        if endIndex > startIndex: # 若已有停泊事件生成
                            # 判断是否存在暂存停泊事件
                            if tmpNavBool:  # 若存在暂存停泊事件
                                # 判断是否需要进行合并
                                mergeBool = self.__mergeMoor(shipAISList=shipAISArray,
                                                             startIndex=startIndex,
                                                             lastEndIndex=tmpNavEndIndex)
                                if mergeBool:  # 若需要进行合并
                                    tmpNavEndIndex = endIndex
                                else:  # 若不需要进行合并
                                    # 输出暂存停泊事件
                                    tmpOutList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                                          staticDF=staticDF,
                                                                          startIndex=tmpNavStartIndex,
                                                                          endIndex=tmpNavEndIndex,
                                                                          lastEndIndex=pre_endIndex)
                                    nav_event.append(tmpOutList)
                                    pre_endIndex = tmpNavEndIndex
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                            else:  # 若不存在暂存停泊事件
                                tmpNavStartIndex = startIndex
                                tmpNavEndIndex = endIndex
                                tmpNavBool = True
                            startIndex = endIndex
                            break
                        else:  # 若没有产生过停泊事件，即又窗口没有产生过，左窗口向右移动一行
                            startIndex = endIndex + 1
                            break
                # 特殊处理：当右端窗口达到倒数第二条，判断是否存在暂存停泊事件需要输出
                if endIndex == (aisLen - 2):
                    # 判断是否存在暂存停泊事件
                    if tmpNavBool:  # 若存在暂存停泊事件
                        # 输出暂存停泊事件
                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISArray,
                                                              staticDF=staticDF,
                                                              startIndex=tmpNavStartIndex,
                                                              endIndex=tmpNavEndIndex,
                                                              lastEndIndex=pre_endIndex)
                        nav_event.append(tmpOutList)
                        tmpNavBool = False
                    else:  # 若不存在暂存停泊事件
                        pass
                    startIndex = endIndex + 1
        moorStr = self.__getNavStr(nav_event)
        return moorStr

    #########################################################################################
    # 判断停泊事件与多边形港口的位置关系
    # 输入参数：polyPortGDF -- 多边形港口数据分组后数据；moorLon -- 停泊事件所在经度
    # moorLat -- 停泊事件所在纬度
    def __moorPoly(self, polyPortDF, moorLon, moorLat):
        name_list = list(set(polyPortDF["portName"]))
        name_list.sort()
        for port_name in name_list:
            # 获取"多边形港口"的坐标集合
            portNameStr = port_name
            aPolyPortDF = polyPortDF[polyPortDF["portName"] == port_name]
            aPolyPortCorNum = len(aPolyPortDF)
            aPolyPortCoorList = []
            for aPolyPortDFIndex in range(aPolyPortCorNum):
                tmpPolyPortCorList = [aPolyPortDF.iloc[aPolyPortDFIndex, 0],
                                      aPolyPortDF.iloc[aPolyPortDFIndex, 1]]
                aPolyPortCoorList.append(tmpPolyPortCorList)
            # 求出"多边形港口"的中心坐标点，用平均值来求得
            lonList = [lon[0] for lon in aPolyPortCoorList]
            latList = [lat[1] for lat in aPolyPortCoorList]
            portAvgLon = sum(lonList) / len(lonList)
            portAvgLat = sum(latList) / len(latList)
            aPolyPortCoorArray = np.array(aPolyPortCoorList)
            # 判断停泊事件是否存在于次多边形内

            if point_poly(moorLon, moorLat, aPolyPortCoorArray):
                # 在原来的停泊事件字段内添加港口名称、港口经度、港口纬度数据
                moorPortList = [portNameStr, -1, -1, -1, portAvgLon, portAvgLat]
                navPolyBool = True
                return moorPortList, navPolyBool
            else:  # 不在该多边形内出现
                moorPortList = [None] * 6
                navPolyBool = False
        return moorPortList, navPolyBool

    # 获取离停泊事件发生地点最近的地点ID
    def __get_closest_port(self, close_port_df, moorLon, moorLat, min_radius):
        min_dst = 999999999.
        closest_port_name = "no port"
        closest_breth_id = -1
        closest_terminal_id = -1
        closest_port_id = -1
        closest_lon = -999
        closest_lat = -999

        # 找到距离最近的地点信息
        close_port_array = np.array(close_port_df)
        for aPort in close_port_array:
            aPortName = aPort[0]
            aPortLon = float(aPort[4])
            aPortLat = float(aPort[5])

            # 获取两点间的距离
            tmp_dst = getDist(lon1=moorLon, lat1=moorLat, lon2=aPortLon, lat2=aPortLat)
            if tmp_dst < min_dst:
                min_dst = tmp_dst
                closest_port_name = aPortName
                closest_breth_id = aPort[3]
                closest_terminal_id = aPort[2]
                closest_port_id = aPort[1]
                closest_lon = aPortLon
                closest_lat = aPortLat

        # 判断最近的地点信息是否满足距离阈值条件
        if min_dst < min_radius:  # 若满足条件
            moorPortList = [closest_port_name, closest_port_id, closest_terminal_id, closest_breth_id, closest_lon,
                            closest_lat]
            moorPointBool = True
        else:
            moorPortList = [None] * 6
            moorPointBool = False
        return moorPortList, moorPointBool


    # 判断停泊事件与点港口之间的位置关系
    # 输入参数：pointPortArray -- 点港口数据；
    # moorLon -- 停泊事件所在经度；moorLat -- 停泊事件所在纬度
    def __moorPoint(self, pointPortDF, moorLon, moorLat):
        # 根据停泊事件的经纬度坐标，获取该坐标附近区域的areaID，记作moorCloseAreaID
        moorCloseAreaID = getCloseArea(areaCenterLon=moorLon, areaCenterLat=moorLat,
                                       dst=25., grade=0.1)
        closePortDF = pointPortDF[pointPortDF["areaID"].isin(moorCloseAreaID)]

        # 找出BrethID不为-1的
        close_breth_df = closePortDF[closePortDF["BrethID"] != -1]
        if len(close_breth_df) != 0:
            close_breth_res_df, close_breth_res_bool = self.__get_closest_port(close_port_df=close_breth_df,
                                                                               moorLon=moorLon,
                                                                               moorLat=moorLat,
                                                                               min_radius=self.moorDst)
        else:
            close_breth_res_bool = False

        # 找出TerminalID不为-1的
        close_terminal_res_bool = False
        if not close_breth_res_bool:
            close_terminal_df = closePortDF[(closePortDF["TerminalID"] != -1) & (closePortDF["BrethID"] == -1)]
            if len(close_terminal_df) != 0:
                close_terminal_res_df, close_terminal_res_bool = self.__get_closest_port(close_port_df=close_terminal_df,
                                                                                         moorLon=moorLon,
                                                                                         moorLat=moorLat,
                                                                                         min_radius=self.moorDst)
            else:
                close_terminal_res_bool = False

        # 找出PortID不为-1的
        if not (close_breth_res_bool | close_terminal_res_bool):
            close_port_df = closePortDF[(closePortDF["TerminalID"] == -1) & (closePortDF["BrethID"] == -1)]
            close_port_res_df, close_port_res_bool = self.__get_closest_port(close_port_df=close_port_df,
                                                                             moorLon=moorLon,
                                                                             moorLat=moorLat,
                                                                             min_radius=self.moorDst)
            return close_port_res_df
        else:
            if close_breth_res_bool:
                return close_breth_res_df
            if close_terminal_res_bool:
                return close_terminal_res_df


    # 判断停泊事件与港口数据之间的关系
    # 输入参数：moorRDD -- 停泊事件数据；polyPort -- 多边形港口数据；pointPort -- 点港口数据
    def moorPort(self, moorRDD, polyPortDF, pointPortDF):
        # 初始化停泊事件数据列表
        moorList = []
        # 将moorRDD按行进行分割
        moorRDDList = moorRDD.split("\n")

        for moorRDDLine in moorRDDList:
            if(moorRDDLine):
                # 分割每行停泊事件数据
                moorLineList = moorRDDLine.split(",")
                # print moorLineList[0]
                # 获取停泊事件中的平均经纬度数据，areaID
                moorAvgLon = float(moorLineList[15]) / 1000000.
                moorAvgLat = float(moorLineList[16]) / 1000000.
                moorAreaID = int(float(moorLineList[28]))
                # 对每个"多边形港口"中的人工标定码头进行判断
                # 判断停泊事件与多边形港口之间的位置关系
                # moorPortList存放所在的多边形港口信息，navPolyBool判断是否在多边形港口内出现过
                moorPortList, moorPolyBool = self.__moorPoly(polyPortDF=polyPortDF,
                                                             moorLon=moorAvgLon,
                                                             moorLat=moorAvgLat)
                # 判断该停泊事件是否在多边形港口内出现过
                if(moorPolyBool):  # 若在多边形内出现，不判断点港口数据
                    pass
                else:  # 若没有在多边形港口内出现过，判断点港口数据
                    # 循环每条点港口数据
                    moorPortList = self.__moorPoint(pointPortDF=pointPortDF,
                                                    moorLon=moorAvgLon,
                                                    moorLat=moorAvgLat)
                # 判断该停泊事件是否存在于多边形港口或点港口内
                moorLineList.extend(moorPortList)
                moorList.append(moorLineList)
        moorStr = self.__getNavStr(moorList)
        return moorStr


if __name__ == "__main__":
    MASTER_HOME = "local[2]"
    conf = SparkConf()
    conf.setMaster(MASTER_HOME)
    conf.setAppName("ais_moor")
    conf.set("spark.driver.maxResultSize", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")

    sc = SparkContext(conf=conf)
    fc = format_convert()
    mr = moor()

    polyPortDF = pd.read_csv("./data/Asia_anchores.csv")
    polyPortDF.columns = ["longitude", "latitude", "portName"]

    GlobalPortDF = pd.read_csv("./data/GlobalPort.csv")

    staticDF = pd.DataFrame(columns=["shipid", "time", "shiptype", "length", "width", "left",
                                     "trail", "imo", "name", "callsign", "draught", "destination", "eta"])

    shipsAISRDD = sc.textFile("/Users/qiujiayu/Downloads/bm_ais_test.tsv") \
        .map(lambda line: fc.bm_to_thr(line)) \
        .filter(lambda line: line != None) \
        .map(lambda line: line.split(",")) \
        .groupBy(lambda v: v[0])

    navsRDD = shipsAISRDD.map(lambda group: mr.moorShipGroup(shipAIS=group, staticDF=staticDF)) \
                         .filter(lambda group: group != None)

    moorPortRDD = navsRDD.map(lambda group: mr.moorPort(moorRDD=group, pointPortDF=GlobalPortDF,
                                                        polyPortDF=polyPortDF)) \
                         .repartition(1) \
                         .saveAsTextFile("./moortest")
    sc.stop()
