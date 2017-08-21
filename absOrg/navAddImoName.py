# coding:utf-8

import pandas as pd
import numpy as np

from avgSpeed import steadyAvgSpeed
from base_func import getDist

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

# 对每条船舶的停泊进行计算，形成最终表格
# 参数说明：shipAISDF--船舶的AIS动态数据；shipNavDF--船舶停泊事件数据；shipStaticDF--船舶AIS静态数据
def getTableData(shipAISDF, shipNavDF, shipStaticDF):
    # 初始化输出列表
    outList = []
    # 获取停泊事件长度
    shipNavLen = len(shipNavDF)

    # 从第一行开始循环停泊事件至倒数第二行，根据当前索引的结束时间与下一个索引的开始时间找到航段
    for shipNavIndex in range(shipNavLen - 1):
        # 获得启航时间与航行终止时间、船舶mmsi
        # 起航时间为起始港的开始停泊事件，航行终止时间为停靠港的开始停靠时间
        uniqueID = shipNavDF.iloc[shipNavIndex, 1]
        TEU = shipNavDF.iloc[shipNavIndex, 8]
        sailStartTime = shipNavDF.iloc[shipNavIndex, 2]
        sailEndTime = shipNavDF.iloc[shipNavIndex + 1, 2]
        # 获取该航段内的航程
        sailDF = shipAISDF[(shipAISDF["unique_ID"] == uniqueID) &
                           (shipAISDF["acquisition_time"] >= sailStartTime) &
                           (shipAISDF["acquisition_time"] <= sailEndTime)]
        sailArray = np.array(sailDF)
        distance = getSailDst(lonList=np.array(sailDF["longitude"] / 1000000.),
                              latList=np.array(sailDF["latitude"] / 1000000.))
        # 得到该航段内的静态数据
        tmpStaticDF = shipStaticDF[(shipStaticDF["shipid"] == uniqueID) &
                                   (shipStaticDF["time"] >= sailStartTime) &
                                   (shipStaticDF["time"] <= sailEndTime)]
        # 获取该航段内出现过的吃水深度个数
        draughtList = list(set(tmpStaticDF.iloc[:, 10]))
        draughtLen = len(draughtList)
        # 若有且仅有一次吃水深度更新
        if(draughtLen == 2):
            tmpDraught = draughtList[1]
        else:
            tmpDraught = None

        # 设定输出数据
        IMO = shipNavDF.iloc[0, 6]
        shipName = shipNavDF.iloc[0, 7]
        MMSI = shipNavDF.iloc[0, 1]
        TEU = TEU
        startTime = shipNavDF.iloc[shipNavIndex, 3]
        startPortName = shipNavDF.iloc[shipNavIndex, 0]
        startPortLon = shipNavDF.iloc[shipNavIndex, 4]
        startPortLat = shipNavDF.iloc[shipNavIndex, 5]
        startPortingTime = (shipNavDF.iloc[shipNavIndex, 3] - shipNavDF.iloc[shipNavIndex, 2]) / (3600 * 24)
        endTime = shipNavDF.iloc[shipNavIndex + 1, 2]
        endPortName = shipNavDF.iloc[shipNavIndex + 1, 0]
        endPortLon = shipNavDF.iloc[shipNavIndex + 1, 4]
        endPortLat = shipNavDF.iloc[shipNavIndex + 1, 5]
        endPortingTime = (shipNavDF.iloc[shipNavIndex + 1, 3] - shipNavDF.iloc[shipNavIndex + 1, 2]) / (3600 * 24)
        outDistance = distance
        seaGoingTime = (endTime - startTime) / (3600 * 24)
        portTime = ((shipNavDF.iloc[shipNavIndex, 3] - shipNavDF.iloc[shipNavIndex, 2]) + \
                   (shipNavDF.iloc[shipNavIndex + 1, 3] - shipNavDF.iloc[shipNavIndex + 1, 2])) / \
                   (3600 * 24)
        outDraught = tmpDraught
        outAvgSpeed = steadyAvgSpeed.getOneShipSteadySpeed(sailArray)

        tmpList = [IMO, shipName, MMSI, TEU, startTime, startPortName, startPortLon, startPortLat, startPortingTime,
                   endTime, endPortName, endPortLon, endPortLat, endPortingTime, outDistance, seaGoingTime,
                   portTime, outDraught, outAvgSpeed]
        outList.append(tmpList)
    return outList


if __name__ == "__main__":
    # 获取1000-4000TEU船舶的AIS动态数据
    aisDF = pd.read_csv("/home/qiu/Documents/data/dynamicSpecAIS2016All.csv")
    # 获取停泊事件数据
    navEventsDF = pd.read_csv("/home/qiu/Documents/data/data/mergedNav2016All.csv")
    navEventsDF.columns = ["portName", "MMSI", "beginTime", "endTime", "portLon", "portLat"]
    navEventsDF = navEventsDF.sort_values(by=["MMSI", "beginTime"])
    # 获取船舶静态数据
    ABSStaticDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/ABSStatic.csv")
    # ABSStaticDF = ABSStaticDF.drop_duplicates()
    ABSStaticDF = ABSStaticDF.iloc[:, [0, 1, 2, 3]]
    # 合并静态数据与停泊事件数据
    navEventsDF = navEventsDF.merge(ABSStaticDF)
    navEventsDF = navEventsDF.drop_duplicates()
    navEventsDF = navEventsDF.sort_values(by=["MMSI", "beginTime"])
    # 获取1000-4000TEU船舶的AIS静态数据
    staticDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/staticData2016/"
                           "staticSpecData2016ALL.csv")

    # 输出格式定义：IMO--船舶IMO呼号；shipName--船名；MMSI--船舶MMSI；startTime--开始航行时间；
    # startPortName--起航港口名称；startPortLon--起航港口经度；startPortLat--起航港口纬度；
    # leavingStartPortDraught--离开起航港口时的吃水深度；endTime--结束航行时间；endPortName--停靠港口名称
    # endPortLon--停靠港口经度；endPortLat--停靠港口纬度；leavingEndPortDraught--离开停靠港口时的吃水深度；
    # distance--航行距离；seaGoingTime--航行时间；portTime--停泊时间；avgSpeed--平均航速
    ABSTbLsit = []
    steadyAvgSpeed = steadyAvgSpeed()

    navEventsLen = len(navEventsDF)

    # 对停泊事件数据按MMSI进行分组
    navEventsMMSIGDF = navEventsDF.groupby("MMSI")
    # 对每条船的数据分别进行分析
    for navEventsGroup in navEventsMMSIGDF:
        navGroupList = list(navEventsGroup)
        navMMSI = navGroupList[0]
        print navMMSI
        navGroupDF = navGroupList[1]
        aisGroupDF = aisDF[aisDF["unique_ID"] == navMMSI]
        staticGroupDF = staticDF[staticDF["shipid"] == navMMSI]
        groupTableList = getTableData(shipAISDF=aisGroupDF, shipNavDF=navGroupDF, shipStaticDF=staticGroupDF)
        ABSTbLsit.extend(groupTableList)
    ABSTbDF = pd.DataFrame(data=ABSTbLsit,
                           columns=["IMO", "shipName", "MMSI", "TEU", "startTime", "startPortName",
                                    "startPortLon", "startPortLat", "startPortingTime", "endTime", "endPortName",
                                    "endPortLon", "endPortLat", "endPortingTime", "distance", "seaGoingTime",
                                    "portTime", "sailDraught", "avgSpeed"])
    ABSTbDF = ABSTbDF[ABSTbDF["seaGoingTime"] > 0.0]
    print ABSTbDF.head()
    ABSTbDF.to_csv("/home/qiu/Documents/data/ABSResult/ABSResult2016_20170509.csv", index=None)
