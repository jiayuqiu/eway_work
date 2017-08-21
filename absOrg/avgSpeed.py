# coding:utf-8

import pandas as pd
import numpy as np
import time

from base_func import getDist
from getStaticData import getFileNameList

# 获取船舶平稳状态下的平均速度
class steadyAvgSpeed(object):
    def __init__(self):
        self.minSlowAvgSpeed = 5 * 0.514444  # 最小平均速度
        self.minInstSpeed = 5 * 0.514444  # 最小瞬时速度
        self.minA = 0.514444 * 0.01  # 最小加速度

    # 获取稳定速度
    # 输入参数：sailAISArray -- numpy的矩阵类型，存放一条船的AIS数据
    def getOneShipSteadySpeed(self, sailAISArray):
        # 获取一条船舶AIS数据的长度
        oneShipLen = len(sailAISArray)
        # 初始化平稳状态下的AIS数据索引
        steadyIndexList = []
        if(oneShipLen > 1):  # 若一条船的AIS数据条数大于1条
            # 循环每条AIS数据，循环至倒数第二条
            for aisIndex in range(0, (oneShipLen - 1)):
                # 获取第i条与第i+1条AIS数据之间，地球间两点间距离，单位：千米
                tmpDst = getDist(lon1=(sailAISArray[aisIndex, 5])/1000000.,
                                 lat1=(sailAISArray[aisIndex, 6])/1000000.,
                                 lon2=(sailAISArray[aisIndex + 1, 5])/1000000.,
                                 lat2=(sailAISArray[aisIndex + 1, 6])/1000000.)
                tmpDst = tmpDst * 1000  # 将单位从千米转换为米
                # 第i条与第i+1条AIS数据之间的时间差，单位：秒
                detaTime = sailAISArray[aisIndex + 1, 1] - sailAISArray[aisIndex, 1]
                if (detaTime == 0):  # 若时间差为0，则设置时间差为1s
                    detaTime = detaTime + 1
                # 利用距离除以时间，获取第i行与第i+1行时间的平均速度
                avgSpeed = tmpDst / detaTime
                # 获取第i行与第i+1行的瞬时速度
                preInstSpeed = (sailAISArray[aisIndex, 8]) * 0.001
                nowInstSpeed = (sailAISArray[aisIndex + 1, 8]) * 0.001
                # 获取第i行与第i+1行之间的加速度
                tmpA = preInstSpeed - nowInstSpeed
                detaA = tmpA / detaTime
                # 判断是否在满足平均速度、加速度、瞬时速度条件
                avgSpeedBool = (avgSpeed > self.minSlowAvgSpeed)
                detaABool = (detaA < self.minA)
                instSpeedBool = (preInstSpeed > self.minInstSpeed) & (nowInstSpeed > self.minInstSpeed)

                if(avgSpeedBool & detaABool & instSpeedBool):  # 若满足上述平均速度、加速度、瞬时速度条件。即为平稳状态
                    # 讲第i行与第i+1行的索引进行记录
                    steadyIndexList.append(aisIndex)
                    steadyIndexList.append(aisIndex + 1)
                else:  # 若不满足平均速度、加速度、瞬时速度条件。不做任何处理
                    pass
            # 将平稳状态下的索引去重
            steadyIndexList = list(set(steadyIndexList))

            if(len(steadyIndexList) > 1):  # 若平稳状态的数据条数大于1条
                # 获取对应索引的所有瞬时速度
                steadySpeedList = sailAISArray[steadyIndexList, 8]
                # 求得所有瞬时速度的平均值
                steadySpeed = (sum(steadySpeedList) / len(steadySpeedList)) * 0.001 / 0.514444
                # 返回平均速度数据
                return steadySpeed
            else:
                pass
        else:  # 若船舶的AIS数据仅有1条，不做任何处理
            pass


if __name__ == "__main__":
    # 获取1000-4000TEU船舶的AIS动态数据
    aisDF = pd.read_csv("./data/dynamicData/specDF09.csv")
    # 获取1000-4000TEU船舶的AIS静态数据
    staticDF = pd.read_csv("./data/staticData/staticSpecDF09.csv")
    # 获取1000-4000TEU船舶的停泊信息
    navEventsDF = pd.read_csv("./data/mergedNav.csv")
    navEventsDF.columns = ["portName", "unique_ID", "beginTime", "endTime", "portLon", "portLat"]
    navEventsDF = navEventsDF.sort_values(by=["unique_ID", "beginTime"])

    steadySPeedList = []

    # 获取计算平均航速类
    getAvgSpeed = steadyAvgSpeed()

    # 对停泊信息中的unique_ID信息进行分组
    navEventsIDGDF = navEventsDF.groupby("unique_ID")
    for navEventsIDGroup in navEventsIDGDF:
        # 将group的数据转换为list进行利用
        navEventsOneIDList = list(navEventsIDGroup)
        print navEventsOneIDList[0]  # 回显mmsi
        # 获取一条船舶的停泊事件
        navEventsOneIDDF = navEventsOneIDList[1]
        navEventsOneIDArray = np.array(navEventsOneIDDF)
        # 获取每条船停泊事件的个数
        navEventsOneIDLen = len(navEventsOneIDArray)

        # 循环每个船舶的停泊事件，循环至倒数第二条
        for navEventsOneIDIndex in range(navEventsOneIDLen - 1):
            # 获得启航时间与航行终止时间、船舶mmsi
            # 起航时间为起始港的开始停泊事件，航行终止时间为停靠港的开始停靠时间
            unique_ID = navEventsOneIDArray[navEventsOneIDIndex, 1]
            sailStartTime = navEventsOneIDArray[navEventsOneIDIndex, 2]
            sailEndTime = navEventsOneIDArray[navEventsOneIDIndex + 1, 2]
            # 找出这段时间内的静态数据
            sailStaticDF = staticDF[(staticDF["shipid"] == unique_ID) & (staticDF["time"] >= sailStartTime) &
                                    (staticDF["time"] <= sailEndTime)]
            # 获取该航段内出现过的吃水深度个数
            draughtList = list(set(sailStaticDF.iloc[:, 10]))
            droughtLen = len(draughtList)
            # 有且仅更新一次时，求得这段航程的平均航速
            if(droughtLen == 2):
                # 获取吃水深度更新事件
                upDatedDraught = draughtList[1]
                upDatedStaticDF = sailStaticDF[sailStaticDF["draught"] == upDatedDraught]
                upDatedTime = min(upDatedStaticDF.iloc[:, 1])

                # 获取航行时的AIS动态数据
                sailAISDF = aisDF[(aisDF["unique_ID"] == unique_ID) & (aisDF["acquisition_time"] >= sailStartTime) &
                                  (aisDF["acquisition_time"] <= sailEndTime)]
                sailAISArray = np.array(sailAISDF)
                # 计算平均航速
                avgSpeed = getAvgSpeed.getOneShipSteadySpeed(sailAISArray)
                tmp = [unique_ID, upDatedDraught, avgSpeed]
                steadySPeedList.append(tmp)
    steadySPeedDF = pd.DataFrame(steadySPeedList)
    steadySPeedDF.columns = ["unique_ID", "draught", "sailSpeed"]
    print steadySPeedDF.head()
    steadySPeedDF = steadySPeedDF[~steadySPeedDF["sailSpeed"].isnull()]
