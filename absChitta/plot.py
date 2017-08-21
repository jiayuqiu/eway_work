# coding:utf-8

import pandas as pd
import numpy as np

###############################################
# 注意请在最后加上斜杠
def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

##############################################
# 获得航线上的visit time等数据
def getRouteTbData(routeData, routeName):
    if(routeName != "other"):
        routeDF = routeData[routeData[routeName] == 1]
    else:
        routeDF = routeData[(routeData["Singapore & North Port"] == 0) &
                            (routeData["Tanjung Bin"] == 0) &
                            (routeData["Colombo"] == 0)]
    visitTimeList = []
    durationTimeList = []
    cargoList = []
    for index, value in routeDF.iterrows():
        TEU = value["TEU"]
        visitTime = value["navNum"]
        durationTime = value["avgSailTime"] / (24 * 60 * 60)
        cargo = TEU * value["avgSailDst"]

        visitTimeList.append(visitTime)
        durationTimeList.append(durationTime)
        cargoList.append(cargo)
    sumVisitTime = sum(visitTimeList)
    sumDurationTime = np.mean(durationTimeList)
    sumCargo = sum(cargoList)
    print sumVisitTime, sumDurationTime, sumCargo

#####################################################
# 获取分船型的每日航行距离
def getDaySailDst(routeData, routeName, TEUList):
    if (routeName != "other"):
        routeDF = routeData[(routeData[routeName] == 1) & (routeData["TEU"] >= TEUList[0]) &
                            (routeData["TEU"] <= TEUList[1])]
    else:
        routeDF = routeData[(routeData["Singapore & North Port"] == 0) &
                            (routeData["Tanjung Bin"] == 0) &
                            (routeData["Colombo"] == 0) & (routeData["TEU"] >= TEUList[0]) &
                            (routeData["TEU"] <= TEUList[1])]
    print len(routeDF)
    avgSpeedList = []
    for index, value in routeDF.iterrows():
        sailDst = value["avgSailDst"]
        sailTime = value["avgSailTime"] / (24 * 60 * 60)
        avgSpeed = sailDst / sailTime
        avgSpeedList.append(avgSpeed)
    dayAvgSpeed = np.mean(avgSpeedList)
    print dayAvgSpeed, TEUList, routeName

##############################################
#

if __name__ == "__main__":
    # navFilePath = "/home/qiu/Documents/data/allNavEvents/"
    # navFileNameList = getFileNameList(navFilePath)
    # navEventsDF = pd.DataFrame()
    # for navFileName in navFileNameList:
    #     print navFileName
    #     tmpNavDF = pd.read_csv(navFilePath + navFileName)
    #     navEventsDF = navEventsDF.append(tmpNavDF)
    # navEventsDF.to_csv("navEventsAll.csv", index=None)

    routeData = pd.read_csv("chittagongSailRouteDataThreeRoute.csv")
    getDaySailDst(routeData=routeData, routeName="Singapore & North Port", TEUList=[3000, 4000])
    # getRouteTbData(routeData=routeData, routeName="other")
    # ColomboDF = routeData[routeData["Colombo"] == 1]
    # TBDF = routeData[routeData["Tanjung Bin"] == 1]
    # SNDF = routeData[routeData["Singapore & North Port"] == 1]
    # UnknewDF = routeData[(routeData["Singapore & North Port"] == 0) &
    #                        (routeData["Tanjung Bin"] == 0) &
    #                        (routeData["Colombo"] == 0)]


    # ColomboMMSI = routeData[routeData["Colombo"] == 1]["MMSI"]
    # TBMMSI = routeData[routeData["Tanjung Bin"] == 1]["MMSI"]
    # SNMMSI = routeData[routeData["Singapore & North Port"] == 1]["MMSI"]
    # UnknewMMSI = routeData[(routeData["Singapore & North Port"] == 0) &
    #                        (routeData["Tanjung Bin"] == 0) &
    #                        (routeData["Colombo"] == 0)]["MMSI"]


    # aisDF = pd.read_csv("/home/qiu/Documents/data/data/dynamicData/dynamicData2016/dynamicSpecAIS2016All.csv")
    # aisDF = aisDF[aisDF["unique_ID"].isin(UnknewMMSI)]
    # aisDF.to_csv("UnkonwAIS.csv", index=None)
    print "done!"
