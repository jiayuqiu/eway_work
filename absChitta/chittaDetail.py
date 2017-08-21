# coding:utf-8

import pandas as pd
import numpy as np
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

###############################################
# 注意请在最后加上斜杠
def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

if __name__ == "__main__":
    routeStr = "Colombo"

    chittaRouteDF = pd.read_csv("chittagongSailRouteDataThreeRoute.csv")
    specRouteDF = chittaRouteDF[chittaRouteDF[routeStr] == 1]

    navFilePath = "/home/qiu/Documents/data/allNavEvents/"
    navFileNameList = getFileNameList(navFilePath)
    navEventsDF = pd.DataFrame()
    for navFileName in navFileNameList:
        tmpNavDF = pd.read_csv(navFilePath + navFileName)
        navEventsDF = navEventsDF.append(tmpNavDF)
    specNavEventsDF = navEventsDF[navEventsDF["unique_ID"].isin(specRouteDF["MMSI"])]

    chittaPolyDF = pd.read_csv("/home/qiu/Documents/data/chittaPoly.csv")
    chittaPolyGDF = chittaPolyDF.groupby("portName")
    chittaNavList = []
    for chittaPolyGroup in chittaPolyGDF:
        chiitaPolyGroupList = list(chittaPolyGroup)
        chiitaPolyName = chiitaPolyGroupList[0]
        print chiitaPolyName
        chiitaPolyDF = chiitaPolyGroupList[1]
        chiitaPolyArray = np.array(chiitaPolyDF)
        for navIndex, navValue in specNavEventsDF.iterrows():
            if(point_poly(pointLon=(navValue["begin_longitude"]/1000000.),
                          pointLat=(navValue["begin_latitude"]/1000000.),
                          polygon=chiitaPolyArray)):
                portName = chiitaPolyName
                MMSI = navValue["unique_ID"]
                beginTime = navValue["begin_time"]
                endTime = navValue["end_time"]
                interval = (endTime - beginTime) / 3600.
                chittaNavList.append([portName, MMSI, beginTime, endTime, interval])
    chittaNavDF = pd.DataFrame(chittaNavList)
    chittaNavDF.columns = ["portName", "MMSI", "beginTime", "endTime", "interval"]
    chittaNavDF.to_csv("/home/qiu/Documents/data/ABSResult/%s.csv" % routeStr, index=None)
