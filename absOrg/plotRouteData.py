# coding:utf-8

import pandas as pd
from base_func import getFileNameList

if __name__ == "__main__":
    aisDF = pd.read_csv("/home/qiu/Documents/data/data/dynamicData/dynamicData2016/dynamicSpecAIS201609.csv")
    aisDF = aisDF.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]

    resultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult2016.csv")
    resultDF = resultDF.loc[:, ["MMSI", "startTime", "endTime"]]

    plotRouteData = pd.DataFrame()
    resultLen = len(resultDF)
    paintID = 0

    for index, value in resultDF.iterrows():
        print "(%d/%d)" % (index, resultLen)
        MMSI = value["MMSI"]
        startTime = value["startTime"]
        endTime = value["endTime"]

        tmpDF = aisDF[(aisDF["unique_ID"] == MMSI) & (aisDF["acquisition_time"] >= startTime) &
                      (aisDF["acquisition_time"] <= endTime)]
        tmpDF["paintID"] = paintID
        tmpDFLen = len(tmpDF)
        for index in range(tmpDFLen - 1):
            if(abs(tmpDF.iloc[index, 2] - tmpDF.iloc[index + 1, 2]) > (200*1000000)):
                print tmpDF
                paintID += 1
                tmpDF.iloc[(index + 1):, 4] = paintID
            else:
                pass
        paintID += 1
        plotRouteData = plotRouteData.append(tmpDF)
    plotRouteData.to_csv("/home/qiu/Documents/data/plotRouteData.csv", index=None)
