# coding:utf-8

import pandas as pd
import time

from base_func import getFileNameList

def convertPortName(orgPortName):
    excPortName = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                   "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                   "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                   "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08",
                   "hongkong03", "busan03", "singapore03", "newjersey03", "rotterdam04"]
    if(orgPortName in excPortName):
        outPortName = ""
        for letter in orgPortName:
            if(not letter.isdigit()):
                outPortName = outPortName + letter
            else:
                pass
        return outPortName.capitalize()
    else:
        return orgPortName

def convertResult(resultDF):
    optMergeList = []
    IDGDF = resultDF.groupby("MMSI")
    for IDGroup in IDGDF:
        IDList = list(IDGroup)
        ID = IDList[0]
        print ID
        IDDF = IDList[1]
        IDDFLen = len(IDDF)

        IDIndex = 0
        while (IDIndex < (IDDFLen - 1)):
            prePortName = IDDF.iloc[IDIndex, 5]
            prePortNameCon = convertPortName(prePortName)
            nextPortName = IDDF.iloc[IDIndex, 10]
            nextPortNameCon = convertPortName(nextPortName)
            if ((prePortNameCon == nextPortNameCon) & (IDDF.iloc[IDIndex, 14] <= 40.)):
                if(IDIndex != 0):
                    startMergeIndex = IDIndex - 1
                else:
                    startMergeIndex = IDIndex
                endMergeIndex = IDIndex + 1
                while(endMergeIndex < IDDFLen):
                    if(endMergeIndex != (IDDFLen - 1)):
                        tmpDst = sum(IDDF.iloc[startMergeIndex:(endMergeIndex + 1), 14])
                        if((tmpDst < 40.) & (prePortName == IDDF.iloc[endMergeIndex, 10])):
                            endMergeIndex += 1
                        else:
                            break
                    else:
                        break

                startTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(IDDF.iloc[startMergeIndex, 4]))
                endTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(IDDF.iloc[startMergeIndex, 9]))
                endPortName = IDDF.iloc[endMergeIndex, 10]
                endPortNameCon = convertPortName(endPortName)
                tmpList = [IDDF.iloc[startMergeIndex, 0], IDDF.iloc[startMergeIndex, 1],
                           IDDF.iloc[startMergeIndex, 2], IDDF.iloc[startMergeIndex, 3],
                           startTime, prePortNameCon, IDDF.iloc[startMergeIndex, 6],
                           IDDF.iloc[startMergeIndex, 7], IDDF.iloc[startMergeIndex, 8], endTime, endPortNameCon,
                           IDDF.iloc[endMergeIndex, 11], IDDF.iloc[endMergeIndex, 12],
                           sum(IDDF.iloc[startMergeIndex:(endMergeIndex + 1), 13]),
                           sum(IDDF.iloc[startMergeIndex:(endMergeIndex + 1), 14]) * 0.5399568,
                           sum(IDDF.iloc[startMergeIndex:(endMergeIndex + 1), 15]),
                           sum(IDDF.iloc[startMergeIndex:(endMergeIndex + 1), 8]) + IDDF.iloc[endMergeIndex, 13],
                           IDDF.iloc[startMergeIndex, 17], IDDF.iloc[startMergeIndex, 18]]
                optMergeList.append(tmpList)
                IDIndex = endMergeIndex
                IDIndex += 1
            else:
                startTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(IDDF.iloc[IDIndex, 4]))
                endTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(IDDF.iloc[IDIndex, 9]))
                tmpList = [IDDF.iloc[IDIndex, 0], IDDF.iloc[IDIndex, 1], IDDF.iloc[IDIndex, 2],
                           IDDF.iloc[IDIndex, 3], startTime, prePortNameCon, IDDF.iloc[IDIndex, 6],
                           IDDF.iloc[IDIndex, 7], IDDF.iloc[IDIndex, 8], endTime,
                           nextPortNameCon, IDDF.iloc[IDIndex, 11], IDDF.iloc[IDIndex, 12],
                           IDDF.iloc[IDIndex, 13], IDDF.iloc[IDIndex, 14] * 0.5399568, IDDF.iloc[IDIndex, 15],
                           IDDF.iloc[IDIndex, 16], IDDF.iloc[IDIndex, 17], IDDF.iloc[IDIndex, 18]]
                optMergeList.append(tmpList)
            IDIndex += 1

    return optMergeList

def checkMerge(resultDF):
    for index, value in resultDF.iterrows():
        if((value["startPortName"] == value["endPortName"]) & (value["distance"] < (40. * 0.5399568))):
            print value["startPortName"]
            print value["endPortName"]
            print value["distance"]
            print value["MMSI"]

if __name__ == "__main__":
    resultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSResult2016_20170509.csv")
    # resultDF = resultDF[resultDF["MMSI"] == 235050811]
    print len(resultDF)

    optMergeList = convertResult(resultDF)
    optMergeDF = pd.DataFrame(optMergeList)
    optMergeDF.columns = ["IMO", "shipName", "MMSI", "TEU", "startTime", "startPortName",
                          "startPortLon", "startPortLat", "startPortingTime", "endTime", "endPortName",
                          "endPortLon", "endPortLat", "endPortingTime", "distance", "seaGoingTime",
                          "portTime", "sailDraught", "avgSpeed"]
    raw_input("==============convert done!================")
    print len(optMergeDF)
    # optMergeDF = optMergeDF[(optMergeDF["startPortName"] != optMergeDF["endPortName"]) |
    #                         (optMergeDF["distance"] > (40 * 0.5399568))]
    checkMerge(optMergeDF)
    print len(optMergeDF)
    tmpDF = optMergeDF[(optMergeDF["startPortName"] == optMergeDF["endPortName"]) &
                            (optMergeDF["distance"] > (40 * 0.5399568))]
    print len(tmpDF)
    optMergeDF.to_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv", index=None)
