# coding:utf-8

import pandas as pd
import numpy as np

def groupDraught(countDataDF):
    groupDraughtList = []
    for index, value in countDataDF.iterrows():
        groupNum = value["draught"] / 500
        countDataDF.loc[index, "group"] = str(groupNum * 0.5) + "-" + str((groupNum + 1) * 0.5)
    countDataGDF = countDataDF.groupby("group")
    for group in countDataGDF:
        groupList = list(group)
        groupName = groupList[0]
        groupDF = groupList[1]

        avgSpeed = np.mean(groupDF["speed"])
        count = sum(groupDF["count"])
        groupDraughtList.append([groupName, avgSpeed, count])
    return groupDraughtList

if __name__ == "__main__":
    resultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv")

    avgSpeedDF = resultDF[~resultDF["sailDraught"].isnull()].loc[:, ["TEU", "sailDraught", "avgSpeed"]]
    avgSpeedDF = avgSpeedDF[~avgSpeedDF["avgSpeed"].isnull()]
    avgSpeedDF = avgSpeedDF[(avgSpeedDF["TEU"] >= 3000) & (avgSpeedDF["TEU"] <= 4000)]

    draughtSpeedList = []
    for index, row in avgSpeedDF.iterrows():
        tmpRow = (int(row[1]), int(row[2] + 0.5))
        draughtSpeedList.append(tmpRow)
    itemSet = set(draughtSpeedList)

    countDataList = []
    for item in itemSet:
        countNum = draughtSpeedList.count(item)
        draught = list(item)[0]
        speed = list(item)[1]
        # print draught, speed, countNum
        countDataList.append([draught, speed, countNum])
    countDataDF = pd.DataFrame(countDataList)
    countDataDF.columns = ["draught", "speed", "count"]
    print "====================================="
    groupDraughtList = groupDraught(countDataDF)
    groupDraughtDF = pd.DataFrame(groupDraughtList)
    groupDraughtDF.columns = ["group", "avgSpeed", "count"]
    print groupDraughtDF.head()
    countDataDF.to_csv("/home/qiu/Documents/data/ABSResult/avgPaintDataOptUpdate(3000-4000).csv", index=None)
