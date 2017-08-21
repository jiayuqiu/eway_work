# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    resultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv")
    print resultDF.head()
    raw_input("==========")
    avgSpeedDF = resultDF[~resultDF["avgSpeed"].isnull()].loc[:, ["IMO", "sailDraught", "avgSpeed"]]

    ABSStaticDF = pd.read_csv("./data/staticData/ABSStatic.csv")
    IMOTEUDF = ABSStaticDF.loc[:, ["IMO", "ABSTEU"]]

    avgSpeedWithTEUDF = avgSpeedDF.merge(IMOTEUDF)
    avgSpeedWithTEUDF = avgSpeedWithTEUDF.drop_duplicates()
    avgSpeedWithTEUDF = avgSpeedWithTEUDF[(avgSpeedWithTEUDF["ABSTEU"] >= 3000) & (avgSpeedWithTEUDF["ABSTEU"] <= 3999)]

    draughtSpeedList = []

    for index, row in avgSpeedWithTEUDF.iterrows():
        tmpRow = (int(row[1]), int(row[2] + 0.5))
        draughtSpeedList.append(tmpRow)
    itemSet = set(draughtSpeedList)

    countDataList = []
    for item in itemSet:
        countNum = draughtSpeedList.count(item)
        draught = list(item)[0]
        speed = list(item)[1]
        print draught, speed, countNum
        countDataList.append([draught, speed, countNum])
    countDataDF = pd.DataFrame(countDataList)
    countDataDF.columns = ["draught", "speed", "count"]
    countDataDF.to_csv("./data/countData(3000-4000).csv", index=None)
