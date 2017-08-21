# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    data = pd.read_csv("./data/avgSpeedPaintData.csv")
    tmpDF = data[(data["ABSTEU"] >= 1000) & (data["ABSTEU"] <= 1999)]

    draughtSpeedList = []

    for index, row in tmpDF.iterrows():
        tmpRow = (int(row[3]), int(row[4] + 0.5))
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
    countDataDF.to_csv("./data/countData.csv", index=None)
