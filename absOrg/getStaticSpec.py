# coding:utf-8

import pandas as pd

# 注意请在最后加上斜杠
def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

if __name__ == "__main__":
    filePath = "./data/staticData/staticData2016/"
    fileNameList = getFileNameList(filePath=filePath)

    ABSStaticDF = pd.read_excel("./data/staticData/"
                                "1000 to 4000 TEU Container Carrier List.xlsx")

    ABSStaticDF = ABSStaticDF.iloc[:, [0, 1, 2]]
    ABSStaticDF.columns = ["IMO", "ABSTEU", "ABSShipName"]

    staticData2016 = pd.DataFrame()
    for fileName in fileNameList:
        print fileName
        tmpStaticDF = pd.read_csv(filePath + fileName)
        tmpStaticDF.columns = ["shipid", "time", "shiptype", "length", "width", "left",
                               "trail", "imo", "name", "callsign", "draught", "destination", "eta"]
        tmpStaticDF = tmpStaticDF[tmpStaticDF["imo"].isin(ABSStaticDF.loc[:, "IMO"])]

        staticData2016 = staticData2016.append(tmpStaticDF)
    staticData2016 = staticData2016.drop_duplicates()
    staticData2016.to_csv("./data/staticData/staticData2016/staticSpecData2016ALL.csv", index=None)
