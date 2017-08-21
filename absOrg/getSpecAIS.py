# coding:utf-8

import pandas as pd

def convertDate2Str(date):
    if (date < 10):
        date = "0" + str(date)
    else:
        date = str(date)
    return date

# 注意请在最后加上斜杠
def getFileNameList(filePath):
    import os

    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

if __name__ == "__main__":
    ABSStaticDF = pd.read_csv("./data/staticData/ABSStatic.csv")
    # ABSStaticDF = ABSStaticDF.drop_duplicates()
    ABSStaticDF = ABSStaticDF.iloc[:, [0, 1, 2, 3]]
    print ABSStaticDF.head()

    ABSSpecAISDF = pd.DataFrame()
    # for date07 in range(4, 32):
    #     date07 = convertDate2Str(date07)
    #     print "07" + date07
    #     tmpAISDF = pd.read_csv("/Volumes/qiuSeagate/201607ships/ships_201607%s.csv" % date07)
    #     tmpAISDF = tmpAISDF[tmpAISDF["unique_ID"].isin(ABSStaticDF["MMSI"])]
    #     print len(tmpAISDF)
    #     ABSSpecAISDF = ABSSpecAISDF.append(tmpAISDF)

    # for date08 in range(1, 32):
    #     date08 = convertDate2Str(date08)
    #     print "08" + date08
    #     tmpAISDF = pd.read_csv("/Volumes/qiuSeagate/201608/ships_201608%s.csv" % date08)
    #     tmpAISDF = tmpAISDF[tmpAISDF["unique_ID"].isin(ABSStaticDF["MMSI"])]
    #     print len(tmpAISDF)
    #     ABSSpecAISDF = ABSSpecAISDF.append(tmpAISDF)

    for date09 in range(1, 31):
        date09 = convertDate2Str(date09)
        print "09" + date09
        tmpAISDF = pd.read_csv("/Volumes/qiuSeagate/201609/ships_201609%s.csv" % date09)
        tmpAISDF = tmpAISDF[tmpAISDF["unique_ID"].isin(ABSStaticDF["MMSI"])]
        print len(tmpAISDF)
        ABSSpecAISDF = ABSSpecAISDF.append(tmpAISDF)

    ABSSpecAISDF.to_csv("./data/dynamicData/dynamicData2016/dynamicSpecAIS201609.csv", index=None)
