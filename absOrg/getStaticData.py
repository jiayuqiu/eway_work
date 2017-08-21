# coding:utf-8

import pandas as pd
import sqlite3
import os

# 注意请在最后加上斜杠
def getFileNameList(filePath):
    fileNameList = []
    for fileName in os.listdir(filePath):
        fileNameList.append(fileName)
    return fileNameList

if __name__ == "__main__":
    filePath = "/Volumes/qiuSeagate/船讯网数据/亿海蓝数据/ShipTrack07-09/"
    fileNameList = getFileNameList(filePath)
    print fileNameList

    for fileName in fileNameList:
        print fileName
        outFileNameStr = fileName.split(".db")[0]
        conn = sqlite3.connect(filePath + fileName)
        cursor = conn.execute("SELECT * FROM static")
        staticData = []

        for row in cursor:
            unique_ID = row[0]
            acq_time = row[1]
            shiptype = row[2]
            length = row[3]
            width = row[4]
            left = row[5]
            trail = row[6]
            imo = row[7]
            name = row[8]
            callsign = row[9]
            draught = row[10]
            destination = str(row[11]).replace(",", " ")
            eta = row[12]

            tmp = [unique_ID, acq_time, shiptype, length, width, left,
                   trail, imo, name, callsign, draught, destination, eta]
            staticData.append(tmp)
        staticDataDF = pd.DataFrame(staticData)
        staticDataDF.to_csv("./data/staticData/staticData2016/static_%s" % outFileNameStr, index=None)
        conn.close()
