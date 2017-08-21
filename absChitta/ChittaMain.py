# coding:utf-8

import pandas as pd

def timeStrToInt(timeStr):
    import time
    timeArray = time.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp

if __name__ == "__main__":
    ABSOptResultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv")
    # chittaResultDF = ABSOptResultDF[(ABSOptResultDF["startPortName"] == "Chittagong") |
    #                                 (ABSOptResultDF["endPortName"] == "Chittagong")]

    chittaDataList = []
    # 对MMSI进行分组
    ABSIDGDF = ABSOptResultDF.groupby("MMSI")
    # 循环每组数据
    for ABSIDgroup in ABSIDGDF:
        # 将group转换为list
        ABSIDList = list(ABSIDgroup)
        # 获取船舶MMSI
        shipID = ABSIDList[0]
        print shipID
        # 获取一条船的数据
        ABSIDDF = ABSIDList[1]
        # 获取在吉大港停泊过的数据
        chittaIDDF = ABSIDDF[(ABSIDDF["startPortName"] == "Chittagong")]
        # 若在吉大港发生过停泊
        if(len(chittaIDDF) >= 2):
            MMSI = shipID  # 船舶MMSI
            TEU = ABSIDDF.iloc[0, 3]  # 船舶TEU
            # 获取起始港口为吉大港的数据
            startChittaDF = ABSIDDF[ABSIDDF["startPortName"] == "Chittagong"]
            # 航程次数
            sailRouteNum = len(startChittaDF) - 1
            # 获取第一个起始港为吉大港的索引，最后一个终点港为吉大港的索引
            chittaStartIndex = ABSIDDF[ABSIDDF["startPortName"] == "Chittagong"].index[0]
            chittaEndIndex = ABSIDDF[ABSIDDF["endPortName"] == "Chittagong"].index[-1]
            ABSIDDFIndexList = ABSIDDF.index
            if(chittaEndIndex == ABSIDDFIndexList[-1]):
                sailRouteNum += 1
            # 获取总航程距离
            allSailDst = sum(ABSIDDF.loc[chittaStartIndex:chittaEndIndex, "distance"])
            # 获取总航行时间
            allSailTime = timeStrToInt(ABSIDDF.loc[chittaEndIndex, "endTime"]) - \
                          timeStrToInt(ABSIDDF.loc[chittaStartIndex, "startTime"])
            # 获取一个闭合航线的平均航行时间
            avgSailTime = allSailTime / sailRouteNum
            # 获取一个闭合航线的平均航程
            avgSailDst = (allSailDst / sailRouteNum)
            # 获取经过的码头列表
            crossPortList = ABSIDDF.loc[chittaStartIndex:chittaEndIndex, "startPortName"]
            crossPortStr = ""
            for eachPortStr in crossPortList:
                crossPortStr = crossPortStr + eachPortStr + "*"
            crossPortStr = crossPortStr + "Chittagong"
            tmpList = [MMSI, TEU, sailRouteNum, avgSailDst, avgSailTime, crossPortStr]
            chittaDataList.append(tmpList)
    chittaDataDF = pd.DataFrame(chittaDataList)
    chittaDataDF.columns = ["MMSI", "TEU", "navNum", "avgSailDst", "avgSailTime", "throughPort"]
    chittaDataDF.to_csv("chittagongSailRouteData.csv", index=None)
    print chittaDataDF.head()
