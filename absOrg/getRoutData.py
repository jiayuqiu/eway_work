# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    ###########################
    # 获得航线的数据
    # aisDF = pd.read_csv("./data/dynamicData/dynamicData2016/dynamicSpecAIS201609.csv")
    #
    # ABSResultDF = pd.read_csv("ABSResult2016.csv")
    # ABSResultDF = ABSResultDF[ABSResultDF["startTime"] >= 1472659200]
    #
    # resultGDF = ABSResultDF.groupby(by=["startPortName", "endPortName"])
    # paintID = 0
    # portNameList = []
    # routePaintDF = pd.DataFrame()
    # for group in resultGDF:
    #     gList = list(group)
    #     portName1 = list(gList[0])[0]
    #     portName2 = list(gList[0])[1]
    #     bool1 = [portName1, portName2] in portNameList
    #     bool2 = [portName2, portName1] in portNameList
    #
    #     if(not(bool1 | bool2)):
    #         print portName1, portName2
    #         print bool1, bool2
    #         portNameList.append([portName1, portName2])
    #         portNameList.append([portName2, portName1])
    #         resultDF1 = gList[1]
    #         resultDF2 = ABSResultDF[(ABSResultDF["endPortName"] == portName1) &
    #                                 (ABSResultDF["startPortName"] == portName2)]
    #         size1 = len(resultDF1)
    #         size2 = len(resultDF2)
    #         newSize = size1 + size2
    #
    #         MMSI = resultDF1.iloc[0, 2]
    #         startTime = resultDF1.iloc[0, 3]
    #         endTime = resultDF1.iloc[0, 7]
    #
    #         routeData = aisDF[(aisDF["unique_ID"] == MMSI) &
    #                           (aisDF["acquisition_time"] >= startTime) &
    #                           (aisDF["acquisition_time"] <= endTime)]
    #         routeData = routeData.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    #         routeData["portName1"] = portName1
    #         routeData["portName2"] = portName2
    #         routeData["size"] = newSize
    #         routePaintDF = routePaintDF.append(routeData)
    # print routePaintDF.head()


    ############################
    # 将单项航线的数据合并为双向
    # data = pd.read_csv("routeData2016All.csv")
    # print data.head()
    #
    # outData = pd.DataFrame(data=None, columns=["PortName1", "PortName2", "longitude", "latitude",
    #                                            "paintID", "size"])
    # paintID = 0
    # portNameList = []
    # for index, value in data.iterrows():
    #     portName1 = value.iloc[2]
    #     portName2 = value.iloc[3]
    #
    #     # bool1 = (portName1 in outData["PortName1"]) & (portName2 in outData["PortName2"])
    #     # bool2 = (portName2 in outData["PortName1"]) & (portName1 in outData["PortName2"])
    #     bool1 = [portName1, portName2] in portNameList
    #     bool2 = [portName2, portName1] in portNameList
    #     if(not (bool1 & bool2)):
    #         portNameList.append([portName1, portName2])
    #         portNameList.append([portName2, portName1])
    #
    #         size1 = data[(data["startPortName"] == portName1) & (data["endPortName"] == portName2)]["size"]
    #         size2 = data[(data["startPortName"] == portName2) & (data["endPortName"] == portName1)]["size"]
    #
    #         if((len(size1) > 0) & (len(size2) > 0)):
    #             # raw_input("==============")
    #             newSize = size1.iloc[0] + size2.iloc[0]
    #             lonList = data[(data["startPortName"] == portName1) & (data["endPortName"] == portName2)]["longitude"]
    #             latList = data[(data["startPortName"] == portName1) & (data["endPortName"] == portName2)]["latitude"]
    #
    #             tmpDF = pd.DataFrame()
    #             tmpDF["longitude"] = lonList
    #             tmpDF["latitude"] = latList
    #             tmpDF["PortName1"] = portName1
    #             tmpDF["PortName2"] = portName2
    #             tmpDF["paintID"] = paintID
    #             tmpDF["size"] = newSize
    #             print "(%d / %d)" % (index, len(data)), len(tmpDF)
    #             outData = outData.append(tmpDF)
    #             # raw_input("=======")
    #         else:
    #             if(len(size1) > 0):
    #                 portNameList.append([portName1, portName2])
    #                 outData = outData.append(data[(data["startPortName"] == portName1) &
    #                                               (data["endPortName"] == portName2)])
    #             if(len(size2) > 0):
    #                 portNameList.append([portName2, portName1])
    #                 outData = outData.append(data[(data["startPortName"] == portName2) &
    #                                               (data["endPortName"] == portName1)])


    ############################
    # 获取所有航行的点
    aisDF = pd.read_csv("./data/dynamicData/dynamicData2016/dynamicSpecAIS201609.csv")
    ABSResultDF = pd.read_csv("ABSResult2016.csv")
    ABSResultDF = ABSResultDF[ABSResultDF["startTime"] >= 1472659200]
    paintID = 0
    paintData = pd.DataFrame()
    i = 0
    for index, value in ABSResultDF.iterrows():
        print "(%d / %d)" % (i, len(ABSResultDF))
        i += 1
        MMSI = value.iloc[2]
        startTime = value.iloc[3]
        endTime = value.iloc[7]

        tmpAIS = aisDF[(aisDF["unique_ID"] == MMSI) &
                       (aisDF["acquisition_time"] >= startTime) &
                       (aisDF["acquisition_time"] <= endTime)]
        tmpAIS = tmpAIS.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
        tmpAIS["unique_ID"] = paintID
        paintID += 1
        paintData = paintData.append(tmpAIS)
        print len(paintData)
    paintData.to_csv("./data/paintData.csv", index=None)
