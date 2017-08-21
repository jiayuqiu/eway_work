# coding:utf-8

import pandas as pd

# 计算前十挂靠艘次码头
def getTopTenPort(resultDF):
    resultPortGDF = resultDF.groupby("startPortName")
    portRankList = []
    for portGroup in resultPortGDF:
        portName = list(portGroup)[0]
        portDF = list(portGroup)[1]
        portNavNum = len(portDF)
        print portName, portNavNum
        portRankList.append([portName, portNavNum])
    portRankDF = pd.DataFrame(portRankList)
    portRankDF.columns = ["portName", "portNum"]
    return portRankDF

# 计算前十航线
def getTopTenRoute(resultDF):
    routeGDF = resultDF.groupby(["startPortName", "endPortName"])
    routeSingleList = []
    for routeGroup in routeGDF:
        routePortNameList = list(list(routeGroup)[0])
        routePortLen = len(list(routeGroup)[1])
        print routePortNameList, routePortLen
        routeSingleList.append([routePortNameList[0], routePortNameList[1], routePortLen])
    routeSingleDF = pd.DataFrame(routeSingleList)
    routeSingleDF.columns = ["startPortName", "endPortName", "number"]
    print "----------------------------1-------------------------------"
    print routeSingleDF[(routeSingleDF["startPortName"] == "Keihin") &
                        (routeSingleDF["endPortName"] == "Tokyo")]
    print routeSingleDF[(routeSingleDF["startPortName"] == "Tokyo") &
                        (routeSingleDF["endPortName"] == "Keihin")]
    print "----------------------------2--------------------------------"
    # raw_input("")

    routeSingleDF["doubleSide"] = False
    routeDoubleList = []
    for index, value in routeSingleDF.iterrows():
        print "(%d / %d)" % (index, len(routeSingleDF))
        if(not value["doubleSide"]):
            tmpValue = routeSingleDF[(routeSingleDF["startPortName"] == value["endPortName"]) &
                                     (routeSingleDF["endPortName"] == value["startPortName"])]
            if(len(tmpValue.index) != 0):
                tmpIndex = tmpValue.index[0]
                routeSingleDF.loc[tmpIndex, "doubleSide"] = True
                routeSingleDF.loc[index, "doubleSide"] = True
                routeDoubleList.append([value["startPortName"], value["endPortName"],
                                        value["number"] + tmpValue.iloc[0, 2]])
        else:
            routeDoubleList.append([value["startPortName"], value["endPortName"],
                                    value["number"]])
    routeDoubleDF = pd.DataFrame(routeDoubleList)
    routeDoubleDF.columns = ["startPortName", "endPortName", "number"]
    return routeDoubleDF


if __name__ == "__main__":
    ABSResultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv")
    portRankDF = getTopTenRoute(ABSResultDF)
    print portRankDF.head()
    print len(portRankDF)
    portRankDF = portRankDF[portRankDF["startPortName"] != portRankDF["endPortName"]]
    portRankDF.to_csv("/home/qiu/Documents/data/ABSResult/portRouteNumData.csv", index=None)
