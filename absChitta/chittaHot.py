# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    ABSOptResultDF = pd.read_csv("/home/qiu/Documents/data/ABSResult/ABSOptResult2016.csv")
    portList1 = ["fuzhou", "quanzhou", "guangzhou", "haikou", "beihai", "haiphong", "hachiminh",
                 "portklang", "penang", "jakarta", "yangon", "laemchabang", "manila"]
    portList2 = ["kolkata", "colombo", "jebelali", "mombasa", "salalah"]

    portTurnOverList = []
    ABSStartPortGDF = ABSOptResultDF.groupby("startPortName")
    for portGroup in ABSStartPortGDF:
        portGroupList = list(portGroup)
        portName = portGroupList[0]
        portGroupDF = portGroupList[1]
        portTEUGDF = portGroupDF.groupby("TEU")
        eachPortList = []
        for teuGroup in portTEUGDF:
            teuGroupList = list(teuGroup)
            teuNum = teuGroupList[0]
            teuGroupDF = teuGroupList[1]
            teuGroupLen = len(teuGroupDF)
            turnover = teuNum * teuGroupLen
            tmpList = [portName, teuNum, teuGroupLen, turnover]
            eachPortList.append(tmpList)
        turnoverList = [x[3] for x in eachPortList]
        navNumList = [x[2] for x in eachPortList]
        sumNavNum = sum(navNumList)
        sumTurnOver = sum(turnoverList)
        portTurnOverList.append([portName, sumTurnOver, sumNavNum])

    portTurnOverDF = pd.DataFrame(portTurnOverList)
    portTurnOverDF.columns = ["portName", "turnOver", "navNum"]
    print portTurnOverDF.head()
    # portTurnOverDF.to_csv("/home/qiu/Documents/data/ABSResult/turnOverData.csv", index=None)
    raw_input("=============================================")

    portTurnOverList1 = []
    portTurnOverList2 = []
    for index, value in portTurnOverDF.iterrows():
        portName = value["portName"].replace(" ", "").lower()
        if(portName in portList1):
            portTurnOverList1.append([value["portName"], value["turnOver"], value["navNum"]])
        elif(portName in portList2):
            portTurnOverList2.append([value["portName"], value["turnOver"], value["navNum"]])
    portTurnOverDF1 = pd.DataFrame(portTurnOverList1)
    portTurnOverDF2 = pd.DataFrame(portTurnOverList2)
    portTurnOverDF1.columns = ["portName", "turnOver", "navNum"]
    portTurnOverDF2.columns = ["portName", "turnOver", "navNum"]
    portTurnOverDF1.to_csv("/home/qiu/Documents/data/ABSResult/turnOverData_group1.csv", index=None)
    portTurnOverDF2.to_csv("/home/qiu/Documents/data/ABSResult/turnOverData_group2.csv", index=None)
