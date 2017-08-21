# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    portABS = pd.read_excel("/Users/Marcel/Data/tblPort.xlsx")
    portOur = pd.read_csv("/Users/Marcel/Data/merged_ports.csv")
    polyPort = pd.read_csv("/Users/Marcel/Data/Asia_anchores.csv")
    print polyPort.head()
    polyPortNameList = []
    polyPortGDF = polyPort.groupby("anchores_id")
    for polyPortGroup in polyPortGDF:
        aPolyPortList = list(polyPortGroup)
        aPolyPortDF = aPolyPortList[1]

        aPolyDockName = aPolyPortDF.iloc[0, 2]
        aPolyPortName = ""
        for letter in aPolyDockName:
            if(letter.isdigit()):
                pass
            else:
                aPolyPortName = aPolyPortName + letter
        polyPortNameList.append(aPolyPortName.lower())
    polyPortNameList = list(set(polyPortNameList))
    print len(polyPortNameList)
    # raw_input("==========")

    # ABS给出的港口表格，获取码头名称、国家、经纬度信息
    # 我们的码头表格，获取国家、城市、码头名称、经纬度信息
    portABSConvert = portABS.iloc[:, [1, 4, 26, 30]]
    print len(portABSConvert)
    portOurConvert = portOur.iloc[:, [2, 4, 5, 7, 8]].drop_duplicates()
    print len(portOurConvert)

    portABSPortNameList = list(portABSConvert.iloc[:, 0].fillna(""))
    portOurPortNameList = list(portOurConvert.iloc[:, 2].fillna(""))

    portABSPortNameList = [x.replace(" ", "").lower() for x in portABSPortNameList]
    portOurPortNameList = [x.replace(" ", "").lower().encode("utf-8") for x in portOurPortNameList]

    print set(portABSPortNameList)
    print set(portOurPortNameList)
    print len(list(set(portABSPortNameList).intersection(set(portOurPortNameList))))
    print len(list(set(portABSPortNameList).intersection(set(polyPortNameList))))
    print set(polyPortNameList).difference(set(portABSPortNameList))

    print "done!"
