# coding:utf-8

import pandas as pd
from convert_resulat import convert_result

if __name__ == "__main__":
    navEventsFileNameList = ["navPortDF201607pre.csv", "navPortDF201607aft.csv",
                         "navPortDF201608pre.csv", "navPortDF201608aft.csv",
                         "navPortDF201609aft.csv"]
    navEventsDF = pd.read_csv("./data/mergedNav201609pre.csv")
    navEventsDF.columns = ["portName", "unique_ID", "begin_time", "end_time", "portLon", "portLat"]
    for navEventsFileName in navEventsFileNameList:
        tmpNav = pd.read_csv("./data/%s" % navEventsFileName)
        navEventsDF = navEventsDF.append(tmpNav)

    cr = convert_result()

    navEventsDF = navEventsDF.iloc[:, [5, 6, 0, 1, 4, 3]]
    uniqueIDGDF = navEventsDF.groupby("unique_ID")

    mergedNavList = []
    for uniqueIDGroup in uniqueIDGDF:
        oneShipMergedList = []
        IDList = list(uniqueIDGroup)
        print IDList[0]
        IDDF = IDList[1]
        IDDF = IDDF.sort_values(by=["unique_ID", "begin_time"])

        portGDF = IDDF.groupby("portName")
        for portGroup in portGDF:
            tmp = cr.getMergeABS(list(portGroup)[1])
            mergedNavList.extend(tmp)
    print len(navEventsDF)
    print len(mergedNavList)

    mergedNavDF = pd.DataFrame(mergedNavList)
    mergedNavDF.to_csv("./data/mergedNav2016All.csv", index=None)
