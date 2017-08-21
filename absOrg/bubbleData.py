# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    navDataDF = pd.read_csv("./data/mergedNav2016All.csv")
    navDataDF.columns = ["portName", "unique_ID", "begin_time", "end_time", "portLon", "portLat"]
    print navDataDF.head()

    navPortGDF = navDataDF.groupby("portName")
    bubbleDataList = []
    for navPortGroup in navPortGDF:
        navPortGroupList = list(navPortGroup)
        navPortName = navPortGroupList[0]
        print navPortName
        navPortDF = navPortGroupList[1]
        portLon = navPortDF.iloc[0, 4]
        portLat = navPortDF.iloc[0, 5]

        bubbleSize = len(navPortDF)
        tmpBubble = [navPortName, portLon, portLat, bubbleSize]
        bubbleDataList.append(tmpBubble)
    bubbleDataDF = pd.DataFrame(bubbleDataList)
    bubbleDataDF.columns = ["portName", "portLon", "portLat", "size"]
    bubbleDataDF.to_csv("ABSBubblePaintData2016.csv", index=None)
