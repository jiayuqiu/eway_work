# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    print "开始获取停泊事件"
    navEventsDF = pd.read_csv("/Users/Marcel/Data/allNavEvents/nav_event_700_201609_more_pre.csv")
    # 增加字段，用于判断是否已经在"多边形港口"内
    navEventsDF["isInPoly"] = False
    shipImoDF = pd.read_excel("/Users/Marcel/Downloads/1000 to 4000 TEU Container Carrier List.xlsx")
    staticDF = pd.read_csv("/Users/Marcel/Downloads/unique_static_2016.csv")
    print len(shipImoDF)
    matchDF = staticDF[staticDF["imo"].isin(shipImoDF.iloc[:, 0])]
    imoList = matchDF.loc[:, "imo"]
    print len(set(imoList))
