# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    avgSpeedDF = pd.read_csv("./data/sailSpeedWithDraught.csv")
    avgSpeedDF.columns = ["MMSI", "draught", "sailSpeed"]
    ABSStaticDF = pd.read_csv("./data/staticData/ABSStatic.csv")

    tmpDF = avgSpeedDF.merge(ABSStaticDF)
    tmpDF = tmpDF.iloc[:, [3, 0, 5, 1, 2]]
    tmpDF = tmpDF.drop_duplicates()
    print tmpDF.head()
    tmpDF.to_csv("./data/avgSpeedPaintData.csv", index=None)

    ######################################################################
    # 获取1000-4000TEU船舶的MMSI，IMO，TEU，NAME

    # shipImoDF = pd.read_excel("/home/qiu/PycharmProjects/ABS/data/staticData/"
    #                           "1000 to 4000 TEU Container Carrier List.xlsx")
    # staticDF = pd.read_csv("/home/qiu/PycharmProjects/ABS/data/staticData/"
    #                        "staticSpecDF09.csv")
    #
    # shipImoDF = shipImoDF.iloc[:, [0, 1, 2]]
    # shipImoDF.columns = ["IMO", "ABSTEU", "ABSShipName"]
    #
    # staticDF = staticDF.iloc[:, [0, 7, 8]]
    # staticDF.columns = ["MMSI", "IMO", "ShipName"]
    #
    # print shipImoDF.head()
    # print "============================================"
    # print staticDF.head()
    #
    # ABSStaticDF = staticDF.merge(shipImoDF)
    # print ABSStaticDF.drop_duplicates().head()
    # ABSStaticDF.to_csv("./data/staticData/ABSStatic.csv", index=None)
    ############################################################################
