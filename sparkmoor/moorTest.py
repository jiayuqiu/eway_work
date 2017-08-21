# coding:utf-8

import pandas as pd
import time
from moor2017 import moor

if __name__ == "__main__":
    aisDF = pd.read_csv("/home/qiu/Documents/data/ships_20160901.csv")
    aisDF["data_supplier"] = "248"
    data_supplier = aisDF.pop("data_supplier")
    aisDF.insert(3, "data_supplier", data_supplier)

    staticDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/staticData2016/"
                           "static_ships_20160901")
    staticDF.columns = ["shipid", "time", "shiptype", "length", "width", "left",
                        "trail", "imo", "name", "callsign", "draught", "destination", "eta"]

    aisGDF = aisDF.groupby("unique_ID")
    mr = moor()
    # outFile = open("/home/qiu/Documents/sparkTestResult/testFile", "w")
    outStrList = []
    startTime = time.time()
    for group in aisGDF:
        tmpStr = mr.moorShip(group, staticDF)
        if tmpStr:
            outStrList.append(tmpStr + "\n")
    endTime = time.time()
    print("use %f" % (endTime - startTime))
    # outFile.writelines(outStrList)
    # outFile.close()
