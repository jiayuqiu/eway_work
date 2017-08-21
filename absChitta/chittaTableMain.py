# coding:utf-8

import pandas as pd
from chittagongFunc import chittaTable

if __name__ == "__main__":
    chittaRouteDataDF = pd.read_csv("chittagongSailRouteData.csv")
    chittaRouteDataDF["Colombo"] = 0
    chittaRouteDataDF["Tanjung Bin"] = 0
    chittaRouteDataDF["Singapore & North Port"] = 0

    for index, value in chittaRouteDataDF.iterrows():
        throughPortList = value["throughPort"].split("*")
        if("Colombo" in throughPortList):
            print "Colombo"
            chittaRouteDataDF.loc[index, "Colombo"] = 1
        elif("Tanjung Bin" in throughPortList):
            print "Tanjung Bin"
            chittaRouteDataDF.loc[index, "Tanjung Bin"] = 1
        elif(("Singapore" in throughPortList) & ("North Port" in throughPortList)):
            print "Singapore & North Port"
            chittaRouteDataDF.loc[index, "Singapore & North Port"] = 1
    print chittaRouteDataDF
    chittaRouteDataDF.to_csv("chittagongSailRouteDataThreeRoute.csv", index=None)
