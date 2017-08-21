# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    containerDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/ABSStatic.csv")
    zhouDF = pd.read_csv("/home/qiu/Documents/data/data/staticData/船舶匹配（第三次）.csv")

    containerAIS = pd.DataFrame()
    zhouAIS = pd.DataFrame()
    for date in range(1, 31):
        if(date < 10):
            dateStr = "0" + str(date)
        else:
            dateStr = str(date)
        print date
        tmpAIS = pd.read_csv("ftp://qiu:qiu@192.168.18.61/201609/ships_201609%s.csv" % dateStr)
        # tmpContainer = tmpAIS[tmpAIS["unique_ID"].isin(containerDF["MMSI"])]
        # containerAIS = containerAIS.append(tmpContainer)
        tmpZhou = tmpAIS[tmpAIS["unique_ID"].isin(zhouDF["MMSI"])]
        zhouAIS = zhouAIS.append(tmpZhou)
    # containerAIS.to_csv("/home/qiu/Documents/data/DP-DATA/集装箱AIS/201609_container.csv", index=None)
    zhouAIS.to_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/201609_chinaCoast.csv", index=None)
