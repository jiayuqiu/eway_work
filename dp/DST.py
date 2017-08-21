# coding:utf-8

import math
import pandas as pd
import numpy as np

############################################################
# 检查压缩算法产生的航程差

class check_deviation(object):
    def __init__(self):
        self.EARTH_RADIUS = 6378.137  # 地球半径，单位千米
        self.D_DST = 150000           # 距离阈值，单位毫米
        self.MINV = 100               # 速度最小阈值，单位毫米/秒
        self.precision = 1000000.0    # 精度1-km 1000-m 1000000-mm

    ########################################################
    # 获得地球间两点间距离，返回结果单位为千米
    def getRadian(self, x):
        return x * math.pi / 180.0

    def getDist(self, lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
        radLat1 = self.getRadian(lat1)
        radLat2 = self.getRadian(lat2)

        a = radLat1 - radLat2
        b = self.getRadian(lon1) - self.getRadian(lon2)

        dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                      math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

        dst = dst * self.EARTH_RADIUS
        dst = round(dst * 100000000.0) / 100000000.0

        return dst

    ##########################################################
    # 获得一条船舶的航程
    def get_distance(self, group):
        dst = 0
        group_list = list(group)
        group_num = len(group_list[1])
        each_ship = np.array(group_list[1].iloc[0:])
        # raw_input("====================================")
        each_ship = [[float(x) for x in inner] for inner in each_ship]
        each_ship.sort(key=lambda v: v[1])
        ais_length = len(each_ship)
        if(ais_length > 1):
            index = 0
            while(index < (ais_length - 1)):
                tmp_dst = self.getDist(each_ship[index][2], each_ship[index][3],
                                       each_ship[index + 1][2], each_ship[index + 1][3])
                dst = dst + tmp_dst
                index += 1
            # outStr = str(each_ship[0][0]) + "," + str(dst)
            return dst
        else:
            return 0

if __name__ == "__main__":
    cd = check_deviation()
    ais_compress = pd.read_csv("/home/qiu/Documents/data/DP-DATA/QDPOPT/"
                               "201609_chinaCoast_qdpopt_200m.csv")
    ais_compress = ais_compress.sort_values(by=["unique_ID", "acquisition_time"])
    grouped_data_compress = ais_compress.groupby("unique_ID")
    dstCompress = []
    print("file1, len = %d" % len(ais_compress))
    for group in grouped_data_compress:
        mmsi = list(group)[0]
        dst1 = cd.get_distance(group)
        dstCompress.append([mmsi, dst1])

    ais = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    ais = ais.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    ais = ais.sort_values(by=["unique_ID", "acquisition_time"])
    ais["longitude"] = ais["longitude"] / 1000000.0
    ais["latitude"]  = ais["latitude"] / 1000000.0
    grouped_data_org = ais.groupby("unique_ID")
    dstOrg = []
    print("file2, len = %d" % len(ais))
    for group in grouped_data_org:
        mmsi = list(group)[0]
        dst2 = cd.get_distance(group)
        dstOrg.append([mmsi, dst2])

    dstCompressDF = pd.DataFrame(dstCompress)
    dstCompressDF.columns = ["mmsi", "dst1"]
    dstOrgDF = pd.DataFrame(dstOrg)
    dstOrgDF.columns = ["mmsi", "dst2"]
    dstDF = dstCompressDF.merge(dstOrgDF)
    dstDF.to_csv("/home/qiu/Documents/data/DP-DATA/deviation/"
                 "chinaCoast_qdpopt_200m_deviation.csv", index=None)
