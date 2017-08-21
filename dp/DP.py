# coding:utf-8 #
import time
import math
import pandas as pd
import numpy as np
from pandas import DataFrame
import sys
from base_func import getDist
EARTH_RADIUS = 6378.137  # 地球半径，单位千米
MINV         = 100  # 速度最小阈值，单位毫米/秒
D_DST        = 200 * 1000. # 距离阈值，单位毫米
precision    = 1000000.0  # 精度1-km 1000-m 1000000-mm
print D_DST

def speed_point(each_data):
    list_x = [0, len(each_data)]
    if len(each_data) > 2:
        for i in range(1, len(each_data) - 1):
            v1 = (getDist(each_data[i - 1][2], each_data[i - 1][3], each_data[i][2], \
                        each_data[i][3]) * 1000000.) / (each_data[i][1] - each_data[i - 1][1])
            v2 = (getDist(each_data[i][2], each_data[i][3], each_data[i + 1][2], \
                        each_data[i + 1][3]) * 1000000.) / (each_data[i + 1][1] - each_data[i][1])
            # print v1,v2
            if (v1 < 300 and v2 > 300) | (v1 > 300 and v2 < 300):
                list_x.append(i)
                list_x.sort()
    else:
        list_x
    return list_x

# 对分组后的数据调用压缩算法
def compress_QDP(input_data):
    compressed_str = []
    for group in input_data:
        # 将grouped数据转换为list进行调用，最终转换为数组array
        group_list  = list(group)
        group_num   = len(group_list[1])
        each_ship   = np.array(group_list[1].iloc[0:group_num])
        # 初始化压缩后的数据，加入起点与终点
        ais_length = len(each_ship)
        output_data = []
        if (ais_length > 1):
            output_data.append(each_ship[0])
            output_data.append(each_ship[-1])
            # 调用压缩算法
            output_data = classic_dp(grouped_datas=each_ship, outdata=output_data)
            compressed_str.extend(output_data)
        else:
            output_data.append(each_ship[0])
            compressed_str.extend(output_data)
    return compressed_str

def classic_dp(grouped_datas, outdata):
    if len(grouped_datas) > 2:
        max_dst = 0
        max_dst_index = 0
        #####################################################
        # 定义起点与终点坐标,ed
        str_lon = grouped_datas[0][2]
        str_lat = grouped_datas[0][3]
        end_lon = grouped_datas[len(grouped_datas) - 1][2]
        end_lat = grouped_datas[len(grouped_datas) - 1][3]

        #####################################################
        # 得到在经纬度平面上起点与终点之间的距离，单位：1000000-mm 1000-m 1-km,ed
        dst_se = getDist(str_lon, str_lat, end_lon, end_lat)*1000000.0
        if dst_se == 0:
            dst_se = dst_se + 0.000001
        for i in range(1, (len(grouped_datas)-1)):
            dst_is = getDist(str_lon, str_lat, grouped_datas[i][2], grouped_datas[i][3]) * 1000000.0
            dst_ie = getDist(grouped_datas[i][2], grouped_datas[i][3], end_lon, end_lat) * 1000000.0
            p = (dst_se + dst_is+dst_ie)/2.0
            s = math.sqrt(p*abs(p-dst_is)*abs(p-dst_ie)*abs(p-dst_se))
            dst_ise = ((2*s)/dst_se)
            if dst_ise > max_dst:
                max_dst = dst_ise
                max_dst_index = i
        # print max_dst
        if max_dst > D_DST:
            outdata.append(grouped_datas[max_dst_index])
            classic_dp(grouped_datas[0:max_dst_index], outdata)
            classic_dp(grouped_datas[max_dst_index:], outdata)
            # print "end out_data = ", out_data

    return outdata

if __name__ == "__main__":
    # 读取原始数据
    print "正在读取ais数据"
    data = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    data = data.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    # print data.head()
    # raw_input("==============================")
    data["longitude"] = data["longitude"]/1000000.0
    data["latitude"] = data["latitude"] / 1000000.0
    # data.columns = ['unique_ID', 'acquisition_time', 'longitude', 'latitude']
    # 对unique_ID进行分组
    grouped_datas = data.groupby('unique_ID')
    print len(grouped_datas)
    # print grouped_datas
    # 初始化压缩后的数据，用于存放压缩后的数据
    print "正在压缩"
    start_time = time.time()
    compressed_data = compress_QDP(grouped_datas)
    end_time = time.time()
    print "压缩用时%d" % (end_time - start_time)
    print "压缩完成"
    # 获得数据后转换为Dataframe
    compressed_data_DF = pd.DataFrame(compressed_data)
    # print compressed_data_DF.head()
    compressed_data_DF.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # 对压缩后的数据对时间元素进行排序
    compressed_data_DF = compressed_data_DF.sort_index(by=["unique_ID", "acquisition_time"])
    # 显示前5条数据
    print "压缩前数据总共：%d条" % len(data)
    print "压缩后数据总共：%d条" % len(compressed_data_DF)
    # 输出
    compressed_data_DF.to_csv("/home/qiu/Documents/data/DP-DATA/DP/"
                              "chinaCoast_dp_%sm.csv" % int(D_DST / 1000.), index=None)
