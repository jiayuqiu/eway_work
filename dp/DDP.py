# coding:utf-8 #
import time
import math
import pandas as pd
import numpy as np
import sys
from pandas import DataFrame
import time

EARTH_RADIUS = 6378.137  # 地球半径，单位千米
MINV         = 100  # 速度最小阈值，单位毫米/秒
D_DST        = 200. * 1000 # onestage距离阈值，单位毫米
precision    = 1000000.0  # 精度1-km 1000-m 1000000-mm
print D_DST

# 获得地球两点间距离
EARTH_RADIUS = 6378.137  # 地球半径，单位千米

def getRadian(x):
    return x * math.pi / 180.0

def getDist(lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                  math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000

    return dst

def compress_QDP(input_data):
    compressed_str = []
    for group in input_data:
        # 将grouped数据转换为list进行调用，最终转换为数组array
        group_list = list(group)
        group_num = len(group_list[1])
        each_data = np.array(group_list[1].iloc[0:group_num])
        # print each_data,len(each_data)
        # print each_data[:,2]
        # raw_input('=======================')
        list_x = speed_point(each_data)
        # print list_x,len(list_x)
        i=0
        while (i < len(list_x) - 1):
            a = list_x[i]
            b = list_x[i + 1]
            eachship = each_data[a:b, :]
            # print len(eachship)
            i = i + 1
            # print eachship
            ais_length = len(eachship)
        # print "unique_ID = ", each_ship[0][0]
        # 初始化压缩后的数据，加入起点与终点
        # print ais_length
            output_data = []
            if ais_length > 1:
                output_data.append(eachship[0])
                output_data.append(eachship[-1])
            # 调用压缩算法
                output_data = dongtai_dp(onestage=eachship, outdata=output_data)
            # print output_data
                compressed_str.extend(output_data)
            else:
                output_data.append(eachship[0])
                compressed_str.extend(output_data)
            # raw_input("===========================================")
    return compressed_str

def speed_point(each_data):
    list_x = [0, len(each_data)]
    if len(each_data) > 2:
        for i in range(1, len(each_data) - 1):
                    # raw_input('=======================')
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


def dongtai_dp(onestage,outdata):
    if len(onestage) > 2:
        max_dst = 0
        max_dst_index = 0
        #####################################################
        # 定义起点与终点坐标
        str_time = onestage[0][1]
        str_lon = onestage[0][2]
        str_lat = onestage[0][3]
        end_time = onestage[len(onestage) - 1][1]
        end_lon = onestage[len(onestage) - 1][2]
        end_lat = onestage[len(onestage) - 1][3]
        # print grouped_datas
        # print len(grouped_datas)
        # raw_input("==================================================")
        if(end_time == str_time):
            end_time += 1
        w = (math.sqrt((end_lon - str_lon) ** 2 + (end_lat - str_lat) ** 2)) / (end_time - str_time)
        w2 = math.cos(((end_lat + str_lat) * (math.pi/180.)) / 2)
        #####################################################
        # 得到在经纬度平面上起点与终点之间的距离，单位：1000000-mm 1000-m 1-km,ed
        dst_se = getDist(str_lon, str_lat, end_lon, end_lat)*1000000.0
        if dst_se == 0:
            dst_se = dst_se + 0.000001
        for i in range(1, (len(onestage)-1)):
            startTime1 = time.time()
            dst_is = math.sqrt((getDist(str_lon, str_lat, onestage[i][2], onestage[i][3]) * 1000000.0)**2\
                       + (w*(onestage[i][1]-str_time))**2)
            dst_ie = math.sqrt((getDist(onestage[i][2], onestage[i][3], end_lon, end_lat) * 1000000.0)**2\
                       + (w*(end_time-onestage[i][1]))**2)
            p = (dst_se + dst_is + dst_ie)/2.0
            s = math.sqrt(p*abs(p-dst_is)*abs(p-dst_ie)*abs(p-dst_se))
            dst_ise = ((2.0*s)/dst_se)
            if dst_ise > max_dst:
                max_dst = dst_ise
                max_dst_index = i
                # print "start if"
            endTime1 = time.time()
            # raw_input("======================")
        # print max_dst
        if max_dst > D_DST:
            outdata.append(onestage[max_dst_index])
            dongtai_dp(onestage[0:max_dst_index], outdata)
            dongtai_dp(onestage[max_dst_index:], outdata)
            # print "end out_data = ", outdata
    return outdata

if __name__ == "__main__":
    # 读取原始数据
    print "正在读取ais数据"
    data = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    # data = pd.read_csv("/home/qiu/桌面/before_oneship.csv")
    data = data.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    # print data.head()
    # raw_input("==============================")
    data["longitude"] = data["longitude"]/1000000.0
    data["latitude"] = data["latitude"] / 1000000.0
    data.columns = ['unique_ID', 'acquisition_time', 'longitude', 'latitude']
    # 对unique_ID进行分组
    grouped_datas = data.groupby('unique_ID')
    print len(grouped_datas)
    # raw_input('==========')
    # 初始化压缩后的数据，用于存放压缩后的数据
    print "正在压缩"
    start_time = time.time()
    compressed_data = compress_QDP(grouped_datas)
    end_time = time.time()
    print "压缩完成"
    print "压缩用时%f" % (end_time - start_time)
    # 获得数据后转换为Dataframe
    compressed_data_DF = pd.DataFrame(compressed_data)
    # print compressed_data_DF.head()
    compressed_data_DF.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # 对压缩后的数据对时间元素进行排序
    compressed_data_DF = compressed_data_DF.sort_values(by=["unique_ID", "acquisition_time"])
    # 显示前5条数据
    print "压缩前数据总共：%d条" % len(data)
    print "压缩后数据总共：%d条" % len(compressed_data_DF)
    # print compressed_data.head()
    # 输出
    # compressed_data_DF.to_csv("/home/qiu/PycharmProjects/get_data/dongtai_after_250m.csv", index=None)
    compressed_data_DF.to_csv("/home/qiu/Documents/data/DP-DATA/DDP/"
                              "chinaCoast_ddp200m.csv", index=None)
