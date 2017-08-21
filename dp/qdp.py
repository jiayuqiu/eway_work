# coding:utf-8 #

import time
import math
from base_func import getDist, getDist_3d, WaiJi, NeiJi, cosVector
import pandas as pd
import numpy as np

D_DST        = 200 * 1000.  # 距离阈值，单位毫米
print("距离阈值 = %f" % D_DST)
MINV         = 100  # 速度最小阈值，单位毫米/秒
precision    = 1000000.0  # 精度1-km 1000-m 1000000-mm

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
                output_data = quick_DP_opt(onestage=eachship, out_data=output_data)
            # print output_data
                compressed_str.extend(output_data)
            else:
                output_data.append(eachship[0])
                compressed_str.extend(output_data)
            # raw_input("===========================================")
    return compressed_str

# 压缩算法程序段
def quick_DP(onestage, out_data):
    if(len(onestage) > 2):
        #####################################################
        # 获得这段AIS数据的长度,ed
        stage_len = len(onestage)

        #####################################################
        # 初始化距离最大值与最大值的索引,ed
        max_dst       = 0
        max_dst_index = 0

        #####################################################
        # 定义起点与终点坐标,ed
        str_time = onestage[0][1]
        str_lon  = onestage[0][2]
        str_lat  = onestage[0][3]
        end_time = onestage[stage_len - 1][1]
        end_lon  = onestage[stage_len - 1][2]
        end_lat  = onestage[stage_len - 1][3]

        #####################################################
        # 得到在经纬度平面上起点与终点之间的距离，单位：1000000-mm 1000-m 1-km,ed
        dst_str2end_2d = (1000000 * getDist(str_lon, str_lat, end_lon, end_lat))

        ######################################################
        # 获得时间轴在三维坐标系中权重，W可理解为速度，W2可理解为维度对经度的权重转换
        W = dst_str2end_2d / (end_time - str_time)
        if(W < MINV):
            W = MINV
        W2 = math.cos(((end_lat + str_lat) * (math.pi/180.)) / 2)

        ######################################################
        # 获得三维坐标系中，起点到终点间的距离
        dst_str2end_3d = getDist_3d(str_lon, str_lat, str_time, end_lon, end_lat, end_time, W)

        ######################################################
        # 获得起点与终点形成的向量，记作向量l,ed
        vectorL_time = (end_time - str_time) * W
        vectorL_lon  = (end_lon  - str_lon)  * W2
        vectorL_lat  =  end_lat  - str_lat

        ######################################################
        # 初始化状态信息，最初状态设为远离，目前状态设为空,ed
        status_pre = 1

        ######################################################
        # 循环判断中间点，进行压缩步骤;返回起点与终点间所有点中距离起点终点
        # 连线的最大值
        i = 1 # 从第二条开始循环到倒数第二条判断
        while(i < (stage_len-1)):
            # 得到点i-1与点S形成的向量，记作向量I,ed
            vectorI_time = (onestage[i][1] - onestage[0][1]) * W
            vectorI_lon  = (onestage[i][2] - onestage[0][2]) * W2
            vectorI_lat  =  onestage[i][3] - onestage[0][3]

            # 得到向量I外积,ed
            vectorI_wj = WaiJi(vectorI_lon, vectorI_lat, vectorI_time,
                               vectorL_lon, vectorL_lat, vectorL_time)

            # 得到点i与点i+1形成的向量，记作向量Iplus
            vectorIplus_time = (onestage[i + 1][1] - onestage[i][1]) * W
            vectorIplus_lon  = (onestage[i + 1][2] - onestage[i][2]) * W2
            vectorIplus_lat  =  onestage[i + 1][3] - onestage[i][3]

            # 得到向量Iplus外既,ed
            vectorIplus_wj = WaiJi(vectorIplus_lon, vectorIplus_lat, vectorIplus_time,
                                   vectorL_lon, vectorL_lat, vectorL_time)

            # 对两个外积求内积，得出远近状态是否发生改变,ed
            status_now = NeiJi(vectorI_wj[0], vectorI_wj[1], vectorI_wj[2], vectorIplus_wj[0],
                               vectorIplus_wj[1], vectorIplus_wj[2])

            # 判断点i的状态如何改变
            if((status_now * status_pre < 0)):  # 若现在的状态发生改变
                status_pre = -status_pre  # 状态变量改变

                # 得到点i与起点终点连线的距离
                dst_i2s_3d = getDist_3d(onestage[i][2], onestage[i][3], onestage[i][1], str_lon,
                                         str_lat, str_time, W)
                dst_i2e_3d = getDist_3d(onestage[i][2], onestage[i][3], onestage[i][1], end_lon,
                                         end_lat, end_time, W)

                # 利用海伦公式求得点i到起点与终点连线的距离
                pi        = (dst_i2s_3d + dst_i2e_3d + dst_str2end_3d)/2.0  # 获得半周长

                if(((pi - dst_i2s_3d) < 0) or ((pi - dst_i2e_3d) < 0) or ((pi - dst_str2end_3d) < 0)):
                    areaI = (pi * abs(pi - dst_i2s_3d) * abs(pi - dst_i2e_3d) * abs(pi - dst_str2end_3d)) ** 0.5
                else:
                    areaI = (pi * (pi - dst_i2s_3d) * (pi - dst_i2e_3d) * (pi - dst_str2end_3d)) ** 0.5

                dst_i2se = (2 * areaI) / dst_str2end_3d

                if((dst_i2se - max_dst) > 0):
                    max_dst       = dst_i2se
                    max_dst_index = i
            i = i + 1
        if(max_dst > D_DST):
            out_data.append(onestage[max_dst_index])
            quick_DP(onestage[0:max_dst_index+1], out_data)
            quick_DP(onestage[max_dst_index:], out_data)
    return out_data

# 压缩算法程序段
def quick_DP_opt(onestage, out_data):
    if(len(onestage) > 2):
        #####################################################
        # 获得这段AIS数据的长度,ed
        stage_len = len(onestage)
        #####################################################
        # 初始化距离最大值与最大值的索引,ed
        max_dst       = 0
        max_dst_index = 0
        #####################################################
        # 定义起点与终点坐标,ed
        str_time = onestage[0][1]
        str_lon  = onestage[0][2]
        str_lat  = onestage[0][3]
        end_time = onestage[stage_len - 1][1]
        end_lon  = onestage[stage_len - 1][2]
        end_lat  = onestage[stage_len - 1][3]
        #####################################################
        # 得到在经纬度平面上起点与终点之间的距离，单位：1000000-mm 1000-m 1-km,ed
        dst_str2end_2d = (1000000 * getDist(str_lon, str_lat, end_lon, end_lat))
        ######################################################
        # 获得时间轴在三维坐标系中权重，W可理解为速度，W2可理解为维度对经度的权重转换
        if(end_time == str_time):
            end_time += 1
        W = dst_str2end_2d / (end_time - str_time)
        if(W < MINV):
            W = MINV
        W2 = math.cos(((end_lat + str_lat) * (math.pi/180.)) / 2)
        ######################################################
        # 获得三维坐标系中，起点到终点间的距离
        dst_str2end_3d = getDist_3d(str_lon, str_lat, str_time, end_lon, end_lat, end_time, W)
        ######################################################
        # 获得起点与终点形成的向量，记作向量l,ed
        vectorL_time = (end_time - str_time) * W
        vectorL_lon  = (end_lon  - str_lon)  * W2
        vectorL_lat  =  end_lat  - str_lat
        ######################################################
        # 循环判断中间点，获得中间点到平面M上的投影点
        projectionList = []
        i = 1  # 从第二条开始循环到倒数第二条判断
        while i < (stage_len - 1):
            i += 1
        # 判断是否满足距离阈值
        if max_dst > (((D_DST/1000.) ** 2) * 1000.):  # 若满足输出，并分段递归压缩
            out_data.append(onestage[max_dst_index])
            quick_DP(onestage[0:max_dst_index+1], out_data)
            quick_DP(onestage[max_dst_index:], out_data)
    # 返回压缩保留的数据
    return out_data


if __name__ == "__main__":
    # 读取原始数据
    print "正在读取ais数据"
    # raw_input("=============================================")
    data = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    data = data.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    ##################################################################################
    # aisFile = open("/home/qiu/problem_mmsi_ais/part-00000", "r")
    # ais = []
    # for line in aisFile:
    #     # print line
    #     x = re.findall(r"u'(.*?)'", line, re.S)
    #     ais.append([float(y) for y in x])
    #     # print ais
    #     # raw_input("=================================")
    # aisFile.close()
    # ais = pd.DataFrame(ais)
    # data = pd.DataFrame(ais)
    data.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # data = data[data["unique_ID"] == 100887535]
    ###################################################################################
    data["longitude"] = data["longitude"]/1000000.0
    data["latitude"]  = data["latitude"] / 1000000.0
    # data = data[data["unique_ID"] == 413437680]
    # data = data[data["unique_ID"] == 235072651]
    # print data
    # data.columns = ['unique_ID', 'acquisition_time', 'longitude', 'latitude']
    # 对unique_ID进行分组
    grouped_datas = data.groupby('unique_ID')
    # 初始化压缩后的数据，用于存放压缩后的数据
    print "正在压缩"
    start_time = time.time()
    compressed_data = compress_QDP(grouped_datas)
    end_time = time.time()
    print "压缩完成"
    print "压缩用时%d" % (end_time - start_time)
    # 获得数据后转换为Dataframe
    compressed_data_DF = pd.DataFrame(compressed_data)
    compressed_data_DF.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # 对压缩后的数据对时间元素进行排序
    compressed_data_DF = compressed_data_DF.sort_index(by=["unique_ID", "acquisition_time"])
    # 显示前5条数据
    print "压缩前数据总共：%d条" % len(data)
    print "压缩后数据总共：%d条" % len(compressed_data_DF)
    # 输出
    compressed_data_DF.to_csv("/home/qiu/Documents/data/DP-DATA/QDP/"
                              "chinaCoast_qdpopt_%sm_tmp.csv" % int(D_DST / 1000.0), index=None)
