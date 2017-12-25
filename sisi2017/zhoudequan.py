# coding:utf-8

import numpy as np
import pandas as pd
import math


#########################################################
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


def get_test_ship_ais(month_str, day_num, test_ship_list):
    """获取测试船舶AIS数据"""
    month_tst_ais_df = pd.DataFrame(columns=["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
                                             "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
                                             "true_head", "power", "ext", "extend"])
    date_range = range(1, day_num+1)
    for date in date_range:
        try:
            if date < 10:
                date_str = "0" + str(date)
            else:
                date_str = str(date)
            print(date_str)
            # ais_df = pd.read_csv("ftp://qiu:qiu@192.168.18.201/%s/date_%s" % (month_str, date_str), header=None)
            ais_df = pd.read_csv("/mnt/data/%s/date_%s" % (month_str, date_str), header=None)
            ais_df.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
                              "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
                              "true_head", "power", "ext", "extend"]
            ais_df['longitude'] = ais_df['longitude'] * 1000000
            ais_df['latitude'] = ais_df['latitude'] * 1000000
            ais_df.to_csv("/media/qiu/193D-D9CB/201708_compress_ais/2017%s%s_compress.csv" % (month_str, date_str),
                          index=None, header=None)
            # ais_df = ais_df[ais_df['unique_ID'].isin(test_ship_list)]
            # month_tst_ais_df = month_tst_ais_df.append(ais_df)
        except Exception as e:
            print(e)
    return month_tst_ais_df


def calc_data(ais_df, moor_df):
    """计算结果"""
    ais_num = len(ais_df.iloc[:, 0])
    sail_num = 0
    port_list = []
    moor_time = 0
    moor_port_list = []
    if len(moor_df) > 1:
        for index in range(len(moor_df) - 1):
            moor_time += (moor_df.iloc[index, 2] - moor_df.iloc[index, 1])
            if moor_df.iloc[index, 30] != moor_df.iloc[index + 1, 30]:
                port_dst = getDist(lon1=float(moor_df.iloc[index, 31]), lat1=float(moor_df.iloc[index, 32]),
                                   lon2=float(moor_df.iloc[index+1, 31]), lat2=float(moor_df.iloc[index+1, 32]))
                if (moor_time > (0.5 * 24 * 3600)):
                    sail_num += 1
                moor_time = 0
                port_list.extend([moor_df.iloc[index + 1, 30], moor_df.iloc[index + 1, 30]])

    dst = 0
    for index in range(ais_num - 1):
        # 计算距离
        lon1 = ais_df.iloc[index, 6] / 1000000.
        lat1 = ais_df.iloc[index, 7] / 1000000.
        lon2 = ais_df.iloc[index + 1, 6] / 1000000.
        lat2 = ais_df.iloc[index + 1, 7] / 1000000.
        tmpDst = getDist(lon1, lat1, lon2, lat2)
        dst = dst + tmpDst

    moor_df['interval'] = moor_df['end_time'] - moor_df['begin_time']
    all_time = (max(ais_df.iloc[:, 1]) - min(ais_df.iloc[:, 1]))
    run_time = (all_time - sum(moor_df['interval']))
    work_time = (all_time - sum(moor_df[moor_df['interval'] > 10 * 24 * 3600]['interval']))
    if run_time != 0:
        avg_speed = dst / (run_time / 3600)
    else:
        avg_speed = 0

    return all_time / (3600 * 24), run_time / (3600 * 24), work_time / (3600 * 24), avg_speed, sail_num, dst


if __name__ == "__main__":
    # # for month in ["01", "02", "03", "04", "05", "06", "07"]:
    # month = "08"
    # day_num = 31
    # test_ship_df = pd.read_excel('/home/qiu/Documents/sisi2017/zhoudequan_data/船舶匹配第一次.xlsx')
    # test_ship_ais_df = get_test_ship_ais(month, day_num, test_ship_df['MMSI'])
    # # test_ship_ais_df.to_csv("/media/qiu/193D-D9CB/201708_compress_ais/%s_compress_ais.csv" %
    # #                         month, index=None, header=None)

    # month_list = ["201701", "201702", "201703", "201704", "201705", "201706", "201707", "201708"]
    # for month in month_list:
    #     print(month)
    #     tst_ais_df = pd.read_csv("/home/qiu/Documents/sisi2017/zhoudequan_data/first_ship_ais_%s.csv" % month, header=None)
    #     tst_ais_df.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                           "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                           "true_head", "power", "ext", "extend"]
    #     tst_ais_df = tst_ais_df.sort_values(by=["unique_ID", "acquisition_time"])
    #
    #     tst_moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/zhoudequan_data/%s_first_moor/part-00000" % month, header=None)
    #     tst_moor_df.columns = ["mmsi", "begin_time", "end_time", "apart_time",
    #                            "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
    #                            "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
    #                            "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
    #                            "avg_sog", "var_sog", "max_sog", "maxSog_cog",
    #                            "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
    #                            "nowPortName", "nowPortLon", "nowPortLat"]
    #     tst_moor_df = tst_moor_df[tst_moor_df['nowPortName'] != 'None']
    #
    #     # 统计数据
    #     tst_ais_gdf = tst_ais_df.groupby('unique_ID')
    #     result_list = []
    #     print("正在统计")
    #     for mmsi, ais_group in tst_ais_gdf:
    #         group_moor_df = tst_moor_df[tst_moor_df['mmsi'] == mmsi]
    #         all_time, run_time, work_time, avg_speed, sail_num, dst = calc_data(ais_group, group_moor_df)
    #         # print(mmsi, all_time, run_time, work_time, avg_speed, sail_num, dst)
    #         result_list.append([mmsi, all_time, run_time, work_time, avg_speed, sail_num, dst])
    #
    #     result_df = pd.DataFrame(result_list, columns=["mmsi", " all_time", " run_time", "work_time", " avg_speed",
    #                                                    " sail_num", "dst"])
    #     result_df.to_csv("/home/qiu/Documents/sisi2017/zhoudequan_data/%s_first_result.csv" % month, index=None)

    month_list = ["201701", "201702", "201703", "201704", "201705", "201706", "201707", "201708"]

    result_df_list = []
    for month in month_list:
        data = pd.read_csv("/home/qiu/Documents/sisi2017/zhoudequan_data/%s_first_result.csv" % month)
        result_df_list.append(data)
        print(len(data))
    result_df = pd.concat(result_df_list, ignore_index=True)
    print(result_df.head())

    result_gdf = result_df.groupby("mmsi")

    sum_result_list = []
    # sum_result_df = pd.DataFrame(columns=["mmsi", "all_time", "run_time", "work_time", "avg_speed", "sail_num", "dst"])
    for mmsi, value in result_gdf:
        sum_result_list.append([mmsi, sum(value.iloc[:, 1]), sum(value.iloc[:, 2]), sum(value.iloc[:, 3]),
                                np.mean(value.iloc[:, 4]), sum(value.iloc[:, 5]), sum(value.iloc[:, 6])])
    sum_result_df = pd.DataFrame(sum_result_list, columns=["mmsi", "all_time", "run_time", "work_time",
                                                           "avg_speed", "sail_num", "dst"])
    print(sum_result_df)
    sum_result_df.to_csv("/home/qiu/Documents/sisi2017/zhoudequan_data/2017_8months_first_result.csv", index=None)
