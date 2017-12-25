# coding:utf-8

import numpy as np
import pandas as pd
from base_func import format_convert

def get_north_ship():
    """
    获取北极圈的船舶AIS数据，纬度高于66°34′
    :return:
    """
    north_ship_df = pd.DataFrame()
    for date in range(23, 31):
        if date < 10:
            date_str = "0" + str(date)
        else:
            date_str = str(date)
        print(date_str)
        data = pd.read_csv("/media/qiu/Seagate Backup Plus Drive/201509AIS/ships_201509%s.csv" % date_str)
        north_ship_df = north_ship_df.append(data[data['latitude'] >= (66.56666666666666 * 1000000.)])

    north_ship_df.to_csv('/home/qiu/Documents/sisi2017/xukai_data/north_ship_forth_week.csv', index=None)

def sum_ship_type():
    """
    统计70、80船舶数量
    :return:
    """
    static_df = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/unique_static_2015.csv")

    north_ship_df = pd.DataFrame()
    for file_name in ['/home/qiu/Documents/sisi2017/xukai_data/north_ship_first_week.csv',
                      '/home/qiu/Documents/sisi2017/xukai_data/north_ship_forth_week.csv',
                      '/home/qiu/Documents/sisi2017/xukai_data/north_ship_second_week.csv',
                      '/home/qiu/Documents/sisi2017/xukai_data/north_ship_third_week.csv']:
        data = pd.read_csv(file_name)
        north_ship_df = north_ship_df.append(data)

    static700_df = static_df[(static_df['shiptype'] >= 70) & (static_df['shiptype'] <= 79)]
    static800_df = static_df[(static_df['shiptype'] >= 80) & (static_df['shiptype'] <= 89)]

    north_ship700_df = north_ship_df[north_ship_df['unique_ID'].isin(static700_df['mmsi'])]
    north_ship800_df = north_ship_df[north_ship_df['unique_ID'].isin(static800_df['mmsi'])]

    print("北极圈内700船舶数量：", len(set(north_ship700_df['unique_ID'])))
    print("北极圈内800船舶数量：", len(set(north_ship800_df['unique_ID'])))


def bm_ais_mmsi():
    """
    从博懋的原始动态AIS数据中，筛选出给定mmsi的动态数据
    :return:
    """
    fc = format_convert()

    bm_ais_file = open("/media/qiu/新加卷/2017/201707", "r")
    out_bm_ais_file = open("/home/qiu/Documents/sisi2017/xukai_data/bm_north_ship_201707.csv", "w")
    for line in bm_ais_file:
        convert_line = fc.bm_to_thr(line)
        if convert_line:
            convert_line_list = convert_line.split(",")
            if int(convert_line_list[7]) >= int(66.56666666666666 * 1000000.):
                out_bm_ais_file.write(convert_line + "\n")
                print(convert_line_list[0])
    bm_ais_file.close()
    out_bm_ais_file.close()

if __name__ == "__main__":
    bm_ais_mmsi()
