# coding:utf-8

import numpy as np
import pandas as pd
import pymysql
import gc

from base_func import format_convert, getDist

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


def get_moor():
    """
    获取4条船舶的停泊港口序列
    :return:
    """
    mmsi_list = [219882000, 372278000, 370016000, 357780000]

    sum_moor_df = pd.DataFrame()
    for i in range(1, 9):
        print(i)
        moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/wrf/20170%s_moor_compress/part-00000" % i, header=None)
        moor_df.columns = ["mmsi", "begin_time", "end_time", "apart_time",
                           "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
                           "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
                           "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
                           "avg_sog", "var_sog", "max_sog", "maxSog_cog",
                           "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
                           "nowPortName", "nowPortLon", "nowPortLat"]
        print(len(moor_df))
        tmp_moor_df = moor_df[moor_df['mmsi'].isin(mmsi_list)]
        sum_moor_df = sum_moor_df.append(tmp_moor_df)
        print(len(tmp_moor_df))
    sum_moor_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_moor.csv", index=None)


def get_mmsi_ais():
    mmsi_list = [219882000, 372278000, 370016000, 357780000]
    month_str = "08"

    month_tst_ais_df = pd.DataFrame(
        columns=["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
                 "true_head", "power", "ext", "extend"])
    date_range = range(1, 32)
    for date in date_range:
        try:
            if date < 10:
                date_str = "0" + str(date)
            else:
                date_str = str(date)
            print(date_str)

            ais_df = pd.read_csv("/media/qiu/新加卷/2017/%s/date_%s" % (month_str, date_str), header=None)
            ais_df.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
                              "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
                              "true_head", "power", "ext", "extend"]

            ais_df = ais_df[ais_df['unique_ID'].isin(mmsi_list)]
            month_tst_ais_df = month_tst_ais_df.append(ais_df)
            del ais_df
            gc.collect()
        except Exception as e:
            print(e)
    month_tst_ais_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/"
                            "4ship_ais_2017%s.csv" % month_str, index=None)


def convert_timestamp(timestamp):
    import time
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    return dt


def moor_analysis():
    """
    获取4条船舶的港口停靠序列
    :return:
    """
    moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_moor.csv")
    moor_df = moor_df[moor_df['nowPortName'] != "None"]
    moor_df = moor_df.sort_values(by=['mmsi', 'begin_time'])
    begin_time_list = []
    end_time_list = []
    for x in moor_df['begin_time']:
        begin_time_list.append(convert_timestamp(int(x)))

    for y in moor_df['end_time']:
        end_time_list.append(convert_timestamp(int(y)))

    moor_df = moor_df.loc[:, ["mmsi", "nowPortName", "nowPortLon", "nowPortLat"]]
    moor_df['begin_time'] = begin_time_list
    moor_df['end_time'] = end_time_list
    moor_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_moor_port_list.csv", index=None)
    print(moor_df)


def cut_line():
    """
    将跨越地球的连线分割开
    :return:
    """
    ais_df1 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201707.csv")
    ais_df2 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201708.csv")
    # ais_df3 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201703.csv")

    ais_df = pd.concat([ais_df1, ais_df2], ignore_index=True)
    ais_df = ais_df.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    ais_df = ais_df.sort_values(by=["unique_ID", "acquisition_time"])
    ais_df['group'] = 0
    ais_array = np.array(ais_df)

    index = 1
    group_num = 0
    while index < len(ais_array):
        if ais_array[index, 0] != ais_array[index - 1, 0]:
            print(ais_array[index, 0])
            group_num += 1

        if abs(ais_array[index, 2] - ais_array[index - 1, 2]) > 200.:
            group_num += 1

        ais_array[index, 4] = group_num
        index += 1
        print(index)
    convert_df = pd.DataFrame(ais_array, columns=["unique_ID", "acquisition_time", "longitude", "latitude", "group"])
    convert_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201707-08_paint.csv", index=None)

def find_criminal_moor():
    """
    查找San Nicolas (Peru)与上海有来往的船舶
    :return:
    """

    for date in range(1, 9):

        moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/wrf/20170%s_moor_compress/part-00000" % date, header=None)
        moor_df.columns = ["mmsi", "begin_time", "end_time", "apart_time",
                           "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
                           "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
                           "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
                           "avg_sog", "var_sog", "max_sog", "maxSog_cog",
                           "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
                           "nowPortName", "nowPortLon", "nowPortLat"]
        moor_df = moor_df[moor_df['nowPortName'] != "None"]

        criminal_mmsi_list = []

        moor_gdf = moor_df.groupby('mmsi')
        for mmsi, value in moor_gdf:
            print(date, mmsi)
            value_array = np.array(value)
            # shanghai_bool = False
            nicolas_bool = False

            for line in value_array:
                nowPortName = line[-3].lower()

                # if "shanghai" in nowPortName:
                #     shanghai_bool = True

                if "San Nicolas (Peru)" == line[-3]:

                    nicolas_bool = True

                if nicolas_bool:
                    criminal_mmsi_list.append(mmsi)

        criminal_moor_df = moor_df[moor_df['mmsi'].isin(criminal_mmsi_list)]
        criminal_moor_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/nicolas_moor_20170%s.csv" % date, index=None)


def nm():
    """
    内贸船舶任务
    :return:
    """
    # 链接数据库，获取洋山海事局船舶数据
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    # 获取洋山泊位信息
    select_sql = """
                        SELECT * FROM cbjbxx
                        """
    cur.execute(select_sql)

    nm_ship_list = []
    for line in list(cur.fetchall()):
        try:
            nm_chineses_name = line[3]
            nm_english_name = line[4]
            imo = line[4].replace(" ", "")
            company1 = line[31]
            company2 = line[32]
            mmsi = int(line[40].replace(" ", ""))
            nm_ship_list.append([nm_chineses_name, nm_english_name, imo, company1, company2, mmsi])
        except Exception as e:
            print(e)

    ship_static_df = pd.DataFrame(nm_ship_list)
    ship_static_df.columns = ["chinese_name", "english_name", "imo", "company1", "company2", "mmsi"]
    ship_static_df['mmsi'] = ship_static_df['mmsi'].astype(int)
    print(ship_static_df.dtypes)
    print(len(ship_static_df))
    print("--------------------------------")

    # 获取周部门的船舶数据
    zhou_static_df = pd.read_excel("/home/qiu/Documents/sisi2017/zhoudequan_data/船舶匹配第一次.xlsx")
    print(zhou_static_df.dtypes)
    print(len(zhou_static_df))
    print("--------------------------------")
    # input("--------")

    # 匹配mmsi
    intersection_df = ship_static_df[ship_static_df['mmsi'].isin(zhou_static_df["MMSI"])]
    print(len(intersection_df))
    return intersection_df


def get_nm_moor(nm_statif_df):
    """
    获取内贸船舶的停泊事件
    :return:
    """
    for month in range(1, 9):
        print(month)
        moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_20170%s.csv" % month)
        # moor_df.columns = ["mmsi", "begin_time", "end_time", "apart_time",
        #                    "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
        #                    "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
        #                    "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
        #                    "avg_sog", "var_sog", "max_sog", "maxSog_cog",
        #                    "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
        #                    "nowPortName", "nowPortLon", "nowPortLat"]
        nm_moor_df = moor_df[moor_df['mmsi'].isin(nm_statif_df['mmsi'])]
        nm_moor_df = nm_moor_df[nm_moor_df["nowPortName"] != "None"]
        nm_moor_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_20170%s.csv" % month, index=None)


def filter_nm_ningbo():
    """
    筛选出内贸船舶中到过宁波的停泊事件
    :return:
    """
    nm_moor_df1 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_201701.csv")
    nm_moor_df2 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_201702.csv")
    nm_moor_df3 = pd.read_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_201703.csv")
    nm_moor_df = pd.concat([nm_moor_df1, nm_moor_df2, nm_moor_df3], ignore_index=True)

    nm_moor_df = nm_moor_df.loc[:, ['mmsi', "begin_time", "end_time", "nowPortName"]]
    nm_moor_df = nm_moor_df.sort_values(by=['mmsi', "begin_time"])

    nm_statif_df = pd.read_csv('/home/qiu/Documents/sisi2017/xukai_data/nm_static.csv')

    nm_ningbo_moor = []
    for line in np.array(nm_moor_df):
        if 'ningbo' in line[3]:
            nm_ningbo_moor.append([line[0], line[1], line[2], line[3]])

    nm_ningbo_moor_df = pd.DataFrame(nm_ningbo_moor, columns=['mmsi', "begin_time", "end_time", "nowPortName"])
    nm_moor_statif_df = pd.merge(nm_ningbo_moor_df, nm_statif_df, how='outer', on='mmsi')
    print(nm_moor_statif_df.head())

    company_list = []
    for line in np.array(nm_moor_statif_df):
        company1_bool = False
        company2_bool = False

        if str(line[7]).replace(" ", "").replace("-", ""):
            company_list.append(str(line[7]).replace(" ", "").replace("-", ""))
            continue
        else:
            company1_bool = True

        if str(line[8]).replace(" ", "").replace("-", ""):
            company_list.append(str(line[8]).replace(" ", "").replace("-", ""))
            continue
        else:
            company2_bool = True

        if ~(company1_bool | company2_bool):
            company_list.append('暂无')

    print(len(company_list), len(nm_moor_statif_df))
    # input("------1-----")
    nm_moor_statif_df['company'] = company_list
    nm_moor_statif_gdf = nm_moor_statif_df.groupby('company')
    company_ship_list = []
    for company, value in nm_moor_statif_gdf:
        count = 1
        index = 0
        value_array = np.array(value)
        while index < len(value) - 1:
            if (value_array[index, 2] - value_array[index+1, 1]) > 12 * 3600:
                count += 1
            index += 1
        company_ship_list.append([company.replace(",", ""), count])

    company_ship_df = pd.DataFrame(company_ship_list, columns=['company', 'count'])
    company_ship_df.to_csv("/home/qiu/Documents/sisi2017/xukai_data/nm_moor_result_201701-03.csv", index=None)
    return company_ship_df


if __name__ == "__main__":
    # intersection_df = nm()
    # get_nm_moor(intersection_df)
    company_ship_df = filter_nm_ningbo()
    # company_ship_df.to_csv('/home/qiu/Documents/sisi2017/xukai_data/nm_result_201701.csv', index=None)
