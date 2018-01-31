# coding:utf-8

import numpy as np
import pandas as pd

import gc


def match_cape_moor():
    """
    匹配cape型船舶mmsi
    :return:
    """
    cape_df = pd.read_excel("/home/qiu/Documents/sisi2017/zhangyongfeng_data/海岬型数据（clarksons）.xlsx",
                            sheetname="Sheet1")

    bm_static_df = pd.read_csv("/home/qiu/Documents/sisi2017/wrf/bm_unique_static_201708.csv")
    bm_static_df = bm_static_df[bm_static_df['IMO'] != 0]
    bm_cape_df = pd.merge(bm_static_df, cape_df, left_on="IMO", right_on="IMO Number")

    # for month in range(1, 9):
    #     print("month = %s" % month)
    #     moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/wrf/20170%s_moor_compress/part-00000" % month, header=None)
    #     moor_df.columns =  ["mmsi", "begin_time", "end_time", "apart_time",
    #                         "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
    #                         "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
    #                         "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
    #                         "avg_sog", "var_sog", "max_sog", "maxSog_cog",
    #                         "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
    #                         "nowPortName", "nowPortLon", "nowPortLat"]
    #
    #     cape_moor_df = moor_df[moor_df['mmsi'].isin(bm_cape_df['MMSI'])]
    #     cape_moor_df.to_csv("/home/qiu/Documents/sisi2017/zhangyongfeng_data/cape_moor_20170%s.csv" % month, index=None)

    return bm_cape_df


def get_port_moor():
    """
    根据给到4个出口港口，3个进口港口来筛选停泊事件
    :return:
    """
    import_port_list = ["rotterdam", "qingdao", "ningbo"]
    export_port_list = ["tubarao", "dampier", "hedland", "newcastle (australia)"]

    bm_moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/zhangyongfeng_data/2017_quarterly_moor_3.csv")
    bm_moor_df = bm_moor_df.sort_values(by=["mmsi", "begin_time"])
    bm_moor_array = np.array(bm_moor_df)

    # 从cape型船的停泊事件中，找到发生在主要航线码头的停泊事件
    main_routes_moor_list = []
    index = 0
    for bm_moor_line in bm_moor_array:
        if index % 10000 == 0:
            print(index)
        index += 1

        for export_port in export_port_list:
            if export_port in bm_moor_line[30].lower():
                main_routes_moor_list.append([bm_moor_line[0], bm_moor_line[1], bm_moor_line[2], bm_moor_line[30]])

        for import_port in import_port_list:
            if import_port in bm_moor_line[30].lower():
                main_routes_moor_list.append([bm_moor_line[0], bm_moor_line[1], bm_moor_line[2], bm_moor_line[30]])

    # 对在7个港口之间的cape型船组合港口对
    main_routes_pair_list = []
    moor_index = 0
    while moor_index < (len(main_routes_moor_list) - 1):
        if main_routes_moor_list[moor_index][3] != main_routes_moor_list[moor_index+1][3]:
            main_routes_pair_list.append([main_routes_moor_list[moor_index][0],
                                          main_routes_moor_list[moor_index][2],
                                          main_routes_moor_list[moor_index+1][1],
                                          main_routes_moor_list[moor_index][3],
                                          main_routes_moor_list[moor_index+1][3]])
        moor_index += 1
    main_routes_pair_df = pd.DataFrame(main_routes_pair_list, columns=['mmsi', 'str_time', 'end_time', 'str_port',
                                                                       'end_port'])
    main_routes_pair_df.to_csv('/home/qiu/Documents/sisi2017/zhangyongfeng_data/2017_quarterly_main_route_moor_3.csv',
                               index=None)


def main_route_statistic(cape_df):
    """
    统计主航线上的运力等信息
    :return:
    """
    import_port_list = ["rotterdam", "qingdao", "ningbo"]
    export_port_list = ["tubarao", "dampier", "hedland", "newcastle (australia)"]

    # mmsi,str_time,end_time,str_port,end_port
    bm_moor_df = pd.read_csv("/home/qiu/Documents/sisi2017/zhangyongfeng_data/2017_quarterly_main_route_moor_2.csv")
    bm_moor_df = bm_moor_df.sort_values(by=['mmsi', 'str_time'])
    bm_moor_array = np.array(bm_moor_df)
    print(len(bm_moor_df))

    main_route_list = []
    for export_port in export_port_list:
        for import_port in import_port_list:
            main_route_list.append([export_port, import_port])
    print(main_route_list)

    main_route_result = []

    # 对每条船的停泊时间进行分析
    for port_pair in main_route_list:
        port_pair_moor_list = []
        for bm_moor_line in bm_moor_array:
            port1_bool = False
            port2_bool = False

            if port_pair[0] in bm_moor_line[3].lower():
                port1_bool = True
            if port_pair[1] in bm_moor_line[3].lower():
                port2_bool = True
            if port_pair[0] in bm_moor_line[4].lower():
                port1_bool = True
            if port_pair[1] in bm_moor_line[4].lower():
                port2_bool = True

            if port1_bool & port2_bool:
                port_pair_moor_list.append(bm_moor_line)

        main_route_result_df = pd.DataFrame(port_pair_moor_list, columns=['mmsi', 'str_time', 'end_time', 'str_port',
                                                                          'end_port'])
        main_route_result_df = pd.merge(main_route_result_df, cape_df, left_on='mmsi', right_on='MMSI')
        main_route_result_df = main_route_result_df.loc[:, ["mmsi", "str_time", "end_time", "str_port",
                                                            "end_port", "Dwt"]]
        print(len(main_route_result_df))
        main_route_result_df.to_csv("/home/qiu/Documents/sisi2017/zhangyongfeng_data/2017年主航线结果/201704-201706/"
                                    "%s--%s.csv" % (port_pair[0], port_pair[1]), index=None)


def get_cape_ais_dynamic(bm_cape_df):
    """
    获取cape型船舶的动态AIS数据
    :param bm_cape_df: cape型船舶的静态数据
    :return:
    """
    month_str = "01"
    for date in range(1, 32):
        if date < 10:
            date_str = "0" + str(date)
        else:
            date_str = str(date)

        ais_df = pd.read_csv("/media/qiu/新加卷/2017/%s/date_1" % month_str, header=None)


if __name__ == "__main__":
    bm_cape_df = match_cape_moor()

    # main_routes_mmsi = get_port_moor()

    main_route_statistic(bm_cape_df)
    print('Hello World!')
