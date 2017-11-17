# coding:utf-8

import pandas as pd
import numpy as np


def sum_port_dock_time(ship_ais_poly_df, dock_list, port_list):
    """
    计算船舶在码头和水域内的停泊时间
    :param ship_ais_poly_df: 在多边形内的时间数据，类型：data frame
    :param loc_list: 码头与水域列表，类型：list
    :return: 在码头与水域内的时间，类型：data frame
    """
    # sh_dock_list = ['shanghai01', 'shanghai02', 'shanghai03', 'shanghai04', 'shanghai05']
    # sh_port_list = ['shanghai06', 'shanghai07', 'shanghai08']
    print(dock_list)
    print(port_list)

    ship_ais_poly_gdf = ship_ais_poly_df.groupby('unique_ID')
    sum_time_list = []
    for mmsi, value in ship_ais_poly_gdf:
        dock_time = 0
        port_time = 0
        value_array = np.array(value)
        for line in value_array:
            if line[0] in dock_list:
                dock_time += line[4]
            elif line[0] in port_list:
                port_time += line[4]
        sum_time_list.append([mmsi, dock_time, port_time])
    return sum_time_list


def sum_port_dock_time_each(ship_ais_poly_df, loc_list):
    ship_ais_poly_gdf = ship_ais_poly_df.groupby('unique_ID')
    sum_time_list = []
    for mmsi, value in ship_ais_poly_gdf:
        value_array = np.array(value)
        # print(mmsi)
        for loc_ in loc_list:
            tmp_value_array = value_array[value_array[:, 0] == loc_]
            if len(tmp_value_array) > 0:
                sum_time_list.append([mmsi, loc_, np.sum(tmp_value_array[:, 4])])
    return sum_time_list


def match_teu():
    """
    对船舶匹配teu
    :return:
    """


if __name__ == "__main__":
    # error_file = open('/home/qiu/Documents/sisi2017/zhaonan_data/ais_poly_guangzhou_201708/part-00000', 'r')
    # right_file = open('/home/qiu/Documents/sisi2017/zhaonan_data/ais_poly_guangzhou_201708/convert-00000', 'w')
    # # data = ''
    # tmp_str = ''
    # count = 0
    # for line in error_file:
    #     if count % 2 == 0:
    #         tmp_str = tmp_str + line.split('\n')[0]
    #     else:
    #         tmp_str = tmp_str + line
    #         right_file.write(tmp_str)
    #         # data += tmp_str
    #         tmp_str = ''
    #         # print(data)
    #     count += 1
    #     print(count)
    #     # input("----------------------------")
    # error_file.close()
    # right_file.close()

    # # 按要求统计在码头停泊时间与在水域停泊时间
    # year_list = [201708]
    # for year in year_list:
    #     print(year)
    #     ais_poly_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/ais_poly_guangzhou_201708/convert-00000', header=None)
    #     print(ais_poly_df.head())
    #     # input("---------------------")
    #     ais_poly_df.columns = ['dock', 'unique_ID', 'begin_time', 'end_time', 'interval']
    #
    #     sh_list = ['shanghai01', 'shanghai02', 'shanghai03', 'shanghai04',
    #                'shanghai05', 'shanghai06', 'shanghai07', 'shanghai08']
    #     nb_list = ['ningbo01', 'ningbo02', 'ningbo03', 'ningbo04',
    #                'ningbo05', 'ningbo06', 'ningbo07', 'ningbo08']
    #     dl_list = ['dalian01', 'dalian02', 'dalian03']
    #     # gz_list = ['guangzhou01', 'guangzhou02', 'guangzhou03', 'guangzhou04',
    #     #            'guangzhou05', 'guangzhou06', 'guangzhou07', 'guangzhou08',
    #     #            'guangzhou09', 'guangzhou10', 'guangzhou11', 'guangzhou12',
    #     #            'guangzhou13']
    #     gz_list = ['guangzhou01', 'guangzhou02', 'guangzhou03', 'guangzhou04',
    #                'guangzhou05', 'guangzhou06', 'guangzhou07']
    #     lyg_list = ['lianyungang01', 'lianyungang02', 'lianyungang03']
    #     qz_list = ['qinzhou01', 'qinzhou02']
    #
    #     # sh_ais_poly_df = ais_poly_df[ais_poly_df['dock'].isin(sh_list)]
    #     # sum_time_list = sum_port_dock_time(ais_poly_df, gz_list[:-1], [gz_list[-1]])
    #     sum_time_list = sum_port_dock_time_each(ais_poly_df, gz_list)
    #     sum_time_df = pd.DataFrame(sum_time_list, columns=['unique_ID', 'dock', 'interval'])
    #     # sum_time_df = pd.DataFrame(sum_time_list, columns=['unique_ID', 'dock_time', 'port_time'])
    #     # sum_time_df = sum_time_df[sum_time_df['dock_time'] != 0.]
    #     sum_time_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/广州_分_%s_.csv' % year, index=None)
    #     # print(sum_time_df[sum_time_df['dock_time'] != 0].describe())

    # 获取集装箱船舶静态信息
    container_ship_df = pd.read_excel('/home/qiu/Documents/sisi2017/zhaonan_data/船型类别.xlsx')
    container_ship_df = container_ship_df[~container_ship_df['MMSI'].isnull()]
    container_ship_df['MMSI'] = container_ship_df['MMSI'].astype('int')
    print(container_ship_df.columns)

    # 获取进出多边形数据
    location = '钦州'

    ais_poly_sum_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_总/%s_总_201708_.csv' % location)
    ais_poly_dock_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_分/%s_分_201708_.csv' % location)
    ais_poly_dock_df.columns = ['MMSI', 'dock', 'interval']
    ais_poly_sum_df.columns = ['MMSI', 'dock_time', 'port_time']

    ais_poly_sum_df['MMSI'] = ais_poly_sum_df['MMSI'].astype('int')
    ais_poly_dock_df['MMSI'] = ais_poly_dock_df['MMSI'].astype('int')

    merge_sum_df = pd.merge(ais_poly_sum_df, container_ship_df, how='left', on='MMSI')
    merge_sum_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_总/%s_总_201708_teu.csv' % location, index=None)

    merge_dock_df = pd.merge(ais_poly_dock_df, container_ship_df, how='left', on='MMSI')
    merge_dock_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_分/%s_分_201708_teu.csv' % location, index=None)
