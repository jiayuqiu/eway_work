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


if __name__ == "__main__":
    year_list = [201408, 201508, 201608, 201708]
    for year in year_list:
        print(year)
        ais_poly_df = pd.read_csv('/home/qiu/Documents/ais_poly_%s/part-00000' % year, header=None)
        ais_poly_df.columns = ['dock', 'unique_ID', 'begin_time', 'end_time', 'interval']

        sh_list = ['shanghai01', 'shanghai02', 'shanghai03', 'shanghai04',
                   'shanghai05', 'shanghai06', 'shanghai07', 'shanghai08']
        nb_list = ['ningbo01', 'ningbo02', 'ningbo03', 'ningbo04',
                   'ningbo05', 'ningbo06', 'ningbo07', 'ningbo08']
        dl_list = ['dalian01', 'dalian02', 'dalian03']
        gz_list = ['guangzhou01', 'guangzhou02', 'guangzhou03', 'guangzhou04',
                   'guangzhou05', 'guangzhou06', 'guangzhou07', 'guangzhou08',
                   'guangzhou09', 'guangzhou10', 'guangzhou11', 'guangzhou12',
                   'guangzhou13']
        lyg_list = ['lianyungang01', 'lianyungang02', 'lianyungang03']
        qz_list = ['qinzhou01', 'qinzhou02']

        # sh_ais_poly_df = ais_poly_df[ais_poly_df['dock'].isin(sh_list)]
        # sum_time_list = sum_port_dock_time(sh_ais_poly_df, [qz_list[0]], [qz_list[-1]])
        sum_time_list = sum_port_dock_time_each(ais_poly_df, qz_list)
        sum_time_df = pd.DataFrame(sum_time_list, columns=['unique_ID', 'dock', 'interval'])
        sum_time_df = sum_time_df[sum_time_df['interval'] != 0]
        sum_time_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/钦州_分_%s_.csv' % year, index=None)
        # print(sum_time_df[sum_time_df['dock_time'] != 0].describe())
