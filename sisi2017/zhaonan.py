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
    # # sh_dock_list = ['shanghai01', 'shanghai02', 'shanghai03', 'shanghai04', 'shanghai05']
    # # sh_port_list = ['shanghai06', 'shanghai07', 'shanghai08']

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
        sum_time_list.append([int(mmsi), dock_time, port_time])
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
                sum_time_list.append([int(mmsi), loc_, np.sum(tmp_value_array[:, 4])])
    return sum_time_list


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

    # 获取水域与码头名称对应关系
    dock_port_df = pd.read_csv("/home/qiu/Documents/sisi2017/zhaonan_data/水域码头区分.csv")
    dock_port_array = np.array(dock_port_df)

    # 获取集装箱船舶静态数据
    container_ship_df = pd.read_excel('/home/qiu/Documents/sisi2017/zhaonan_data/船型类别.xlsx')

    # 按要求统计在码头停泊时间与在水域停泊时间
    year_list = [201609, 201610, 201611, 201612]
    # year_list = [201708]
    for year in year_list:
        print(year)
        # 循环获取每年的进出多边形信息
        ais_poly_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/ais_poly_16ports_%s/part-00000' % year,
                                  header=None)
        ais_poly_df.columns = ['dock', 'unique_ID', 'begin_time', 'end_time', 'interval']

        # 循环判断16个港口中每个港口
        for location_line in dock_port_array:
            location_string = location_line[0]
            print(location_string)

            # 获取码头、水域编号
            all_list = []
            dock_list = list(range(1, (location_line[1]+1)))
            dock_string_list = []
            for dock_id_int in dock_list:
                if dock_id_int < 10:
                    dock_id_string = "0" + str(dock_id_int)
                else:
                    dock_id_string = str(dock_id_int)

                dock_string = location_string + dock_id_string
                dock_string_list.append(dock_string)

            port_list = [int(port_id) for port_id in location_line[2].split(";")]
            port_string_list = []
            for port_id_int in port_list:
                if port_id_int < 10:
                    port_id_string = "0" + str(port_id_int)
                else:
                    port_id_string = str(port_id_int)

                port_string = location_string + port_id_string
                port_string_list.append(port_string)

            all_list.extend(dock_string_list)
            all_list.extend(port_string_list)

            # 统计一个港口总停泊时间数据，每个码头作为一个统计单位
            each_dock_port_df = pd.DataFrame(sum_port_dock_time_each(ais_poly_df, all_list),
                                             columns=['MMSI', 'dock', 'interval'])
            merge_each_df = pd.merge(each_dock_port_df, container_ship_df, how='left', on='MMSI')
            merge_each_df.to_csv("/home/qiu/Documents/sisi2017/zhaonan_data/16港口_分/"
                                 "%s_%s_分.csv" % (location_string, year), index=None)
            print(len(merge_each_df))

            # 统计一个港口总停泊时间数据，统计一条船舶在水域与码头的总停泊时间
            all_dock_port_df = pd.DataFrame(sum_port_dock_time(ais_poly_df, dock_string_list, port_string_list),
                                            columns=['MMSI', 'dock_time', 'port_time'])
            all_dock_port_df = all_dock_port_df[all_dock_port_df['dock_time'] != 0]
            merge_sum_df = pd.merge(all_dock_port_df, container_ship_df, how='left', on='MMSI')
            merge_sum_df.to_csv("/home/qiu/Documents/sisi2017/zhaonan_data/16港口_总/"
                                "%s_%s_总.csv" % (location_string, year), index=None)
            print(len(merge_sum_df))


    # # 获取集装箱船舶静态信息
    # container_ship_df = pd.read_excel('/home/qiu/Documents/sisi2017/zhaonan_data/船型类别.xlsx')
    # container_ship_df = container_ship_df[~container_ship_df['MMSI'].isnull()]
    # container_ship_df['MMSI'] = container_ship_df['MMSI'].astype('int')
    # print(container_ship_df.columns)
    #
    # # 获取进出多边形数据
    # location = '钦州'
    #
    # ais_poly_sum_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_总/%s_总_201708_.csv' % location)
    # ais_poly_dock_df = pd.read_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_分/%s_分_201708_.csv' % location)
    # ais_poly_dock_df.columns = ['MMSI', 'dock', 'interval']
    # ais_poly_sum_df.columns = ['MMSI', 'dock_time', 'port_time']
    #
    # ais_poly_sum_df['MMSI'] = ais_poly_sum_df['MMSI'].astype('int')
    # ais_poly_dock_df['MMSI'] = ais_poly_dock_df['MMSI'].astype('int')
    #
    # merge_sum_df = pd.merge(ais_poly_sum_df, container_ship_df, how='left', on='MMSI')
    # merge_sum_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_总/%s_总_201708_teu.csv' % location, index=None)
    #
    # merge_dock_df = pd.merge(ais_poly_dock_df, container_ship_df, how='left', on='MMSI')
    # merge_dock_df.to_csv('/home/qiu/Documents/sisi2017/zhaonan_data/6港口_分/%s_分_201708_teu.csv' % location, index=None)
