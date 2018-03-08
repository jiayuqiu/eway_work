# coding:utf-8

import numpy as np
import pandas as pd
import time
# import matplotlib.pyplot as plt

from base_func import point_poly

"""
根据谢文卿的要求，对水域内停泊事件进行优化。
优化要求：水域内的停泊时间采用进水域与离开水域的时间差。
"""

def ais_poly(ais, poly):
    try:
        inside_stage_index = []
        tmp_index = []

        pre_point_bool = point_poly(ais[0][2], ais[0][3], poly)

        for i in range(1, len(ais) - 1):
            # print poly
            i_point_bool = pre_point_bool
            ip1_point_bool = point_poly(ais[i + 1][2], ais[i + 1][3], poly)

            if (i_point_bool or ip1_point_bool):
                tmp_index.append(i)
                tmp_index.append(i + 1)
                if (i == (len(ais) - 2)):
                    if (len(tmp_index) != 0):
                        inside_stage_index.append(tmp_index)
                        tmp_index = []
            elif (not i_point_bool or ip1_point_bool):
                if (len(tmp_index) != 0):
                    inside_stage_index.append(tmp_index)
                    tmp_index = []
            pre_point_bool = ip1_point_bool

        outStr = ""
        for x in inside_stage_index:
            max_index = x[-1]
            min_index = x[0]

            anchor = poly[0][2]
            unique_id = ais[0][0]
            begin_time = ais[min_index][1]
            end_time = ais[max_index][1]
            interval = end_time - begin_time

            tmp_str = str(anchor) + "," + str(unique_id) + "," + str(begin_time) + "," + \
                      str(end_time) + "," + str(interval) + "\n"
            outStr = outStr + tmp_str
        return outStr
    except Exception as e:
        print(e)

if __name__ == "__main__":

    print("reading anchores ....")
    anchor = pd.read_csv("/home/qiu/文档/data/anchor/Asia_anchores.csv")  # 获得码头坐标信息
    # water_list = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03", "qingdao08",
    #               "tianjin06", "shanghai06", "shanghai07", "shanghai08", "shenzhen11", "shenzhen12",
    #               "rizhao03", "humen03", "yantai03", "qinzhou02", "quanzhou03", "xiamen06", "yingkou02",
    #               "ningbo08", "busan01", "hongkong01", "newyork02", "newjersey03", "rotterdam04",
    #               "Singapre03"]
    water_list = ["dalian03", "fuzhou04"]

    grouped_anchor = anchor.groupby('anchores_id')

    nav_str = ""
    print("reading ais ....")
    ais = pd.read_csv("ftp://qiu:qiu@192.168.1.72/data700_201509_more_aft_20161130.csv")
    # ais = ais.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    # print ais.head()
    # raw_input("=====================================")
    ais["longitude"] = ais["longitude"] / 1000000.0
    ais["latitude"] = ais["latitude"] / 1000000.0
    print("sorting ais ....")
    ais = ais.sort_index(by=['unique_ID', 'acquisition_time'])
    grouped_ais = ais.groupby("unique_ID")
    start_time = time.time()
    for group_anchor in grouped_anchor:
        group_list = list(group_anchor)
        group_num = len(group_list[1])

        each_anchor = []
        # mmsi-0 time-1 target_type-2 data_supplier-3 data_source-4 status -5 longitude-6 latitude -7 area_id-8
        # speed-9 conversion-10 cog-11 true_head-12 power-13 ext-14 extend-15
        each_anchor = np.array(group_list[1].iloc[0:group_num])

        if(each_anchor[1][2] in water_list):
            print(each_anchor[1][2], 'cor = ', group_num)
            for ship in grouped_ais:
                group_list = list(ship)
                group_num = group_list[1]['unique_ID'].count()
                each_ship = np.array(group_list[1].iloc[0:group_num])

                tmp_nav = ais_poly(each_ship, each_anchor)
                if(tmp_nav):
                    # print(tmp_nav)
                    # raw_input("=====================================")
                    nav_str = nav_str + tmp_nav
                else:
                    pass
        else:
            pass
    end_time = time.time()
    interval = end_time - start_time
    print("interval = %d" % interval)
    outFile = open("test_new_nav_2.csv", "w")
    outFile.write(nav_str)
    outFile.close()
    # print(ais.head())
