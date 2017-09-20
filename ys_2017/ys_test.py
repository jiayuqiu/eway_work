# coding:utf-8

import pandas as pd
import numpy as np

from base_func import getDist, getFileNameList


def coordinates_kml(file_path):
    """
    从kml文件中获取经纬度数据
    :param file_path: kml文件所在路径，类型：string
    :return: 经纬坐标，用“;”分割，类型：list
    """
    kml_file = open(file_path, "r")
    kml_string = kml_file.read()
    coordinates_string = kml_string.split("<coordinates>")[1]\
                                   .split("</coordinates>")[0]\
                                   .replace("\t", "") \
                                   .replace("\n", "")
    coordinates_list = coordinates_string.split(" ")
    out_put_coordinates_list = []
    for coordinate in coordinates_list:
        if coordinate:
            tmp_coordinate = coordinate.split(",")
            out_put_coordinates_list.append([tmp_coordinate[0], tmp_coordinate[1]])
    return out_put_coordinates_list


def point_poly(pointLon, pointLat, polygon):
    """
    判断点是否在多边形内
    :param pointLon: 该点的经度，类型：float
    :param pointLat: 该点的唯独，类型：float
    :param polygon: 多边形的经纬度坐标列表，类型：list
    :return: t/f
    """
    polygon = np.array(polygon)
    polygon = np.array([[float(x) for x in line] for line in polygon])
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while (i < cor):
        if ((((polygon[i, 1] < pointLat) & (polygon[j, 1] >= pointLat))
                 | ((polygon[j, 1] < pointLat) & (polygon[i, 1] >= pointLat)))
                & ((polygon[i, 0] <= pointLon) | (polygon[j, 0] <= pointLon))):
            a = (polygon[i, 0] +
                 (pointLat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if (a < pointLon):
                inside = not inside
        j = i
        i = i + 1

    return inside


def get_paint_data(ys_ais_df):
    """
    获取作图数据
    :param ys_ais_df: 洋山AIS数据，类型data frame
    :return: mmsi船舶的mmsi号，类型data frame
    """
    print("all mmsi length is %d" % len(list(set(ys_ais_df["unique_ID"]))))
    gdf = ys_ais_df.groupby("unique_ID")

    mmsiList = []
    for key, group in gdf:
        if (len(group) > 500) & (len(group) < 1000):
            # print(key)
            mmsiList.append(key)
    mmsiDF = pd.DataFrame(mmsiList)
    print("paint mmsi length is %d " % len(mmsiDF))
    mmsiDF.columns = ["mmsi"]
    return mmsiDF


def move_time_axis(ys_ais_data):
    """
    移动时间轴
    :param ys_ais_data: 警戒区内的AIS数据，类型data frame
    :return: 移动过时间轴后的警戒区内AIS数据，类型list
    """
    ys_ais_data = ys_ais_data.sort_values(by=["unique_ID", "acquisition_time"])
    ys_ais_data = np.array(ys_ais_data)
    moved_list = []
    index = 0
    group = 0
    while index < len(ys_ais_data):
        if index == 0:
            str_time = ys_ais_data[index][1]
        else:
            if ys_ais_data[index][0] != ys_ais_data[index - 1][0]:
                group = group_num
                group += 1
                str_time = ys_ais_data[index][1]
        new_time = int(ys_ais_data[index][1] - str_time)
        now_count = new_time // 10800
        new_time = new_time % 10800
        group_num = group + now_count
        moved_list.append([group_num, new_time,
                          ys_ais_data[index][2], ys_ais_data[index][3]])
        index += 1
    return moved_list


def diff_enter_out_stage(ys_ais, area_poly, group_num):
    """
    利用主巷道的多边形范围，判断船舶进出多边形的航段
    :param ys_ais: 洋山的AIS数据，类型：data frame
    :param area_poly: 主航道多边形经纬度数据，类型：list
    :param group_num: 船舶分组序号，类型：int
    :return: 将unique_ID变为group来区分，进出多边形的航段，类型：data frame
    """
    ys_ais = np.array(ys_ais)

    inside_stage_index = []
    tmp_index = []

    ais = [[float(x) for x in inner] for inner in ys_ais]
    # ais.sort(key=lambda y: y[1])
    pre_point_bool = point_poly(ais[0][2], ais[0][3], area_poly)

    if len(ais) > 1:
        for i in range(0, len(ais) - 1):
            # print poly
            i_point_bool = pre_point_bool
            ip1_point_bool = point_poly(ais[i + 1][2], ais[i + 1][3], area_poly)

            if i_point_bool | ip1_point_bool:
                tmp_index.append(i)
                # tmp_index.append(i + 1)
                if i == (len(ais) - 2):
                    if len(tmp_index) != 0:
                        tmp_index.append(i + 1)
                        inside_stage_index.append(tmp_index)
                        tmp_index = []
            else:
                if len(tmp_index) != 0:
                    tmp_index.append(i)
                    inside_stage_index.append(tmp_index)
                    tmp_index = []
            pre_point_bool = ip1_point_bool

    elif len(ais) == 2:
        if pre_point_bool:
            tmp_index.append(0)
            tmp_index.append(1)
            inside_stage_index.append(tmp_index)

    out_put_list = []
    for index, x in enumerate(inside_stage_index):
        str_time = ys_ais[x[0]][1]
        for inside_index in x:
            out_put_list.append([group_num, ys_ais[inside_index][1] - str_time,
                                 ys_ais[inside_index][2], ys_ais[inside_index][3]])
        group_num += 1
    return out_put_list, group_num


def inside_poly_ais(ys_ais, area_poly, group_num):
    """
    找出在多边形内的AIS数据
    :param ys_ais: 洋山的AIS数据，类型：data frame
    :param area_poly: 主航道多边形经纬度数据，类型：list
    :param group_num: 船舶分组序号，类型：int
    :return: 返回在多边形内的AIS数据，类型：data frame
    """
    ys_ais = np.array(ys_ais)
    out_list = []

    index = 0
    ais_len = len(ys_ais)
    if ais_len > 1:
        while index < ais_len:
            if point_poly(ys_ais[index][2], ys_ais[index][3], area_poly):
                out_list.append([group_num, ys_ais[index][1], ys_ais[index][2],
                                 ys_ais[index][3]])
            else:
                str_time = 0
                group_num += 1
            index += 1
    return out_list, group_num


def get_ais_inside_circle(ys_ais, center, radius=1.852 * 20):
    """
    找到在警戒区内，进入过警戒范围圆形的AIS数据
    :param ys_ais: 洋山的AIS数据，类型：data frame
    :param center: 圆形警戒范围的圆心坐标，类型：list
    :param radius: 圆形警戒范围的半径，单位，海里。类型：float
    :return: 在警戒范围内的出现过的船舶MMSI，类型：list
    """
    ys_ais = np.array(ys_ais)

    out_mmsi = []
    for line in ys_ais:
        dst = getDist(line[2], line[3], center[0], center[1])
        if dst < radius:
            out_mmsi.append(line[0])
    return list(set(out_mmsi))


def sub_main_channel(main_channel_ais, enter_poly_df):
    """
    分辨出主航道内的不同航段
    :param main_channel_ais: 主航道内的AIS数据，类型：data frame
    :param enter_poly_df: 航段入口多边形数据，类型：data frame
    :return: 在main_channel_ais的基础上增加一个字段，用于区分主航道内航段，类型：data frame
    """
    main_channel_ais_gdf = main_channel_ais.groupby("unique_ID")
    enter_poly_gdf = enter_poly_df.groupby("poly_id")
    sub_channel_ais_list = []
    for key, value in main_channel_ais_gdf:
        value_length = len(value)
        print(key)
        inside_poly_str = ""
        inside_poly_id = None
        for index in range(value_length):
            for poly_id, poly in enter_poly_gdf:
                if point_poly(float(value.iloc[index, 2]), float(value.iloc[index, 3]),
                              poly):
                    # inside_poly_str = inside_poly_str + str(poly_id) + ";"
                    inside_poly_id = poly_id
                    break
            sub_channel_ais_list.append([value.iloc[index, 0], value.iloc[index, 1],
                                         value.iloc[index, 2], value.iloc[index, 3], inside_poly_id])
    return sub_channel_ais_list


def sub_main_channel_poly_filter(warning_area_ais, enter_num, out_num, poly_df):
    """
    在警戒范围内的AIS数据，根据北上，南下，东西，西东航向来区分
    :param warning_area_ais: 警戒范围内的AIS数据
    :param enter_num: 入口编号，类型：int
    :param out_num: 出口编号，类型:int
    :param poly_df: 入口（出口）多边形坐标数据，类型：data frame
    :return: 
    """
    warning_area_ais_gdf = warning_area_ais.groupby("unique_ID")
    print(poly_df)
    enter_poly_df = poly_df[poly_df["poly_id"] == enter_num]
    out_poly_df = poly_df[poly_df["poly_id"] == out_num]

    sub_channel_mmsi = []
    for key, value in warning_area_ais_gdf:
        value_array = np.array(value)
        value_length = len(value)
        print(key)
        mid_idx = value_length // 2
        # 初始化对入口、出口的判断参数
        enter_bool = False
        out_bool = False

        if value_length >= 3:
            # 找到入口
            for enter_idx in range(mid_idx):
                if point_poly(value_array[enter_idx, 2], value_array[enter_idx, 3], enter_poly_df):
                    input("=========enter==========")
                    enter_bool = True
                else:
                    pass

            # 找到出口
            for out_idx in range(mid_idx, value_length):
                if point_poly(value_array[enter_idx, 2], value_array[enter_idx, 3], out_poly_df):
                    input("=========out==========")
                    out_bool = True
                else:
                    pass

        # 若满足出口与入口都是指定的编号，则记录mmsi编号
        if enter_bool & out_bool:
            sub_channel_mmsi.append(key)

    # 根据获取到的mmsi编号抽取AIS数据
    sub_channel_ais = warning_area_ais[warning_area_ais["unique_ID"].isin(sub_channel_mmsi)]
    return sub_channel_ais


def multiple_poly_list(kml_list):
    """
    从多个kml文件中整合多边形坐标数据
    :param kml_list: kml文件路径列表，类型：list
    :return: 多个多边形坐标数据，类型：data frame
    """
    poly_list = []
    for kml in kml_list:
        coordinates_list = coordinates_kml(kml)
        for line in coordinates_list:
            line.append(kml[-5])
            poly_list.append(line)
    poly_df = pd.DataFrame(poly_list)
    poly_df.columns = ["longitude", "latitude", "poly_id"]
    return poly_df


def sum_ship_warning_area(ys_ais, center, radius=1.852 * 20, interval=10 * 60):
    """
    统计洋山警戒范围每10分钟内的船舶数量
    :param ys_ais: 洋山水域范围内的AIS数据，类型：data frame
    :param center: 警戒范围圆心，类型：list
    :param radius: 警戒范围半径，单位：公里，类型：float
    :param interval: 统计间隔时间，单位：秒，类型：int
    :return: 每隔10分钟，警戒范围内的船舶数量，类型：data frame
    """

def enter_out_poly(sub_channel_ais):
    """
    利用进出警戒区的多边形编号来区分形式的子航道
    :param sub_channel_ais: 洋山水域范围内的AIS数据，类型：data frame
    :return: 暂定
    """
    sub_channel_df = sub_channel_ais[~sub_channel_ais["poly_id"].isnull()]
    print(sub_channel_df.head())
    for index, value in sub_channel_df.iterrows():
        print("index = %d" % index)
        if value["poly_id"]:
            poly_id_list = value["poly_id"].split(";")
            if len(set(poly_id_list)) == 3:
                print(set(poly_id_list))
                input("-----------------------")


if __name__ == "__main__":
    # # 获取洋山水域内AIS数据
    # data = pd.read_csv("/home/qiu/Documents/ys_ais/pre_201606_ys.csv", header=None)
    # data.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                 "true_head", "power", "ext", "extend"]
    # data = data.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    # print(data.head())
    # data["longitude"] = data["longitude"] / 1000000.
    # data["latitude"] = data["latitude"] / 1000000.
    #
    # # 获取境界范围多边形数据
    # kml_path = "/home/qiu/Documents/ys_ais/洋山警戒范围.kml"
    # coordinates_list = coordinates_kml(kml_path)
    # coordinates_list = [[float(x) for x in line] for line in coordinates_list]
    #
    # gdf = data.groupby("unique_ID")
    # str_group_num = 0
    # out_list = []
    # for mmsi, value in gdf:
    #     print(mmsi)
    #     out_put_list, group_num = inside_poly_ais(ys_ais=value, area_poly=coordinates_list,
    #                                               group_num=str_group_num)
    #     if len(out_put_list) > 0:
    #         out_list.extend(out_put_list)
    # out_df = pd.DataFrame(out_list)
    # out_df.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # out_df = out_df.sort_values(by=["unique_ID", "acquisition_time"])
    # out_df.to_csv("inside_poly_ais.csv", index=None)

    #--------------------------------------------------------------
    # # 找出在给定多边形的AIS数据
    # out_df = pd.read_csv("inside_poly_ais.csv")
    # print(len(set(out_df["unique_ID"])))
    # inside_circle_mmsi = get_ais_inside_circle(out_df, [122.1913, 30.5658])
    # print(inside_circle_mmsi)
    # out_df = out_df[out_df["unique_ID"].isin(inside_circle_mmsi)]
    # moved_out_df = pd.DataFrame(move_time_axis(out_df))
    # moved_out_df.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # print(len(moved_out_df))
    # moved_out_df = pd.DataFrame(move_time_axis(moved_out_df))
    # moved_out_df.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    # print(len(moved_out_df))
    # moved_out_df.to_csv("/home/qiu/Documents/ys_ais/201606_paint_ais_opt_2.csv", index=None)

    #----------------------------------------------------------------
    # 区分进入主航道是的入口编号
    kml_path_list = ["/Users/qiujiayu/data/警戒区进口1.kml",
                     "/Users/qiujiayu/data/警戒区进口2.kml",
                     "/Users/qiujiayu/data/警戒区进口3.kml",
                     "/Users/qiujiayu/data/警戒区进口4.kml",
                     "/Users/qiujiayu/data/警戒区进口5.kml",
                     "/Users/qiujiayu/data/警戒区进口6.kml"]
    poly_df = multiple_poly_list(kml_path_list)
    
    print(poly_df.head())
    main_channel_ais = pd.read_csv("/Users/qiujiayu/data/201606_paint_ais_opt_2.csv")
    print(main_channel_ais.head())
    sub_channel_ais = sub_main_channel_poly_filter(main_channel_ais, 6, 2, poly_df)
    # sub_channel_ais = pd.DataFrame(sub_channel_ais)
    # sub_channel_ais.columns = ["unique_ID", "acquisition_time", "longitude", "latitude", "poly_id"]
    # sub_channel_ais = sub_channel_ais[~sub_channel_ais["poly_id"].isnull()]
    sub_channel_ais.to_csv("/Users/qiujiayu/data/sub_channel_ais_index_poly.csv", index=None)
    print(sub_channel_ais)

    # # 找到进入警戒区的入口与出口编号
    # sub_channel_ais = pd.read_csv("/home/qiu/Documents/ys_ais/sub_channel_ais.csv")
    # enter_out_poly(sub_channel_ais)
