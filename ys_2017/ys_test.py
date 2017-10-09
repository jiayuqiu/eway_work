# coding:utf-8

import pandas as pd
import numpy as np
import urllib.request
import json
import re
import time
import lxml

from base_func import getDist, getFileNameList
from lxml.html import tostring


####################################################

# xpath爬取网页数据，中文解码
def _callback(matches):
    id = matches.group(1)
    try:
        return chr(int(id))
    except:
        return id


def decode_unicode_references(data):
    return re.sub("&#(\d+)(;|(?=\s))", _callback, str(data))


def coordinates_kml(file_path):
    """
    从kml文件中获取经纬度数据
    :param file_path: kml文件所在路径，类型：string
    :return: 经纬坐标，用“;”分割，类型：list
    """
    # 打开kml文件
    kml_file = open(file_path, "r")

    # 将kml中的文件转换成string后，将经纬度坐标信息转换成list
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


def move_time_axis(ys_ais_data, max_time=14400):
    """
    移动时间轴
    :param ys_ais_data: 警戒区内的AIS数据，类型data frame
    :param max_time: 若移动时间轴后，时间仍然过大，则设置最大时间再次移动时间轴，类型：int
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
        now_count = new_time // max_time
        new_time = new_time % max_time
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
    # 将ais数据data frame转换为矩阵
    ys_ais = np.array(ys_ais)

    # 初始化在多边形内的航段索引
    inside_stage_index = []
    tmp_index = []

    # 将ais数据中的数据转换为数值类型
    ais = [[float(x) for x in inner] for inner in ys_ais]

    # 对时间轴进行排序
    ais.sort(key=lambda y: y[1])

    # 特殊判断：判断第一条数据是否在多边形内
    pre_point_bool = point_poly(ais[0][2], ais[0][3], area_poly)

    # 判断：若ais数据条数大于或等于2条时，才进行找航段操作
    if len(ais) > 1:
        # 从第一条数据开始循环判断，直到倒数第二条
        for i in range(0, len(ais) - 1):
            # 判断当前ais数据与下条ais数据是否在多边形内
            i_point_bool = pre_point_bool
            ip1_point_bool = point_poly(ais[i + 1][2], ais[i + 1][3], area_poly)

            # 判断：若有一条在多边形内，则记录当前行所在的索引
            if i_point_bool | ip1_point_bool:
                tmp_index.append(i)

                # 判断：若当前已经循环到倒数第三条
                if i == (len(ais) - 2):
                    # 判断：是否存在在多边形内的航段
                    if len(tmp_index) != 0:
                        # 将最后一条数据进行输出，并清空暂存航段
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


def enter_circle_ais(ys_ais, pre_time=5400, aft_time=5400):
    """
    找出进入过警戒范围的ais数据，并追溯其进入前与离开后1.5小时的ais数据
    :param ys_ais: 洋山水域范围内的ais数据，类型：data frame
    :param pre_time: 进入前追溯的时间，单位：秒，类型：int
    :param aft_time: 离开后追溯的时间，单位：秒，类型：int
    :return: 进入过警戒范围的ais数据，并追溯其进入前与离开后1.5小时的ais数据，类型:data frame
    """
    entered_circle_ais_array = []
    # 对ys_ais进行分组
    ys_ais_gdf = ys_ais.groupby('unique_ID')

    # 若存在进入过警戒区，则追溯其进入前与离开后1.5小时的ais数据
    for key, value in ys_ais_gdf:
        print("mmsi = %d" % key)
        group_ais_array = np.array(value)
        # 对单船ais数据按时间维度进行排序
        # group_ais_array = group_ais_array[group_ais_array[:, 1].argsort()]

        # 循环每行，找到在警戒范围内的数据点，并追溯
        group_ais_index = 0
        while group_ais_index < len(value):
            # 计算当前ais数据点与警戒范围中心的距离，单位：km
            dst_center = getDist(lon1=group_ais_array[group_ais_index, 2], lat1=group_ais_array[group_ais_index, 3],
                                 lon2=122.1913, lat2=30.5658)

            # 判断是否在警戒范围内
            if dst_center < 1.852*2.0:  # 若在境界范围内，记录进入时间
                # 记录进入时间
                enter_time = int(group_ais_array[group_ais_index, 1])
            else:  # 若不在境界范围内，循环下条
                group_ais_index += 1
                continue

            # 找出进入警戒范围前、后1.5小时的ais数据索引，下次循环的索引
            entered_circle_ais_one_stage = group_ais_array[(group_ais_array[:, 1] <= enter_time + aft_time) &
                                                           (group_ais_array[:, 1] >= enter_time - pre_time)]
            entered_circle_ais_array.extend(entered_circle_ais_one_stage)
            max_time_one_stage = np.max(entered_circle_ais_one_stage[:, 1])
            group_ais_index = np.max(np.where(group_ais_array[:, 1] == max_time_one_stage)) + 1
    return entered_circle_ais_array


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
    enter_poly_df = poly_df[poly_df["poly_id"] == enter_num]
    out_poly_df = poly_df[poly_df["poly_id"] == out_num]
    other_poly_df = poly_df[(poly_df["poly_id"] != enter_num) & (poly_df["poly_id"] != out_num)]

    sub_channel_mmsi = []
    for key, value in warning_area_ais_gdf:
        value_array = np.array(value)
        value_length = len(value)
        mid_idx = value_length // 2
        print(key)
        # 初始化对入口、出口的判断参数，是否进如果其他出入口参数
        enter_bool = False
        out_bool = False
        other_bool = False

        if value_length >= 3:
            # 获取其余出入口的编号
            other_poly_id_list = list(set(other_poly_df["poly_id"]))

            # 找到入口
            for enter_idx in range(mid_idx):
                # 判断是否进入过入口
                if point_poly(value_array[enter_idx, 2], value_array[enter_idx, 3], enter_poly_df):
                    enter_bool = True
                else:
                    pass

                # 判断是否进如果除出口外的多边形
                for other_poly_id in other_poly_id_list:
                    each_other_poly_df = other_poly_df[other_poly_df["poly_id"] == other_poly_id]
                    if point_poly(value_array[enter_idx, 2], value_array[enter_idx, 3], each_other_poly_df):
                        other_bool = True
                        break

            # 找到出口
            for out_idx in range(mid_idx, value_length):
                # 判断是否进入过出口多边形
                if point_poly(value_array[out_idx, 2], value_array[out_idx, 3], out_poly_df):
                    out_bool = True
                else:
                    pass

                # 判断是否进入过除入口、出口外其他的多边形
                if not other_bool:
                    for other_poly_id in other_poly_id_list:
                        each_other_poly_df = other_poly_df[other_poly_df["poly_id"] == other_poly_id]
                        if point_poly(value_array[out_idx, 2], value_array[out_idx, 3], each_other_poly_df):
                            other_bool = True
                            break

        # 若满足出口与入口都是指定的编号，则记录mmsi编号
        if (enter_bool & out_bool) & (not other_bool):
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
    # 获取多个多边形经纬度数据，整合到data frame中
    poly_list = []
    for kml in kml_list:
        coordinates_list = coordinates_kml(kml)
        for line in coordinates_list:
            line.append(int(kml[-5]))
            poly_list.append(line)
    poly_df = pd.DataFrame(poly_list)
    poly_df.columns = ["longitude", "latitude", "poly_id"]
    return poly_df


def sum_ship_warning_area(ys_ais, center, radius=1.852*1, interval=10*60):
    """
    统计洋山警戒范围每10分钟内的船舶数量
    :param ys_ais: 洋山水域范围内的AIS数据，类型：data frame
    :param center: 警戒范围圆心，类型：list
    :param radius: 警戒范围半径，单位：公里，类型：float
    :param interval: 统计间隔时间，单位：秒，类型：int
    :return: 每隔10分钟，警戒范围内的船舶数量，类型：data frame
    """
    # 获取数据集中最小的AIS数据时间
    min_time = min(ys_ais["acquisition_time"])

    # 对所有AIS数据中的时间字段，减去最小时间
    ys_ais["acquisition_time"] = ys_ais["acquisition_time"] - min_time

    # 根据间隔时间进行分段
    ys_ais["partition"] = ys_ais["acquisition_time"] // interval

    # 计算每一段中，在警戒范围内的AIS数据条数
    partition_list = list(set(ys_ais["partition"]))
    partition_ship_list = []
    for partition in partition_list:
        print("now_partition is %d, all_partition is %d" % (partition, max(partition_list)))
        sub_partition_array = np.array(ys_ais[ys_ais["partition"] == partition])
        ship_list = []
        for line in sub_partition_array:
            dst = getDist(line[2], line[3], center[0], center[1])
            if dst < radius:
                ship_list.append(line[0])
        ship_count = len(set(ship_list))
        partition_ship_list.append([partition, ship_count])

    # 输出结果
    partition_df = pd.DataFrame(partition_ship_list)
    partition_df.columns = ["partition", "count"]
    return partition_df


def fit_sub_channel(sub_channel_ais, interval_time=10*60):
    """
    拟合子航道曲线
    :param sub_channel_ais: 子航道的AIS数据，类型：data frame
    :param interval_time: 间隔时间（默认10分钟），单位：秒，类型：int
    :return: 由n个点组成的点组成的三维分段直线，类型：data frame
    """
    # 根据间隔时间，获取子航道每条数据所处第几段
    sub_channel_ais["partition"] = sub_channel_ais["acquisition_time"] // interval_time

    # 获取分段列表
    partition_list = list(set(sub_channel_ais["partition"]))
    fit_points_list = []

    for partition in partition_list:
        partition_ais = sub_channel_ais[sub_channel_ais["partition"] == partition]
        mean_lon = np.mean(partition_ais["longitude"])
        mean_lat = np.mean(partition_ais["latitude"])
        mean_time = np.mean(partition_ais["acquisition_time"])
        fit_points_list.append([mean_lon, mean_lat, mean_time])

    # 返回拟合函数的点
    fit_points_df = pd.DataFrame(fit_points_list)
    fit_points_df.columns = ["longitude", "latitude", "acquisition_time"]
    return fit_points_df


def filter_moor_ship(sub_channel_ais, interval_lon=0.5):
    """
    过滤子航道内停泊的船舶ais数据
    :param sub_channel_ais: 子航道内的ais数据，类型：data frame
    :param interval_lon: 最大最小经纬度之差，类型：float
    :return: 无停泊船舶的子航道ais数据，类型：data frame
    """
    # 初始化需要过滤的mmsi列表
    filter_mmsi_list = []

    # 对子航道ais数据按mmsi进行分组
    ship_gdf = sub_channel_ais.groupby('unique_ID')

    # 判断每条船的最大最小经纬度之差
    for key, value in ship_gdf:
        max_lon = np.max(value['longitude'])
        min_lon = np.min(value['longitude'])
        if max_lon - min_lon < interval_lon:
            filter_mmsi_list.append(key)
        else:
            pass
    return filter_mmsi_list


def filter_point_before_circle(fit_line_df):
    """
    从拟合曲线中找到离开圆之前的轨迹点
    :param fit_line_df: 拟合曲线数据，类型：data frame
    :return: 离开圆之前的拟合曲线
    """
    # 找到每条曲线中，离开圆之前的点
    filtered_point_df = pd.DataFrame()
    inside_circel_bool_list = []
    for key, value in fit_line_df.groupby('line_id'):
        inside_circle_bool = False
        value = np.array(value)
        for index, line in enumerate(value):
            dst_center = getDist(lon1=line[0], lat1=line[1], lon2=122.1913, lat2=30.5658)
            if dst_center < 1.852*2:
                inside_circle_bool = True
            else:
                if inside_circle_bool:
                    filtered_point_df = filtered_point_df.append(pd.DataFrame(value[:index, :]))
                    # filtered_point_df.insert(4, 'inside_circie', inside_circel_bool_list)
                    break
            inside_circel_bool_list.append(inside_circle_bool)
    filtered_point_df.insert(4, 'inside_circie', inside_circel_bool_list)
    return filtered_point_df



def predict_circle_ship_number(ys_ais, fit_line_df, str_time=1464739031, end_time=1465920000):
    """
    获取某10分钟时限内的ais数据，结合拟合后的曲线，预测1小时后船舶在警戒范围内的船舶数量
    :param ys_ais: 洋山水域内的ais数据，类型：data frame
    :param fit_line_df: 拟合曲线数据，类型：data frame
    :param str_time: ais数据开始时间，类型：int
    :param end_time: ais数据结束时间，类型：int
    :return: 1小时后圆内的船舶条数，类型：int
    """
    # 初始化预测结果
    predict_res = []

    # 将拟合曲线数据转换为矩阵
    fit_line_array = np.array(fit_line_df)
    enter_out_array = fit_line_array[fit_line_array[:, 4] == True]

    # 获取某10分钟时限的ais数据
    predict_str_time = np.random.random_integers(str_time, end_time-600)
    ys_ais = ys_ais[(ys_ais['acquisition_time'] >= predict_str_time) &
                    (ys_ais['acquisition_time'] <= predict_str_time + 600)]
    print("10分钟内，ais数据有%s" % len(ys_ais))
    print("开始时间为 %s" % predict_str_time)

    # 找到船舶最新一条的轨迹点，与拟合曲线中的点的位置关系
    for mmsi, value in ys_ais.groupby('unique_ID'):
        # print("mmsi = %d" % mmsi)
        newest_point = value.iloc[-1, :]
        min_dst = 99999999
        min_dst_channel = False

        # 用最新的ais数据点与拟合曲线数据中每个点进行对比，找到该点属于哪条航道
        for row in fit_line_array:
            point_line_dst = getDist(lon1=newest_point['longitude'], lat1=newest_point['latitude'],
                                     lon2=row[0], lat2=row[1])
            if point_line_dst < min_dst:
                min_dst = point_line_dst
                min_dst_channel = row[3]
                now_time = row[2]
                enter_time = enter_out_array[enter_out_array[:, 3] == min_dst_channel][0, 2] - now_time
                out_time = enter_out_array[enter_out_array[:, 3] == min_dst_channel][-1, 2] - now_time


        if (min_dst < 1.2) & (enter_time > 0) & (out_time > 0):
            predict_res.append([mmsi, enter_time // 600, out_time // 600])
        else:
            pass
    return predict_res, predict_str_time


def real_ship_circle(ys_ais, predict_str_time, interval_time=30*60):
    """
    检验预测结果
    :param ys_ais: 洋山水域内ais数据，类型：data frame
    :param predict_str_time: 预测开始时间，类型：int
    :param interval_time: 预测开始时间追溯时间，类型：int
    :return: 真实圆内的船舶数量，类型：int
    """
    ys_ais = np.array(ys_ais[(ys_ais['acquisition_time'] >= predict_str_time + interval_time) &
                             (ys_ais['acquisition_time'] <= predict_str_time + interval_time + 600)])
    mmsi_list = []
    for row in ys_ais:
        dst_center = getDist(lon1=row[2], lat1=row[3], lon2=122.1913, lat2=30.5658)
        if dst_center < 1.852 * 2:
            mmsi_list.append(row[0])
    return len(set(mmsi_list)), set(mmsi_list)


def north_port_get():
    from bs4 import BeautifulSoup

    now_timestamp = int(time.time() * 1000)
    url = r'http://115.231.126.81/Forecast/PopupContent?stationNum=58472&interval=24&_=%s' % now_timestamp
    headers = {'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
               'Referer': r'http://www.lagou.com/zhaopin/Python/?labelWords=label',
               'Connection': 'keep-alive'}
    req = urllib.request.Request(url, headers=headers)
    page_source_json = urllib.request.urlopen(req).read().decode('utf-8')
    page_source = json.loads(page_source_json)['html']
    bs_page = BeautifulSoup(page_source, 'html.parser').decode('utf-8')
    root_page = lxml.etree.HTML(bs_page)

    # 初始化日期，时间，风力，能见度列表
    all_date_list = []
    all_clock_list = []
    all_wind_list = []
    all_njd_list = []

    # xpath获取时间数据
    time_xpath = '//*[@style="min-width:60px;"]'
    root_time_list = root_page.xpath(time_xpath)
    for time_element in root_time_list:
        time_str = decode_unicode_references(tostring(time_element)).split(r'<td style="min-width:60px;">\n    ')[1]\
            .split(r'\n   </td>')[0]
        time_date = int(time_str.split('日')[0])
        time_clock = int(time_str.split('日')[1].split('时')[0])
        all_date_list.append(time_date)
        all_clock_list.append(time_clock)

    # xpath获取风力数据
    wind_xpath = '//table/tbody/tr'
    root_wind_list = root_page.xpath(wind_xpath)
    wind_str = decode_unicode_references(tostring(root_wind_list[0]))
    wind_pattern = re.compile(u'<span style="background:.*?;width:auto;margin-left:2px;margin-right:2px;height:15px;line-height:15px;">'
                              u'(.*?)</span>', re.S)
    wind_elements_list = re.findall(wind_pattern, wind_str)
    for wind_element in wind_elements_list:
        wind_level = (wind_element.split(r'\n     ')[1]).split(r'\n    ')[0]
        all_wind_list.append(int(wind_level))

    # xpath获取能见度数据
    njd_xpath = '//table/tbody/tr'
    root_njd_list = root_page.xpath(njd_xpath)
    njd_str = decode_unicode_references(tostring(root_njd_list[4]))
    njd_pattern = re.compile(r'<span>(.*?)</span>', re.S)
    njd_elements_list = re.findall(njd_pattern, njd_str)
    for njd_element in njd_elements_list:
        njd_dst = (njd_element.split(r'\n     ')[1]).split(r'\n    ')[0]
        all_njd_list.append(int(njd_dst))
    weather_predick_df = pd.DataFrame(columns=['date', 'clock', 'wind', 'njd'])
    weather_predick_df['date'] = all_date_list
    weather_predick_df['clock'] = all_clock_list
    weather_predick_df['wind'] = all_wind_list
    weather_predick_df['njd'] = all_njd_list
    now_time_index = weather_predick_df[(weather_predick_df['date'] == int(time.strftime('%d'))) &
                                        (weather_predick_df['clock'] == int(time.strftime('%H')))].index.tolist()[0]
    weather_predick_df = weather_predick_df.iloc[now_time_index:, :]
    return weather_predick_df


if __name__ == "__main__":
    # # --------------------------------------------------------------------------
    # # 获取洋山水域内AIS数据
    # data = pd.read_csv("/Users/qiujiayu/data/pre_201606_ys.csv", header=None)
    # data.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                 "true_head", "power", "ext", "extend"]
    # data = data.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    # data = data.sort_values(by=["unique_ID", "acquisition_time"])
    # data["longitude"] = data["longitude"] / 1000000.
    # data["latitude"] = data["latitude"] / 1000000.

    # ------------------------------------------------------------------------
    # 找到在警戒区内出现过的船舶ais，并追溯前后1.5小时
    # entered_circle_ais_df = pd.DataFrame(enter_circle_ais(ys_ais=data), columns=["unique_ID", "acquisition_time",
    #                                                                              "longitude", "latitude"])
    # print(entered_circle_ais_df.head())
    # print(len(entered_circle_ais_df))
    # entered_circle_ais_df.to_csv("/home/qiu/Documents/ys_ais/entered_circle_ais.csv", index=None)

    # # -------------------------------------------------------------------------
    # # 获取境界范围多边形数据，并用进出多边形来对ais数据进行分组
    # kml_path = "/home/qiu/Documents/ys_ais/新警戒区观测区.kml"
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
    # out_df.to_csv("/home/qiu/Documents/ys_ais/new_inside_poly_ais.csv", index=None)

    # # --------------------------------------------------------------
    # # 对分组后的数据，平移时间轴
    # out_df = pd.read_csv("/home/qiu/Documents/ys_ais/new_inside_poly_ais.csv")
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
    # moved_out_df.to_csv("/home/qiu/Documents/ys_ais/new_moved_time_ais.csv", index=None)

    # # ----------------------------------------------------------------
    # # 区分进入主航道是的入口编号
    # kml_path_list = ["/home/qiu/Documents/ys_ais/警戒区进口1.kml",
    #                  "/home/qiu/Documents/ys_ais/新警戒区入口2.kml",
    #                  "/home/qiu/Documents/ys_ais/新警戒区入口3.kml",
    #                  "/home/qiu/Documents/ys_ais/新警戒区入口4.kml",
    #                  "/home/qiu/Documents/ys_ais/新警戒区入口5.kml",
    #                  "/home/qiu/Documents/ys_ais/新警戒区入口6.kml"]
    # poly_df = multiple_poly_list(kml_path_list)
    # main_channel_ais = pd.read_csv("/home/qiu/Documents/ys_ais/new_moved_time_ais.csv")
    # # test_df = poly_df[poly_df["poly_id"] == 1]
    # sub_channel_ais = sub_main_channel_poly_filter(main_channel_ais, 3, 5, poly_df)
    # # tmp_df = sub_channel_ais[(sub_channel_ais["latitude"] > 30.62) |
    # #                          (sub_channel_ais["latitude"] == 30.535891) |
    # #                          (sub_channel_ais["acquisition_time"] >= 5000)]
    # # filter_mmsi_list = list(set(tmp_df["unique_ID"]))
    # # sub_channel_ais = sub_channel_ais[~sub_channel_ais["unique_ID"].isin(filter_mmsi_list)]
    # # sub_channel_ais = pd.DataFrame(sub_channel_ais)
    # # sub_channel_ais.columns = ["unique_ID", "acquisition_time", "longitude", "latitude", "poly_id"]
    # # sub_channel_ais = sub_channel_ais[~sub_channel_ais["poly_id"].isnull()]
    # sub_channel_ais.to_csv("/home/qiu/Documents/ys_ais/new_sub_channel_ais_north_up.csv", index=None)
    # # print(sub_channel_ais)

    # # ------------------------------------------------
    # # 子航道拟合
    # # 122156281, 30551332, 7101
    # sub_channel_ais = pd.read_csv("/Users/qiujiayu/data/new_sub_channel_ais_west_east.csv")
    # # print(sub_channel_ais.head())
    # # filter_mmsi = sub_channel_ais[(sub_channel_ais["acquisition_time"] == 3474)]
    # # print(filter_mmsi)
    # filter_mmsi = filter_moor_ship(sub_channel_ais)
    # print((filter_mmsi))
    # input("==============")
    # filter_mmsi.extend([6, 31, 56])
    # sub_channel_ais = sub_channel_ais[~sub_channel_ais["unique_ID"].isin(filter_mmsi)]
    # sub_channel_ais.to_csv("/Users/qiujiayu/data/new_west_east_ais_paint.csv", index=None)
    # fit_channel_df = fit_sub_channel(sub_channel_ais)
    # fit_channel_df.to_csv("/Users/qiujiayu/data/new_west_east_fit_line.csv", index=None)
    # print(fit_channel_df)

    # # ----------------------------------------------------------------
    # # 计算警戒范围内可能出现的最大船舶数量
    # data = pd.read_csv("/home/qiu/Documents/ys_ais/pre_201606_ys.csv", header=None)
    # data.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                 "true_head", "power", "ext", "extend"]
    # data = data.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    # print(data.head())
    # data["longitude"] = data["longitude"] / 1000000.
    # data["latitude"] = data["latitude"] / 1000000.
    # partition_count_df = sum_ship_warning_area(data, [122.1913, 30.5658], 1.852*2.0)
    # partition_count_df.to_csv("/home/qiu/Documents/ys_ais/partition_count.csv", index=None)
    # print(partition_count_df)
    # print(partition_count_df.describe())
    # print(set(partition_count_df["count"]))

    # # ------------------------------------------------------------------
    # # 找到拟合曲线中，离开圆之前的点
    # fit_line_east_west_df = pd.read_csv('/Users/qiujiayu/data/east_west_fit_line.csv')
    # fit_line_north_up_df = pd.read_csv('/Users/qiujiayu/data/new_north_up_fit_line.csv')
    # fit_line_south_down_df = pd.read_csv('/Users/qiujiayu/data/new_south_down_fit_line.csv')
    # fit_line_west_east_df = pd.read_csv('/Users/qiujiayu/data/new_west_east_fit_line.csv')
    # fit_line_df = pd.concat([fit_line_east_west_df, fit_line_north_up_df,
    #                          fit_line_south_down_df, fit_line_west_east_df], ignore_index=True)
    # filtered_fit_line_df = filter_point_before_circle(fit_line_df=fit_line_df)
    # print(filtered_fit_line_df)
    #
    # # 检验模型
    # predict_res, predict_str_time = predict_circle_ship_number(ys_ais=data, fit_line_df=filtered_fit_line_df)
    # predict_res_df = pd.DataFrame(predict_res, columns=['mmsi', 'enter_time', 'out_time'])
    # print(predict_res_df)
    # # res = predict_res[((predict_res['enter_time'] >= 6.) & (predict_res['enter_time'] <= 7.)) |
    # #                   ((predict_res['out_time'] >= 6.) & (predict_res['out_time'] <= 7.))]
    # # print(len(res))
    # real_number, mmsi_list = real_ship_circle(ys_ais=data, predict_str_time=predict_str_time)
    # print(real_number, mmsi_list)
    # print(predict_str_time)

    # -------------------------------------------------------------------
    # 测试网页数据get
    weather_predict_df = north_port_get()
    print(weather_predict_df)
