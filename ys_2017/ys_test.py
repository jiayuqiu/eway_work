# coding:utf-8

import pandas as pd
import numpy as np
import urllib.request
import json
import re
import time
import lxml
import pymysql


from lxml.html import tostring
from base_func import getDist, getFileNameList, is_line_cross
from bridge import Bridge
from emergency import Emergency
from traffic import Traffic
from weather import Weather
from port import Port


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
    """
    获取北部港区风力、能见度数据
    :return: 返回日期，时间，风力，能见度数据，类型：data frame
    """
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
    wind_pattern = re.compile(u'<span style="background:.*?">'
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
                                        (weather_predick_df['clock'] == int(time.strftime('%H')))].index.tolist()
    if len(now_time_index) > 0:
        weather_predick_df = weather_predick_df.iloc[now_time_index:, :]
    else:
        weather_predick_df = weather_predick_df.iloc[-1, :]

    return np.max(weather_predick_df['wind']), np.min(weather_predick_df['njd'])


def bridge_cross_poly(ys_ais):
    """
    找到东海大桥通航孔多边形
    :param ys_ais: 洋山水域ais数据，类型：data frame
    :return: 返回通过东海大桥的ais数据点，类型：data frame
    """
    bridge_line_1 = [[121.9101275192141, 30.85886907127729], [121.9743845226048, 30.74938456976765]]
    bridge_line_2 = [[121.9740364308981, 30.74907407855846], [121.9754932694055, 30.70342865962283]]
    bridge_line_3 = [[121.9758474404916, 30.70342475630693], [122.0105453839071, 30.65993827754983]]
    ys_ais_gdf = ys_ais.groupby('unique_ID')

    crossing_points = []
    for mmsi, value in ys_ais_gdf:
        print('mmsi = %s' % mmsi)
        value_array = np.array(value)
        value_array = value_array[value_array[:, 1].argsort()]

        # 从第一条开始循环一条船的ais数据
        index = 0
        if len(value_array) > 1:
            while index < (len(value_array) - 1):
                if is_line_cross(str1=[value_array[index, 2], value_array[index, 3]],
                                 end1=[value_array[index + 1, 2], value_array[index + 1, 3]],
                                 str2=bridge_line_3[0], end2=bridge_line_3[1]):
                    crossing_points.append([value_array[index, 2], value_array[index, 3]])
                    crossing_points.append([value_array[index + 1, 2], value_array[index + 1, 3]])
                index += 1
    return crossing_points


def get_bridge_poly():
    """
    获取bridge_data中，通航孔的多边形数据
    :return: 多边形数据的矩阵，类型np.array
    """
    # 获取通航孔多边形坐标
    bridge_poly_list = []
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic')
    cur = conn.cursor()
    cur.execute("SELECT * FROM bridge_data")
    for row in cur.fetchall():
        poly_id = row[0]
        poly_coordinate_string = row[-1]
        print(poly_coordinate_string)
        poly_coordinate_list = []
        for coordinate in poly_coordinate_string.split(';'):
            if coordinate:
                lon_ = float(coordinate.split('*')[0])
                lat_ = float(coordinate.split('*')[1])
                poly_coordinate_list.append([lon_, lat_])
        bridge_poly_list.append([poly_id, poly_coordinate_list])
    cur.close()
    conn.close()
    return bridge_poly_list


def bridge_main(ys_ais):
    """
    每10分钟判断一次，经过东海大桥通航孔的船舶数量
    :param ys_ais: 10分钟洋山水域内的ais数据，类型：data frame
    :return: 1-5号多边形内的船舶数量与mmsi列表
    """
    # 将ais的data frame转换为array
    ys_ais_array = np.array(ys_ais)

    # 获取多边形数据
    poly_coordinate_list = get_bridge_poly()

    # 找出多边形中的船舶数量
    for poly_ in poly_coordinate_list:
        poly_id = poly_[0]
        print(poly_id)
        each_coordinate_array = np.array(poly_[1])
        inside_poly_mmsi_list = []
        for ais_row in ys_ais_array:
            if point_poly(pointLon=ais_row[2], pointLat=ais_row[3], polygon=each_coordinate_array):
                cross_bool = check_cross(ais_row[0])


def merge_ship_static(file_path):
    """
    合并船舶信息表
    :param file_path: 表格所在路径，类型：string
    :return: 船舶信息合并后的数据，类型：data frame
    """
    file_name_list = getFileNameList(file_path)
    ship_static_df_list = []
    for file_name in file_name_list:
        print(file_name)
        ship_static_df = pd.read_excel(file_path + file_name)
        ship_static_df_list.append(ship_static_df)
    print("mergeing....")
    all_ship_df = pd.concat(ship_static_df_list)
    return all_ship_df


def if_string(test_string):
    is_string = False
    for x in test_string:
        if (x.isdigit()) | (x == '.'):
            is_string = True
        else:
            is_string = False
            break
    if not is_string:
        return '0'
    else:
        return test_string


def ship_static_mysql():
    """
    船舶静态数据入库
    :return:
    """
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    # 读取船舶静态数据
    ship_static_df = pd.read_csv('/home/qiu/Documents/ys_ais/all_ship_static_ys_opt.csv').fillna(0)
    ship_static_array = np.array(ship_static_df)
    print(ship_static_df.columns)

    # 循环导入船舶静态数据
    ssd_id = 4
    error_count = 0
    for line in ship_static_array:
        try:
            mmsi = if_string(str(line[10]))
            imo = if_string(str(line[8]))
            tonnage = if_string(str(line[20]))
            dwt = if_string(str(line[22]))
            monitor_rate = if_string(str(line[23]))
            length = if_string(str(line[17]))
            width = if_string(str(line[18]))
            insert_sql = """
                         INSERT INTO ship_static_data VALUES ('%d', '%d', '%d', '%s', '%s', '%s', null, '%s', '%s', '%s',
                         '%s', '%f', '%f', '%f', '%f', '%f', null, null)
                         """ % (ssd_id, int(float(mmsi)), int(float(imo)), line[1], line[2], line[7], line[11], line[13], line[3],
                                line[4], float(tonnage), float(dwt), float(monitor_rate), float(length),
                                float(width))
            ssd_id += 1
            print(ssd_id)
            cur.execute(insert_sql)
        except Exception as e:
            error_count += 1
            print(e)

    print("error count is %d" % error_count)
    conn.commit()
    cur.close()
    conn.close()

def ship_static_opt(file_path):
    """
    找到有mmsi数据的船舶基本信息
    :param file_path:
    :return:
    """
    file_ = open(file_path)
    ship_static_list = []
    for line in file_:
        if "主机功率" not in line:
            line_list = line.split("\n")[0].split(",")
            out_line_list = []
            for ele in line_list:
                tmp_ele = ele.replace(" ", "").replace(",", "")
                out_line_list.append(tmp_ele)
            ship_static_list.append(out_line_list)
    ship_static_df = pd.DataFrame(ship_static_list)
    return ship_static_df


def emergency_ship_mysql():
    """
    应急船舶数据导入数据库
    :return:
    """
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    emergency_ship_array = np.array(pd.read_csv('/home/qiu/Documents/ys_ais/船舶数据清单/'
                                    '上海海事局辖区社会应急力量配置情况统计 (辖区汇总表)-20171020.csv'))
    print(emergency_ship_array)
    count = 20
    for line in emergency_ship_array:
        insert_sql = """
                     INSERT INTO emergency_ship VALUE('%s', '%s', null, null, null, null, '%s', '%s', '%s', null, null, 
                     '%s', '%s', null, null, '%s', '%s', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', null, '%s', '%s', '%s', '%s', 
                     null, '%s', '%s', '%s', '%s', '%s', '%s')
                     """ % (count, line[6], int(line[27]), line[26], line[5],
                            line[7], line[8],
                            line[1], line[3], line[10], line[11], line[12], line[13], line[14], line[15],
                            line[16], line[17], line[18], line[20], line[21], line[22], line[23],
                            str(line[25]), str(line[28]), str(line[29]), str(line[30]), str(line[31]), str(line[32]))
        count += 1
        cur.execute(insert_sql)
    conn.commit()
    cur.close()
    conn.commit()


def ais_static_mysql():
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    # 创建ais_static表
    create_sql = """
                 CREATE TABLE IF NOT EXISTS ship_static_data_eway(
                 ssde_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY ,
                 mmsi INT,
                 create_time DATETIME ,
                 shiptype VARCHAR(100) ,
                 IMO VARCHAR(100) ,
                 callsign VARCHAR(100) ,
                 ship_length FLOAT ,
                 ship_width FLOAT ,
                 pos_type VARCHAR(100) ,
                 eta VARCHAR(100) ,
                 draught FLOAT ,
                 destination VARCHAR(100)
                 )
                 """
    cur.execute(create_sql)
    conn.commit()
    cur.close()
    conn.close()


def breth_data_mysql():
    breth_df = pd.read_excel('/home/qiu/Documents/ys_ais/码头表.xlsx')
    breth_df = breth_df[~breth_df[u'序号'].isnull()]
    breth_df = breth_df.fillna(0)
    breth_array = np.array(breth_df)
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()
    bd_id = 5
    for line in breth_array:
        breth_name = line[1]
        breth_length = int(line[2])
        if ('-' in str(line[3])) & ('万' in str(line[3])):
            max_dwt = float(line[3].split('万')[0].split('-')[1])
            min_dwt = float(line[3].split('万')[0].split('-')[0])
        elif ('-' in str(line[3])) & (not '万' in str(line[3])):
            max_dwt = float(line[3].split('-')[1])
            min_dwt = float(line[3].split('-')[0])
        else:
            max_dwt = float(line[3])
            min_dwt = float(line[3])
        breth_type = line[5]
        font_depth = float(line[6])
        roundabout_area = line[7]
        roundabout_depth = line[8]
        remarks = line[9]

        insert_sql = """
                     INSERT INTO breth_data VALUES ('%d', '%s', '%d', '%f', '%f', '%s', '%f', '%s', '%s', '%s')
                     """ % (bd_id, breth_name, breth_length, min_dwt, max_dwt, breth_type, font_depth,
                            roundabout_area, roundabout_depth, remarks)
        cur.execute(insert_sql)
        bd_id += 1
    conn.commit()
    cur.close()
    conn.close()


def ship_static_eway_mysql():
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    ship_static_df = pd.read_csv('/home/qiu/Documents/unique_static_2016.csv')
    ship_static_array = np.array(ship_static_df)
    error_count = 0
    for line in ship_static_array:
        try:
            insert_sql = """
                                 INSERT INTO ship_static_data_eway(mmsi, create_time, shiptype, IMO, callsign, ship_length, ship_width,
                                 pos_type, eta, draught, destination, ship_english_name) 
                                 VALUE('%d', NULL , '%s', '%s', '%s', '%f', '%f', '%s', '%s', '%f', NULL , '%s')
                                 """ % (int(line[7]), line[9], line[5], line[1], line[6], float(line[12]),
                                        0.0, line[4], float(line[3]), line[8])
            # print(insert_sql)
            cur.execute(insert_sql)
        except Exception as e:
            print(e)
            error_count += 1
    print("error count is %d" % error_count)
    conn.commit()
    cur.close()
    conn.close()


def tb_ship_data_mysql():
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()

    ship_static_df = pd.read_csv('/home/qiu/Documents/unique_static_2016.csv')
    print(ship_static_df.head())
    input("-----------")
    ship_static_array = np.array(ship_static_df)
    error_count = 0
    for line in ship_static_array:
        try:
            insert_sql = """
                         INSERT INTO tb_ship_data(mmsi, imo, ship_callsign, ship_english_name,
                         shiptype, width, length, eta, draught, destination)
                         VALUE('%d', NULL , '%s', '%s', '%s', '%f', '%f', '%s', '%s', '%f', NULL , '%s')
                         """ % (int(line[7]), line[9], line[5], line[1], line[6], float(line[12]),
                                0.0, line[4], float(line[3]), line[8])
            # print(insert_sql)
            cur.execute(insert_sql)
        except Exception as e:
            print(e)
            error_count += 1
    print("error count is %d" % error_count)
    conn.commit()
    cur.close()
    conn.close()


def get_ship_static_mysql():
    # 链接数据库
    conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                           db='dbtraffic', charset='utf8')
    cur = conn.cursor()
    select_sql = """
                 SELECT * FROM ship_static_data
                 """
    cur.execute(select_sql)
    ship_static_eway_df = pd.DataFrame(list(cur.fetchall()))
    ship_static_eway_df.columns = ['ssd_id', 'mmsi', 'imo', 'ship_chinese_name', 'ship_english_name', 'ship_callsign',
                                   'sea_or_river', 'flag', 'sail_area', 'ship_port', 'ship_type', 'tonnage', 'dwt',
                                   'monitor_rate', 'length', 'width', 'wind_resistance_level', 'height_above_water']
    print(ship_static_eway_df.head())


if __name__ == "__main__":
    # # --------------------------------------------------------------------------
    # # 获取洋山水域内AIS数据
    # data = pd.read_csv("/home/qiu/Documents/ys_ais/pre_201606_ys.csv", header=None)
    # data.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                 "true_head", "power", "ext", "extend"]
    # data = data.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    # data = data.sort_values(by=["unique_ID", "acquisition_time"])
    # data["longitude"] = data["longitude"] / 1000000.
    # data["latitude"] = data["latitude"] / 1000000.

    # # 获取某10分钟时限的ais数据
    # str_time = 1464739031
    # end_time = 1465920000
    # predict_str_time = np.random.random_integers(str_time, end_time - 600)
    # ys_ais = data[(data['acquisition_time'] >= predict_str_time) &
    #               (data['acquisition_time'] <= predict_str_time + 600)]
    # ys_ais.to_csv('/home/qiu/Documents/ys_ais/pre_201606_ys_10mins.csv', index=None)

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
    # fit_line_east_west_df = pd.read_csv('/home/qiu/Documents/ys_ais/east_west_fit_line.csv')
    # fit_line_north_up_df = pd.read_csv('/home/qiu/Documents/ys_ais/new_north_up_fit_line.csv')
    # fit_line_south_down_df = pd.read_csv('/home/qiu/Documents/ys_ais/new_south_down_fit_line.csv')
    # fit_line_west_east_df = pd.read_csv('/home/qiu/Documents/ys_ais/new_west_east_fit_line.csv')
    # fit_line_df = pd.concat([fit_line_east_west_df, fit_line_north_up_df,
    #                          fit_line_south_down_df, fit_line_west_east_df], ignore_index=True)
    # filtered_fit_line_df = filter_point_before_circle(fit_line_df=fit_line_df)
    # filtered_fit_line_df.to_csv("/home/qiu/Documents/filtered_fit_line.csv", index=None)
    # print(filtered_fit_line_df)

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

    # # -------------------------------------------------------------------
    # # 测试网页数据get
    # max_wind, min_njd = north_port_get()
    # print(max_wind, min_njd)

    # # -------------------------------------------------------------------
    # # 获取通过东海大桥的点
    # crossing_points = bridge_cross_poly(data)
    # crossing_points_df = pd.DataFrame(crossing_points)
    # crossing_points_df.columns = ['longitude', 'latitude']
    # crossing_points_df.to_csv('/home/qiu/Documents/ys_ais/crossing_bridge_points_3.csv', index=None)

    # ----------------------------------------------------------------------
    # 东海大桥通航孔坐标列表转字符串
    # kml_path_list = ["/home/qiu/Documents/ys_ais/通航孔1.kml",
    #                  "/home/qiu/Documents/ys_ais/通航孔2.kml",
    #                  "/home/qiu/Documents/ys_ais/通航孔3.kml",
    #                  "/home/qiu/Documents/ys_ais/通航孔4.kml",
    #                  "/home/qiu/Documents/ys_ais/通航孔5.kml"]
    # bridge_ploy = multiple_poly_list(kml_path_list)
    # print(bridge_ploy)
    # for poly_id, value in bridge_ploy.groupby('poly_id'):
    #     print(poly_id)
    #     value_array = np.array(value)
    #     coordinate_string = ""
    #     for row in value_array:
    #         coordinate_string = coordinate_string + \
    #                             str(round(float(row[0]), 4)) + "*" + str(round(float(row[1]), 4)) + ";"
    #     print(coordinate_string)

    # ----------------------------------------------------
    # 获取最新10分钟内，东海大桥的通航情况
    # bridge = Bridge()
    # ys_ais_10mins = pd.read_csv('/home/qiu/Documents/ys_ais/pre_201606_ys.csv', header=None)
    # print(ys_ais_10mins)
    # bridge_cross_df = bridge.bridge_main(ys_ais=ys_ais_10mins)

    # # ------------------------------------------------------
    # # 应急决策
    # emergency = Emergency()
    # emergency.emergency_main(ys_ais_10mins=data)

    # # --------------------------------------------------------
    # # 交通预警
    # ys_ais_10mins = pd.read_csv('/home/qiu/Documents/ys_ais/pre_201606_ys_10mins.csv')
    # traffic = Traffic()
    # predict_res_df = pd.DataFrame(traffic.traffic_main(ys_ais=ys_ais_10mins))

    # # -------------------------------------------------------
    # # 天气报告
    # weather = Weather()
    # weather.report_mysql()

    # # --------------------------------------------------------
    # # 靠离泊
    # port = Port()
    # port.port_main()

    # --------------------------------------------------------
    # 合并从洋山获取到的船舶静态数据
    # file_path = "/home/qiu/Documents/ys_ais/船舶数据清单/FULL/"
    # all_ship_static_df = merge_ship_static(file_path)
    # all_ship_static_df_length = len(all_ship_static_df)
    # all_ship_static_array = np.array(all_ship_static_df)
    # for index in range(all_ship_static_df_length):
    #     print(index)
    #     columns_length = len(all_ship_static_array[index, :])
    #     for column in range(columns_length):
    #         if ',' in str(all_ship_static_array[index, column]):
    #             all_ship_static_array[index, column] = str(all_ship_static_array[index, column]).replace(',', '')\
    #                                                      .replace(' ', '')
    # all_ship_static_opt_df = pd.DataFrame(all_ship_static_array)
    # all_ship_static_opt_df.to_csv('/home/qiu/Documents/ys_ais/all_ship_static_ys.csv', index=None)
    # print(all_ship_static_df.head())
    #-------------------------------------------------------------
    # file_path = "/home/qiu/Documents/ys_ais/all_ship_static_ys.csv"
    # header = pd.read_excel('/home/qiu/Documents/ys_ais/船舶数据清单/FULL/hsData_0.xls').columns
    # df = ship_static_opt(file_path)
    # df.columns = header
    # df.to_csv('/home/qiu/Documents/ys_ais/all_ship_static_ys_opt.csv', index=None)
    # print(df.head())

    # get_ship_static_mysql()

    # from email.mime.text import MIMEText
    #
    # msg = MIMEText('hello，send by python...', 'plain', 'utf-8')
    #
    # # 发送邮箱地址
    # from_addr = 'qiujiayu0212@163.com'
    #
    # # 邮箱授权码，非登陆密码
    # password = 'yingming0403'
    #
    # # 收件箱地址
    # to_addr = 'sohu.321@qq.com'
    #
    # # smtp服务器
    # smtp_server = 'smtp.163.com'
    # # 发送邮箱地址
    # msg['From'] = from_addr
    # # 收件箱地址
    # msg['To'] = to_addr
    # # 主题
    # msg['Subject'] = 'the frist mail'
    # import smtplib
    #
    # server = smtplib.SMTP(smtp_server, 25)
    #
    # server.set_debuglevel(1)
    #
    # print(from_addr)
    # print(password)
    # server.login(from_addr, password)
    #
    # a = server.sendmail(from_addr, [to_addr], msg.as_string())
    # print(a)
    #
    # server.quit()

    tb_ship_data_mysql()
