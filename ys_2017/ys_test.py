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

    #----------------------------------------------------------------
    # 区分进入主航道是的入口编号
    kml_path_list = ["/home/qiu/Documents/ys_ais/警戒区进口1.kml",
                     "/home/qiu/Documents/ys_ais/新警戒区入口2.kml",
                     "/home/qiu/Documents/ys_ais/新警戒区入口3.kml",
                     "/home/qiu/Documents/ys_ais/新警戒区入口4.kml",
                     "/home/qiu/Documents/ys_ais/新警戒区入口5.kml",
                     "/home/qiu/Documents/ys_ais/新警戒区入口6.kml"]
    poly_df = multiple_poly_list(kml_path_list)
    main_channel_ais = pd.read_csv("/home/qiu/Documents/ys_ais/new_moved_time_ais.csv")
    # test_df = poly_df[poly_df["poly_id"] == 1]
    sub_channel_ais = sub_main_channel_poly_filter(main_channel_ais, 3, 5, poly_df)
    # tmp_df = sub_channel_ais[(sub_channel_ais["latitude"] > 30.62) |
    #                          (sub_channel_ais["latitude"] == 30.535891) |
    #                          (sub_channel_ais["acquisition_time"] >= 5000)]
    # filter_mmsi_list = list(set(tmp_df["unique_ID"]))
    # sub_channel_ais = sub_channel_ais[~sub_channel_ais["unique_ID"].isin(filter_mmsi_list)]
    # sub_channel_ais = pd.DataFrame(sub_channel_ais)
    # sub_channel_ais.columns = ["unique_ID", "acquisition_time", "longitude", "latitude", "poly_id"]
    # sub_channel_ais = sub_channel_ais[~sub_channel_ais["poly_id"].isnull()]
    sub_channel_ais.to_csv("/home/qiu/Documents/ys_ais/new_sub_channel_ais_north_up.csv", index=None)
    # print(sub_channel_ais)

    # # ------------------------------------------------
    # # 子航道拟合
    # # 122156281, 30551332, 7101
    # sub_channel_ais = pd.read_csv("/home/qiu/Documents/ys_ais/sub_channel_ais_west_east.csv")
    # # sub_channel_ais = sub_channel_ais[~sub_channel_ais["unique_ID"].isin([2092, 1623])]
    # # sub_channel_ais.to_csv("/home/qiu/Documents/ys_ais/south_down_ais_paint.csv", index=None)
    # fit_channel_df = fit_sub_channel(sub_channel_ais)
    # fit_channel_df.to_csv("/home/qiu/Documents/ys_ais/west_east_fit_line.csv", index=None)
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
