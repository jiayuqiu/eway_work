# coding:utf-8

import pandas as pd
import numpy as np
import time
import pymysql

from base_func import getDist, getFileNameList, is_line_cross, point_poly, tb_pop_mysql, get_ship_static_mysql, coor_cog
from ais_analysis import AIS

class Bridge(object):
    def __init__(self):
        pass

    def bridge_cross_poly(self, ys_ais):
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

    def get_bridge_poly(self):
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
            max_wind = int(row[8])
            min_njd = int(row[9])
            max_dwt = int(row[6])
            poly_coordinate_string = row[13]
            poly_coordinate_list = []
            for coordinate in poly_coordinate_string.split(';'):
                if coordinate:
                    lon_ = float(coordinate.split('*')[0])
                    lat_ = float(coordinate.split('*')[1])
                    poly_coordinate_list.append([lon_, lat_])
            bridge_poly_list.append([poly_id, poly_coordinate_list, max_wind, min_njd, max_dwt])
        cur.close()
        conn.close()
        return bridge_poly_list

    def get_bridge_cross_parameter(self):
        """
        获取当前天气预警数据
        :return: 通航孔通航条件数据，类型：data frame
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()
        cur.execute("SELECT * FROM weather_conf order by wp_id DESC limit 1")
        newest_weahter_conf_df = pd.DataFrame(list(cur.fetchall()))
        newest_weahter_conf_df.columns = ['wp_id', 'pub_date', 'pub_clock', 'src_loc', 'max_avg_wind', 'max_zf_wind',
                                          'min_njd', 'suggest_warn', 'if_conf', 'conf_man', 'conf_level', 'conf_time',
                                          'suggest_njd_warn', 'conf_njd_level', 'if_conf_njd', 'conf_man_njd',
                                          'conf_time_njd', 'pub_time', 'tmr_max_avg_wind', 'tmr_max_zf_wind']
        return newest_weahter_conf_df

    def inside_poly_mysql(self, inside_poly_df):
        """
        将即将通过通航孔的数据入库
        :param inside_poly_df: 在不同多边形内的船舶数据
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()
        print(inside_poly_df)
        inside_poly_array = np.array(inside_poly_df)

        hole_id_list = range(6)
        create_time = time.strftime("%Y-%m-%d %H:%M:%S")
        for hole_id in hole_id_list:
            value_array = np.array(inside_poly_df[inside_poly_df['hole_id'] == hole_id])
            non_conformity_array = value_array[value_array[:, 2] == 2]
            if len(non_conformity_array) == 0:
                non_conformity_number = 2
            else:
                non_conformity_number = 1

            # 数据导入bridge_data表
            bridge_data_update_sql = """
                                     UPDATE bridge_data SET ship_number='%d', non_conformity='%s' WHERE bridge_id='%d'
                                     """ % (len(value_array), non_conformity_number, int(hole_id))
            cur.execute(bridge_data_update_sql)

        # 数据导入bridge_history表
        cur.execute('truncate bridge_history')
        for line in inside_poly_array:
            bridge_history_update_sql = """
                                        INSERT INTO bridge_history(mmsi, ship_chinese_name, ship_type,
                                        bridge_hole, is_cross, create_time, reason, dwt, draught) 
                                        VALUE('%s', '%s', '%s', '%s', '%d', '%s', '%s', '%f', '%f')""" \
                                        % (line[0], line[5], line[4], line[1], line[2], create_time,
                                           line[3], float(line[6]), float(line[7]))
            cur.execute(bridge_history_update_sql)
            if line[2] == 2:
                create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                tb_pop_mysql(create_time=create_time, warn_type='东海大桥', warn_text='有东海大桥预警，请注意！', if_pop=0)
        conn.commit()
        conn.close()

    def bridge_main(self, ys_ais, ship_static_df):
        """
        每10分钟判断一次，经过东海大桥通航孔的船舶数量
        :param ys_ais: 10分钟洋山水域内的ais数据，类型：data frame
        :param ship_static_df: 船舶静态数据，类型:data frame
        :return: 1-5号多边形内的船舶数量与mmsi列表
        """
        # 将ais的data frame转换为array
        newest_ais_list = []
        for key, value in ys_ais.groupby('unique_ID'):
            value = value.sort_values(by=['unique_ID', 'acquisition_time'])
            newest_ais_list.append([value.iloc[-1, 0], value.iloc[-1, 1], value.iloc[-1, 2], value.iloc[-1, 3]])
        ys_ais_array = np.array(newest_ais_list)

        # 获取多边形数据
        poly_coordinate_list = self.get_bridge_poly()

        # 找出多边形中的船舶数量
        inside_poly_mmsi_list = []  # col0:chinese name, col1:mmsi, col2:hole_id, col3:if_cross, col4:reason
        for poly_ in poly_coordinate_list:
            poly_id = poly_[0]
            each_coordinate_array = np.array(poly_[1])

            # 获取东海大桥通航孔的预警条件
            bridge_max_wind = int(poly_[2])
            bridge_min_njd = int(poly_[3])
            for ais_row in ys_ais_array:
                # 判断该条ais数据是否在多边形内
                if point_poly(pointLon=ais_row[2], pointLat=ais_row[3], polygon=each_coordinate_array):
                    # 若在多边形内
                    newest_weather_conf_df = self.get_bridge_cross_parameter()
                    newest_avg_max_wind = newest_weather_conf_df.iloc[0, 4]
                    newest_min_njd = newest_weather_conf_df.iloc[0, 6]
                    bridge_cross_wind_bool = (int(bridge_max_wind) > int(newest_avg_max_wind))
                    bridge_cross_njd_bool = (int(bridge_min_njd) < int(newest_min_njd))

                    # 判断是否符合通航条件
                    reason = ""
                    if bridge_cross_wind_bool & bridge_cross_njd_bool:
                        bool_num = 1
                        reason = "符合条件"
                    if not bridge_cross_wind_bool:
                        bool_num = 2
                        reason = reason + "风力条件不符合;"
                    if not bridge_cross_njd_bool:
                        bool_num = 2
                        reason = reason + "能见度条件不符合;"
                    if poly_id == 5:
                        bool_num = 2
                        reason = "违规通航孔"
                    inside_poly_mmsi_list.append([int(ais_row[0]), poly_id, bool_num,
                                                  reason])
        inside_poly_df = pd.DataFrame(inside_poly_mmsi_list, columns=['mmsi', 'hole_id', 'if_cross', 'reason'])
        inside_poly_df = inside_poly_df.drop_duplicates()
        shiptype_list = []
        chinese_name = []
        dwt_list = []
        draught_list = []
        # 逐行匹配静态数据
        for index in range(len(inside_poly_df)):
            # tmp_ship_static = self.ship_static_df[self.ship_static_df['mmsi'] == int(value['mmsi'])]
            # tmp_ship_static_ewary = self.ship_static_eway_df[self.ship_static_eway_df['mmsi'] == int(value['mmsi'])]
            tmp_ship_static_merge = ship_static_df[ship_static_df['mmsi_x'] == int(inside_poly_df.iloc[index, 0])]

            # 查看是否匹配到静态数据
            if len(tmp_ship_static_merge) > 0:
                # 判断目前是否符合通航条件
                # if inside_poly_df.iloc[index, 2] == 1:
                # 获取对应通航孔信息
                for poly_ in poly_coordinate_list:
                    poly_id = poly_[0]
                    if poly_id == int(inside_poly_df.iloc[index, 1]):
                        hole_max_dwt = poly_[-1]
                        break

                if hole_max_dwt < float(tmp_ship_static_merge.iloc[0, 24]):
                # if hole_max_dwt < 10000.:
                    inside_poly_df.iloc[index, 2] = 2
                    inside_poly_df.iloc[index, 3] = '载重吨条件不符合'

                shiptype_list.append(tmp_ship_static_merge.iloc[0, 22])
                chinese_name.append(tmp_ship_static_merge.iloc[0, 11])
                draught_list.append(tmp_ship_static_merge.iloc[0, 9])
                dwt_list.append(tmp_ship_static_merge.iloc[0, 24])

            else:
                shiptype_list.append('其他')
                chinese_name.append('暂无')
                draught_list.append(0)
                dwt_list.append(0)
        inside_poly_df['ship_type'] = shiptype_list
        inside_poly_df['chinese_name'] = chinese_name
        inside_poly_df['dwt'] = dwt_list
        inside_poly_df['draught'] = draught_list
        self.inside_poly_mysql(inside_poly_df)


class BridgeOPT(object):
    """
    东海大桥防撞模型优化
    """
    def __init__(self):
        pass

    def get_bridge_poly(self):
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
            max_wind = float(row[8])
            min_njd = int(row[9])
            max_dwt = int(row[6])
            poly_coordinate_string = row[13]
            poly_coordinate_list = []
            hole_point_string_1 = row[14]
            hole_point_string_2 = row[15]
            point1 = [float(x) for x in hole_point_string_1.split("*")]
            point2 = [float(x) for x in hole_point_string_2.split("*")]
            max_angle = int(row[16])
            for coordinate in poly_coordinate_string.split(';'):
                if coordinate:
                    lon_ = float(coordinate.split('*')[0])
                    lat_ = float(coordinate.split('*')[1])
                    poly_coordinate_list.append([lon_, lat_])
            bridge_poly_list.append([poly_id, poly_coordinate_list, max_wind, min_njd,
                                     max_dwt, point1, point2, max_angle])
        cur.close()
        conn.close()
        return bridge_poly_list

    def get_weather_conf_data(self):
        """
        获取当前天气预警数据
        :return: 通航孔通航条件数据，类型：data frame
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()
        cur.execute("SELECT * FROM weather_conf order by wp_id DESC limit 1")
        newest_weahter_conf_df = pd.DataFrame(list(cur.fetchall()))
        newest_weahter_conf_df.columns = ['wp_id', 'pub_date', 'pub_clock', 'src_loc', 'max_avg_wind',
                                          'max_zf_wind',
                                          'min_njd', 'suggest_warn', 'if_conf', 'conf_man', 'conf_level',
                                          'conf_time',
                                          'suggest_njd_warn', 'conf_njd_level', 'if_conf_njd', 'conf_man_njd',
                                          'conf_time_njd', 'pub_time', 'tmr_max_avg_wind', 'tmr_max_zf_wind']
        return newest_weahter_conf_df

    def inside_poly_mysql(self, inside_poly_df):
        """
        将即将通过通航孔的数据入库
        :param inside_poly_df: 在不同多边形内的船舶数据
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()
        inside_poly_array = np.array(inside_poly_df)

        hole_id_list = range(6)
        create_time = time.strftime("%Y-%m-%d %H:%M:%S")
        for hole_id in hole_id_list:
            value_array = np.array(inside_poly_df[inside_poly_df['hole_id'] == hole_id])
            non_conformity_array = value_array[value_array[:, 2] == 2]
            if len(non_conformity_array) == 0:
                non_conformity_number = 2
            else:
                non_conformity_number = 1

            # 数据导入bridge_data表
            bridge_data_update_sql = """
                                     UPDATE bridge_data SET ship_number='%d', non_conformity='%s' WHERE bridge_id='%d'
                                     """ % (len(value_array), non_conformity_number, int(hole_id))
            cur.execute(bridge_data_update_sql)

        # 数据导入bridge_history表
        cur.execute('truncate bridge_history')
        for line in inside_poly_array:
            bridge_history_update_sql = """
                                        INSERT INTO bridge_history(mmsi, ship_chinese_name, ship_type,
                                        bridge_hole, is_cross, create_time, reason, dwt, draught, update_time) 
                                        VALUE('%s', '%s', '%s', '%s', '%d', '%s', '%s', '%f', '%f', '%s')""" \
                                        % (line[0], line[5], line[4], line[1], line[2], create_time,
                                           line[3], float(line[6]), float(line[7]), create_time)
            cur.execute(bridge_history_update_sql)
            if line[2] == 2:
                create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                tb_pop_mysql(create_time=create_time, warn_type='东海大桥', warn_text='有东海大桥预警，请注意！', if_pop=0)
                bridge_warning_history_update_sql = """
                                                    INSERT INTO bridge_warning_history(mmsi, ship_chinese_name, ship_type,
                                                    bridge_hole, is_cross, create_time, reason, dwt, draught, update_time) 
                                                    VALUE('%s', '%s', '%s', '%s', '%d', '%s', '%s', '%f', '%f', '%s')""" \
                                                    % (line[0], line[5], line[4], line[1], line[2], create_time,
                                                       line[3], float(line[6]), float(line[7]), create_time)
                cur.execute(bridge_warning_history_update_sql)

        conn.commit()
        conn.close()

    def get_deta_cog(self, ais_cog, hole_data):
        """
        求出通航孔形成的cog与AIS数据cog的形成的夹角
        :param hole_data: 通航孔数据，类型：list
        :param ais_cog: ais的cog，类型：int，单位：0.1度
        :return: deta_cog，类型:float
        """
        hole_cog_float = coor_cog(lon1=hole_data[5][0], lat1=hole_data[5][1],
                                  lon2=hole_data[6][0], lat2=hole_data[6][1])
        deta_cog = abs(hole_cog_float - ais_cog/10.)
        if deta_cog < 90.:
            return deta_cog
        elif 90. <= deta_cog < 180.:
            return 180 - deta_cog
        elif 180. <= deta_cog < 270.:
            return deta_cog - 180.
        elif 270. <= deta_cog < 360.:
            return 360 - deta_cog

    def judge_weather(self, bridge_max_wind, bridge_min_njd, poly_id):
        """
        判断天气条件是否满足通航条件
        :param bridge_max_wind: 通航孔最大通航风力，类型：float
        :param bridge_min_njd: 通航孔最小通航能见度，类型：int
        :param poly_id: 通航孔编号
        :return: bool_num，是否满足条件，1-满足,2-不满足；reason，理由，类型：string
        """
        # 获取最新天气情况
        newest_weather_conf_df = self.get_weather_conf_data()
        newest_avg_max_wind = newest_weather_conf_df.iloc[0, 4]
        newest_min_njd = newest_weather_conf_df.iloc[0, 6]

        # 获取天气是否满足条件
        bridge_cross_wind_bool = (float(bridge_max_wind) > float(newest_avg_max_wind))
        # print(poly_id, float(bridge_max_wind), float(newest_avg_max_wind))
        # input("---------------2---------------")
        bridge_cross_njd_bool = (int(bridge_min_njd) < int(newest_min_njd))

        # 判断是否符合通航条件
        reason = ""
        bool_num = 1
        if bridge_cross_wind_bool & bridge_cross_njd_bool:
            bool_num = 1
            reason = "符合条件"
        if not bridge_cross_wind_bool:
            bool_num = 2
            reason = reason + "风力条件不符合;"
        if not bridge_cross_njd_bool:
            bool_num = 2
            reason = reason + "能见度条件不符合;"
        if poly_id == 5:
            bool_num = 2
            reason = "违规通航孔"
        return bool_num, reason

    def judge_ship_static(self, ship_mmsi, hole_data, ship_static_df):
        # 匹配船舶静态数据
        tmp_ship_static_merge = ship_static_df[ship_static_df['mmsi_x'] == int(ship_mmsi)]

        # 查看是否匹配到静态数据
        hole_max_dwt = hole_data[4]
        if len(tmp_ship_static_merge) > 0:
            if hole_max_dwt < float(tmp_ship_static_merge.iloc[0, 24]):
                bool_num = 2
                reason = '载重吨条件不符合;'
            else:
                bool_num = 1
                reason = '符合条件;'

            shiptype = tmp_ship_static_merge.iloc[0, 22]
            chinese_name = tmp_ship_static_merge.iloc[0, 11]
            draught = tmp_ship_static_merge.iloc[0, 9]
            dwt = tmp_ship_static_merge.iloc[0, 24]

        else:
            bool_num = 1
            reason = '符合条件;'
            shiptype = '其他'
            chinese_name = '暂无'
            draught = 0
            dwt = 0
        return [bool_num, reason, shiptype, chinese_name, draught, dwt]

    def judge_pass_hole(self, lon1, lat1, lon2, lat2, ais_cog):
        """
        判断船舶当前位置与通航孔形成cog与AIS当前cog的差值
        :param lon1: AIS longtide
        :param lat1: AIS latitude
        :param lon2: 通航孔经度
        :param lat2: 通航孔纬度
        :param ais_cog: ais cog
        :return:
        """
        ship_hole_cog = coor_cog(lon1=lon1, lat1=lat1, lon2=lon2, lat2=lat2)
        deta_pass_cog = abs(ais_cog - ship_hole_cog)
        # 若小于90度或大于270则未通过，若大于90度或小于270度则通过
        if (deta_pass_cog <= 90.) | (deta_pass_cog >= 270.):
            return True
        else:
            return False

    def bridge_main(self, ys_ais, ship_static_df):
        """
        每10分钟判断一次，经过东海大桥通航孔的船舶数量
        :param ys_ais: 10分钟洋山水域内的ais数据，类型：data frame
        :param ship_static_df: 船舶静态数据，类型:data frame
        :return: 1-5号多边形内的船舶数量与mmsi列表
        """
        # 将ais的data frame转换为array
        newest_ais_list = []
        for key, value in ys_ais.groupby('unique_ID'):
            value = value.sort_values(by=['unique_ID', 'acquisition_time'])
            newest_ais_list.append([value.iloc[-1, 0], value.iloc[-1, 1], value.iloc[-1, 2], value.iloc[-1, 3],
                                    value.iloc[-1, 4]])
        ys_ais_array = np.array(newest_ais_list)

        # 获取多边形数据
        # poly_id, poly_coordinate_list, max_wind, min_njd, max_dwt, point1, point2, max_angle
        poly_coordinate_list = self.get_bridge_poly()
        # print(poly_coordinate_list)
        # input("-------------1-----------------")

        # 找出多边形中的船舶数量
        inside_poly_mmsi_list = []  # col0:chinese name, col1:mmsi, col2:hole_id, col3:if_cross, col4:reason
        # 循环判断每个多边形与AIS数据点的位置关系
        inside_poly_list = []
        for poly_ in poly_coordinate_list:
            poly_id = poly_[0]
            each_coordinate_array = np.array(poly_[1])

            # 获取东海大桥通航孔的预警条件
            bridge_max_wind = float(poly_[2])
            bridge_min_njd = float(poly_[3])
            for ais_row in ys_ais_array:
                # 判断该条ais数据是否在多边形内
                if point_poly(pointLon=ais_row[2], pointLat=ais_row[3], polygon=each_coordinate_array):
                    # 若在多边形内
                    # 1. 判断船舶当前AIS数据的cog与通航孔线段形成的cog进行对比，若大于某个值，则说明有可能会通过通航孔
                    # 2. 用AIS位置点与通航孔线段中点的位置形成的cog与AIS当前cog进行判断，夹角为锐角或钝角，锐角为即将通过，钝角已通过
                    mean_hole_lon = (poly_[5][0] + poly_[6][0]) / 2.
                    mean_hole_lat = (poly_[5][1] + poly_[6][1]) / 2.
                    deta_cog_float = self.get_deta_cog(hole_data=poly_, ais_cog=ais_row[4])
                    # pass_angle_bool: T - 未通过， F - 已通过
                    pass_angle_bool = self.judge_pass_hole(lon1=ais_row[2], lat1=ais_row[3],
                                                           lon2=mean_hole_lon, lat2=mean_hole_lat,
                                                           ais_cog=ais_row[4])

                    # 判断deta_cog是否大于给定的值，则有可能通过通航孔，若小于给定的值则判断为不会通过通航孔
                    if (deta_cog_float >= poly_[7]) & pass_angle_bool:
                        # 判断天气条件是否满足
                        weather_bool_num, weather_reason = self.judge_weather(bridge_max_wind=bridge_max_wind,
                                                                              bridge_min_njd=bridge_min_njd,
                                                                              poly_id=poly_id)
                        # ship_static_result: [bool_num, reason, shiptype, chinese_name, draught, dwt]
                        ship_static_result = self.judge_ship_static(ship_mmsi=ais_row[0], hole_data=poly_,
                                                                    ship_static_df=ship_static_df)
                        reason = ''
                        if_cross_bool_num = 1
                        if weather_bool_num == 2:
                            reason += weather_reason
                            if_cross_bool_num = 2
                        if ship_static_result[0] == 2:
                            reason += ship_static_result[1]
                            if_cross_bool_num = 2

                        inside_poly_list.append([ais_row[0], poly_id, if_cross_bool_num, reason, ship_static_result[2],
                                                 ship_static_result[3], ship_static_result[5], ship_static_result[4]])
        inside_poly_df = pd.DataFrame(inside_poly_list, columns=['mmsi', 'hole_id', 'if_cross', 'reason', 'ship_type',
                                                                 'chinese_name', 'dwt', 'draught'])
        inside_poly_df = inside_poly_df.drop_duplicates()
        print(inside_poly_df)
        self.inside_poly_mysql(inside_poly_df)

if __name__ == "__main__":
    # ----------------------------------------------------
    # 获取最新10分钟内，东海大桥的通航情况
    while True:
        ship_static_df = get_ship_static_mysql()

        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()

        bridge = BridgeOPT()
        bridge.bridge_main(ys_ais=ys_ais_10mins, ship_static_df=ship_static_df)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))
        print('----------------------------------')
        time.sleep(300)
