# coding:utf-8

import pandas as pd
import numpy as np
import time
import pymysql

from base_func import getDist, getFileNameList, is_line_cross, point_poly
from ais_analysis import AIS

class Bridge(object):
    def __init__(self):
        print('loading ship static data...')
        # 链接数据库
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()
        select_sql_eway = """
                         SELECT * FROM ship_static_data_eway
                         """
        cur.execute(select_sql_eway)
        self.ship_static_eway_array = np.array(list(cur.fetchall()))
        for index in range(len(self.ship_static_eway_array)):
            if self.ship_static_eway_array[index, 11]:
                self.ship_static_eway_array[index, 11] = self.ship_static_eway_array[index, 11].replace(' ', '')

        self.ship_static_eway_df = pd.DataFrame(self.ship_static_eway_array)
        self.ship_static_eway_df.columns = ['mmsi', 'create_time', 'ship_type', 'imo', 'callsign',
                                            'ship_length', 'ship_width', 'pos_type', 'eta', 'draught', 'destination',
                                            'ship_name']

        select_sql = """
                     SELECT * FROM ship_static_data
                     """
        cur.execute(select_sql)
        self.ship_static_df = pd.DataFrame(list(cur.fetchall()))
        self.ship_static_df.columns = ['ssd_id', 'mmsi', 'imo', 'ship_chinese_name', 'ship_english_name', 'ship_callsign',
                                       'sea_or_river', 'flag', 'sail_area', 'ship_port', 'ship_type', 'tonnage', 'dwt',
                                       'monitor_rate', 'length', 'width', 'wind_resistance_level', 'height_above_water']
        # self.ship_static_df = pd.read_csv('/home/qiu/Documents/ys_ais/all_ship_static_ys_opt.csv')
        self.ship_static_df = self.ship_static_df[~self.ship_static_df['mmsi'].isnull()]

        self.ship_static_merge = pd.merge(self.ship_static_eway_df, self.ship_static_df, how='outer',
                                          left_on='ship_name', right_on='ship_english_name').fillna(0)
        cur.close()
        conn.close()
        print('finish loading...')

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
            poly_coordinate_string = row[-1]
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
                                          'conf_time_njd', 'pub_time']
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
            non_conformity_array = value_array[value_array[:, 2] == 0]
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
            # print(bridge_history_update_sql)
            cur.execute(bridge_history_update_sql)

        conn.commit()
        conn.close()

    def bridge_main(self, ys_ais):
        """
        每10分钟判断一次，经过东海大桥通航孔的船舶数量
        :param ys_ais: 10分钟洋山水域内的ais数据，类型：data frame
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
                    reason = ""
                    if bridge_cross_wind_bool & bridge_cross_njd_bool:
                        bool_num = 1
                        reason = "符合条件"
                    if not bridge_cross_wind_bool:
                        bool_num = 0
                        reason = reason + "风力条件不符合;"
                    if not bridge_cross_njd_bool:
                        bool_num = 0
                        reason = reason + "能见度条件不符合;"
                    if poly_id == 5:
                        bool_num = 0
                        reason = "违规通航孔"
                    inside_poly_mmsi_list.append([int(ais_row[0]), poly_id, bool_num,
                                                  reason])
        inside_poly_df = pd.DataFrame(inside_poly_mmsi_list, columns=['mmsi', 'hole_id', 'if_cross', 'reason'])
        inside_poly_df = inside_poly_df.drop_duplicates()
        shiptype_list = []
        chinese_name = []
        dwt_list = []
        draught_list = []
        for index, value in inside_poly_df.iterrows():
            tmp_ship_static = self.ship_static_df[self.ship_static_df['mmsi'] == int(value['mmsi'])]
            # tmp_ship_static_ewary = self.ship_static_eway_df[self.ship_static_eway_df['mmsi'] == int(value['mmsi'])]
            tmp_ship_static_merge = self.ship_static_merge[self.ship_static_merge['mmsi_x'] == int(value['mmsi'])]
            if len(tmp_ship_static) > 0:
                shiptype_list.append(tmp_ship_static.iloc[0, 4])
                chinese_name.append(tmp_ship_static.iloc[0, 1])
                dwt_list.append(tmp_ship_static.iloc[0, 12])
                draught_list.append(tmp_ship_static_merge.iloc[0, 24])
            else:
                if len(tmp_ship_static_merge) > 0:
                    shiptype_list.append('其他')
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

if __name__ == "__main__":
    # ----------------------------------------------------
    # 获取最新10分钟内，东海大桥的通航情况
    while True:
        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()

        bridge = Bridge()
        bridge_cross_df = bridge.bridge_main(ys_ais=ys_ais_10mins)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))
        print('----------------------------------')
        time.sleep(300)

