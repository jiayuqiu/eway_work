# coding:utf-8

import numpy as np
import pandas as pd
import time
import pymysql
import math

from base_func import getDist, tb_pop_mysql, get_ship_static_mysql
from ais_analysis import AIS


class Traffic(object):
    def __init__(self):
        self.fit_line_df = pd.read_csv('/home/qiu/Documents/filtered_fit_line.csv')

    def coor_cog(self, lon1, lat1, lon2, lat2):
        """
        根据AIS数据两点，得到cog
        :param lon1:
        :param lat1:
        :param lon2:
        :param lat2:
        :return:
        """
        import math
        deta_lon = lon2 - lon1
        deta_lat = lat2 - lat1
        if (deta_lon >= 0.) & (deta_lat <= 0.):
            return 90 - (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
        elif (deta_lon >= 0.) & (deta_lat >= 0.):
            return 90 + (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
        elif (deta_lon <= 0.) & (deta_lat >= 0.):
            return 270 - (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))
        elif (deta_lon <= 0.) & (deta_lat <= 0.):
            return 270 + (math.atan(abs(deta_lat / deta_lon)) * (180. / math.pi))

    def predict_ship_without_channel(self, ship_longitude, ship_latitude, ship_cog, ship_speed):
        """
        若AIS数据不属于任意航道，则利用cog、sog进行计算是否会进入警戒区
        :param ship_longitude: AIS经度
        :param ship_latitude: AIS维度
        :param ship_cog: AIS对地航向
        :param ship_speed: AIS瞬时速度
        :return: 进入、离开警戒区的时间
        """
        # 1. 根据AIS数据的坐标，与警戒区圆心坐标，得到cog的取值范围
        # 2. 判断当前AIS的cog是否在取值范围内，若在范围内则利用距离与速度求出进\出圆心的时间

        # 获取cog的变化值
        dst_circle_float = getDist(lon1=ship_longitude, lat1=ship_latitude, lon2=122.1913, lat2=30.5658)
        deta_cog_tan = math.tan((2.*1.852)/dst_circle_float)
        deta_cog = abs(math.atan(deta_cog_tan) * (180. / math.pi))

        # 获取cog的范围
        ship_circle_cog_float = self.coor_cog(lon1=ship_longitude, lat1=ship_latitude, lon2=122.1913, lat2=30.5658)
        max_cog_float = ship_circle_cog_float + deta_cog
        min_cog_float = ship_circle_cog_float - deta_cog

        # 判断AIS的cog是否在取值范围内
        if (ship_cog > min_cog_float) & (ship_cog < max_cog_float):  # 在取值范围内
            if ship_speed == 0: ship_speed += 1
            # 利用AIS数据的位置到圆心所在位置的距离，与AIS数据中的sog字段进行计算，进入与离开圆心所需要的时间，时间单位：秒
            enter_time = (dst_circle_float*1000. - 2*1.850*1000.) / (ship_speed*0.514444)  # 距离单位：米，速度单位：米/秒
            outer_time = (dst_circle_float*1000. + 2*1.850*1000.) / (ship_speed*0.514444)  # 距离单位：米，速度单位：米/秒
            return enter_time, outer_time
        else:  # 不在取值范围内
            return 0, 0

    def predict_circle_ship_number(self, ys_ais):
        """
        获取某10分钟时限内的ais数据，结合拟合后的曲线，预测1小时后船舶在警戒范围内的船舶数量
        :param ys_ais: 洋山水域内的ais数据，类型：data frame
        :return: 1小时后圆内的船舶条数，类型：int
        """
        # 初始化预测结果
        predict_res = []

        # 将拟合曲线数据转换为矩阵
        fit_line_array = np.array(self.fit_line_df)
        enter_out_array = fit_line_array[fit_line_array[:, 4] == True]

        # 找到船舶最新一条的轨迹点，与拟合曲线中的点的位置关系
        for mmsi, value in ys_ais.groupby('unique_ID'):
            # print("mmsi = %d" % mmsi)
            newest_point = value.iloc[-1, :]
            min_dst = 99999999.  # 最小距离判断值
            min_deta_cog = 91.  # 最小角度偏差判断值
            enter_time, out_time = 0, 0

            # 用最新的ais数据点与拟合曲线数据中每个点进行对比，找到该点属于哪条航道
            for row in fit_line_array:
                # 获取AIS数据点到拟合曲线的距离
                point_line_dst = getDist(lon1=newest_point['longitude'], lat1=newest_point['latitude'],
                                         lon2=row[0], lat2=row[1])

                # 获取AIS数据点cog与拟合曲线cog的差值
                deta_cog = abs(row[4] - newest_point['cog']/10.)

                if deta_cog < 90.:
                    if point_line_dst < min_dst:
                        min_dst = point_line_dst
                        min_dst_channel = row[3]
                        now_time = row[2]
                        enter_time = enter_out_array[enter_out_array[:, 3] == min_dst_channel][0, 2] - now_time
                        out_time = enter_out_array[enter_out_array[:, 3] == min_dst_channel][-1, 2] - now_time

            # 判断该AIS数据点是否属于拟合曲线
            if (min_dst < 1.5) & (enter_time > 0) & (out_time > 0):  # 若属于一条拟合曲线
                predict_res.append([int(mmsi), int(enter_time // 600), int(out_time // 600)])
            else:  # 若不属于任何拟合曲线，利用cog、speed与圆心求切线进行判断
                enter_time_without_channel, outer_time_without_channel = \
                    self.predict_ship_without_channel(ship_longitude=newest_point['longitude'],
                                                      ship_latitude=newest_point['latitude'],
                                                      ship_cog=(newest_point['cog']/10.),
                                                      ship_speed=newest_point['sog']/10.)
                if (enter_time_without_channel > 0) & (outer_time_without_channel > 0):
                    predict_res.append([int(mmsi), int(enter_time_without_channel // 600),
                                        int(outer_time_without_channel // 600)])
        print(predict_res)
        return predict_res

    def sum_predict_res(self, predict_res):
        """
        对预测结果进行加总
        :param predict_res: 预测数据，类型：data frame
        :return: 加总数据
        """
        sum_predict_res_list = []
        for index, value in predict_res.iterrows():
            time_range = list(range(value['enter_time'], value['out_time'] + 1))
            show_time_range = [1, 3, 5, 7, 9]
            cross_time_list = list(set(time_range).intersection(set(show_time_range)))
            for cross_time in cross_time_list:
                sum_predict_res_list.append([value['ship_type'], value['mmsi'], cross_time])
        sum_predict_res_array = np.array(sum_predict_res_list)

        out_predict_res_list = []
        create_time = time.strftime('%Y-%m-%d %H:%M:%S')
        # 获取所有的船舶类型数据
        if len(sum_predict_res_array) > 0:
            unique_ship_type_list = list(set(sum_predict_res_array[:, 0]))
            for ship_type in unique_ship_type_list:
                # tmp_predict_res_array = sum_predict_res_array[sum_predict_res_array[:, 0] == ship_type]
                for predict_hour in [1, 3, 5, 7, 9]:
                    hour_predict_res_array = sum_predict_res_array[(sum_predict_res_array[:, 0] == ship_type) &
                                                                   (sum_predict_res_array[:, 2] == str(predict_hour))]
                    if len(hour_predict_res_array) > 0:
                        mmsi_str = ""
                        for each_mmsi in hour_predict_res_array[:, 1]:
                            mmsi_str += (each_mmsi + ",")
                        out_predict_res_list.append([mmsi_str[:-1], predict_hour, ship_type, len(hour_predict_res_array),
                                                     create_time])
            return out_predict_res_list
        else:
            return out_predict_res_list

    def sum_predict_mysql(self, sum_predict_list):
        """
        将获取到的预测数据入库
        :param sum_predict_list: 预测数据，类型：list
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        truncate_sql = """
                       truncate warning_area_data 
                       """
        cur.execute(truncate_sql)
        conn.commit()

        for line in sum_predict_list:
            # print(line)
            insert_sql = """
                         INSERT INTO warning_area_data(mmsi_list, warn_time, ship_type, ship_num) VALUE('%s', '%d', 
                         '%s', '%d')
                         """ % (line[0], (line[1] * 10), line[2], line[3])
            cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()

    def traffic_main(self, ys_ais, ship_static_df):
        """
        交通预警主函数
        :param ys_ais: 洋山水域内的ais数据，类型：data frame
        :param ship_static_df: 船舶静态数据，类型：data frame
        :return:
        """
        predict_res_df = pd.DataFrame(self.predict_circle_ship_number(ys_ais=ys_ais),
                                      columns=['mmsi', 'enter_time', 'out_time'])
        # predict_res_df.columns = ['mmsi', 'enter_time', 'out_time']
        shiptype_list = []
        for index, value in predict_res_df.iterrows():
            tmp_ship_static = ship_static_df[ship_static_df['mmsi_x'] == int(value['mmsi'])]
            if len(tmp_ship_static) > 0:
                if tmp_ship_static.iloc[0, 23] == 0:
                    shiptype_list.append('其他')
                else:
                    shiptype_list.append(tmp_ship_static.iloc[0, 23])
            else:
                shiptype_list.append('其他')
        predict_res_df['ship_type'] = shiptype_list
        sum_predict_list = self.sum_predict_res(predict_res_df)
        print(sum_predict_list)
        self.sum_predict_mysql(sum_predict_list)


if __name__ == "__main__":
    # --------------------------------------------------------
    # 交通预警
    while True:
        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()
        print(ys_ais_10mins)
        # ys_ais_10mins = pd.read_csv('/home/qiu/Documents/ys_ais/pre_201606_ys_10mins.csv')
        ship_static_df = get_ship_static_mysql()

        traffic = Traffic()
        predict_res_df = pd.DataFrame(traffic.traffic_main(ys_ais=ys_ais_10mins, ship_static_df=ship_static_df))
        time.sleep(300)
        print("-------------------------------------")
