# coding:utf-8

import numpy as np
import pandas as pd
import time
import pymysql

from base_func import getDist
from ais_analysis import AIS

class Traffic(object):
    def __init__(self):
        # 链接数据库
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        self.fit_line_df = pd.read_csv('/home/qiu/Documents/filtered_fit_line.csv')
        select_sql = """
                                 SELECT * FROM ship_static_data
                                 """
        cur.execute(select_sql)
        self.ship_static_df = pd.DataFrame(list(cur.fetchall()))
        self.ship_static_df.columns = ['ssd_id', 'mmsi', 'imo', 'ship_chinese_name', 'ship_english_name',
                                       'ship_callsign',
                                       'sea_or_river', 'flag', 'sail_area', 'ship_port', 'ship_type', 'tonnage', 'dwt',
                                       'monitor_rate', 'length', 'width', 'wind_resistance_level', 'height_above_water']
        # self.ship_static_df = pd.read_csv('/home/qiu/Documents/ys_ais/all_ship_static_ys_opt.csv')
        self.ship_static_df = self.ship_static_df[~self.ship_static_df['mmsi'].isnull()]

    def predict_circle_ship_number(self, ys_ais):
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
        fit_line_array = np.array(self.fit_line_df)
        enter_out_array = fit_line_array[fit_line_array[:, 4] == True]

        # # 获取某10分钟时限的ais数据
        # predict_str_time = np.random.random_integers(str_time, end_time-600)
        # ys_ais = ys_ais[(ys_ais['acquisition_time'] >= predict_str_time) &
        #                 (ys_ais['acquisition_time'] <= predict_str_time + 600)]
        # print("10分钟内，ais数据有%s" % len(ys_ais))
        # print("开始时间为 %s" % predict_str_time)

        # 找到船舶最新一条的轨迹点，与拟合曲线中的点的位置关系
        for mmsi, value in ys_ais.groupby('unique_ID'):
            # print("mmsi = %d" % mmsi)
            newest_point = value.iloc[-1, :]
            min_dst = 99999999.
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

            if (min_dst < 1.5) & (enter_time > 0) & (out_time > 0):
                predict_res.append([mmsi, int(enter_time // 600), int(out_time // 600)])
            else:
                pass
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
            show_time_range = list(range(1, 7))
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
                for predict_hour in range(1, 7):
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
            print(line)
            insert_sql = """
                         INSERT INTO warning_area_data(mmsi_list, warn_time, ship_type, ship_num) VALUE('%s', '%d', 
                         '%s', '%d')
                         """ % (line[0], (line[1] * 10), line[2], line[3])
            cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()


    def traffic_main(self, ys_ais):
        """
        交通预警主函数
        :param ys_ais: 洋山水域内的ais数据，类型：data frame
        :return:
        """
        predict_res_df = pd.DataFrame(self.predict_circle_ship_number(ys_ais=ys_ais),
                                      columns=['mmsi', 'enter_time', 'out_time'])
        # predict_res_df.columns = ['mmsi', 'enter_time', 'out_time']
        shiptype_list = []
        for index, value in predict_res_df.iterrows():
            tmp_ship_static = self.ship_static_df[self.ship_static_df['mmsi'] == value['mmsi']]
            if len(tmp_ship_static) > 0:
                shiptype_list.append(tmp_ship_static.iloc[0, 4])
            else:
                shiptype_list.append('其他')
        predict_res_df['ship_type'] = shiptype_list
        print(predict_res_df)
        print("---------------------")
        sum_predict_list = self.sum_predict_res(predict_res_df)
        self.sum_predict_mysql(sum_predict_list)


if __name__ == "__main__":
    # --------------------------------------------------------
    # 交通预警
    while True:
        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()
        # ys_ais_10mins = pd.read_csv('/home/qiu/Documents/ys_ais/pre_201606_ys_10mins.csv')

        traffic = Traffic()
        predict_res_df = pd.DataFrame(traffic.traffic_main(ys_ais=ys_ais_10mins))
        time.sleep(300)
