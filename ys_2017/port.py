# coding:utf-8

import pymysql
import pandas as pd
import numpy as np
import time

class Port(object):
    def __init__(self):
        self.general_max_wind = 8
        self.general_min_njd = 2000
        self.general_max_dwt = 16
        self.lng_oil_max_wind = 7
        self.lng_oil_min_njd = 1500
        self.lng_oil_max_dwt = 15

    def get_newest_weather_conf(self):
        """
        获取最新的天气预警信息
        :return: 最新的天气预警信息，类型：data frame
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

    def get_port_data(self):
        """
        从数据库中获取没有判断过是否可以靠离泊的数据
        :return: 未进行判断的靠离泊的数据，类型：data frame
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        select_sql = """
                     SELECT * from ship_port_data WHERE berthing_control IS NULL 
                     """

        cur.execute(select_sql)
        # 获取待计算的数据
        ship_port_data = list(cur.fetchall())
        # ship_port_data = []
        # for row in cur.fetchall():
        #     brething_control = int(row[13])
        #     # 找出没判断的数据
        #     if brething_control == 0:
        #         ship_port_data.append(list(row))
        cur.close()
        conn.close()
        return ship_port_data

    def update_port_data(self, breth_control, ship_port_data):
        """
        更新数据库中，ship_port_data中是否符合条件的字段
        :param breth_control: 是否符合靠离泊条件，1 - 符合 2 - 不符合
        :param ship_port_data: 原始ship_port_data
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()

        ship_port_data_len = len(ship_port_data)
        for index in range(ship_port_data_len):
            update_sql = """
                         UPDATE ship_port_data SET berthing_control='%d' WHERE ipd_id='%d'
                         """ % (breth_control[index], ship_port_data[index, 0])
            cur.execute(update_sql)
        conn.commit()
        cur.close()
        conn.close()

    def port_main(self):
        """
        判断靠离泊主函数
        :return:
        """
        ship_port_data = self.get_port_data()
        ship_port_array = np.array(ship_port_data)
        berthing_control_list = []
        if len(ship_port_data) > 1:
            newest_weather_array = np.array(self.get_newest_weather_conf())
            # 初始化风力、能见度条件
            wind_port_bool = False
            njd_port_bool = False
            dwt_port_bool = False

            newest_max_avg_wind = newest_weather_array[0, 4]
            newest_min_njd = newest_weather_array[0, 6]

            for line in ship_port_array:
                # 判断是否是LNG或油船码头
                if ('LNG' in line[6]) | ('油' in line[6]):
                    if newest_max_avg_wind < self.lng_oil_max_wind:
                        wind_port_bool = True

                    if newest_min_njd > self.lng_oil_min_njd:
                        njd_port_bool = True

                    if line[4]:
                        if line[4] < self.lng_oil_max_dwt:
                            dwt_port_bool = True
                    else:
                        dwt_port_bool = True
                else:
                    if newest_max_avg_wind < self.general_max_wind:
                        wind_port_bool = True

                    if newest_min_njd > self.general_min_njd:
                        njd_port_bool = True

                    if line[4]:
                        if line[4] < self.general_max_dwt:
                            dwt_port_bool = True
                    else:
                        dwt_port_bool = True

                if wind_port_bool & njd_port_bool & dwt_port_bool:
                    berthing_control_list.append(1)
                else:
                    berthing_control_list.append(2)
            self.update_port_data(berthing_control_list, ship_port_array)

if __name__ == "__main__":
    # --------------------------------------------------------
    # 靠离泊
    while True:
        port = Port()
        port.port_main()
        print(time.strftime('%Y-%m-%d %H:%M:%S'))
        print('----------------------------------')
        time.sleep(60)
