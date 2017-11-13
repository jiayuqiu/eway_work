# coding:utf-8

import pymysql
import pandas as pd
import numpy as np
import time
import datetime

from base_func import tb_pop_mysql, get_ship_static_mysql


class Port(object):
    def __init__(self):
        # 链接数据库
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        # 获取洋山泊位信息
        select_sql = """
                     SELECT * FROM breth_data
                     """
        cur.execute(select_sql)

        self.breth_df = pd.DataFrame(list(cur.fetchall()))
        self.breth_df.columns = ['bd_id', 'breth_name', 'breth_length', 'min_dwt', 'max_dwt', 'breth_type',
                                 'font_depth', 'roundabout_area', 'roundabout_depth', 'remarks']
        # print(self.breth_array)

        self.general_max_wind = 8
        self.general_min_njd = 2000
        self.lng_oil_max_wind = 7
        self.lng_oil_min_njd = 1500

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
                                          'conf_time_njd', 'pub_time', 'tmr_max_avg_wind', 'tmr_max_zf_wind']
        return newest_weahter_conf_df

    def judge_pass_port(self, newest_weather_array, ship_port_line):
        """
        判断该靠离泊事件是否满足天气、码头条件
        :param newest_weather_array: 最新天气情况，类型：np.array
        :param ship_port_line: 一条靠离泊事件
        :return:
        """
        # 初始化风力、能见度条件
        wind_port_bool = False
        njd_port_bool = False
        dwt_port_bool = False
        draught_port_bool = False
        breth_port_bool = True

        newest_max_avg_wind = newest_weather_array[0, 4]
        newest_min_njd = newest_weather_array[0, 6]
        newest_tmr_max_avg_wind = newest_weather_array[0, 18]
        newest_tmr_max_zf_wind = newest_weather_array[0, 19]
        print("max_avg_wind = %d, min_njd = %d" % (newest_max_avg_wind, newest_min_njd))
        ship_port_date_time = datetime.datetime.strptime(ship_port_line[18], '%Y-%m-%d')
        today_date_time = datetime.datetime.now()

        # 判断是否是LNG或油船码头
        if ('LNG' in ship_port_line[6]) | ('油' in ship_port_line[6]):
            # 获取对应码头信息
            tmp_breth_data = self.breth_df[self.breth_df['breth_name'] == 'LNG 码头']
            print('油')

            if len(tmp_breth_data) > 0:
                # 判断是否为当天数据
                if today_date_time.day == ship_port_date_time.day:
                    # 判断风力条件
                    if newest_max_avg_wind < self.lng_oil_max_wind:
                        wind_port_bool = True

                    # 判断能见度条件
                    if newest_min_njd > self.lng_oil_min_njd:
                        njd_port_bool = True
                else:
                    # 判断风力条件
                    if newest_tmr_max_avg_wind < self.lng_oil_max_wind:
                        wind_port_bool = True

                    # 判断能见度条件
                    if newest_min_njd > self.lng_oil_min_njd:
                        njd_port_bool = True

                # 判断载重吨
                if tmp_breth_data.iloc[0, 4] < 100:
                    breth_dwt = tmp_breth_data.iloc[0, 4] * 10000.
                else:
                    breth_dwt = tmp_breth_data.iloc[0, 4]
                if ship_port_line[4]:
                    if ship_port_line[4] < breth_dwt:
                        dwt_port_bool = True
                else:
                    dwt_port_bool = True

                # 判断吃水深度
                if ship_port_line[20]:
                    if ship_port_line[20] < tmp_breth_data.iloc[0, 6]:
                        draught_port_bool = True
                else:
                    draught_port_bool = True
            else:
                breth_port_bool = False
        else:  # 不是油船或LNG船
            port_name = "洋山" + ship_port_line[6][1] + "期" + ship_port_line[6][2] + "泊"
            print(port_name)
            tmp_breth_data = self.breth_df[self.breth_df['breth_name'] == port_name]

            if len(tmp_breth_data) > 0:
                # 判断是否为当天数据
                if today_date_time.day == ship_port_date_time.day:
                    # 判断风力条件
                    if newest_max_avg_wind < self.general_max_wind:
                        wind_port_bool = True

                    # 判断能见度条件
                    if newest_min_njd > self.general_min_njd:
                        njd_port_bool = True
                else:
                    # 判断风力条件
                    if newest_tmr_max_avg_wind < self.general_max_wind:
                        wind_port_bool = True

                    # 判断能见度条件
                    if newest_min_njd > self.general_min_njd:
                        njd_port_bool = True

                # 判断载重吨
                if tmp_breth_data.iloc[0, 4] < 100:
                    breth_dwt = tmp_breth_data.iloc[0, 4] * 10000.
                else:
                    breth_dwt = tmp_breth_data.iloc[0, 4]

                if ship_port_line[4]:
                    if ship_port_line[4] < breth_dwt:
                        dwt_port_bool = True
                else:
                    dwt_port_bool = True

                # 判断吃水深度
                if ship_port_line[20]:
                    if ship_port_line[20] < tmp_breth_data.iloc[0, 6]:
                        draught_port_bool = True
                else:
                    draught_port_bool = True
            else:  # 没有匹配到码头信息
                breth_port_bool = False

        if wind_port_bool & njd_port_bool & dwt_port_bool & draught_port_bool & breth_port_bool:
            return 1, '符合靠离条件'
        else:
            print('不符合条件')
            reason = ''
            if not breth_port_bool:
                reason += '码头数据匹配失败;'
            else:
                if not wind_port_bool:
                    reason += '风力条件不符合;'
                if not njd_port_bool:
                    reason += '能见度条件不符合;'
                if not dwt_port_bool:
                    reason += '载重吨条件不符合;'
                if not draught_port_bool:
                    reason += '吃水深度条件不符合;'
            return 2, reason

    def get_port_data(self):
        """
        从数据库中获取没有判断过是否可以靠离泊的数据
        :return: 未进行判断的靠离泊的数据，类型：data frame
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        today_date = time.strftime('%Y-%m-%d 0:0:0', time.localtime(time.time()))
        select_sql = """
                     SELECT * from ship_port_data WHERE berthing_control=0 OR entry_depart_time>='%s' 
                     OR berthing_control IS NULL 
                     """ % today_date

        cur.execute(select_sql)
        # 获取待计算的数据
        ship_port_data = list(cur.fetchall())
        cur.close()
        conn.close()
        return ship_port_data

    def update_port_data(self, breth_control, ship_port_data, ship_static_df):
        """
        更新数据库中，ship_port_data中是否符合条件的字段
        :param breth_control: 是否符合靠离泊条件与reason，1 - 符合 2 - 不符合
        :param ship_port_data: 原始ship_port_data
        :param ship_static_df: 船舶静态数据
        :return:
        """
        # 链接数据库
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic', charset='utf8')
        cur = conn.cursor()

        ship_port_data_len = len(ship_port_data)
        for index in range(ship_port_data_len):
            # 获取船舶静态属性
            one_ship_static = ship_static_df[ship_static_df['ship_name'] == ship_port_data[index, 17]]
            if len(one_ship_static) > 0:
                ship_type = one_ship_static.iloc[0, 22]
                ship_dwt = float(one_ship_static.iloc[0, 24])
                ship_wind_resistance_level = one_ship_static.iloc[0, 28]
            else:
                ship_type = '暂无'
                ship_dwt = 0
                ship_wind_resistance_level = 0
            update_sql = """
                         UPDATE ship_port_data SET ship_type='%s', dwt='%f', wind_resistance_level='%d', 
                         berthing_control='%d', reason='%s' WHERE ipd_id='%d'
                         """ % (ship_type, ship_dwt, ship_wind_resistance_level,
                                breth_control[index][0], breth_control[index][1], ship_port_data[index, 0])
            if breth_control[index][0] == 2:
                create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                tb_pop_mysql(create_time=create_time, warn_type='船舶靠离泊', warn_text='有船舶靠离泊预警，请注意！', if_pop=0)
            cur.execute(update_sql)
        conn.commit()
        cur.close()
        conn.close()

    def port_main(self, ship_static_df):
        """
        判断靠离泊主函数
        :return:
        """
        # 读取表ship_port_data中brething_control为0的字段（0 - 待计算）
        ship_port_data = self.get_port_data()
        ship_port_array = np.array(ship_port_data)
        berthing_control_list = []
        print(ship_port_data)

        # 用ship_port_data中英文船名字段匹配ship_static_data_eway中获取船舶静态数据
        # 获取中文船名、英文船名、船舶种类、载重吨、抗风等级
        tmp_ship_static_df = ship_static_df[ship_static_df['ship_name'].isin(ship_port_array[:, 17])]

        # 判断天气、码头信息是否符合靠离泊条件
        if len(ship_port_data) > 1:
            newest_weather_array = np.array(self.get_newest_weather_conf())
            for ship_port_line in ship_port_data:
                brething_control_bool, reason = self.judge_pass_port(newest_weather_array, ship_port_line)
                berthing_control_list.append([brething_control_bool, reason])
                print('--------------------------------------------')
            # 更新ship_port_data数据
            self.update_port_data(berthing_control_list, ship_port_array, tmp_ship_static_df)

if __name__ == "__main__":
    # --------------------------------------------------------
    # 靠离泊
    # while True:
    ship_static_df = get_ship_static_mysql()
    print(ship_static_df.columns)
    print("----------------")
    # ship_static_df = pd.DataFrame(columns=['mmsi_x', 'create_time', 'ship_type_x', 'imo_x', 'callsign',
    #    'ship_length', 'ship_width', 'pos_type', 'eta', 'draught',
    #    'destination', 'ship_name', 'ssd_id', 'mmsi_y', 'imo_y',
    #    'ship_chinese_name', 'ship_english_name', 'ship_callsign',
    #    'sea_or_river', 'flag', 'sail_area', 'ship_port', 'ship_type_y',
    #    'tonnage', 'dwt', 'monitor_rate', 'length', 'width',
    #    'wind_resistance_level', 'height_above_water'])
    port = Port()
    port.port_main(ship_static_df)
    # print(time.strftime('%Y-%m-%d %H:%M:%S'))
    # print('----------------------------------')
    # time.sleep(60)
