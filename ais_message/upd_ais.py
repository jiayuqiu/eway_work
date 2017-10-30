# coding:utf-8

import numpy as np
import pandas as pd
import socket
import time
import pymysql

from ascii_string import AISAnalysis


class AisMes(object):
    def __init__(self):
        self.ascii_string = AISAnalysis()
        self.conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                                    db='dbtraffic')
        self.cur = self.conn.cursor()

    def ais_org_analysis(self, ais_org_text):
        """
        ais原始报文解析
        :param ais_org_text: ais原始报文，类型：string
        :return: 解析后的ais数据，col0:mmsi, col1:create_time, col2:longitude, col3:latitude
        """
        msg_body = ais_org_text.split(',')[-2]
        if (len(msg_body) != 0) & (ais_org_text[0] == '!'):
            if msg_body[0] == '1':
                print(ais_org_text)
                msg_ascii = self.ascii_string.str_to_ascii(msg_body)

                mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
                longitude = int(self.ascii_string.python_substring(msg_ascii, 62, 27), 2) / 600000.
                latitude = int(self.ascii_string.python_substring(msg_ascii, 90, 26), 2) / 600000.
                # time_stamp = int(self.ascii_string.python_substring(msg_ascii, 138, 5), 2)
                return [mmsi, longitude, latitude]

    def ais_bm_analysis(self, ais_bm_text):
        """
        ais博懋数据解析
        :param ais_bm_text: 博懋ais报文，类型：string
        :return: 解析后的ais数据，col0:mmsi, col1:create_time, col2:longitude, col3:latitude
        """
        ais_bm_list = ais_bm_text.split('@')
        if ais_bm_list[0] == '0':
            # 解析动态数据
            mmsi = int(ais_bm_list[2])
            longitude = float(ais_bm_list[7])
            latitude = float(ais_bm_list[8])
            if(int(longitude) >= 121) & (int(longitude) <= 123) &\
              (int(latitude) >= 30) & (int(latitude) <= 32):
                # print(mmsi, longitude, latitude)
                # input("-----------------------")
                return [mmsi, longitude, latitude]
        elif ais_bm_list[0] == '1':
            # 解析静态数据
            mmsi = int(ais_bm_list[2])
            ship_type = ais_bm_list[3]
            imo = ais_bm_list[4]
            callsign = ais_bm_list[5]
            ship_length = float(ais_bm_list[6])
            ship_width = float(ais_bm_list[7])
            pos_type = ais_bm_list[8]
            eta = ais_bm_list[9]
            draught = float(ais_bm_list[10])
            destination = ais_bm_list[12]
            return [mmsi, ship_type, imo, callsign, ship_length, ship_width, pos_type, eta, draught, destination]

    def ais_dynamic_mysql(self, ais_msg, create_time, data_source):
        """
        将接受到的ais原始报文入库
        :return:
        """
        insert_sql = """
                     INSERT INTO ais_dynamic(mmsi, create_time, longitude, latitude, data_source) VALUE ('%s', '%s', '%s',
                     '%s', '%s')
                     """ % (ais_msg[0], create_time, ais_msg[1], ais_msg[2], data_source)
        self.cur.execute(insert_sql)
        self.conn.commit()

    def ais_static_mysql(self, ais_msg, create_time):
        """
        将接受到的ais静态数据入库
        :param ais_msg:
        :param create_time:
        :return:
        """
        insert_sql = """
                     INSERT INTO ship_static_data_eway(mmsi, shiptype, IMO, callsign, ship_length, ship_width,
                     pos_type, eta, draught, destination, ship_english_name) 
                     VALUE('%d', '%s', '%s', '%s', '%f', '%f', '%s', '%s', '%f', '%s', NULL)
                     """ % (int(ais_msg[0]), ais_msg[1], ais_msg[2], ais_msg[3], float(ais_msg[4]),
                            float(ais_msg[5]), ais_msg[6], ais_msg[7], float(ais_msg[8]), ais_msg[9])
        self.cur.execute(insert_sql)
        self.conn.commit()

    def get_ais_org_ys(self):
        """
        获取ais数据原始报文，数据源：洋山
        :return: 返回ais原始报文列表，类型：list
        """
        address = ('192.168.18.202', 16197)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(address)

        while True:
            try:
                data, addr = s.recvfrom(2048)
                create_time = time.strftime('%Y-%m-%d %H:%M:%S')
                if not data:
                    print("client has exist")
                    break
                ais_analysised = self.ais_org_analysis(data.decode())
                if ais_analysised:
                    self.ais_dynamic_mysql(ais_analysised, create_time, 0)
            except Exception as e:
                print(e)
        s.close()

    def get_ais_org_bm(self):
        """
        获取博懋ais动态数据
        :return:
        """
        address = ('192.168.18.202', 16198)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(address)

        while True:
            try:
                data, addr = s.recvfrom(2048)
                create_time = time.strftime('%Y-%m-%d %H:%M:%S')
                if not data:
                    print("client has exist")
                    break
                data_analysised = self.ais_bm_analysis(data.decode())
                if data_analysised:
                    if len(data_analysised) == 3:
                        self.ais_dynamic_mysql(data_analysised, create_time, 1)
                    else:
                        self.ais_static_mysql(data_analysised, create_time)

            except Exception as e:
                if 'PRIMARY' in str(e):
                    pass
                else:
                    print(e)

    def ais_message_main(self, ais_org_list):
        """
        解析ais报文主函数
        :return:
        """
        ais_list = []
        for tmp_string in ais_org_list:
            try:
                pass
            except Exception as e:
                print("==============")
                print(e)
                print("==============")
                # print(e)
        return ais_list

if __name__ == "__main__":
    aisMes = AisMes()
    aisMes.get_ais_org_bm()
    # ais_df = pd.DataFrame(ais_list, columns=['longitude', 'latitude'])
    # ais_df = ais_df[(ais_df['longitude'] <= 180.0) & (ais_df['longitude'] >= -180.0) &
    #                 (ais_df['latitude'] <= 90.0) & (ais_df['latitude'] >= -90.0)]
    # ais_df.to_csv('/home/qiu/Documents/ys_ais/ysAIS_test.csv', index=None)
