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

    def ais_org_mysql(self, ais_msg, create_time):
        """
        将接受到的ais原始报文入库
        :return:
        """
        ais_msg = ais_msg.decode().replace('\\r\\n', '')
        insert_sql = """
                     INSERT INTO ais_org(ais_org_text, create_time) VALUE ('%s', '%s')
                     """ % (ais_msg, create_time)
        self.cur.execute(insert_sql)
        self.conn.commit()

    def get_ais_org(self):
        """
        获取ais数据原始报文
        :return: 返回ais原始报文列表，类型：list
        """
        address = ('192.168.18.202', 16197)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(address)

        while True:
            data, addr = s.recvfrom(2048)
            create_time = time.strftime('%Y-%m-%d %H:%M:%S')
            if not data:
                print("client has exist")
                break
            print(data, create_time)
            self.ais_org_mysql(data, create_time)
        s.close()

    def ais_message_main(self, ais_org_list):
        """
        解析ais报文主函数
        :return:
        """
        ais_list = []
        for tmp_string in ais_org_list:
            try:
                msg_body = tmp_string.split(',')[-2]
                if (len(msg_body) != 0) & (tmp_string[0] == '!'):
                    if msg_body[0] == '1':
                        print(tmp_string)
                        msg_ascii = self.ascii_string.str_to_ascii(msg_body)

                        mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
                        longitude = int(self.ascii_string.python_substring(msg_ascii, 62, 27), 2) / 600000
                        latitude = int(self.ascii_string.python_substring(msg_ascii, 90, 26), 2) / 600000
                        ais_list.append([longitude, latitude])
                        # time_stamp = int(self.ascii_string.python_substring(msg_ascii, 138, 5), 2)
                        print("mmsi = %s" % mmsi)
                        print("longitude = %s" % longitude)
                        print("latitude = %s" % latitude)
                        print("--------------------------")
            except Exception as e:
                msg_type = int(self.ascii_string.python_substring(msg_ascii, 0, 2), 2)
                print("==============")
                print(msg_type)
                print("==============")
                # print(e)
        return ais_list

if __name__ == "__main__":
    aisMes = AisMes()
    aisMes.get_ais_org()
    # ais_df = pd.DataFrame(ais_list, columns=['longitude', 'latitude'])
    # ais_df = ais_df[(ais_df['longitude'] <= 180.0) & (ais_df['longitude'] >= -180.0) &
    #                 (ais_df['latitude'] <= 90.0) & (ais_df['latitude'] >= -90.0)]
    # ais_df.to_csv('/home/qiu/Documents/ys_ais/ysAIS_test.csv', index=None)
