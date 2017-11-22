# coding:utf-8

import pandas as pd
import pymysql
import time


class AISAnalysis(object):
    def python_substring(self, input_string, str_index, string_length):
        """
        c#中的Substring功能
        :param str_index: 开始的索引
        :param string_length: 需要截取字符串的长度
        :return: 需要的字符串
        """
        return input_string[str_index:(str_index + string_length)]

    def str_to_ascii(self, msg_string):
        """
        将ais报文转换为ASCII码
        :param msg_string: ais报文
        :return: ais报文的ascii码
        """
        ascii_string = ""
        for letter in msg_string:
            if letter == "0":
                ascii_string += "000000"

            elif letter == "1":
                ascii_string += "000001"

            elif letter == "2":
                ascii_string += "000010"

            elif letter == "3":
                ascii_string += "000011"

            elif letter == "4":
                ascii_string += "000100"

            elif letter == "5":
                ascii_string += "000101"

            elif letter == "6":
                ascii_string += "000110"

            elif letter == "7":
                ascii_string += "000111"

            elif letter == "8":
                ascii_string += "001000"

            elif letter == "9":
                ascii_string += "001001"

            elif letter == ":":
                ascii_string += "001010"

            elif letter == "":
                ascii_string += "001011"

            elif letter == "<":
                ascii_string += "001100"

            elif letter == "=":
                ascii_string += "001101"

            elif letter == ">":
                ascii_string += "001110"

            elif letter == "?":
                ascii_string += "001111"

            elif letter == "@":
                ascii_string += "010000"

            elif letter == "A":
                ascii_string += "010001"

            elif letter == "B":
                ascii_string += "010010"

            elif letter == "C":
                ascii_string += "010011"

            elif letter == "D":
                ascii_string += "010100"

            elif letter == "E":
                ascii_string += "010101"

            elif letter == "F":
                ascii_string += "010110"

            elif letter == "G":
                ascii_string += "010111"

            elif letter == "H":
                ascii_string += "011000"

            elif letter == "I":
                ascii_string += "011001"

            elif letter == "J":
                ascii_string += "011010"

            elif letter == "K":
                ascii_string += "011011"

            elif letter == "L":
                ascii_string += "011100"

            elif letter == "M":
                ascii_string += "011101"

            elif letter == "N":
                ascii_string += "011110"

            elif letter == "O":
                ascii_string += "011111"

            elif letter == "P":
                ascii_string += "100000"

            elif letter == "Q":
                ascii_string += "100001"

            elif letter == "R":
                ascii_string += "100010"

            elif letter == "S":
                ascii_string += "100011"

            elif letter == "T":
                ascii_string += "100100"

            elif letter == "U":
                ascii_string += "100101"

            elif letter == "V":
                ascii_string += "100110"

            elif letter == "W":
                ascii_string += "100111"

            elif letter == "`":
                ascii_string += "101000"

            elif letter == "a":
                ascii_string += "101001"

            elif letter == "b":
                ascii_string += "101010"

            elif letter == "c":
                ascii_string += "101011"

            elif letter == "d":
                ascii_string += "101100"

            elif letter == "e":
                ascii_string += "101101"

            elif letter == "f":
                ascii_string += "101110"

            elif letter == "g":
                ascii_string += "101111"

            elif letter == "h":
                ascii_string += "110000"

            elif letter == "i":
                ascii_string += "110001"

            elif letter == "j":
                ascii_string += "110010"

            elif letter == "k":
                ascii_string += "110011"

            elif letter == "l":
                ascii_string += "110100"

            elif letter == "m":
                ascii_string += "110101"

            elif letter == "n":
                ascii_string += "110110"

            elif letter == "o":
                ascii_string += "110111"

            elif letter == "p":
                ascii_string += "111000"

            elif letter == "q":
                ascii_string += "111001"

            elif letter == "r":
                ascii_string += "111010"

            elif letter == "s":
                ascii_string += "111011"

            elif letter == "t":
                ascii_string += "111100"

            elif letter == "u":
                ascii_string += "111101"

            elif letter == "v":
                ascii_string += "111110"

            elif letter == "w":
                ascii_string += "111111"

        return ascii_string

class AIS(object):
    def __init__(self):
        self.ais_analysis = AISAnalysis()

    def load_newest_ais(self, update_time=10):
        """
        获取最新的AIS数据，取出字段：col0:mmsi, col1:time, col2:longitude, col3:latitude
        :param update_time: 更新时间
        :return:
        """
        str_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - update_time*60))
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()

        # 获取最近10分钟的AIS数据
        select_sql = """
                     SELECT * FROM ais_dynamic WHERE create_time>='%s'
                     """ % str_time
        cur.execute(select_sql)
        ais_org_list = list(cur.fetchall())

        # 解析获取到的AIS数据
        ais_list = []
        for row in ais_org_list:
            mmsi = int(row[1])
            create_time = row[2]
            time_array = time.strptime(str(create_time), "%Y-%m-%d %H:%M:%S")
            time_stamp = time.mktime(time_array)
            longitude = float(row[3])
            latitude = float(row[4])
            cog = int(row[10])
            ais_list.append([mmsi, time_stamp, longitude, latitude, cog])
        ais_df = pd.DataFrame(ais_list, columns=["unique_ID", "acquisition_time", "longitude", "latitude", "cog"])
        return ais_df


if __name__ == "__main__":
    ais = AIS()
    ais_df = ais.load_newest_ais()
    print(ais_df.head())
