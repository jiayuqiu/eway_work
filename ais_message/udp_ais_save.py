# coding:utf-8

import socket
import time
import os
import sys

from ascii_string import AISAnalysis


class AisMes(object):
    def __init__(self):
        self.ascii_string = AISAnalysis()
    
    def __check_nav_static(self, nav_status):
        if 0 <= nav_status <= 15:
            return nav_status
        else:
            return -999
    
    def __check_rot(self, rot):
        if -127 <= rot <= 127:
            return rot
        else:
            return -999
    
    def __check_sog(self, sog):
        if 0 <= sog <= 1022:
            return sog
        else:
            return -999
    
    def __check_longitude(self, longitude):
        longitude = float(longitude) / 600000.
        if -180. <= longitude <= 180.:
            return longitude
        else:
            return -999
    
    def __check_latitude(self, latitude):
        latitude = float(latitude) / 600000.
        if -90. <= latitude <= 90.:
            return latitude
        else:
            return -999
    
    def __check_cog(self, cog):
        if 0 <= cog <= 3599:
            return cog
        else:
            return -999
    
    def __check_ture_head(self, true_head):
        if 0 <= true_head <= 3599:
            return true_head
        else:
            return -999
    
    def ais_org_analysis_dynamic123(self, msg_ascii):
        """
        ais原始报文解析，1,2,3类消息
        :param msg_ascii: ais原始报文的二进制字符串，类型：string
        :return: 解析后的ais数据
        """
        try:
            acqusisition_time = int(time.time())
            msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
            
            mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
            nav_status = self.__check_nav_static(int(self.ascii_string.python_substring(msg_ascii, 38, 4), 2))
            rot = self.__check_rot(int(self.ascii_string.python_substring(msg_ascii, 42, 8), 2))
            sog = self.__check_sog(int(self.ascii_string.python_substring(msg_ascii, 50, 10), 2))
            pos_acc = int(self.ascii_string.python_substring(msg_ascii, 60, 1), 2)
            longitude = \
                self.__check_longitude(int(self.ascii_string.python_substring(msg_ascii, 61, 28), 2))
            latitude = \
                self.__check_latitude(int(self.ascii_string.python_substring(msg_ascii, 89, 27), 2))
            area_id = areaID(longitude=longitude, latitude=latitude)
            cog = self.__check_cog(int(self.ascii_string.python_substring(msg_ascii, 116, 12), 2))
            true_head = self.__check_ture_head(int(self.ascii_string.python_substring(msg_ascii, 128, 9), 2))
            regional_app = int(self.ascii_string.python_substring(msg_ascii, 143, 4), 2)
            spare = int(self.ascii_string.python_substring(msg_ascii, 147, 1), 2)
            raim_flag = int(self.ascii_string.python_substring(msg_ascii, 148, 1), 2)
            communicate_status = int(self.ascii_string.python_substring(msg_ascii, 149, 19), 2)
            
            out_list = [mmsi, acqusisition_time, 0, 247, 0, nav_status, longitude * 1000000, latitude * 1000000,
                        area_id, sog, 0.514444, cog, true_head, '', '', str(rot) + "&" + str(pos_acc) + "&&&&"]
            
            out_str = ''
            for ele in out_list:
                out_str += str(ele) + ','
            return out_str[:-1] + '\n'
        except:
            pass
    
    def ais_org_analysis_static5(self, ais_org_text):
        """
        解析5类消息，船舶静态信息
        :param ais_org_text: 原始报文
        :return:
        """
        try:
            acqusisition_time = int(time.time())
            msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
            
            mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
            ais_version_indicator = int(self.ascii_string.python_substring(msg_ascii, 38, 2), 2)
            imo_number = int(self.ascii_string.python_substring(msg_ascii, 40, 30), 2)
            call_sign = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 70, 42))
            ship_name = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 112, 120))
            ship_type = int(self.ascii_string.python_substring(msg_ascii, 232, 8), 2)
            dimension = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 240, 30))
            elec_type = int(self.ascii_string.python_substring(msg_ascii, 270, 4), 2)
            eta = str(int(self.ascii_string.python_substring(msg_ascii, 290, 4), 2)) + \
                  str(int(self.ascii_string.python_substring(msg_ascii, 285, 5), 2)) + \
                  str(int(self.ascii_string.python_substring(msg_ascii, 280, 5), 2)) + \
                  str(int(self.ascii_string.python_substring(msg_ascii, 274, 6), 2))
            draught = int(self.ascii_string.python_substring(msg_ascii, 294, 8), 2)
            destination = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 302, 30))
            dte = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 422, 1))
            spare = self.ascii_string.str_to_ascii(self.ascii_string.python_substring(msg_ascii, 423, 1))
            out_list = [mmsi, acqusisition_time, ais_version_indicator, imo_number, call_sign, ship_name, ship_type,
                        dimension, elec_type, eta, draught, destination, dte, spare]
            out_str = ''
            for ele in out_list:
                out_str += str(ele) + ','
            return out_str[:-1] + '\n'
        except:
            pass
    
    def ais_org_analysis(self, ais_org_text):
        """
        解析ais原始报文
        :param ais_org_text: 原始报文
        :return:
        """
        msg_body = ais_org_text.split(',')[-2]
        msg_ascii = self.ascii_string.str_to_ascii(msg_body)
        if (len(msg_body) != 0) & (ais_org_text[0] == '!'):
            msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
            print(msg_id)
            if (msg_id == 1) | (msg_id == 2) | (msg_id == 3):  # 解析1，2,3类消息
                msg_thr_str = self.ais_org_analysis_dynamic(msg_ascii)
                print msg_thr_str
            elif msg_id == 5:
                msg_static_str = self.ais_org_analysis_static5(msg_ascii)
                print msg_static_str
            input("-----------------------")
    
    def get_ais_org_ys(self):
        """
        获取ais数据原始报文，数据源：洋山
        :return: 返回ais原始报文列表，类型：list
        """
        address = ('192.168.1.124', 16197)
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


def areaID(longitude, latitude, grade=0.1):
    import math
    
    longLen = 360 / grade
    longBase = (longLen - 1) / 2
    laBase = (180 / grade - 1) / 2
    
    if (longitude < 0):
        longArea = longBase - math.floor(abs(longitude / grade))
    else:
        longArea = longBase + math.ceil(longitude / grade)
    
    if (latitude < 0):
        laArea = laBase + math.ceil(abs(latitude / grade))
    else:
        laArea = laBase - math.floor(latitude / grade)
    
    area_ID = longArea + (laArea * longLen)
    return int(area_ID)


def get_ais_org_bm():
    """
    获取博懋ais动态数据
    :return:
    """
    address = ('192.168.1.124', 16198)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(address)
    
    if not os.path.exists('ais_bm_org'):
        os.system('mkdir ais_bm_org')
    if not os.path.exists('ais_bm_thr_org'):
        os.system('mkdir ais_bm_thr_org')
    if not os.path.exists('ais_ys_org'):
        os.system('mkdir ais_ys_org')
    if not os.path.exists('ais_ys_thr_org'):
        os.system('mkdir ais_ys_thr_org')
    
    while True:
        try:
            data, addr = s.recvfrom(2048)
            create_date = time.strftime('%Y%m%d')
            bm_ais_org = open('./ais_bm_org/org_%s.tsv' % create_date, 'a')
            dynamic_thr_org = open('./ais_bm_thr_org/dynamic_%s.tsv' % create_date, 'a')
            static_org = open('./ais_bm_thr_org/static_%s.tsv' % create_date, 'a')
            bm_ais_org.write(data.decode())
            if not data:
                print("client has exist")
                break
            data_analysised = ais_bm_analysis(data.decode())
            
            if data_analysised:
                if data_analysised[0] == 0:
                    dynamic_thr_org.write(data_analysised[1])
                else:
                    static_org.write(data_analysised[1])
        except Exception as e:
            if 'PRIMARY' in str(e):
                pass
            else:
                print(e)


def ais_bm_analysis(ais_bm_text):
    """
    ais博懋数据解析
    :param ais_bm_text: 博懋ais报文，类型：string
    :return: 三阶段表的数据，类型：list[]
    """
    ais_bm_list = ais_bm_text.split('@')
    if ais_bm_list[0] == '0':
        # 解析动态数据
        mmsi = int(ais_bm_list[2])
        acquisition_time = int(ais_bm_list[1])
        target_type = 0
        data_supplier = 246
        data_source = 0
        nav_status = int(ais_bm_list[3])
        longitude = int(float(ais_bm_list[7]) * 1000000)
        latitude = int(float(ais_bm_list[8]) * 1000000)
        area_id = areaID(longitude=longitude, latitude=latitude)
        sog = float(ais_bm_list[5])
        conversion = 0.514444
        cog = float(ais_bm_list[9])
        true_head = float(ais_bm_list[10])
        power = ""
        ext = ""
        extend = str(float(ais_bm_list[4])) + "&" + ais_bm_list[6] + "&&&&"
        
        out_list = [mmsi, acquisition_time, target_type, data_supplier, data_source, nav_status, longitude, latitude,
                    area_id, sog, conversion, cog, true_head, power, ext,
                    extend]
        out_str = ''
        for ele in out_list:
            out_str = out_str + str(ele) + ','
        return 0, out_str[:-1] + '\n'
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
        
        out_list = [mmsi, ship_type, imo, callsign, ship_length, ship_width, pos_type, eta, draught, destination]
        out_str = ''
        for ele in out_list:
            out_str = out_str + str(ele) + ','
        return 1, out_str[:-1] + '\n'


if __name__ == '__main__':
    # 判断参数是否输入完全
    if len(sys.argv) < 2:
        print("参数不足")
        exit(1)
    
    ais_rev = AisMes()
    
    if sys.argv[1] == 'bm':
        get_ais_org_bm()
    elif sys.argv[1] == 'ys':
        print("11111")
        ais_rev.get_ais_org_ys()
