# coding:utf-8

import socket
import time
import os
import sys

from ascii_string import AISAnalysis


class AisMes(object):
    def __init__(self, out_path='./'):
        if not os.path.exists('ais_ys_org'):
            os.system('mkdir ais_ys_org')
        if not os.path.exists('ais_ys_thr_org'):
            os.system('mkdir ais_ys_thr_org')
        
        self.out_folder_org = out_path + 'ais_ys_org/'
        self.out_folder_thr = out_path + 'ais_ys_thr_org/'
        self.ascii_string = AISAnalysis()
        
        # # 设置静态数据缓存
        # self.static5_buffer = list()
    
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
        acqusisition_time = int(time.time())
        msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
        repeat_indicator = int(self.ascii_string.python_substring(msg_ascii, 6, 2), 2)
        mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
        nav_status = self.__check_nav_static(int(self.ascii_string.python_substring(msg_ascii, 38, 4), 2))
        rot = int(self.ascii_string.python_substring(msg_ascii, 42, 8), 2)
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
        
        out_list = [msg_id, repeat_indicator, mmsi, nav_status, rot, sog, pos_acc, longitude, latitude, cog, true_head,
                    acqusisition_time, regional_app, spare, raim_flag, communicate_status]
        
        out_str = ''
        for ele in out_list:
            out_str += str(ele) + ','
        return out_str[:-1] + '\n'

    def ais_org_analysis_dynamic18(self, msg_ascii):
        """
        ais原始报文解析，18类消息
        :param msg_ascii: ais原始报文的二进制字符串，类型：string
        :return: 解析后的ais数据
        """
        # try:
        str_index = 0
        acqusisition_time = int(time.time())
        
        msg_id = int(self.ascii_string.python_substring(msg_ascii, str_index, 6), 2)
        str_index += 6
        
        repeat_indicator = int(self.ascii_string.python_substring(msg_ascii, str_index, 2), 2)
        str_index += 2
        
        mmsi = int(self.ascii_string.python_substring(msg_ascii, str_index, 30), 2)
        str_index += 30
        
        backup1 = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 8))
        str_index += 8  # 跳过backup1
        
        sog = int(self.ascii_string.python_substring(msg_ascii, str_index, 10), 2)
        str_index += 10
        
        pos_acc = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        longitude = int(self.ascii_string.python_substring(msg_ascii, str_index, 28), 2)
        str_index += 28
        
        latitude = int(self.ascii_string.python_substring(msg_ascii, str_index, 27), 2)
        str_index += 27
        
        area_id = areaID(longitude=longitude, latitude=latitude)
        
        cog = int(self.ascii_string.python_substring(msg_ascii, str_index, 12), 2)
        str_index += 12
        
        true_head = int(self.ascii_string.python_substring(msg_ascii, str_index, 9), 2)
        str_index += 9
        
        time_stamp = int(time.time())
        str_index += 6  # 跳过timestamp

        backup2 = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 2))
        str_index += 2

        b_singal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        b_display_signal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        b_dsc = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        b_net = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1  # 跳过b_net
        
        b_22 = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1  # 跳过b_22
        
        model_signal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1  # 跳过model_signal
        
        raim_signal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1  # 跳过raim_signal
        
        communicate_signal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1  # 跳过communicate_signal
        communicate_statuc = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 19))
    
        out_list = [msg_id, repeat_indicator, mmsi, backup1, sog, pos_acc, longitude, latitude, cog, true_head,
                    time_stamp, backup2, b_singal, b_display_signal, b_dsc, b_net, b_22, model_signal, raim_signal,
                    communicate_signal, communicate_statuc]
        out_str = ''
        for ele in out_list:
            out_str += str(ele) + ','
        return out_str[:-1] + '\n'
        # except:
        #     pass

    def ais_org_analysis_dynamic19(self, msg_ascii):
        """
        ais原始报文解析，19类消息
        :param msg_ascii: ais原始报文的二进制字符串，类型：string
        :return: 解析后的ais数据
        """
        str_index = 0
        acqusisition_time = int(time.time())

        msg_id = int(self.ascii_string.python_substring(msg_ascii, str_index, 6), 2)
        str_index += 6

        repeat_indicator = int(self.ascii_string.python_substring(msg_ascii, str_index, 2), 2)
        str_index += 2  # 跳过repeat indicator

        mmsi = int(self.ascii_string.python_substring(msg_ascii, str_index, 30), 2)
        str_index += 30

        backup1 = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 8))
        str_index += 8  # 跳过backup1

        sog = self.__check_sog(int(self.ascii_string.python_substring(msg_ascii, str_index, 10), 2))
        str_index += 10

        pos_acc = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1

        longitude = \
            self.__check_longitude(int(self.ascii_string.python_substring(msg_ascii, str_index, 28), 2))
        str_index += 28

        latitude = \
            self.__check_latitude(int(self.ascii_string.python_substring(msg_ascii, str_index, 27), 2))
        str_index += 27

        area_id = areaID(longitude=longitude, latitude=latitude)

        cog = self.__check_cog(int(self.ascii_string.python_substring(msg_ascii, str_index, 12), 2))
        str_index += 12

        true_head = self.__check_ture_head(int(self.ascii_string.python_substring(msg_ascii, str_index, 9), 2))
        str_index += 9
        
        time_stamp = int(time.time())
        str_index += 6  # 跳过timestamp

        backup2 = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 2))
        str_index += 2  # 跳过backup2
        
        ship_name = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 120))
        str_index += 120

        ship_type = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 8))
        str_index += 8
        
        ship_size = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 30))
        str_index += 30
        
        elec_type = int(self.ascii_string.python_substring(msg_ascii, str_index, 4), 2)
        str_index += 4
        
        raim_signal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        dte = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
        
        model_singal = int(self.ascii_string.python_substring(msg_ascii, str_index, 1), 2)
        str_index += 1
    
        out_list = [msg_id, repeat_indicator, mmsi, backup1, sog, pos_acc, longitude, latitude, cog, true_head,
                    time_stamp, backup2, ship_name, ship_type, ship_size, elec_type, raim_signal, dte, model_singal]
        out_str = ''
        for ele in out_list:
            out_str += str(ele) + ','
        return out_str[:-1] + '\n'
    
    def ais_org_analysis_static5(self, msg_ascii):
        """
        解析5类消息，船舶静态信息
        :param msg_ascii: 原始报文
        :return:
        """
        acqusisition_time = int(time.time())
        msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
        repeat_indicator = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 6, 2))
        mmsi = int(self.ascii_string.python_substring(msg_ascii, 8, 30), 2)
        ais_version_indicator = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 38, 2))
        imo_number = int(self.ascii_string.python_substring(msg_ascii, 40, 30), 2)
        call_sign = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 70, 42))
        ship_name = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 112, 120))
        ship_type = int(self.ascii_string.python_substring(msg_ascii, 232, 8), 2)
        ship_size = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 240, 30))
        elec_type = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 270, 4))
        eta = str(int(self.ascii_string.python_substring(msg_ascii, 290, 4), 2)) + \
              str(int(self.ascii_string.python_substring(msg_ascii, 285, 5), 2)) + \
              str(int(self.ascii_string.python_substring(msg_ascii, 280, 5), 2)) + \
              str(int(self.ascii_string.python_substring(msg_ascii, 274, 6), 2))
        draught = int(self.ascii_string.python_substring(msg_ascii, 294, 8), 2)
        destination = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 302, 30))
        dte = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 422, 1))
        spare = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, 423, 1))
        
        out_list = [msg_id, repeat_indicator, mmsi, ais_version_indicator, imo_number, call_sign, ship_name, ship_type,
                    ship_size, elec_type, eta, draught, destination, dte, spare, acqusisition_time]
        out_str = ''
        for ele in out_list:
            out_str += str(ele) + ','
        return out_str[:-1] + '\n'
    
    def ais_org_analysis_static24(self, msg_ascii):
        """
        解析24类消息，船舶静态信息
        :param msg_ascii: 原始报文
        :return:
        """
        if len(msg_ascii) == 160:  # 为A部分24类消息
            str_index = 0
            acqusisition_time = int(time.time())
    
            msg_id = int(self.ascii_string.python_substring(msg_ascii, str_index, 6), 2)
            str_index += 6
    
            repeat_indicator = int(self.ascii_string.python_substring(msg_ascii, str_index, 2), 2)
            str_index += 2  # 跳过repeat indicator
    
            mmsi = int(self.ascii_string.python_substring(msg_ascii, str_index, 30), 2)
            str_index += 30

            part_signal = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 2))
            str_index += 2
            
            name = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 120))
            str_index += 120

            out_list = [msg_id, repeat_indicator, mmsi, part_signal, name, acqusisition_time]
            out_str = ''
            for ele in out_list:
                out_str += str(ele) + ','
            return out_str[:-1] + '\n'
            
        elif len(msg_ascii) == 168:  # 为B部分24类消息
            str_index = 0
            acqusisition_time = int(time.time())
    
            msg_id = int(self.ascii_string.python_substring(msg_ascii, str_index, 6), 2)
            str_index += 6
    
            repeat_indicator = int(self.ascii_string.python_substring(msg_ascii, str_index, 2), 2)
            str_index += 2  # 跳过repeat indicator
    
            mmsi = int(self.ascii_string.python_substring(msg_ascii, str_index, 30), 2)
            str_index += 30
    
            part_signal = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 2))
            str_index += 2
            
            ship_type = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 8))
            str_index += 8
            
            supplier_id = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 42))
            str_index += 42
            
            call_sign = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 42))
            str_index += 42
            
            ship_size = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 30))
            str_index += 30
            
            backup = self.ascii_string.ascii_to_str(self.ascii_string.python_substring(msg_ascii, str_index, 6))
            str_index += 6
    
            out_list = [msg_id, repeat_indicator, mmsi, part_signal, ship_type, supplier_id, call_sign, ship_size,
                        backup, acqusisition_time]
            out_str = ''
            for ele in out_list:
                out_str += str(ele) + ','
            return out_str[:-1] + '\n'
        
    def ais_org_analysis(self, ais_org_text):
        """
        解析ais原始报文
        :param ais_org_text: 原始报文
        :return:
        """
        msg_part_number = int(ais_org_text.split(',')[1])
        msg_part_signal = int(ais_org_text.split(',')[2])
        
        msg_body = ais_org_text.split(',')[5]
        msg_ascii = self.ascii_string.str_to_ascii(msg_body)
        out_str = ""
        
        # 判断是否为5类消息
        if (msg_part_number == 2) & (msg_part_signal == 1):  # 若为5类消息
            ais_org_text_list = ais_org_text.split('\r\n')
            static5_msg_body = ais_org_text_list[0].split(',')[5] + ais_org_text_list[1].split(',')[5]
            msg_id = 5
            msg_ascii_static5 = self.ascii_string.str_to_ascii(static5_msg_body)
            out_str = self.ais_org_analysis_static5(msg_ascii_static5)
            print out_str
        else:
            msg_id = int(self.ascii_string.python_substring(msg_ascii, 0, 6), 2)
            if (msg_id == 1) | (msg_id == 2) | (msg_id == 3):  # 解析1，2,3类消息
                out_str = self.ais_org_analysis_dynamic123(msg_ascii)
            elif msg_id == 18:
                out_str = self.ais_org_analysis_dynamic18(msg_ascii)
            elif msg_id == 19:
                out_str = self.ais_org_analysis_dynamic19(msg_ascii)
            elif msg_id == 24:
                out_str = self.ais_org_analysis_static24(msg_ascii)
                
        if out_str:
            return msg_id, out_str
        else:
            return None, None
                
    def get_ais_org_ys(self):
        """
        获取ais数据原始报文，数据源：洋山
        :return: 返回ais原始报文列表，类型：list
        """
        address = ('192.168.10.100', 16197)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(address)
        
        while True:
            try:
                data, addr = s.recvfrom(2048)
                create_date = time.strftime('%Y-%m-%d')
                ys_ais_org = open(self.out_folder_org + 'org_%s.csv' % create_date, 'a')
                dynamic123_org = open(self.out_folder_thr + 'dynamic123_%s.csv' % create_date, 'a')
                dynamic18_org = open(self.out_folder_thr + 'dynamic18_%s.csv' % create_date, 'a')
                dynamic19_org = open(self.out_folder_thr + 'dynamic19_%s.csv' % create_date, 'a')
                static5_org = open(self.out_folder_thr + 'static5_%s.tsv' % create_date, 'a')
                static24_org = open(self.out_folder_thr + 'static24_%s.tsv' % create_date, 'a')
                # print data.decode()
                ys_ais_org.write(data.decode())
                if not data:
                    print("client has exist")
                    break
                msg_id, ais_analysised = self.ais_org_analysis(data.decode())
                if ais_analysised:
                    if (msg_id == 1) | (msg_id == 2) | (msg_id == 3):
                        dynamic123_org.write(ais_analysised)
                    elif msg_id == 18:
                        dynamic18_org.write(ais_analysised)
                    elif msg_id == 19:
                        dynamic19_org.write(ais_analysised)
                    elif msg_id == 5:
                        static5_org.write(ais_analysised)
                    elif msg_id == 24:
                        static24_org.write(ais_analysised)
            except Exception as e:
                print e
                import traceback
                err_f = open("ys_err_log.txt", 'a')
                err_f.write(str(time.time()) + '\n')
                traceback.print_exc(file=err_f)
                err_f.flush()
                err_f.close()
        s.close()

    def get_ais_org_ys_test(self, text_list):
        """
        获取ais数据原始报文，数据源：洋山
        :return: 返回ais原始报文列表，类型：list
        """
        for text in text_list:
            try:
                # data, addr = s.recvfrom(2048)
                data = text
                create_date = time.strftime('%Y-%m-%d')
                ys_ais_org = open(self.out_folder_org + 'org_%s.csv' % create_date, 'a')
                dynamic123_org = open(self.out_folder_thr + 'dynamic123_%s.csv' % create_date, 'a')
                dynamic18_org = open(self.out_folder_thr + 'dynamic18_%s.csv' % create_date, 'a')
                dynamic19_org = open(self.out_folder_thr + 'dynamic19_%s.csv' % create_date, 'a')
                static5_org = open(self.out_folder_thr + 'static5_%s.tsv' % create_date, 'a')
                static24_org = open(self.out_folder_thr + 'static24_%s.tsv' % create_date, 'a')
                # print data.decode()
                ys_ais_org.write(data.decode())
                if not data:
                    print("client has exist")
                    break
                msg_id, ais_analysised = self.ais_org_analysis(data.decode())
                if ais_analysised:
                    if (msg_id == 1) | (msg_id == 2) | (msg_id == 3):
                        print(ais_analysised)
                    elif msg_id == 18:
                        print(ais_analysised)
                    elif msg_id == 19:
                        print(ais_analysised)
                    elif msg_id == 5:
                        print(ais_analysised)
                    elif msg_id == 24:
                        print(ais_analysised)
            except Exception as e:
                import traceback
                # err_f = open("ys_err_log.txt", 'a')
                # err_f.write(str(time.time()) + '\n')
                print(traceback.print_exc())


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
    address = ('192.168.10.100', 16198)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(address)
    
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
                import traceback
                err_f = open("bm_err_log.txt", 'a')
                print traceback.print_exc()
                traceback.print_exc(file=err_f)
                err_f.flush()
                err_f.close()


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
    
    if not os.path.exists('ais_bm_org'):
        os.system('mkdir ais_bm_org')
    if not os.path.exists('ais_bm_thr_org'):
        os.system('mkdir ais_bm_thr_org')
    
    ais_rev = AisMes()
    
    if sys.argv[1] == 'bm':
        get_ais_org_bm()
    elif sys.argv[1] == 'ys':
        ais_rev.get_ais_org_ys()
