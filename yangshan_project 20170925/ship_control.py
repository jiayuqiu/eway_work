# coding:utf-8

import time
import pandas as pd
import numpy as np
import re

from zsqxj import html_weather_predict as zs_weather

zsqxj_url = r'http://www.zs121.com.cn/weatherforecast/ShortTimeForecast.aspx'

#################################################


def isInRange(num, min, max):
    if(num >= min and num <= max):
        return True
    else:
        return False

###############################################

# 将风速转换为风力等级


def convert_speed2Level(wind_speed):
    if(isInRange(wind_speed, 0.0, 0.2)):
        return int(0)
    if (isInRange(wind_speed, 0.3, 1.6)):
        return int(1)
    if (isInRange(wind_speed, 1.7, 3.4)):
        return int(2)
    if (isInRange(wind_speed, 3.5, 5.5)):
        return int(3)
    if (isInRange(wind_speed, 5.6, 8.0)):
        return int(4)
    if (isInRange(wind_speed, 8.1, 10.8)):
        return int(5)
    if (isInRange(wind_speed, 10.9, 13.9)):
        return int(6)
    if (isInRange(wind_speed, 14.0, 17.2)):
        return int(7)
    if (isInRange(wind_speed, 17.3, 20.8)):
        return int(8)
    if (isInRange(wind_speed, 20.9, 24.5)):
        return int(9)
    if (isInRange(wind_speed, 24.6, 28.5)):
        return int(10)
    if (isInRange(wind_speed, 28.6, 32.6)):
        return int(11)
    if (isInRange(wind_speed, 32.7, 36.9)):
        return int(12)
    if (isInRange(wind_speed, 37.0, 41.4)):
        return int(13)
    if (isInRange(wind_speed, 41.5, 9999.99)):
        return int(14)


###############################################

# 从列表中获取风力等级，返回一个列表，元素为元祖


def get_wind_from_list(tmp_list):
    wind_list = []
    for aw in tmp_list:
        aw_list = aw.split(u'-')
        if(len(aw_list) == 1):
            wind_list.append(int(aw_list[0]))
        else:
            wind_list.append(int(aw_list[1]))

    return wind_list

#########################################################

# 从string中获取风力等级


def wind_in_str(str):
    str = unicode(str, 'utf-8')
    pattern = re.compile(u'风(.*?)级', re.S)
    items = re.findall(pattern, str)

    return items

#########################################################

# 转换码头属性表的格式，方便数据处理

def convert_port_file(port_df):
    id_list = []

    for x in port_df['port']:
        if (unicode(x, 'utf-8') == u'洋山LNG'):
            id_list.append(unicode(x, 'utf-8'))
        else:
            pattern = re.compile(u'洋山(.*?)期(.*?)泊', re.S)
            items = re.findall(pattern=pattern, string=unicode(x, 'utf-8'))
            if (len(items) != 0):
                port_conver = u'洋' + items[0][0] + items[0][1]
                id_list.append(port_conver)
            else:
                id_list.append("0")

    return id_list

#######################################################

# 匹配船舶的静态数据与码头属性

def match_ship_port(index, ship_df, port_df, ship_dwt_df):
    ship_dwt = int(ship_df.loc[index, 'dwt'])
    ship_length = int(ship_df.loc[index, 'length'])
    ship_ioTime = ship_df.loc[index, 'io_time']
    ship_type = (ship_df.loc[index, 'type'])

    ship_dwt_df_len = len(ship_dwt_df['DWT'])

    ship_DWT=0

    for i in range(ship_dwt_df_len):
        max_len = int(ship_dwt_df.iloc[i, 1])
        min_len = int(ship_dwt_df.iloc[i, 2])
        dwt_type = ship_dwt_df.iloc[i, 4].encode('utf-8')

        if(isInRange(ship_length, int(min_len), int(max_len)) and ship_type == dwt_type):
            ship_DWT = ship_dwt_df.iloc[i, 0]

    if(ship_DWT == 0):
        ship_DWT = 150000

    # 找到对应的需要停靠的码头属性

    now_port_list = []
    now_port_str = ship_df.loc[index, 'port']
    # raw_input("==================1====================")

    tmp_list = now_port_str.split('/')

    if(len(tmp_list) > 1):
        mode = re.compile(r'\d+')
        num_list = re.findall(mode, now_port_str)
        for num in num_list:
            if(u'洋一' in now_port_str):
                tmp_str = u'洋一' + num
                now_port_list.append(tmp_str)
            elif(u'洋二' in now_port_str):
                tmp_str = u'洋二' + num
                now_port_list.append(tmp_str)
            elif(u'洋三' in now_port_str):
                tmp_str = u'洋三' + num
                now_port_list.append(tmp_str)
    else:
        now_port_list.append(now_port_str)
    # print now_port_list
    # raw_input("==================2====================")

    if_io = ''
    for port in now_port_list:
        tmp_port = port_df[port_df['id'] == port]
        if (len(tmp_port) > 0):

            if (u'万' in tmp_port.iloc[0, 1]):
                tmp_dwt = tmp_port.iloc[0, 1].split(u'万')[0].split('-')
                if (len(tmp_dwt) != 1):
                    tmp_min_dwt = int(tmp_dwt[0]) * 10000
                    tmp_max_dwt = int(tmp_dwt[1]) * 10000
                else:
                    tmp_min_dwt = int(tmp_dwt[0])
                    tmp_max_dwt = int(tmp_dwt[0])
            else:
                tmp_dwt = tmp_port.iloc[0, 1].split('-')

                if (len(tmp_dwt) != 1):
                    tmp_min_dwt = int(tmp_dwt[0])
                    tmp_max_dwt = int(tmp_dwt[1])
                else:
                    tmp_min_dwt = int(tmp_dwt[0])
                    tmp_max_dwt = int(tmp_dwt[0])
            # print tmp_min_dwt, tmp_max_dwt
            # raw_input("=============1==================")

            # 判断船舶dwt与length是否满足码头的规格

            tmp_port_type = tmp_port.iloc[0, 2]

            print ship_DWT

            if (isInRange(ship_DWT, tmp_min_dwt, tmp_max_dwt) and
                    (ship_type == tmp_port_type)):
                if_io = True
            else:
                if_io = False
        else:
            if_io = False

    if if_io:
        reason = ''
        return reason, if_io, ship_DWT
    else:
        reason = '船舶属性与码头不符'
        return reason, if_io, ship_DWT


##########################################################

# 匹配当前的天气数据

def match_weather(index, ship_df, parameter_df, max_wind_morning, min_njd_morning,
                  max_wind_afthernoon, min_njd_afthernoon):

    ship_type = (ship_df.loc[index, 'type'])
    ship_agst_wind_level = int(parameter_df[parameter_df['type'] == ship_type]['wind'])
    ship_min_njd = int(parameter_df[parameter_df['type'] == ship_type]['njd'])
    ship_ioTime = ship_df.loc[index, 'io_time']
    ship_ioTime_clock = int(ship_ioTime.split('/')[1])

    # 开始对比天气情况

    if(ship_ioTime_clock < 1200):
        wind_level = max_wind_morning
        njd = min_njd_morning

        if (wind_level < ship_agst_wind_level):
            wind_reason = ''
            wind_suit = True
        else:
            wind_reason = '风力条件不符'
            wind_suit = False

        if (njd > ship_min_njd):
            njd_reason = ''
            njd_suit = True
        else:
            njd_reason = '上午能见度条件不符'
            njd_suit = False
    elif(ship_ioTime_clock >= 1200):
        wind_level = max_wind_afthernoon
        njd = min_njd_afthernoon

        if (wind_level < ship_agst_wind_level):
            wind_reason = ''
            wind_suit = True
        else:
            wind_reason = '风力条件不符'
            wind_suit = False

        if (njd > ship_min_njd):
            njd_reason = ''
            njd_suit = True
        else:
            njd_reason = '能见度条件不符'
            njd_suit = False

    return wind_reason, njd_reason, (wind_suit and njd_suit)

# 对船舶的净空高度进行匹配


def match_height(index, ship_df, parameter_df,max_water_height_morning, max_water_height_afternoon,
                 max_height):
    ship_type = ship_df.loc[index, 'type']
    ship_agst_wind_level = int(parameter_df[parameter_df['type'] == ship_type]['wind'])
    ship_min_njd = int(parameter_df[parameter_df['type'] == ship_type]['njd'])
    ship_height = int(ship_df.loc[index, 'height'])
    ship_ioTime = ship_df.loc[index, 'io_time']
    ship_ioTime_clock = int(ship_ioTime.split('/')[1])

    if (ship_ioTime_clock < 1200):
        water_height = max_water_height_morning

        if(water_height + ship_height < max_height):
            height_reason = ""
            return height_reason, True
        else:
            height_reason = "净空高度超过规定"
            return height_reason, False

    elif(ship_ioTime_clock >= 1200):
        water_height = max_water_height_afternoon

        if (water_height + ship_height < max_height):
            height_reason = ""
            return height_reason, True
        else:
            height_reason = "净空高度超过规定"
            return height_reason, False

# 对在泊船舶判断是否符合靠泊条件


def match_stoping_wind(index, ship_df, parameter_df, max_wind_morning, max_wind_afternoon):
    ship_type = ship_df.loc[index, 'type']
    ship_agst_var_wind_level = int(parameter_df[parameter_df['type'] == ship_type]['var_wind'])


    if(ship_agst_var_wind_level > max_wind_morning):
        stop_reason_morning = ""
        wind_bool_morning = True
    else:
        stop_reason_morning = "该船上午不满足停靠条件，请快速撤离"
        wind_bool_morning = False


    if (ship_agst_var_wind_level > max_wind_afternoon):
        stop_reason_afternoon = ""
        wind_bool_afternoon = True

    if (ship_agst_var_wind_level < max_wind_afternoon):
        stop_reason_afternoon = "该船下午不满足停靠条件，请快速撤离"
        wind_bool_afternoon = False

    return stop_reason_morning, stop_reason_afternoon, (wind_bool_morning and wind_bool_afternoon)


if __name__ == "__main__":

    ####################################################

    # 获取靠离泊计划表与码头属性表

    port = pd.read_csv("./data/port_details.csv")
    plan = pd.read_csv("./data/ship_in_out_plan.csv")
    parameter = pd.read_csv("./data/parameter.csv")

    ####################################################

    # 从码头属性表中找出有用的数据，包括长度与设计吨数

    # 将码头属性表的数据格式转换
    # 将名字变为“洋（一/二/三）（1,2,3）”，文字数字为X期，数字为X号泊位

    port['id'] = convert_port_file(port)
    converted_port = port.loc[:, ['id', 'length', 'front_dwt', 'type']]

    ####################################################

    ### 从计划表中循环每一天数据，判断是否满足靠离泊条件

    converted_plan = plan.loc[:, ['io_time', 'port', 'length', 'dwt',
                                  'name', 'chineseName', 'imo', 'type']]

    index = 0
    plan_num = len(converted_plan['io_time'])

    # 获取目前的天气数据

    zs_weather_df, zs_publish_time = zs_weather(zsqxj_url)

    # 初始化if_suit字段，值为T或者F

    if_suit = []

    for index in range(plan_num):

        # 获取当索引为index时的船舶数据

        static_bool = \
            match_ship_port(index=index, ship_df=converted_plan, port_df=converted_port,
                            parameter_df=parameter)


        weather_bool = match_weather(index=index,
                                     ship_df=converted_plan,
                                     parameter_df=parameter,
                                     zs_weather_df=zs_weather_df,
                                     zs_publish_time=zs_publish_time)

        if_suit.append(static_bool and weather_bool)

    plan['if_suit'] = if_suit
    plan.to_csv("./data/port_details_suit.csv", index=None)
