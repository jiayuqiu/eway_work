# coding:utf-8

import pandas as pd
import numpy as np

class chittaFunc(object):
    # 初始化参数
    def __init__(self):
        self.portLon = 91.8086  # 初始化港口经度
        self.portLat = 22.2247  # 初始化港口纬度
        self.maxPortSpeed = 2 * 0.514444  # 初始化判断在港的最大速度

    # 根据停泊事件，获取在吉大港停泊过的AIS数据
    # 输入参数：navEventsDF -- ABS结果，有船舶的停泊码头、开始时间、结束时间、船舶MMSI
    # 输入参数：allShipAIS -- 1000-4000TEU集装箱船舶的所有AIS动态数据
    def chittaShipAIS(self, navEventsDF, allShipAISDF):
        pass

    # 获取入港出港时间
    # 做法：与港口点坐标距离小于25公里的停泊事件，若速度小于2节则视为在港口内
    # 输入参数：shipAISDF--一条船舶的AIS动态数据，类型dataframe
    def enterDepartTime(self, oneShipAISDF):
        pass

    # 根据停泊事件获取锚地内的停泊事件
    # 输入参数：navEventsDF -- ABS停泊结果
    # 输入参数：anchorCoorList -- 锚地的多边形坐标
    def anchorNav(self, navEventsDF, anchorCoorList):
        pass

    # 根据停泊事件获取在集装箱码头内的停泊事件
    # 输入参数：navEventsDF -- ABS停泊结果
    # 输入参数：dockCoorList -- 码头的多边形坐标
    def dockNav(self, navEventsDF, dockCoorList):
        pass

def chittaTable(chittaSailRouteData):
    print chittaSailRouteData.head()
