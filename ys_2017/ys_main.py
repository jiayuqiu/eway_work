# coding:utf-8

import time

from ais_analysis import AIS
from port import Port
from emergency import Emergency
from bridge import Bridge, BridgeOPT
from traffic import Traffic

from base_func import get_ship_static_mysql


if __name__ == "__main__":
    while True:
        # 读取船舶静态数据
        ship_static_df = get_ship_static_mysql()
        # print(ship_static_df.columns)
        # input("-----------------------")

        # 获取最新10分钟的ais数据
        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()

        # 东海大桥
        # try:
        bridge = BridgeOPT()
        bridge.bridge_main(ys_ais=ys_ais_10mins, ship_static_df=ship_static_df)
        # except Exception as e:
        #     print(e)
        #     print('东海大桥功能发生错误！！！')
        #     print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        # 应急决策
        try:
            emergency = Emergency()
            emergency.emergency_main(ys_ais_10mins=ys_ais_10mins)
        except Exception as e:
            print(e)
            print('应急决策功能发生错误！！！')
            print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        # 交通预警
        try:
            traffic = Traffic()
            traffic.traffic_main(ys_ais=ys_ais_10mins, ship_static_df=ship_static_df)
        except Exception as e:
            print(e)
            print('交通预警功能发生错误！！！')
            print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        print(time.strftime('%Y-%m-%d %H:%M:%S'))
        print('----------------------------------')
        time.sleep(300)
