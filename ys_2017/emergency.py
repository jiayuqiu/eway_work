# coding:utf-8

import pandas as pd
import numpy as np
import pymysql
import time

from ais_analysis import AIS


class Emergency(object):
    def __init__(self):
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()
        cur.execute("SELECT * FROM emergency_ship")
        self.emergency_ship_mmsi = []
        for row_emergency_ship in cur.fetchall():
            self.emergency_ship_mmsi.append(row_emergency_ship[1])

    def get_emergency_mmsi(self):
        """
        获取应急船舶的mmsi
        :return:
        """
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()
        cur.execute("SELECT * FROM emergency_ship")


    def emergency_ship_ais(self, ys_ais):
        """
        获取救援船舶ais数据
        :param ys_ais: 洋山水域范围内ais数据，类型：data frame
        :param emergency_ship_mmsi: 救援船舶mmsi列表，类型：list
        :return: 返回救援船舶
        """
        emergency_ship_ais_df = ys_ais[ys_ais["unique_ID"].isin(self.emergency_ship_mmsi)]
        return emergency_ship_ais_df

    def emergency_ship_newest_ais(self, ys_ais):
        """
        获取救援船舶ais数据的最新数据
        :param ys_ais: 洋山水域ais数据，类型：data frame
        :return: 救援船舶最新一条ais数据，类型：data frame
        """
        emergency_ship_ais_df = self.emergency_ship_ais(ys_ais=ys_ais)
        newest_ais = pd.DataFrame(columns=["unique_ID", "acquisition_time", "longitude", "latitude"])
        emergency_ship_ais_gdf = emergency_ship_ais_df.groupby("unique_ID")
        for key, value in emergency_ship_ais_gdf:
            newest_ais = newest_ais.append(value.iloc[-1, :])
        return newest_ais

    def input_emergency_mysql(self, newest_ais):
        """
        将最新的应急船舶ais数据入库
        :param newest_ais: 最新的应急船舶ais数据
        :return: T/F
        """
        # 链接数据库
        conn = pymysql.connect(host='192.168.1.63', port=3306, user='root', passwd='traffic170910@0!7!@#3@1',
                               db='dbtraffic')
        cur = conn.cursor()

        # 更新应急船舶位置信息
        newest_ais_array = np.array(newest_ais)
        for row_newest_ais in newest_ais_array:
            create_time = time.strftime("%Y-%m-%d %H:%M:%S")
            update_sql = """
                         UPDATE emergency_ship SET longitude='%f', latitude='%f', create_time='%s' WHERE mmsi='%d'
                         """ % (row_newest_ais[2], row_newest_ais[3], create_time, row_newest_ais[0])
            cur.execute(update_sql)
        conn.commit()
        cur.close()
        conn.close()


    def emergency_main(self, ys_ais_10mins):
        """
        应急决策主函数
        """
        newest_ais_df = self.emergency_ship_newest_ais(ys_ais=ys_ais_10mins)
        print(newest_ais_df)
        self.input_emergency_mysql(newest_ais=newest_ais_df)


if __name__ == "__main__":

    while True:
        ais = AIS()
        ys_ais_10mins = ais.load_newest_ais()

        # ------------------------------------------------------
        # 应急决策
        emergency = Emergency()
        emergency.emergency_main(ys_ais_10mins=ys_ais_10mins)
        print(time.strftime('%Y-%m-%d %H:%M:%S'))
        print("--------------------------------------")
        time.sleep(600)





    # # ys_ais_df = pd.read_csv("/home/qiu/Documents/ys_ais/pre_201606_ys.csv", header=None)
    # # ys_ais_df.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    # #                      "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    # #                      "true_head", "power", "ext", "extend"]
    #
    # emergency_ship_static_df = pd.read_excel("/home/qiu/Documents/ys_ais/"
    #                                          "附件一：上海海事局辖区社会应急力量配置情况统计 (辖区汇总表).xlsx",
    #                                          sheetname="ys_ship")
    # print(len(set(emergency_ship_static_df["MMSI"])))
    #
    # # emergency_ship_ais_df = emergency_ship_ais(ys_ais_df, emergency_ship_static_df["MMSI"])
    # # emergency_ship_ais_df.to_csv("/home/qiu/Documents/ys_ais/emergency_ship_ais.csv", index=None)
    #
    # emergency_ship_ais_df = pd.read_csv("/home/qiu/Documents/ys_ais/emergency_ship_ais.csv")
    # print(emergency_ship_ais_df.head())
    # newest_ais = emergency_ship_newest_ais(emergency_ship_ais_df)
    # newest_ais["longitude"] = newest_ais["longitude"] / 1000000.
    # newest_ais["latitude"] = newest_ais["latitude"] / 1000000.
    # print(newest_ais)
    # merge_data = pd.merge(left=emergency_ship_static_df, right=newest_ais, left_on="MMSI", right_on="unique_ID")
    # print(merge_data)
    # merge_data.to_csv("/home/qiu/Documents/ys_ais/emergency_ship_merge_ais.csv", index=None)
