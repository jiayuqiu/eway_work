# coding:utf-8

import pandas as pd
import numpy as np


def emergency_ship_ais(ys_ais, emergency_ship_mmsi):
    """
    获取救援船舶ais数据
    :param ys_ais: 洋山水域范围内ais数据，类型：data frame
    :param emergency_ship_mmsi: 救援船舶mmsi列表，类型：list
    :return: 返回救援船舶
    """
    emergency_ship_ais_df = ys_ais[ys_ais["unique_ID"].isin(emergency_ship_mmsi)]
    return emergency_ship_ais_df


def emergency_ship_newest_ais(emergency_ship_ais):
    """
    获取救援船舶ais数据的最新数据
    :param emergency_ship_ais: 救援船舶ais数据，类型：data frame
    :return: 救援船舶最新一条ais数据，类型：data frame
    """
    emergency_ship_ais = emergency_ship_ais.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    emergency_ship_ais = emergency_ship_ais.sort_values(by=["unique_ID", "acquisition_time"])
    newest_ais = pd.DataFrame(columns=["unique_ID", "acquisition_time", "longitude", "latitude"])
    emergency_ship_ais_gdf = emergency_ship_ais.groupby("unique_ID")
    for key, value in emergency_ship_ais_gdf:
        newest_ais = newest_ais.append(value.iloc[-1, :])
    return newest_ais


if __name__ == "__main__":
    # ys_ais_df = pd.read_csv("/home/qiu/Documents/ys_ais/pre_201606_ys.csv", header=None)
    # ys_ais_df.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                      "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                      "true_head", "power", "ext", "extend"]

    emergency_ship_static_df = pd.read_excel("/home/qiu/Documents/ys_ais/"
                                             "附件一：上海海事局辖区社会应急力量配置情况统计 (辖区汇总表).xlsx",
                                             sheetname="ys_ship")
    print(len(set(emergency_ship_static_df["MMSI"])))

    # emergency_ship_ais_df = emergency_ship_ais(ys_ais_df, emergency_ship_static_df["MMSI"])
    # emergency_ship_ais_df.to_csv("/home/qiu/Documents/ys_ais/emergency_ship_ais.csv", index=None)

    emergency_ship_ais_df = pd.read_csv("/home/qiu/Documents/ys_ais/emergency_ship_ais.csv")
    print(emergency_ship_ais_df.head())
    newest_ais = emergency_ship_newest_ais(emergency_ship_ais_df)
    newest_ais["longitude"] = newest_ais["longitude"] / 1000000.
    newest_ais["latitude"] = newest_ais["latitude"] / 1000000.
    print(newest_ais)
    merge_data = pd.merge(left=emergency_ship_static_df, right=newest_ais, left_on="MMSI", right_on="unique_ID")
    print(merge_data)
    merge_data.to_csv("/home/qiu/Documents/ys_ais/emergency_ship_merge_ais.csv", index=None)
