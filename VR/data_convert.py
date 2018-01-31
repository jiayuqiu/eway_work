# coding:utf-8

import numpy as np
import pandas as pd


def convert_moor():
    """
    将停泊时间中，需要传送给VR方的字段抽取出来
    :return:
    """
    moor_df = pd.read_csv("/home/qiu/Documents/moor_data/yangshan_moor.csv", header=None)
    moor_df.columns = ["mmsi", "begin_time", "end_time", "apart_time",
                        "begin_lon", "begin_lat", "begin_hdg", "begin_sog", "begin_cog",
                        "end_lon", "end_lat", "end_hdg", "end_sog", "end_cog",
                        "point_num", "avg_lon", "avg_lat", "var_hdg", "var_cog", "avg_hdgMcog",
                        "avg_sog", "var_sog", "max_sog", "maxSog_cog",
                        "max_rot", "var_rot", "draught", "avgSpeed", "zone_id", "navistate",
                        "nowPortName", "nowPortLon", "nowPortLat"]

    moor_df = moor_df.loc[:, ["mmsi", "begin_time", "end_time", "avg_lon", "avg_lat",
                              "nowPortName", "nowPortLon", "nowPortLat"]]
    moor_df = moor_df[moor_df['nowPortName'] != 'None']
    print(moor_df.head())

if __name__ == "__main__":
    convert_moor()
