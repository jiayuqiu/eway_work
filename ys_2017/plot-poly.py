#coding:utf-8

import pandas as pd
import matplotlib.pyplot as plt

from ys_test import coordinates_kml, multiple_poly_list
from base_func import point_poly

if __name__ == "__main__":
    kml_path_list = ["/home/qiu/Documents/ys_ais/警戒区进口1.kml",
                     "/home/qiu/Documents/ys_ais/警戒区进口2.kml",
                     "/home/qiu/Documents/ys_ais/警戒区进口3.kml",
                     "/home/qiu/Documents/ys_ais/警戒区进口4.kml",
                     "/home/qiu/Documents/ys_ais/警戒区进口5.kml",
                     "/home/qiu/Documents/ys_ais/警戒区进口6.kml"]
    poly_df = multiple_poly_list(kml_path_list)
    enter_poly_gdf = poly_df.groupby("poly_id")
    print(poly_df.head())

    ais_df = pd.read_csv("/home/qiu/Documents/ys_ais/main_channel_ais.csv")
    # ais_df["longitude"] = ais_df["longitude"] / 1000000.
    # ais_df["latitude"] = ais_df["latitude"] / 1000000.
    # ais_df = ais_df[ais_df["longitude"] <= 122.35]
    print(ais_df.head())

    for poly_id, poly in enter_poly_gdf:
        plt.plot(poly["longitude"], poly["latitude"], "-r")

    plt.plot(ais_df["longitude"], ais_df["latitude"], ".b")
    plt.show()
