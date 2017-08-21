# coding:utf-8

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

if __name__ == "__main__":
    MMSIList = [413376760]
    data = pd.read_csv("/home/qiu/Documents/data/DP-DATA/中国沿海AIS/"
                       "201609_chinaCoast.csv")
    data = data.loc[:, ['unique_ID', 'acquisition_time', 'longitude', 'latitude']]
    data = data[data["unique_ID"].isin(MMSIList)]
    data = data[(data["latitude"] < 36.5 * 1000000.) & (data["acquisition_time"] <= 1472691018)]
    data = data.sort_values(by=["unique_ID", "acquisition_time"])
    # data.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    data["longitude"] = data["longitude"] / 1000000.0
    data["latitude"] = data["latitude"] / 1000000.0
    print(len(data))

    X1 = np.array(data["longitude"])
    Y1 = np.array(data["latitude"])
    Z1 = np.array(data["acquisition_time"])
    ###########################################################################
    dataCompressDF = pd.read_csv("/home/qiu/Documents/data/DP-DATA/QDPOPT/"
                                 "413376760_qdpopt_50m_area.csv")
    dataCompressDF = dataCompressDF.sort_values(by=["unique_ID", "acquisition_time"])
    X2 = np.array(dataCompressDF["longitude"])
    Y2 = np.array(dataCompressDF["latitude"])
    Z2 = np.array(dataCompressDF["acquisition_time"])
    print(len(dataCompressDF))

    fig = plt.figure()
    ax = fig.gca(projection='3d')
    ax.plot(X1, Y1, Z1, "b-o")
    ax.plot(X2, Y2, Z2, "r-x")
    ax.legend()
    plt.show()
