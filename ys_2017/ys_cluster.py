# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler


if __name__ == "__main__":
    data = pd.read_csv('/home/qiu/Documents/ys_ais/201606_paint_ais_opt.csv')
    # data.columns = ["unique_ID", "acquisition_time", "target_type", "data_supplier", "data_source",
    #                 "status", "longitude", "latitude", "area_ID", "speed", "conversion", "cog",
    #                 "true_head", "power", "ext", "extend"]
    # data = data[(data["longitude"] >= 122.10 * 1000000.) & (data["longitude"] <= 122.30 * 1000000.) &
    #             (data["latitude"] >= 30.50 * 1000000.) & (data["latitude"] <= 30.65 * 1000000.)]
    # data.to_csv("/home/qiu/Documents/ys_ais/201606_warning_area_ys.csv", index=None)
    data = data.loc[:, ["unique_ID", "acquisition_time", "longitude", "latitude"]]
    data["longitude"] = data["longitude"] * 1000000.
    data["latitude"] = data["latitude"] * 1000000.
    print(data.head())
    # input("==============")
    X = []
    for index, value in data.iterrows():
        X.append([value["longitude"], value["latitude"]])
    X = np.array(X)
    # #############################################################################
    # Compute DBSCAN
    print("start computing...")
    db = DBSCAN(eps=400, min_samples=6).fit(X)
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    core_samples_mask[db.core_sample_indices_] = True
    labels = db.labels_
    # Number of clusters in labels, ignoring noise if present.
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    print('Estimated number of clusters: %d' % n_clusters_)

    # #############################################################################
    # Plot result
    import matplotlib.pyplot as plt

    # Black removed and is used for noise instead.
    unique_labels = set(labels)
    colors = [plt.cm.Spectral(each)
              for each in np.linspace(0, 1, len(unique_labels))]

    # 初始化主航道内的AIS数据
    main_channel_ais = pd.DataFrame()
    for k, col in zip(unique_labels, colors):
        if k == -1:
            # Black used for noise.
            col = [0, 0, 0, 1]
        class_member_mask = (labels == k)

        if col == (0.61960784313725492, 0.0039215686274509803, 0.25882352941176473, 1.0):
            xy = X[class_member_mask & core_samples_mask]
            main_channel_ais = main_channel_ais.append(data[class_member_mask & core_samples_mask])
            plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
                     markeredgecolor='k', markersize=14)

            xy = X[class_member_mask & ~core_samples_mask]
            main_channel_ais = main_channel_ais.append(data[class_member_mask & ~core_samples_mask])
            plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
                     markeredgecolor='k', markersize=6)
    main_channel_ais.columns = ["unique_ID", "acquisition_time", "longitude", "latitude"]
    main_channel_ais["longitude"] = main_channel_ais["longitude"] / 1000000.
    main_channel_ais["latitude"] = main_channel_ais["latitude"] / 1000000.
    print(len(main_channel_ais))
    main_channel_ais.to_csv("/home/qiu/Documents/ys_ais/main_channel_ais.csv", index=None)
    plt.title('Estimated number of clusters: %d' % n_clusters_)
    plt.show()
