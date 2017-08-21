# coding:utf-8

import pandas as pd

if __name__ == "__main__":
    dstDF = pd.read_csv("/home/qiu/Documents/data/DP-DATA/deviation/"
                        "chinaCoast_ddp_200m_deviation.csv")
    dstDF = dstDF[dstDF["dst2"] != 0]
    dstDF["deviation"] = ((dstDF["dst2"] - dstDF["dst1"]) / dstDF["dst2"]) * 100
    # tmpDF = dstDF[(dstDF["deviation"] > 13) & (dstDF["dst2"] > 10.)]
    # print(tmpDF)
    print("mean deviation = %f" % (1 - (sum(dstDF["dst1"]) / sum(dstDF["dst2"]))))
