# coding:utf-8

"""合并port, breth, terminal数据"""

import pandas as pd

if __name__ == "__main__":
    port_df = pd.read_csv("/Users/qiujiayu/PycharmProjects/eway_work/sparkmoor/data/port_areaID.csv", header=None)
    breth_df = pd.read_csv("/Users/qiujiayu/PycharmProjects/eway_work/sparkmoor/data/breth_areaID.csv", header=None)
    terminal_df = pd.read_csv("/Users/qiujiayu/PycharmProjects/eway_work/sparkmoor/data/terminal_areaID.csv", header=None)

    port_df.columns = ["Name", "PortID", "longitude", "latitude", "areaID", "CloseAreaID"]
    terminal_df.columns = ["Name", "PortID", "TerminalID", "longitude", "latitude", "areaID", "CloseAreaID"]
    breth_df.columns = ["Name", "PortID", "TerminalID", "BrethID", "longitude", "latitude", "areaID", "CloseAreaID"]

    port_df["TerminalID"] = -1
    port_df["BrethID"] = -1
    terminal_df["BrethID"] = -1

    port_df = port_df.reindex(columns=["Name", "PortID", "TerminalID", "BrethID", "longitude", "latitude", "areaID", "CloseAreaID"])
    terminal_df = terminal_df.reindex(columns=["Name", "PortID", "TerminalID", "BrethID", "longitude", "latitude", "areaID", "CloseAreaID"])

    global_port_df = pd.concat([port_df, terminal_df, breth_df])
    global_port_df.to_csv("/Users/qiujiayu/PycharmProjects/eway_work/sparkmoor/data/GlobalPort.csv", index=None)
