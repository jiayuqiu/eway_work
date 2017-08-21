# coding:utf-8

import pandas as pd

class convert_result(object):

    def __init__(self):
        self.max = 30*60
        self.water_list = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                           "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                           "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                           "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08"]

    # 将同时出现在两个内的停泊事件删除
    def drop_event_in_anchores(self, ship_event):
        ship_event.sort_index(by=['begin_time'])
        uniqueList = []
        uniqueList.append([ship_event.iloc[0, 0], ship_event.iloc[0, 1], ship_event.iloc[0, 2],
                           ship_event.iloc[0, 3], ship_event.iloc[0, 4]])
        length = len(ship_event)
        for index in range(1, length):
            if((ship_event.iloc[index, 2] == ship_event.iloc[index - 1, 2]) and
                   ((ship_event.iloc[index - 1, 0] not in self.water_list) and
                            (ship_event.iloc[index, 0]) not in self.water_list)):
                pass
            else:
                uniqueList.append([ship_event.iloc[index, 0], ship_event.iloc[index, 1], ship_event.iloc[index, 2],
                                   ship_event.iloc[index, 3], ship_event.iloc[index, 4]])
        return uniqueList

    # 将两个停泊事件之间的时间差小于30分钟的合并成为一个停泊事件
    def get_merge(self, eachShipInOneAnchor):
        # eachShipInOneAnchor为一条船在一个范围内的的停泊事件
        # 首先初始化eachShipInOneAnchor的数据条数即获取length
        length = len(eachShipInOneAnchor)
        startIndex = 0
        merged_result = []  # 初始化合并后的数据，初始状态为空
        if(length != 1):
            while (startIndex < (length - 1)):  # 从0开始遍历至倒数第2条数据
                endIndex = startIndex
                while (endIndex < (length - 1)):  # 从0开始遍历至倒数第2条数据
                    beg_t2 = int(eachShipInOneAnchor.iloc[endIndex + 1, 2])  # 下一条数据的开始时间
                    end_t1 = int(eachShipInOneAnchor.iloc[endIndex, 3])  # 当前数据的结束事件
                    if(beg_t2 - end_t1 < self.max):  # 若两条停泊事件的时间间隔相差小于30分钟，进行合并
                        endIndex = endIndex + 1
                        if(endIndex == (length - 1)):  # 若直到最后一条都需要进行合并，从startIndex到endIndex这段都输出
                            anchor = eachShipInOneAnchor.iloc[0, 0]
                            unique_id = eachShipInOneAnchor.iloc[0, 1]
                            begin_time = eachShipInOneAnchor.iloc[startIndex, 2]
                            end_time = eachShipInOneAnchor.iloc[endIndex, 3]
                            interval = int(end_time) - int(begin_time)

                            tmp = [anchor, unique_id, begin_time, end_time, interval]  # 生成合并后的停泊事件
                            merged_result.append(tmp)
                            startIndex = endIndex
                    else:  # 两条停泊事件之间的时间间隔大于30分钟
                        if(endIndex > startIndex):  # 有2条以上的停泊事件需要合并
                            anchor = eachShipInOneAnchor.iloc[0, 0]
                            unique_id = eachShipInOneAnchor.iloc[0, 1]
                            begin_time = eachShipInOneAnchor.iloc[startIndex, 2]
                            end_time = eachShipInOneAnchor.iloc[endIndex, 3]
                            interval = int(end_time) - int(begin_time)

                            tmp = [anchor, unique_id, begin_time, end_time, interval]  # 生成合并后的停泊事件
                            merged_result.append(tmp)
                            startIndex = endIndex + 1  # 循环继续
                            if(startIndex == (length - 1)):  # 若循环到最后一条数据，直接输出该数据
                                tmp = [anchor, unique_id, eachShipInOneAnchor.iloc[-1, 2],
                                       eachShipInOneAnchor.iloc[-1, 3], eachShipInOneAnchor.iloc[-1, 4]]
                                merged_result.append(tmp)
                            break
                        else:  # 没有需要合并的事件，直接输出当前停泊事件
                            tmp = [eachShipInOneAnchor.iloc[startIndex, 0], eachShipInOneAnchor.iloc[startIndex, 1],
                                   eachShipInOneAnchor.iloc[startIndex, 2], eachShipInOneAnchor.iloc[startIndex, 3],
                                   eachShipInOneAnchor.iloc[startIndex, 4]]
                            merged_result.append(tmp)
                            startIndex = endIndex + 1
                            if (startIndex == (length - 1)):  # 若循环到最后一条数据，直接输出该数据
                                tmp = [eachShipInOneAnchor.iloc[-1, 0], eachShipInOneAnchor.iloc[-1, 1],
                                       eachShipInOneAnchor.iloc[-1, 2], eachShipInOneAnchor.iloc[-1, 3],
                                       eachShipInOneAnchor.iloc[-1, 4]]
                                merged_result.append(tmp)
                            break
        else:
            anchor = eachShipInOneAnchor.iloc[0, 0]
            unique_id = eachShipInOneAnchor.iloc[0, 1]
            begin_time = eachShipInOneAnchor.iloc[0, 2]
            end_time = eachShipInOneAnchor.iloc[0, 3]
            interval = int(end_time) - int(begin_time)
            tmp = [anchor, unique_id, begin_time, end_time, interval]
            merged_result.append(tmp)
        return merged_result

    # 将两个停泊事件之间的时间差小于30分钟的合并成为一个停泊事件
    def getMergeABS(self, eachShipInOneAnchor, mergeTime = (24 * 60 * 60)):
        # eachShipInOneAnchor为一条船在一个范围内的的停泊事件
        # 首先初始化eachShipInOneAnchor的数据条数即获取length
        length = len(eachShipInOneAnchor)
        startIndex = 0
        merged_result = []  # 初始化合并后的数据，初始状态为空
        if (length != 1):
            while (startIndex < (length - 1)):  # 从0开始遍历至倒数第2条数据
                endIndex = startIndex
                while (endIndex < (length - 1)):  # 从0开始遍历至倒数第2条数据
                    beg_t2 = int(eachShipInOneAnchor.iloc[endIndex + 1, 2])  # 下一条数据的开始时间
                    end_t1 = int(eachShipInOneAnchor.iloc[endIndex, 3])  # 当前数据的结束事件
                    if ((beg_t2 - end_t1) < mergeTime):  # 若两条停泊事件的时间间隔相差小于30分钟，进行合并
                        endIndex = endIndex + 1
                        if (endIndex == (length - 1)):  # 若直到最后一条都需要进行合并，从startIndex到endIndex这段都输出
                            anchor = eachShipInOneAnchor.iloc[0, 0]
                            unique_id = eachShipInOneAnchor.iloc[0, 1]
                            begin_time = eachShipInOneAnchor.iloc[startIndex, 2]
                            end_time = eachShipInOneAnchor.iloc[endIndex, 3]
                            port_lon = eachShipInOneAnchor.iloc[startIndex, 4]
                            port_lat = eachShipInOneAnchor.iloc[startIndex, 5]

                            tmp = [anchor, unique_id, begin_time, end_time, port_lon, port_lat]  # 生成合并后的停泊事件
                            merged_result.append(tmp)
                            startIndex = endIndex
                    else:  # 两条停泊事件之间的时间间隔大于30分钟
                        if (endIndex > startIndex):  # 有2条以上的停泊事件需要合并
                            anchor = eachShipInOneAnchor.iloc[0, 0]
                            unique_id = eachShipInOneAnchor.iloc[0, 1]
                            begin_time = eachShipInOneAnchor.iloc[startIndex, 2]
                            end_time = eachShipInOneAnchor.iloc[endIndex, 3]
                            port_lon = eachShipInOneAnchor.iloc[startIndex, 4]
                            port_lat = eachShipInOneAnchor.iloc[startIndex, 5]

                            tmp = [anchor, unique_id, begin_time, end_time, port_lon, port_lat]  # 生成合并后的停泊事件
                            merged_result.append(tmp)
                            startIndex = endIndex + 1  # 循环继续
                            if (startIndex == (length - 1)):  # 若循环到最后一条数据，直接输出该数据
                                tmp = [anchor, unique_id, eachShipInOneAnchor.iloc[-1, 2],
                                       eachShipInOneAnchor.iloc[-1, 3], eachShipInOneAnchor.iloc[-1, 4],
                                       eachShipInOneAnchor.iloc[-1, 5]]
                                merged_result.append(tmp)
                            break
                        else:  # 没有需要合并的事件，直接输出当前停泊事件
                            tmp = [eachShipInOneAnchor.iloc[startIndex, 0], eachShipInOneAnchor.iloc[startIndex, 1],
                                   eachShipInOneAnchor.iloc[startIndex, 2], eachShipInOneAnchor.iloc[startIndex, 3],
                                   eachShipInOneAnchor.iloc[startIndex, 4], eachShipInOneAnchor.iloc[startIndex, 5]]
                            merged_result.append(tmp)
                            startIndex = endIndex + 1
                            if (startIndex == (length - 1)):  # 若循环到最后一条数据，直接输出该数据
                                tmp = [eachShipInOneAnchor.iloc[-1, 0], eachShipInOneAnchor.iloc[-1, 1],
                                       eachShipInOneAnchor.iloc[-1, 2], eachShipInOneAnchor.iloc[-1, 3],
                                       eachShipInOneAnchor.iloc[-1, 4], eachShipInOneAnchor.iloc[-1, 5]]
                                merged_result.append(tmp)
                            break
        else:
            anchor = eachShipInOneAnchor.iloc[0, 0]
            unique_id = eachShipInOneAnchor.iloc[0, 1]
            begin_time = eachShipInOneAnchor.iloc[0, 2]
            end_time = eachShipInOneAnchor.iloc[0, 3]
            port_lon = eachShipInOneAnchor.iloc[0, 4]
            port_lat = eachShipInOneAnchor.iloc[0, 5]

            tmp = [anchor, unique_id, begin_time, end_time, port_lon, port_lat]
            merged_result.append(tmp)
        return merged_result

    # 将合并后的停泊事件中的每条船舶在码头与水域范围内的停泊时间分别加总
    def add_stop_time(self, merged_list):
        anchor_time = 0
        port_time = 0

        # uniqueList.sort(key=lambda v: v[2])
        length = len(merged_list)
        for index in range(length):
            if(merged_list[index][0] in self.water_list):
                port_time = port_time + int(merged_list[index][4])
            else:
                anchor_time = anchor_time + int(merged_list[index][4])
                # raw_input("===========================")
        return anchor_time, port_time


if __name__ == "__main__":
    water_list = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                  "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                  "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                  "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08"]
    city_list = ["dalian", "fuzhou", "guangzhou", "lianyungang", "qingdao", "tianjin", "shanghai",
                 "shenzhen", "rizhao", "humen", "yantai", "qinzhou", "quanzhou", "xiamen",
                 "yingkou", "ningbo"]

    anchorFile = open("/home/qiu/文档/201609_bm_allShips_dock_nav_20170306/part-00000", "r")
    anchorList = []
    for line in anchorFile:
        tmp = line.split("[")[1].split("]")[0].split(",")
        tmp_list = [tmp[0].split("'")[1], round(float(tmp[1])), round(float(tmp[2])),
                        round(float(tmp[3])), round(float(tmp[4]))]
        anchorList.append(tmp_list)
    anchorFile.close()
    anchor = pd.DataFrame(anchorList)
    anchor.columns = ["anchor", "unique_ID", "begin_time", "end_time", "interval"]
    # anchor = anchor[anchor["unique_ID"] == 211378810]
    print "anchor length is %s " % len(anchor)

    waterFile = open("/home/qiu/文档/201609_china_allShip_port_nav_20170225/part-00000", "r")
    waterList = []
    for line in waterFile:
        if(line.split("\n")[0]):
            tmp = line.split("\n")[0].split(",")
            tmp_list = [tmp[0], round(float(tmp[1])), round(float(tmp[2])),
                        round(float(tmp[3])), round(float(tmp[4]))]
            waterList.append(tmp_list)
    waterFile.close()
    water = pd.DataFrame(waterList)
    water.columns = ["anchor", "unique_ID", "begin_time", "end_time", "interval"]
    print "water length is %s " % len(water)

    # print anchor.head()
    # raw_input("==================================")

    anchor = anchor.append(water)
    anchor = anchor.sort_index(by=["unique_ID", "begin_time"])
    # anchor = anchor[anchor["unique_ID"] == 211378810]
    print "anchorMerge length is %s " % len(anchor)
    raw_input("==================================")

    grouped_id = anchor.groupby("unique_ID")

    convert_result = convert_result()
    converted_file = open("/home/qiu/文档/converted_bm_201609_allShips_nav_20170307.csv", "w")
    for group_id in grouped_id:
        print group_id[0]
        merged_result = []
        each_ship = convert_result.drop_event_in_anchores(group_id[1])
        each_ship = pd.DataFrame(each_ship)
        each_ship.columns = ["anchor", "unique_ID", "begin_time", "end_time", "interval"]
        grouped_anchor = each_ship.groupby("anchor")
        for group_anchor in grouped_anchor:
            list = convert_result.get_merge(group_anchor[1])
            merged_result.extend(list)
        # print merged_result
        # raw_input("========================================")
        # merged_result_df = pd.DataFrame(merged_result)
        # merged_result_df.columns = ["anchor", "unique_id", "begin_time", "end_time", "interval"]

        for city in city_list:
            city_event = []
            for event in merged_result:
                if (city in event[0]):
                    city_event.append(event)
            if (len(city_event) != 0):
                anchor_time, port_time = convert_result.add_stop_time(city_event)
                outStr = city + "," + str(city_event[0][1]) + "," + str(anchor_time) + "," + str(port_time) + "\n"
                if(anchor_time > port_time):
                    print outStr
                    raw_input("================================================")
                converted_file.write(outStr)
                # raw_input("=========================================")
            else:
                pass
    converted_file.close()


    ##############################################################
    # 测试程序段
    # for group_id in grouped_id:
    #     if(group_id[0] in mmsi_list):
    #         print group_id[0]
    #         merged_result = []
    #         each_ship = convert_result.drop_event_in_anchores(group_id[1])
    #         each_ship = pd.DataFrame(each_ship)
    #         each_ship.columns = ["anchor", "unique_ID", "begin_time", "end_time", "interval"]
    #         grouped_anchor = each_ship.groupby("anchor")
    #         for group_anchor in grouped_anchor:
    #             list = convert_result.get_merge(group_anchor[1])
    #             merged_result.extend(list)
    #
    #         for city in city_list:
    #             city_event = []
    #             for event in merged_result:
    #                 if(city in event[0]):
    #                     city_event.append(event)
    #             if(len(city_event) != 0):
    #                 anchor_time, port_time = convert_result.add_stop_time(city_event, water_list)
    #                 print city
    #                 # print city_event
    #                 outStr = city + "," + str(city_event[0][1]) + "," + str(anchor_time) + "," + str(port_time) + "\n"
    #                 print outStr
    #             else:
    #                 pass
    #         print "=============================================="


