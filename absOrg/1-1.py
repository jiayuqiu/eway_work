# coding:UTF-8

"""
1° 对人工标定的"多边形港口"进行判断
    利用跑出的停泊事件中的经纬度信息，判断该事件是否在多边形内；若存在于多边形内，标记并输出。
2° 对非人工标定的"点港口"进行判断
    对没有在"多边形港口"内出现过的停泊事件进行判断，暂定以20公里为半径。若该停泊事件与码头
    的距离小于该距离，输出
"""

import pandas as pd

from base_func import point_poly, getDist


#########################################################################
# 判断停泊事件是否在"多边形港口"、"点港口"内
class navInPort(object):

    # 对"多边形港口"坐标点集的string进行分割
    # 带入参数：polyCoordinateStr -- "多边形港口"的坐标点集

    def __getPolyCoordinateList(self, polyCoordinateStr):
        coordinateList = []
        splitAsteriskList = polyCoordinateStr.split("*")
        for coordinateStr in splitAsteriskList:
            if(coordinateStr):
                splitSemicolonList = coordinateStr.split(";")
                splitSemicolonList[1], splitSemicolonList[0] = splitSemicolonList[0], splitSemicolonList[1]
                coordinateList.append(splitSemicolonList)
        coordinateList = [[float(x) for x in inner] for inner in coordinateList]
        return coordinateList

    # 对"多边形港口"进行判断
    # 带入参数：polyPortGDF -- 对City字段groupby后的"多边形码头"数据
    # 带入参数：navEventsDF -- 停泊事件dataframe

    def pointInPolyPort(self, polyPortGDF, navEventsDF):

        # 初始化码头停泊信息
        navPolyPortList = []

        # 获取停泊事件条数
        navEventsLength = len(navEventsDF)

        # 对每个"多边形港口"中的人工标定码头进行判断
        for polyPortGroup in polyPortGDF:
            # 获取"多边形港口"的坐标集合
            portNameStr = list(polyPortGroup)[0]
            print portNameStr
            aPolyPortDF = list(polyPortGroup)[1]
            aPolyPortCorNum = len(aPolyPortDF)
            aPolyPortCoorList = []
            for aPolyPortDFIndex in range(aPolyPortCorNum):
                tmpPolyPortCorList = [aPolyPortDF.iloc[aPolyPortDFIndex, 0], aPolyPortDF.iloc[aPolyPortDFIndex, 1]]
                aPolyPortCoorList.append(tmpPolyPortCorList)
            print aPolyPortCoorList

            # 求出"多边形港口"的中心坐标点，用平均值来求得
            lonList = [lon[0] for lon in aPolyPortCoorList]
            latList = [lat[1] for lat in aPolyPortCoorList]
            avgLon = sum(lonList)/len(lonList)
            avgLat = sum(latList)/len(latList)

            # 将港口范围坐标的字符串转换为list

            # 循环停泊事件的每一行，找出在该码头内的停泊事件，将开始时间与结束时间信息存入navPortList
            navEventsIndex = 0
            while (navEventsIndex < navEventsLength):
                # 回显停泊事件目前的行索引
                if (navEventsIndex % 10000 == 0):
                    print navEventsIndex

                # 判断该行停泊事件是否在该码头中
                if (apply(point_poly,
                          (navEventsDF.iloc[navEventsIndex, 3] / 1000000.,
                           navEventsDF.iloc[navEventsIndex, 4] / 1000000.,
                           aPolyPortCoorList))):  # 若在码头内，记录
                    # print navEventsDF.iloc[navEventsIndex, 3] / 1000000., navEventsDF.iloc[navEventsIndex, 4] / 1000000.
                    navEventsDF.iloc[navEventsIndex, 8] = True
                    tmpNavPortList = [portNameStr, avgLon, avgLat, navEventsDF.iloc[navEventsIndex, 0],
                                      navEventsDF.iloc[navEventsIndex, 1], navEventsDF.iloc[navEventsIndex, 2],
                                      (navEventsDF.iloc[navEventsIndex, 2] - navEventsDF.iloc[navEventsIndex, 1])]
                    print tmpNavPortList
                    navPolyPortList.append(tmpNavPortList)
                else:
                    pass
                # 索引向后移动一位
                navEventsIndex += 1
            print len(navPolyPortList)
            # raw_input("==============================")
        return navPolyPortList

    # 对"点港口"进行判断
    # 带入参数：pointPortGDF -- 对City字段groupby后的"点码头"数据
    # 带入参数：navEventsDF -- 停泊事件dataframe
    # 带入参数：nav2PortDst -- 停泊事件至"点港口"的最大距离，默认为20公里
    def pointInPointPort(self, pointPortGDF, navEventsDF, navInPortDst = 20.):
        # 获取没有在"多边形港口"内出现过的停泊事件以及条数
        navEventsNotInPolyDF = navEventsDF[navEventsDF["isInPoly"] == False]
        navEventsNotInPolyLength = len(navEventsNotInPolyDF)

        # 初始化在"点港口"内的停泊事件
        navPointPortList = []

        # 对每条停泊事件求得到每个"点港口"的距离，找出距离最小值，初始值设为99999999公里
        for navEventsIndex in range(navEventsNotInPolyLength):
            if (navEventsIndex % 10 == 0):
                print navEventsIndex
            # 获取停泊事件的mmsi，起始与终止之间，开始时的经纬度
            navEventsBeginLon = navEventsNotInPolyDF.iloc[navEventsIndex, 3] / 1000000.
            navEventsBeginLat = navEventsNotInPolyDF.iloc[navEventsIndex, 4] / 1000000.
            navEventsBeginTime = navEventsNotInPolyDF.iloc[navEventsIndex, 1]
            navEventsEndTime = navEventsNotInPolyDF.iloc[navEventsIndex, 2]
            navEventsMMSI = navEventsNotInPolyDF.iloc[navEventsIndex, 0]

            # 初始化距离最小值，距离最小对应的码头名
            minDst = 999999999.
            minPortLon = 0
            minPortLat = 0
            minDstPortName = "noPort"

            # 对单个停泊事件循环每个"点码头"并判断是否在对应码头内
            for pointPortGroup in pointPortGDF:
                # 获取码头经纬度、名称信息
                aPointPortList = list(pointPortGroup)
                aPointPortDF = aPointPortList[1]
                aPointPortName = aPointPortDF.iloc[0, 2]
                aPointPortLon = aPointPortDF.iloc[0, 0]
                aPointPortLat = aPointPortDF.iloc[0, 1]

                # 获取停泊事件与"点码头"之间的距离，单位：千米
                # 若停泊事件的经纬度坐标与码头的经纬度坐标之差的绝对值各相差不到10°，则计算距离
                detaLon = abs(navEventsBeginLon - aPointPortLon)
                detaLat = abs(navEventsBeginLat - aPointPortLat)
                if(((detaLon < 10.0) & (detaLat < 10.0)) | (detaLon > 330.0)):
                    nav2PortDst = getDist(lon1=navEventsBeginLon, lat1=navEventsBeginLat,
                                          lon2=aPointPortLon, lat2=aPointPortLat)

                    # 找出距离最小值所在的码头
                    if(nav2PortDst < minDst):  # 若当前距离小于目前的最小距离
                        # 更新最小距离值与最小距离所对应的码头
                        minDst = nav2PortDst
                        minPortLon = aPointPortLon
                        minPortLat = aPointPortLat
                        minDstPortName = aPointPortName
                    else:  # 若当前距离大于目前的最小距离，不做任何操作，判断下个"点码头"
                        pass
                else:
                    pass

            # 判断最小距离所在的码头其距离是否小于给定的阈值
            if(minDst < navInPortDst):  # 若小于给定阈值
                tmpNavPortList = [minDstPortName, minPortLon, minPortLat,
                                  navEventsMMSI, navEventsBeginTime,
                                  navEventsEndTime, (navEventsEndTime - navEventsBeginTime)]
                navPointPortList.append(tmpNavPortList)
                print tmpNavPortList
                # raw_input("================")
            else:
                pass

        return navPointPortList


if __name__ == "__main__":
    #####################################################################
    # 获取全球码头数据，包括点、多边形。其中：多边形为人工标定，点为徐老师给出数据

    # 获取"多边形港口"信息
    portNameList = ["dalian03", "fuzhou04", "fuzhou05", "guangzhou13", "lianyungang03",
                    "qingdao08", "tianjin06", "shanghai06", "shanghai07", "shanghai08",
                    "shenzhen11", "shenzhen12", "rizhao03", "humen03", "yantai03",
                    "qinzhou02", "quanzhou03", "xiamen06", "yingkou02", "ningbo08",
                    "rotterdam04", "newjersey03", "newyork02", "busan03", "singapore03",
                    "hongkong03"]
    allPortDF = pd.read_csv("/Users/Marcel/Data/merged_ports.csv")
    polyPortDF = pd.read_csv("/Users/Marcel/Data/Asia_anchores.csv")
    polyPortDF = polyPortDF[polyPortDF["anchores_id"].isin(portNameList)]
    polyPortDF.columns = ["longitude", "latitude", "portName"]

    # 获取"点港口"信息
    pointPortDF = pd.read_excel("/Users/Marcel/Data/tblPort.xlsx")
    pointPortDF = pointPortDF.iloc[:, [30, 26, 1]]
    pointPortDF.columns = ["longitude", "latitude", "portName"]

    # 对portName信息进行分组
    polyPortGDF = polyPortDF.groupby("portName")
    pointPortGDF = pointPortDF.groupby("portName")
    print "+--------------------港口码头数据获取完成------------------------+"

    #####################################################################
    # 获取停泊事件，先判断"多边形港口"，再判断"点港口"
    print "开始获取停泊事件"
    navEventsDF = pd.read_csv("/Users/Marcel/Data/allNavEvents/nav_event_700_201609_more_pre.csv")
    # 增加字段，用于判断是否已经在"多边形港口"内
    navEventsDF["isInPoly"] = False
    print navEventsDF.head()
    shipImoDF = pd.read_excel("/Users/Marcel/Downloads/1000 to 4000 TEU Container Carrier List.xlsx")
    staticDF = pd.read_csv("/Users/Marcel/Downloads/unique_static_2016.csv")
    print len(shipImoDF)
    mmsiList = staticDF[staticDF["imo"].isin(shipImoDF.iloc[:, 0])]["mmsi"]
    mmsiList = [(x * 1.0) for x in mmsiList]
    navEventsDF = navEventsDF[navEventsDF["unique_ID"].isin(mmsiList)]
    print len(navEventsDF)
    print "+--------------------停泊事件读取完成---------------------------+"

    # 载入 判断停播事件在港口内的类
    nip = navInPort()

    # 初始化存放结果的list
    navPortList = []

    # 对"多边形港口"进行判断，获取对应的停泊事件，navPortList为结果
    # navPortList的数据格式：column 1 - 对应码头名称，column 2 - 船舶mmsi
    # column 3 - 开始停泊时间，column 4 - 结束停泊时间，column 5 - 停泊总事件
    print "开始判断多边形港口数据"
    navPolyPortList = nip.pointInPolyPort(polyPortGDF=polyPortGDF, navEventsDF=navEventsDF)
    navPortList.extend(navPolyPortList)

    # 对"点港口"进行判断，获取对应的停泊事件，在navPortList上增加数据
    print "开始判断点港口数据"
    navPointPortList = nip.pointInPointPort(pointPortGDF=pointPortGDF, navEventsDF=navEventsDF)
    navPortList.extend(navPointPortList)

    #####################################################################
    # 此段代码在测试使用，用于保存"多边形港口"判断完之后的临时结果
    # navEventsDF.to_csv("navEventsDFPolyPort.csv", index=None)
    tmp = pd.DataFrame(navPortList)
    tmp.columns = ["portName", "portLon", "portLat", "unique_ID", "begin_time", "end_time", "interval"]
    tmp.to_csv("navPortDF.csv", index=None)
    #####################################################################
    print "done!"
