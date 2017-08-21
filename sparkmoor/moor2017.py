# coding:utf-8

import numpy as np

from base_func import getDist, getAvgSpeed, point_poly
# from avgSpeed import steadyAvgSpeed

####################################################################
# 获取船舶平稳状态下的平均速度
class steadyAvgSpeed(object):
    def __init__(self):
        self.minSlowAvgSpeed = 5 * 0.514444  # 最小平均速度，单位：节
        self.minInstSpeed = 5 * 0.514444     # 最小瞬时速度，单位：节
        self.minA = 0.514444 * 0.01          # 最小加速度，单位：米/秒² * 0.514444

    # 获取稳定速度
    # 输入参数：sailAISArray -- numpy的矩阵类型，存放一条船的AIS数据
    def getOneShipSteadySpeed(self, sailAISArray):
        # 获取一条船舶AIS数据的长度
        oneShipLen = len(sailAISArray)
        # 初始化平稳状态下的AIS数据索引
        steadyIndexList = []
        if(oneShipLen > 1):  # 若一条船的AIS数据条数大于1条
            # 循环每条AIS数据，循环至倒数第二条
            for aisIndex in range(0, (oneShipLen - 1)):
                # 获取第i条与第i+1条AIS数据之间，地球间两点间距离，单位：千米
                tmpDst = getDist(lon1=(sailAISArray[aisIndex, 5])/1000000.,
                                 lat1=(sailAISArray[aisIndex, 6])/1000000.,
                                 lon2=(sailAISArray[aisIndex + 1, 5])/1000000.,
                                 lat2=(sailAISArray[aisIndex + 1, 6])/1000000.)
                tmpDst = tmpDst * 1000  # 将单位从千米转换为米
                # 第i条与第i+1条AIS数据之间的时间差，单位：秒
                detaTime = sailAISArray[aisIndex + 1, 1] - sailAISArray[aisIndex, 1]
                if (detaTime == 0):  # 若时间差为0，则设置时间差为1s
                    detaTime = detaTime + 1
                # 利用距离除以时间，获取第i行与第i+1行时间的平均速度
                avgSpeed = tmpDst / detaTime
                # 获取第i行与第i+1行的瞬时速度
                preInstSpeed = (sailAISArray[aisIndex, 8]) * 0.001
                nowInstSpeed = (sailAISArray[aisIndex + 1, 8]) * 0.001
                # 获取第i行与第i+1行之间的加速度
                tmpA = preInstSpeed - nowInstSpeed
                detaA = tmpA / detaTime
                # 判断是否在满足平均速度、加速度、瞬时速度条件
                avgSpeedBool = (avgSpeed > self.minSlowAvgSpeed)
                detaABool = (detaA < self.minA)
                instSpeedBool = (preInstSpeed > self.minInstSpeed) & (nowInstSpeed > self.minInstSpeed)

                if(avgSpeedBool & detaABool & instSpeedBool):  # 若满足上述平均速度、加速度、瞬时速度条件。即为平稳状态
                    # 讲第i行与第i+1行的索引进行记录
                    steadyIndexList.append(aisIndex)
                    steadyIndexList.append(aisIndex + 1)
                else:  # 若不满足平均速度、加速度、瞬时速度条件。不做任何处理
                    pass
            # 将平稳状态下的索引去重
            steadyIndexList = list(set(steadyIndexList))

            if(len(steadyIndexList) > 1):  # 若平稳状态的数据条数大于1条
                # 获取对应索引的所有瞬时速度
                steadySpeedList = sailAISArray[steadyIndexList, 8]
                # 求得所有瞬时速度的平均值
                steadySpeed = (sum(steadySpeedList) / len(steadySpeedList)) * 0.001 / 0.514444
                # 返回平均速度数据
                return steadySpeed
            else:
                pass
        else:  # 若船舶的AIS数据仅有1条，不做任何处理
            pass

    # 获取稳定速度
    # 输入参数：sailAISArray -- numpy的矩阵类型，存放一条船的三阶段格式AIS数据
    def shipSteadySpeedThr(self, sailAISArray):
        # 获取一条船舶AIS数据的长度
        oneShipLen = len(sailAISArray)
        # 初始化平稳状态下的AIS数据索引
        steadyIndexList = []
        if (oneShipLen > 1):  # 若一条船的AIS数据条数大于1条
            # 循环每条AIS数据，循环至倒数第二条
            for aisIndex in range(0, (oneShipLen - 1)):
                # 获取第i条与第i+1条AIS数据之间，地球间两点间距离，单位：千米
                tmpDst = getDist(lon1=(sailAISArray[aisIndex, 6]),
                                 lat1=(sailAISArray[aisIndex, 7]),
                                 lon2=(sailAISArray[aisIndex + 1, 6]),
                                 lat2=(sailAISArray[aisIndex + 1, 7]))
                tmpDst = tmpDst * 1000  # 将单位从千米转换为米
                # 第i条与第i+1条AIS数据之间的时间差，单位：秒
                detaTime = int(sailAISArray[aisIndex + 1, 1]) - int(sailAISArray[aisIndex, 1])
                if (detaTime == 0):  # 若时间差为0，则设置时间差为1s
                    detaTime = detaTime + 1
                # 利用距离除以时间，获取第i行与第i+1行时间的平均速度
                avgSpeed = tmpDst / detaTime
                # 获取第i行与第i+1行的瞬时速度
                preInstSpeed = (float(sailAISArray[aisIndex, 9])) * 0.001
                nowInstSpeed = (float(sailAISArray[aisIndex + 1, 9])) * 0.001
                # 获取第i行与第i+1行之间的加速度
                tmpA = preInstSpeed - nowInstSpeed
                detaA = tmpA / detaTime
                # 判断是否在满足平均速度、加速度、瞬时速度条件
                avgSpeedBool = (avgSpeed > self.minSlowAvgSpeed)
                detaABool = (detaA < self.minA)
                instSpeedBool = (preInstSpeed > self.minInstSpeed) & (nowInstSpeed > self.minInstSpeed)

                if (avgSpeedBool & detaABool & instSpeedBool):  # 若满足上述平均速度、加速度、瞬时速度条件。即为平稳状态
                    # 讲第i行与第i+1行的索引进行记录
                    steadyIndexList.append(aisIndex)
                    steadyIndexList.append(aisIndex + 1)
                else:  # 若不满足平均速度、加速度、瞬时速度条件。不做任何处理
                    pass
            # 将平稳状态下的索引去重
            steadyIndexList = list(set(steadyIndexList))
            # 判断是否存在平稳状态下的瞬时速度
            if (len(steadyIndexList) > 1):  # 若平稳状态的数据条数大于1条
                # 获取对应索引的所有瞬时速度
                steadySpeedList = sailAISArray[steadyIndexList, 9]
                steadySpeedList = [float(sog) for sog in steadySpeedList]
                # 求得所有瞬时速度的平均值
                steadySpeed = (sum(steadySpeedList) / len(steadySpeedList)) * 0.001 / 0.514444
                # 返回平均速度数据
                return steadySpeed
            else:  # 若不存在平稳状态下的瞬时速度
                pass
        else:  # 若船舶的AIS数据仅有1条，不做任何处理
            pass

##################################################
# 20170516停泊事件模型类
class moor(object):
    def __init__(self):
        # 实例化平均航速类
        self.steadyAvgSpeed = steadyAvgSpeed()
        # 初始化距离精度 1 -- km; 1000 -- m; 1000000 -- mm;
        self.preision = 1000000.
        # 初始化停泊事件最大位移，单位：毫米。数值200米 * sqrt(2) * 1000
        self.D_DST = 282842.2725
        # 初始化停泊事件最大低速点，单位：毫米/秒
        self.D_SPEED = 100.
        # 初始化判断点港口与停泊事件之间的位置关系距离阈值，单位：千米
        self.moorDst = 25.
        # 初始化判断合并停泊事件条件
        self.mergeDst = 100000.  # 距离阈值，单位：毫米
        self.mergeTime = 30 * 60  # 时间阈值，单位：秒

    # 对三阶段AIS数据求出指定索引范围内的航程
    # 参数输入：shipAISList -- AIS数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引
    def __getSailDst(self, shipAISList, startIndex, endIndex):
        sailDst = 0
        # 获取指定索引内的AIS数据与条数
        tmpShipAISList = shipAISList[startIndex:(endIndex + 1)]
        # 初始化需求航程的经纬度列表
        lonList = []
        latList = []
        # 循环获得经纬度信息
        for line in tmpShipAISList:
            lonList.append(line[6])
            latList.append(line[7])
        # 求出航程
        for index in range((endIndex - startIndex)):
            tmpDst = getDist(lon1=lonList[index], lat1=latList[index],
                             lon2=lonList[index + 1], lat2=latList[index + 1])
            sailDst = sailDst + tmpDst
        # 返回航程，单位：千米
        return sailDst

    # 对三阶段AIS数据形成停泊事件的数据格式
    # 参数输入：shipAISList -- AIS数据；staticDF -- 静态数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引；lastEndIndex -- 上次停泊结束的索引；
    def __convertMoorResult(self, shipAISList, staticDF, startIndex, endIndex, lastEndIndex):
        # 获取停泊事件的输出数据
        begin_time = shipAISList[startIndex][1]   # time of starting nav point
        end_time = shipAISList[endIndex][1]       # time of ending nav point
        # 判断是否存在上一停泊事件
        if(lastEndIndex != 0):  # 若存在上次停泊事件
            # 上一停泊事件结束时间
            last_time = shipAISList[lastEndIndex][1]
            # 获取两个停泊事件之间的间隔时间
            apart_time = begin_time - last_time
            # 获取两个停泊事件之间的航段
            sailArray = np.array(shipAISList[lastEndIndex:(begin_time + 1)])
            # 获取平稳状态下的平均瞬时速度
            avgSpeed = self.steadyAvgSpeed.shipSteadySpeedThr(sailArray)
            # 获取上一停泊结束时间至当前停泊开始时间的静态数据
            tmpStaticDF = staticDF[(staticDF["shipid"] == shipAISList[endIndex][0]) &
                                   (staticDF["time"] >= last_time) &
                                   (staticDF["time"] <= begin_time)]
            # 获取该航段内出现过的吃水深度个数
            draughtList = list(set(tmpStaticDF.iloc[:, 10]))
            draughtLen  = len(draughtList)
            # 若有且仅有一次吃水深度更新
            if (draughtLen >= 2):
                draught = draughtList[-1]
            else:
                draught = None
        else:  # 若不存在上次停泊事件
            apart_time  = None
            avgSpeed    = None
            draught     = None
        mmsi      = shipAISList[endIndex][0]                    # 船舶MMSI
        begin_lon = shipAISList[startIndex][6] * self.preision  # longitude of starting nav point
        begin_lat = shipAISList[startIndex][7] * self.preision  # latitude of starting nav point
        begin_hdg = shipAISList[startIndex][12]                 # true_head of starting nav point
        begin_sog = shipAISList[startIndex][9]                  # sog of starting nav point
        begin_cog = shipAISList[startIndex][11]                 # cog of starting nav point
        end_lon   = shipAISList[endIndex][6] * self.preision    # longitude of ending nav point
        end_lat   = shipAISList[endIndex][7] * self.preision    # latitude of ending nav point
        end_hdg   = shipAISList[endIndex][12]                   # true_head of ending nav point
        end_sog   = shipAISList[endIndex][9]                    # sog of ending nav point
        end_cog   = shipAISList[endIndex][11]                   # cog of ending nav point
        point_num = endIndex - startIndex + 1                   # ais data nums between nav
        zone_id   = shipAISList[endIndex][8]                    # zone_id of ending nav point
        navistate = shipAISList[startIndex + 1][5]              # status of start+1 nav point
        # 判断该停泊事件包含几条AIS数据
        if(point_num == 2):  # 若该停泊事件只由2条AIS数据组成
            # 获取输出数据
            avg_lon = (begin_lon + end_lon) / 2.0
            avg_lat = (begin_lat + end_lat) / 2.0
            avg_hdgMcog = abs(((begin_hdg - begin_cog) + (end_hdg - end_cog)) / 2.0)
            avg_sog = (begin_sog + end_sog) / 2.0
            var_hdg = np.var([begin_hdg, end_hdg])
            var_cog = np.var([begin_cog, end_cog])
            var_sog = np.var([begin_sog, end_sog])
            var_rot = np.var([shipAISList[startIndex][9], shipAISList[endIndex][9]])
            max_sog = max([begin_sog, end_sog])
            maxSog_cog = [begin_cog, end_cog][np.argmax([begin_sog, end_sog])]
            max_rot = max([shipAISList[startIndex][9], shipAISList[endIndex][9]])
        else:
            # 获取输出数据
            tmp_avg_lon, tmp_avg_lat, tmp_avg_hdgMcog, tmp_avg_sog = [], [], [], []
            tmp_var_hdg, tmp_var_cog, tmp_var_sog, tmp_var_rot = [], [], [], []
            for index in range(startIndex, endIndex + 1):
                tmp_avg_lon.append(shipAISList[index][6] * self.preision)
                tmp_avg_lat.append(shipAISList[index][7] * self.preision)
                tmp_avg_hdgMcog.append(abs(shipAISList[index][12] - shipAISList[index][11]))
                tmp_avg_sog.append(shipAISList[index][9])
                tmp_var_hdg.append(shipAISList[index][12])
                tmp_var_cog.append(shipAISList[index][11])
                tmp_var_sog.append(shipAISList[index][9])
                tmp_var_rot.append(shipAISList[index][15])
            # 求出平均值、方差
            avg_lon = np.mean(tmp_avg_lon)
            avg_lat = np.mean(tmp_avg_lat)
            avg_hdgMcog = np.mean(tmp_avg_hdgMcog)
            avg_sog = np.mean(tmp_avg_sog)
            var_hdg = np.var(tmp_var_hdg)
            var_cog = np.var(tmp_var_cog)
            var_sog = np.var(tmp_var_sog)
            var_rot = np.var(tmp_var_rot)
            max_sog = max(tmp_var_sog)
            maxSog_cog = tmp_var_cog[np.argmax(tmp_var_sog)]
            max_rot = max(tmp_var_rot)
        # 返回停泊事件字段
        return [mmsi, begin_time, end_time, apart_time,
                begin_lon, begin_lat, begin_hdg, begin_sog, begin_cog,
                end_lon, end_lat, end_hdg, end_sog, end_cog,
                point_num, avg_lon, avg_lat, var_hdg, var_cog, avg_hdgMcog,
                avg_sog, var_sog, max_sog, maxSog_cog,
                max_rot, var_rot, draught, avgSpeed, zone_id, navistate]

    # 将停泊事件list转换为一个大字符串输出
    # 输入参数：nav_event -- 停泊事件list
    def __getNavStr(self, nav_event):
        if (len(nav_event) != 0):
            try:
                events = nav_event
                row_num = len(nav_event)  # 获取停泊事件的行数
                col_num = len(nav_event[0])  # 获取停泊事件的列数
                temp_events = ""  # 初始化该船舶的所有停泊事件
                for i in range(0, row_num):  # 循环停泊事件列表的每一列
                    if (i == (row_num - 1)):
                        for j in range(0, col_num):  # 循环获取每一列的数据
                            if (j != (col_num - 1)):  # 若不是最后一列，用","分割
                                temp = str(events[i][j]) + ","
                            else:  # 若是最后一行的最后一列，只需要加入数据即可
                                temp = str(events[i][j])
                            temp_events = temp_events + temp
                    else:
                        for j in range(0, col_num):  # 循环获取每一列的数据
                            if (j != (col_num - 1)):  # 若不是最后一列，用","分割
                                temp = str(events[i][j]) + ","
                            else:  # 若不是最后一行的最后一列，只需要加入"\n"
                                temp = str(events[i][j]) + "\n"
                            temp_events = temp_events + temp
                return temp_events
            except Exception as e:
                print('err events')
                print('There is error when outputing nav, error is : %s' % e)

    # 判断当前停泊事件与暂存停泊事件是否需要合并
    # 参数输入：shipAISList -- AIS数据；startIndex -- 停泊开始的索引；
    # endIndex -- 停泊结束的索引；lastEndIndex -- 上次停泊结束的索引；lastStartIndex -- 上次停泊开始的索引；
    def __mergeMoor(self, shipAISList, startIndex, lastEndIndex):
        # 获取上一停泊事件结束时的经纬度
        preTime = shipAISList[lastEndIndex][1]
        preLon  = float(shipAISList[lastEndIndex][6])
        preLat  = float(shipAISList[lastEndIndex][7])
        # 获取当前停泊事件开始时的经纬度
        nowTime = shipAISList[startIndex][1]
        nowLon  = float(shipAISList[startIndex][6])
        nowLat  = float(shipAISList[startIndex][7])

        # 获取停泊事件之间的间隔距离与间隔事件
        apartDst  = getDist(lon1=preLon, lat1=preLat, lon2=nowLon, lat2=nowLat) * self.preision
        apartTime = nowTime - preTime
        # 判断是否满足合并条件
        if((apartDst <= self.mergeDst) | (apartTime <= self.mergeTime)):  # 需要合并
            # 返回值True
            mergeBool = True
        else:  # 不需要合并
            # 返回值False
            mergeBool = False
        return mergeBool

    # 获取停泊事件程序段
    # 输入参数：shipAIS -- sparkRDD分组后的每个元祖
    def moorShip(self, shipAIS, staticDF):
        # 将分组后的AIS数据转换为list
        groupList = list(shipAIS)
        MMSI = groupList[0]         # 船舶MMSI
        # print MMSI
        shipAISList = list(groupList[1])  # 船舶AIS数据
        # shipAISList = np.array(groupList[1])  # 船舶AIS数据
        # 将AIS数据中的str转为整型或浮点型
        for lineAIS in shipAISList:
            lineAIS[1]  = int(lineAIS[1])
            lineAIS[6]  = float(lineAIS[6]) / 1000000.
            lineAIS[7]  = float(lineAIS[7]) / 1000000.
            lineAIS[9]  = int(lineAIS[9])
            lineAIS[11] = int(lineAIS[11])
            lineAIS[12] = int(lineAIS[12])
            lineAIS[15] = int(lineAIS[15].split("&")[0])
        # shipAISList.sort(key=lambda v: v[1])
        # 初始化该船舶形成的最终停泊事件列表，暂存停泊事件索引
        tmpNavBool = False  # 判断是否存在暂存停泊事件
        tmpNavStartIndex = 0
        tmpNavEndIndex = 0
        nav_event = []
        # 获取船舶AIS数据的条数
        aisLen = len(shipAISList)
        # 判断AIS数据是否仅存在一条
        if(aisLen <= 1):  # 若AIS数据只有1条，无法形成停泊事件
            pass
        else:  # 若AIS数据大于1条，找出停泊事件
            # 初始化停泊时间窗口的左窗口
            startIndex = 0
            # 初始化上一条停泊事件的时间与索引
            pre_startIndex = 0
            pre_endIndex = 0
            # 判断停泊时间窗口开启，startIndex为窗口左端
            # startIndex从AIS数据的第一条开始循环，循环制倒数第二条
            while (startIndex < (aisLen - 1)):
                # 初始化窗口右端
                endIndex = startIndex
                # 初始化最大最小经纬度
                maxLon = shipAISList[startIndex][6]
                maxLat = shipAISList[startIndex][7]
                minLon = shipAISList[startIndex][6]
                minLat = shipAISList[startIndex][7]
                # 判断窗口右端是否需要移动
                while(endIndex < (aisLen - 1)):
                    # 获取endIndex 与 endIndex + 1的平均速度
                    tmpDst = getDist(lon1=shipAISList[endIndex][6], lat1=shipAISList[endIndex][7],
                                     lon2=shipAISList[endIndex + 1][6], lat2=shipAISList[endIndex + 1][7])
                    tmpDetaTime = shipAISList[endIndex + 1][1] - shipAISList[endIndex][1]
                    avgSpeed = getAvgSpeed(tmpDst, tmpDetaTime)
                    # 判断平均速度条件是否满足停泊事件的最大低速条件
                    if(avgSpeed < self.D_SPEED):  # 若满足停泊事件的低速阈值条件
                        # 找出次停泊范围内的经纬度极值
                        if maxLon < shipAISList[endIndex + 1][6]:
                            maxLon = shipAISList[endIndex + 1][6]
                        if maxLat < shipAISList[endIndex + 1][7]:
                            maxLat = shipAISList[endIndex + 1][7]
                        if minLon > shipAISList[endIndex + 1][6]:
                            minLon = shipAISList[endIndex + 1][6]
                        if minLat > shipAISList[endIndex + 1][7]:
                            minLat = shipAISList[endIndex + 1][7]
                        # 获取此范围内生成的最大距离
                        maxDst = self.preision * getDist(maxLon, maxLat, minLon, minLat)
                        # 判断是否满足停泊事件的距离阈值条件
                        if(maxDst < self.D_DST):  # 满足距离阈值条件
                            # 满足距离、速度条件，输出数据
                            # 窗口右端向右移动
                            endIndex = endIndex + 1
                            # 特殊处理部分：最后一条仍为停泊事件
                            if(endIndex == (aisLen - 1)):  # 若停泊条件且endIndex为最后一条
                                # 判断是否存在暂存停泊事件
                                if(tmpNavBool):  # 若存在暂存停泊事件
                                    # 判断暂存停泊事件与该停泊事件是否需要合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if(mergeBool):  # 若需要进行合并
                                        # 输出停泊事件，暂存停泊开始至当前停泊结束
                                        outList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=endIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(outList)
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        pre_endIndex = tmpNavEndIndex
                                        # 输出当前停泊事件
                                        outList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                           staticDF=staticDF,
                                                                           startIndex=tmpNavStartIndex,
                                                                           endIndex=tmpNavEndIndex,
                                                                           lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        nav_event.append(outList)
                                    # 清空暂存停泊事件
                                    tmpNavBool = False
                                else:  # 若不存在暂存停泊事件
                                    pass
                                startIndex = endIndex
                                break
                        else:  # 不满足距离阈值条件
                            if(endIndex > startIndex):  # 若已有停泊事件生成
                                # 判断是否存在暂存停泊事件
                                if(tmpNavBool):  # 若存在暂存停泊事件
                                    # 判断是否需要进行合并
                                    mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                                 startIndex=startIndex,
                                                                 lastEndIndex=tmpNavEndIndex)
                                    if(mergeBool):  # 若需要进行合并
                                        tmpNavEndIndex = endIndex
                                    else:  # 若不需要进行合并
                                        # 输出暂存停泊事件
                                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                              staticDF=staticDF,
                                                                              startIndex=tmpNavStartIndex,
                                                                              endIndex=tmpNavEndIndex,
                                                                              lastEndIndex=pre_endIndex)
                                        nav_event.append(tmpOutList)
                                        pre_endIndex = tmpNavEndIndex
                                        tmpNavStartIndex = startIndex
                                        tmpNavEndIndex = endIndex
                                else:  # 若不存在暂存停泊事件
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                                    tmpNavBool = True
                                startIndex = endIndex
                                break
                            else:  # 若没有生成停泊事件
                                startIndex = endIndex + 1
                                break
                    else:  # 若不满足停泊事件低速条件
                        if(endIndex > startIndex): # 若已有停泊事件生成
                            # 判断是否存在暂存停泊事件
                            if (tmpNavBool):  # 若存在暂存停泊事件
                                # 判断是否需要进行合并
                                mergeBool = self.__mergeMoor(shipAISList=shipAISList,
                                                             startIndex=startIndex,
                                                             lastEndIndex=tmpNavEndIndex)
                                if (mergeBool):  # 若需要进行合并
                                    tmpNavEndIndex = endIndex
                                else:  # 若不需要进行合并
                                    # 输出暂存停泊事件
                                    tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                                          staticDF=staticDF,
                                                                          startIndex=tmpNavStartIndex,
                                                                          endIndex=tmpNavEndIndex,
                                                                          lastEndIndex=pre_endIndex)
                                    nav_event.append(tmpOutList)
                                    pre_endIndex = tmpNavEndIndex
                                    tmpNavStartIndex = startIndex
                                    tmpNavEndIndex = endIndex
                            else:  # 若不存在暂存停泊事件
                                tmpNavStartIndex = startIndex
                                tmpNavEndIndex = endIndex
                                tmpNavBool = True
                            startIndex = endIndex
                            break
                        else:  # 若没有产生过停泊事件，即又窗口没有产生过，左窗口向右移动一行
                            startIndex = endIndex + 1
                            break
                # 特殊处理：当右端窗口达到倒数第二条，判断是否存在暂存停泊事件需要输出
                if(endIndex == (aisLen - 2)):
                    # 判断是否存在暂存停泊事件
                    if(tmpNavBool):  # 若存在暂存停泊事件
                        # 输出暂存停泊事件
                        tmpOutList = self.__convertMoorResult(shipAISList=shipAISList,
                                                              staticDF=staticDF,
                                                              startIndex=tmpNavStartIndex,
                                                              endIndex=tmpNavEndIndex,
                                                              lastEndIndex=pre_endIndex)
                        nav_event.append(tmpOutList)
                        tmpNavBool = False
                    else:  # 若不存在暂存停泊事件
                        pass
                    startIndex = endIndex + 1
        moorStr = self.__getNavStr(nav_event)
        return moorStr

    #########################################################################################
    # 判断停泊事件与多边形港口的位置关系
    # 输入参数：polyPortGDF -- 多边形港口数据分组后数据；moorLon -- 停泊事件所在经度
    # moorLat -- 停泊事件所在纬度
    def __moorPoly(self, polyPortGDF, moorLon, moorLat):
        for polyPortGroup in polyPortGDF:
            # 获取"多边形港口"的坐标集合
            portNameStr = list(polyPortGroup)[0]
            aPolyPortDF = list(polyPortGroup)[1]
            aPolyPortCorNum = len(aPolyPortDF)
            aPolyPortCoorList = []
            for aPolyPortDFIndex in range(aPolyPortCorNum):
                tmpPolyPortCorList = [aPolyPortDF.iloc[aPolyPortDFIndex, 0],
                                      aPolyPortDF.iloc[aPolyPortDFIndex, 1]]
                aPolyPortCoorList.append(tmpPolyPortCorList)
            # 求出"多边形港口"的中心坐标点，用平均值来求得
            lonList = [lon[0] for lon in aPolyPortCoorList]
            latList = [lat[1] for lat in aPolyPortCoorList]
            portAvgLon = sum(lonList) / len(lonList)
            portAvgLat = sum(latList) / len(latList)
            aPolyPortCoorArray = np.array(aPolyPortCoorList)
            # 判断停泊事件是否存在于次多边形内
            if (apply(point_poly,
                      (moorLon, moorLat, aPolyPortCoorArray))):
                # 在原来的停泊事件字段内添加港口名称、港口经度、港口纬度数据
                moorPortList = [portNameStr, portAvgLon, portAvgLat]
                navPolyBool = True
                return moorPortList, navPolyBool
            else:  # 不在该多边形内出现
                moorPortList = [None, None, None]
                navPolyBool = False
                return moorPortList, navPolyBool
                pass

    # 判断停泊事件与点港口之间的位置关系
    # 输入参数：pointPortArray -- 点港口数据；moorAreaID -- 停泊事件所在areaID；
    # moorLon -- 停泊事件所在经度；moorLat -- 停泊事件所在纬度
    def __moorPoint(self, pointPortArray, moorAreaID, moorLon, moorLat):
        # 循环每行点港口数据
        for pointPortLine in pointPortArray:
            # 获取该港口信息
            portName = pointPortLine[0]
            portLon  = float(pointPortLine[1])
            portLat  = float(pointPortLine[2])
            # 将临近的areaID按"*"分割
            closePortAreaIDList = pointPortLine[4].split("*")
            # 将临近的areaID转换为int
            closeAreaID = []
            for tmpAreaID in closePortAreaIDList:
                if(tmpAreaID):
                    closeAreaID.append(int(tmpAreaID))
            # 判断停泊事件的areaID是否在该港口临近的areaID列表内
            # 初始化存放所有有可能停泊的港口列表
            closePortList = []
            if(moorAreaID in closeAreaID):  # 若在该港口临近的areaID列表内
                closePortList.append([portName, portLon, portLat])
        # 从有可能停泊的港口列表中找到距离最近的港口
        # 初始化距离最小值，距离最小对应的码头名
        minDst = 999999999.
        minDstPortName = "noPort"
        for closePortData in closePortList:
            # 获取临港口之一的数据
            closePortName = closePortData[0]
            closePortLon  = int(closePortData[1])
            closePortLat  = int(closePortData[2])
            # 求得停泊事件与该港口之间的距离
            tmpDst = getDist(lon1=moorLon, lat1=moorLat, lon2=closePortLon, lat2=closePortLat)
            # 找出距离最小值所在的港口
            if(tmpDst < minDst):  # 若当前距离小于目前的最小距离
                # 更新最小距离值与最小距离所对应的港口
                minDst = tmpDst
                minPortLon = closePortLon
                minPortLat = closePortLat
                minDstPortName = closePortName
        # 判断最小距离所在的码头其距离是否小于给定的阈值
        if (minDst < self.moorDst):  # 若小于给定阈值
            moorPortList  = [minDstPortName, minPortLon, minPortLat]
            moorPointBool = True
            return moorPortList, moorPointBool
        else:  # 若大于给定的距离阈值
            moorPortList  = [None, None, None]
            moorPointBool = False
            return moorPortList, moorPointBool

    # 判断停泊事件与港口数据之间的关系
    # 输入参数：moorRDD -- 停泊事件数据；polyPort -- 多边形港口数据；pointPort -- 点港口数据
    def moorPort(self, moorRDD, polyPortDF, pointPortDF):
        # 初始化停泊事件数据列表
        moorList = []
        # 将moorRDD按行进行分割
        moorRDDList = moorRDD.split("\n")
        # 对多边形港口进行分组
        polyPortGDF = polyPortDF.groupby("portName")
        for moorRDDLine in moorRDDList:
            if(moorRDDLine):
                # 分割每行停泊事件数据
                moorLineList = moorRDDLine.split(",")
                # 获取停泊事件中的平均经纬度数据，areaID
                print moorLineList[0]
                moorAvgLon = float(moorLineList[15]) / 1000000.
                moorAvgLat = float(moorLineList[16]) / 1000000.
                moorAreaID = int(moorLineList[28])
                # 初始化用于判断是否存在于港口内的参数
                moorPolyBool, moorPointBool = False, False
                # 对每个"多边形港口"中的人工标定码头进行判断
                # 判断停泊事件与多边形港口之间的位置关系
                # moorPortList存放所在的多边形港口信息，navPolyBool判断是否在多边形港口内出现过
                moorPortList, moorPolyBool = self.__moorPoly(polyPortGDF = polyPortGDF,
                                                            moorLon=moorAvgLon, moorLat=moorAvgLat)
                # 判断该停泊事件是否在多边形港口内出现过
                if(moorPolyBool):  # 若在多边形内出现，不判断点港口数据
                    pass
                else:  # 若没有在多边形港口内出现过，判断点港口数据
                    # 将点港口数据转换为矩阵
                    pointPortArray = np.array(pointPortDF)
                    # 循环每条点港口数据
                    moorPortList, moorPointBool = self.__moorPoint(pointPortArray=pointPortArray,
                                                                   moorAreaID=moorAreaID,
                                                                   moorLon=moorAvgLon, moorLat=moorAvgLat)
                # 判断该停泊事件是否存在于多边形港口或点港口内
                if(moorPointBool | moorPolyBool):  # 若存在与港口内
                    moorLineList.extend(moorPortList)
                else:
                    moorLineList.extend(moorPortList)
                moorList.append(moorLineList)
        return moorList
