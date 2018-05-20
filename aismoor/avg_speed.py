# coding:utf-8

from base_func import getDist

####################################################################
# 获取船舶平稳状态下的平均速度
class steadyAvgSpeed(object):
    def __init__(self):
        self.minSlowAvgSpeed = 5 * 0.514444  # 最小平均速度，单位：节
        self.minInstSpeed = 5 * 0.514444     # 最小瞬时速度，单位：节
        self.minA = 0.514444 * 0.01          # 最小加速度，单位：米/秒² * 0.514444

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
                if detaTime == 0:  # 若时间差为0，则设置时间差为1s
                    detaTime = detaTime + 1
                # 利用距离除以时间，获取第i行与第i+1行时间的平均速度
                avgSpeed = tmpDst / detaTime
                # 获取第i行与第i+1行的瞬时速度
                preInstSpeed = (float(sailAISArray[aisIndex, 9]) * 0.514444 * 0.1 * 1000) * 0.001
                nowInstSpeed = (float(sailAISArray[aisIndex + 1, 9]) * 0.514444 * 0.1 * 1000) * 0.001
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
            if len(steadyIndexList) > 1:  # 若平稳状态的数据条数大于1条
                # 获取对应索引的所有瞬时速度
                steadySpeedList = sailAISArray[steadyIndexList, 9]
                steadySpeedList = [float(sog) for sog in steadySpeedList]
                # 求得所有瞬时速度的平均值
                steadySpeed = (sum(steadySpeedList) * 1.0 / len(steadySpeedList)) * 0.1
                # 返回平均速度数据
                return steadySpeed
            else:  # 若不存在平稳状态下的瞬时速度
                pass
        else:  # 若船舶的AIS数据仅有1条，不做任何处理
            pass
