#coding:utf-8

import math
import numpy as np

#########################################################
# 获得地球两点间距离
EARTH_RADIUS = 6378.137  # 地球半径，单位千米

def getRadian(x):
    return x * math.pi / 180.0

def getDist(lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
    radLat1 = getRadian(lat1)
    radLat2 = getRadian(lat2)

    a = radLat1 - radLat2
    b = getRadian(lon1) - getRadian(lon2)

    dst = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                                  math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

    dst = dst * EARTH_RADIUS
    dst = round(dst * 100000000) / 100000000

    return dst
##########################################################
# 获得三维空间中点到点的距离,precision:1000000-mm;1000-m;1-km,ed
# W_time为当前阶段的时间权重
def getDist_3d(lon1, lat1, time1, lon2, lat2, time2, W_time, precision = 1000000.):
    dst_2d = getDist(lon1, lat1, lon2, lat2) * precision
    dst_3d = math.sqrt(dst_2d ** 2 + (W_time * (time2 - time1)) ** 2)
    return dst_3d

##########################################################
# 获得向量内既函数段，向量1点乘向量2,ed
def NeiJi(lon1, lat1, time1, lon2, lat2, time2):
    return (lon1 * lon2 + lat1 * lat2 + time1 * time2)

# 获得向量外积函数段，向量1叉乘向量2,ed
def WaiJi(lon1, lat1, time1, lon2, lat2, time2):
    wj_lon  = lat1  * time2 - lat2  * time1
    wj_lat  = time1 * lon2  - time2 * lon1
    wj_time = lon1  * lat2  - lon2  * lat1
    return [wj_lon, wj_lat, wj_time]

##########################################################
# 获取三维空间中，起点与终点连线与经纬度平面所成的夹角的正弦值
def cosVector(x,y):
    if(len(x)!=len(y)):
        print('error input,x and y is not in the same space')
        return
    result1=0.0
    result2=0.0
    result3=0.0
    for i in range(len(x)):
        result1+=x[i]*y[i]   #sum(X*Y)
        result2+=x[i]**2     #sum(X*X)
        result3+=y[i]**2     #sum(Y*Y)
    if ((result2 * result3) ** 0.5 == 0):
        return 0.
    else:
        return (result1 / ((result2 * result3) ** 0.5))  # 结果显示

##########################################################
# 判断点是否在给定多边形内，返回T或F
def point_poly(pointLon, pointLat, polygon):
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while (i < cor):
        if ((((polygon[i, 1] < pointLat) & (polygon[j, 1] >= pointLat))
                 | ((polygon[j, 1] < pointLat) & (polygon[i, 1] >= pointLat)))
                & ((polygon[i, 0] <= pointLon) | (polygon[j, 0] <= pointLon))):
            a = (polygon[i, 0] +
                 (pointLat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if (a < pointLon):
                inside = not inside
        j = i
        i = i + 1

    return inside

# 判断点是否在给定多边形内，返回港口名称
def point_poly_anchor(pointLon, pointLat, polygon):
    cor = len(polygon)
    i = 0
    j = cor - 1
    inside = False
    while (i < cor):
        if ((((polygon[i, 1] < pointLat) & (polygon[j, 1] >= pointLat))
                 | ((polygon[j, 1] < pointLat) & (polygon[i, 1] >= pointLat)))
                & ((polygon[i, 0] <= pointLon) | (polygon[j, 0] <= pointLon))):
            a = (polygon[i, 0] +
                 (pointLat - polygon[i, 1]) / (polygon[j, 1] - polygon[i, 1]) *
                 (polygon[j, 0] - polygon[i, 0]))

            if (a < pointLon):
                inside = not inside
        j = i
        i = i + 1

    if(inside):
        return polygon[1][2]
    else:
        return None
###########################################################
# 判断停泊事件
def get_nav(grouped_ship, PREISION = 1000000.0, D_DST = 282842.712474619, D_SPEED = 100.0):
    nav_event = []  # initialize nav_events
    for group in grouped_ship:
        group_list = list(group)
        group_num = group_list[1]['unique_ID'].count()

        each_ship = []
        # mmsi-0 time-1 target_type-2 data_supplier-3 data_source-4 status -5 longitude-6 latitude -7 area_id-8
        # speed-9 conversion-10 cog-11 true_head-12 power-13 ext-14 extend-15
        each_ship = np.array(group_list[1].iloc[0:group_num])

        temp_nav = []
        if (group_num > 1):
            # print group_num
            # print each_ship[0][0]
            startIndex = 0
            endIndex = 0
            while (startIndex < (group_num - 1)):  # find left, in 0 to n-2
                endIndex = startIndex
                maxLon = each_ship[startIndex][2]  # initialize longitude and latitude
                maxLat = each_ship[startIndex][3]
                minLon = each_ship[startIndex][2]
                minLat = each_ship[startIndex][3]
                while (endIndex < (group_num - 1)):
                    # get average speed of endIndex and endIndex + 1
                    avgSpeed = (
                    PREISION * getDist(each_ship[endIndex][2], each_ship[endIndex][3], each_ship[endIndex + 1][2],
                                       each_ship[endIndex + 1][3])
                    / (each_ship[endIndex + 1][1] - each_ship[endIndex][1]))

                    if (avgSpeed <= D_SPEED):  # satisfy speed factor

                        if maxLon < each_ship[endIndex + 1][2]:  # find max and min of longitude and latitude
                            maxLon = each_ship[endIndex + 1][2]
                        if maxLat < each_ship[endIndex + 1][3]:
                            maxLat = each_ship[endIndex + 1][3]
                        if minLon > each_ship[endIndex + 1][2]:
                            minLon = each_ship[endIndex + 1][2]
                        if minLat > each_ship[endIndex + 1][3]:
                            minLat = each_ship[endIndex + 1][3]

                        maxDst = PREISION * getDist(maxLon, maxLat, minLon, minLat)
                        if (maxDst <= D_DST):  # satisfy distance factor
                            endIndex = endIndex + 1
                            if (endIndex == (group_num - 1)):
                                temp_nav = [
                                    [
                                        each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                        each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                        each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                        (endIndex - startIndex + 1)
                                    ]
                                ]

                                nav_event = nav_event + temp_nav
                                startIndex = endIndex
                                break
                        else:  # do not satisfy distance factor
                            if (endIndex > startIndex):
                                temp_nav = [
                                    [
                                        each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                        each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                        each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                        (endIndex - startIndex + 1)
                                    ]
                                ]

                                nav_event = nav_event + temp_nav
                                startIndex = endIndex
                                break
                            else:
                                startIndex = endIndex + 1
                                break

                    else:  # do not satisfy speed factor
                        if (endIndex > startIndex):
                            temp_nav = [
                                [
                                    each_ship[endIndex][0], each_ship[startIndex][1], each_ship[endIndex][1],
                                    each_ship[startIndex][2] * PREISION, each_ship[startIndex][3] * PREISION,
                                    each_ship[endIndex][2] * PREISION, each_ship[endIndex][3] * PREISION,
                                    (endIndex - startIndex + 1)
                                ]
                            ]

                            nav_event = nav_event + temp_nav
                            startIndex = endIndex
                            break
                        else:
                            startIndex = endIndex + 1
                            break

    return nav_event