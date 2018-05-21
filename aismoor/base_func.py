# coding:utf-8

import math


def is_in_range(num, min_num, max_num):
    """
    判断是否数据某区间，全闭区间
    :param num: 需要判断的数字
    :param min_num: 下界
    :param max_num: 上界
    :return: T/F
    """
    if min_num <= num <= max_num:
        return True
    else:
        return False


#########################################################
# 获得地球两点间距离
EARTH_RADIUS = 6378.137  # 地球半径，单位千米

def getRadian(x):
    return x * math.pi / 180.0

def getDist(lon1, lat1, lon2, lat2):  # 得到地球两点距离，单位千米
    lon1, lat1 = float(lon1), float(lat1)
    lon2, lat2 = float(lon2), float(lat2)
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
# 判断点是否在给定多边形内
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

#########################################################


class format_convert(object):
    def areaID(self, longitude, latitude, grade=0.1):
        """获取area_id"""
        longLen = 360 / grade
        longBase = (longLen - 1) / 2
        laBase = (180 / grade - 1) / 2

        if (longitude < 0):
            longArea = longBase - math.floor(abs(longitude / grade))
        else:
            longArea = longBase + math.ceil(longitude / grade)

        if (latitude < 0):
            laArea = laBase + math.ceil(abs(latitude / grade))
        else:
            laArea = laBase - math.floor(latitude / grade)

        area_ID = longArea + (laArea * longLen)
        return int(area_ID)

    def bm_to_thr(self, line):
        """博贸AIS数据转三阶段表"""
        if "value" not in line:
            n_spl = line.split('\n')[0]
            description_spt = n_spl.split('\t')
            mmsi_time = description_spt[1].split(' ')
            detail_data = description_spt[3].split('@')

            unique_ID = str(mmsi_time[0])
            acq_time = str(mmsi_time[1])
            target_type = "0"
            data_supplier = "246"
            data_source = "0"
            status = detail_data[0]
            longitude = float(detail_data[4])
            latitude = float(detail_data[5])
            areaID = self.areaID(longitude, latitude)
            speed = detail_data[2]
            conversion = "0.514444"
            cog = str(int(detail_data[6]) * 10)
            true_head = str(int(detail_data[7]) * 100)
            power = ""
            ext = ""
            extend = detail_data[1] + "&" + detail_data[3] + "&&&&"

            outStr = unique_ID + "," + acq_time + "," + target_type + "," + data_supplier + "," + \
                     data_source + "," + status + "," + \
                     str(int(longitude * 1000000.)) + "," + str(int(latitude * 1000000.)) + "," + \
                     str(areaID) + "," + speed + "," + conversion + "," + cog + "," + \
                     true_head + "," + power + "," + ext + "," + extend

            return outStr
        else:
            pass

    def cx_to_thr(self, line):
        """船讯网数据转三阶段"""
        if "longitude" not in line:
            detail_data = line.split("\n")[0].split(",")
            outStr = str(int(detail_data[0])) + "," + detail_data[1] + "," + detail_data[2] + "," + "248" + "," + \
                     "0" + "," + detail_data[4] + "," + detail_data[5] + "," + detail_data[6] + "," + \
                     detail_data[7] + "," + str(int(detail_data[8]) * 0.001 * 0.514444 * 10) + "," + \
                     detail_data[9] + "," + detail_data[10] + "," + \
                     detail_data[11] + "," + detail_data[12] + "," + detail_data[13] + "," + detail_data[14]
            return outStr
        else:
            pass