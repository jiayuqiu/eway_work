# coding:utf-8

import pandas as pd
import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf8')

def input_portWeather(inputTime, wind, njd):
    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        print '启动成功'
    except:
        print "启动失败"

    try:
        cur.execute("insert into portWeather(inPutTime, wind, njd) \
                      VALUE('%s', '%f', '%d')" % \
                    (inputTime, float(wind), int(njd)))
        conn.commit()
        print '数据已经导入'
    except Exception as e:
        print '数据导入出错'
        print e
        conn.rollback()

    conn.close()
    print '数据库关闭'


def input_reports(report_str):
    print "正在导入气象报告，REPORT"

    report_str = report_str.encode('utf-8')

    data_list = report_str.split('\n')[0].split(';')
    # for x in data_list:
    #     print x
    #     raw_input("=================================")
    # print data_list

    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        print '启动成功'
    except:
        print "启动失败"

    try:
        cur.execute("insert into REPORT(pubDate, pubClock, report, njd, srcLoc, srcUrl) \
                      VALUE('%d', '%d', '%s', '%d', '%s','%s')" % \
                    (int(data_list[0]), int(data_list[1]), data_list[2].encode('utf-8'),
                     int(data_list[3]), data_list[5].encode('utf-8'),
                     data_list[4].encode('utf-8')))
        conn.commit()
        print '数据已经导入'
    except Exception as e:
        print '数据导入出错'
        print e
        conn.rollback()

    conn.close()
    print '数据库关闭'

# 将从气象报告中获取到的风力数据存入数据库表


def input_weather_data(pubDatei, pubClocki, srcLoci, maxAvgWindi, maxZfWindi,
                       njdi, suggestWarni, suggestionNJDWarni):
    print "正在导入天气数值数据，weatherData"

    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        print '启动成功'
    except:
        print "启动失败"

    try:
        insertStr = """
                    insert into weatherData(pubDate, pubClock, srcLoc, maxAvgWind,
                    maxZfWind, minNJD, suggestWarn, suggestNJDWarn) value ('%d', '%d', '%s', '%d',
                    '%d', '%d', '%d', '%d')
                    """ % (int(pubDatei), int(pubClocki), srcLoci,
                           int(maxAvgWindi), int(maxZfWindi),
                           int(njdi), int(suggestWarni), int(suggestionNJDWarni))

        cur.execute(insertStr)
        conn.commit()
        print '数据已经导入'
    except Exception as e:
        print '数据导入出错'
        print e
        conn.rollback()

    conn.close()
    print '数据库关闭'




if __name__ == "__main__":
    # port = pd.read_csv("/home/qiu/PycharmProjects/NEW_YANGSHAN_FLASK/data/port_details.csv")
    # port_len = len(port['port'])
    # print port.head()


    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    # i = 0
    #
    # for index in range(port_len):
    #     tmp_port = port.iloc[index, 1].encode('utf-8')
    #     tmp_length = int(port.iloc[index, 2])
    #
    #
    #
    #     if (u'万' in port.iloc[index, 3]):
    #         tmp_dwt = port.iloc[index, 3].split(u'万')[0].split('-')
    #         if(len(tmp_dwt) != 1):
    #             tmp_min_dwt = int(tmp_dwt[0])*10000
    #             tmp_max_dwt = int(tmp_dwt[1])*10000
    #         else:
    #             tmp_min_dwt = int(tmp_dwt[0])
    #             tmp_max_dwt = int(tmp_dwt[0])
    #     else:
    #         tmp_dwt = port.iloc[index, 3].split('-')
    #
    #         if (len(tmp_dwt) != 1):
    #             tmp_min_dwt = int(tmp_dwt[0])
    #             tmp_max_dwt = int(tmp_dwt[1])
    #         else:
    #             tmp_min_dwt = int(tmp_dwt[0])
    #             tmp_max_dwt = int(tmp_dwt[0])
    #         print tmp_min_dwt, tmp_max_dwt
    #         raw_input("===============================")
    #
    #     tmp_type = port.iloc[index, 4].encode('utf-8')
    #     tmp_front_dwt = int(port.iloc[index, 5])
    #     tmp_front_port = port.iloc[index, 6].encode('utf-8')
    #     tmp_next_port = port.iloc[index, 7].encode('utf-8')
    #     tmp_holder = port.iloc[index, 8].encode('utf-8')
    #     tmp_tel = port.iloc[index, 9].encode('utf-8')
    #     tmp_channel = int(port.iloc[index, 10])
    #     tmp_remark = port.iloc[index, 11].encode('utf-8')

        # sql = """
        #       insert into PORTS(PORT, PORT_LENGTH, MAX_DWT, MIN_DWT, PORT_TYPE,
        #       FRONT_DEPTH, FRONT_PORT, NEXT_PORT, HOLDER, TEL, CHANNEL, REMARKS)
        #       VALUE ('%s', '%d', '%d', '%d', '%s', '%d', '%s', '%s', '%s', '%s',
        #       '%s', '%s')
        #       """ % (tmp_port, tmp_length, tmp_max_dwt, tmp_min_dwt, tmp_type, tmp_front_dwt,
        #              tmp_front_port, tmp_next_port, tmp_holder, tmp_tel, tmp_channel, tmp_remark)

    para_list = [[1,6,7,7,8,500,1000],
                 [2,8,9,9,10,200,500],
                 [3,10,11,11,12,50,200],
                 [4,12,99,13,99,0,50]]

    for line in para_list:
        para_sql = """
                   insert into WARN_PAR(WARN_LEVEL, MIN_AVG_WIND, MAX_AVG_WIND, MIN_ZF_WIND,
                   MAX_ZF_WIND, MIN_NJD, MAX_NJD) VALUE ('%d','%d','%d','%d','%d','%d','%d')
                   """ % (line[0], line[1], line[2], line[3], line[4], line[5], line[6])

        cur.execute(para_sql)
        conn.commit()

    conn.close()


    # report = pd.read_csv("/home/qiu/文档/tst/report_20161108.csv", sep=';', header=None)
    # report.columns = ['pub_DATE', 'pub_CLOCK', 'REPORT', 'SOURCE_URL', 'SOURCE']
    # report_len = len(report['pub_DATE'])
    #
    # for index in range(report_len):
    #     outStr = unicode(report.iloc[index, 0]) + ';' + unicode(report.iloc[index, 1]) + ';' + report.iloc[index, 2] + \
    #              ';' + report.iloc[index, 3] + ';' +report.iloc[index, 4]
    #     print outStr
    #     input_reports(outStr)