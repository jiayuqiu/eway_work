# coding:utf-8

import os
import datetime
import platform
import time


def main():
    if not os.path.exists('ais_moor'):
        os.system('mkdir ais_moor')

    timeStamp = time.time()
    moor_time_array = time.localtime(timeStamp)
    moor_time_str = time.strftime("%Y%m%d", moor_time_array)
    
    dynamic_time_array = time.localtime(timeStamp - 60 * 60 * 24)
    dynamic_time_str = time.strftime("%Y%m%d", dynamic_time_array)

    os.system('spark/bin/spark-submit sparkmoor/moor_main_spark.py '
              'sparkmoor/staticData/Asia_anchores.csv '
              'sparkmoor/staticData/part-00000 '
              'ais_bm_thr_org/dynamic_%s.tsv '
              'thr '
              'ais_moor/moor_%s &' %
              (dynamic_time_str, dynamic_time_str))

def timeFun():
    flag = 0
    while True:
        now_hour = int(time.strftime("%H", time.localtime()))
        if now_hour == 2:
            timeStamp = time.time()
            moor_time_array = time.localtime(timeStamp)
            moor_time_str = time.strftime("%Y-%m-%d", moor_time_array)
            print(moor_time_str)
            main()
            time.sleep(3600)
        time.sleep(60)


if __name__ == "__main__":
    now_year = int(time.strftime("%Y", time.localtime()))
    now_month = int(time.strftime("%m", time.localtime()))
    now_day = int(time.strftime("%d", time.localtime()))
    # now_hour = int(time.strftime("%H", time.localtime()))
    # sched_timer = datetime.datetime(now_year, now_month, now_day, 10, 0, 0)
    # print('run task at {0}'.format(sched_timer))
    timeFun()
