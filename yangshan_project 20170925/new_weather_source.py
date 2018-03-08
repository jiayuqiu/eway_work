# coding:utf-8

import time
import os
import csv
import sys

from selenium import webdriver

reload(sys)
sys.setdefaultencoding('utf8')


def file_save(outStr, out_path):

    if not os.path.exists(out_path):
        print '创建文件，并放入数据'
        fileout = open(out_path, 'w')
        fileout.write(outStr.encode('utf-8'))
        fileout.close()
    else:
        print '文件存在，在文件最后追加内容'
        fileout = open(out_path, 'a')
        fileout.write(outStr.encode('utf-8'))
        fileout.close()


def get_data(url):

    driver = webdriver.Chrome("/home/qiu/PycharmProjects/NEW_YANGSHAN_FLASK/data/chromedriver")
    driver.set_page_load_timeout(30)
    driver.get(url)
    username = driver.find_element_by_xpath('//*[@id="txtUser"]')
    username.clear()
    username.send_keys('LF')
    password = driver.find_element_by_xpath('//*[@id="txtPWD"]')
    password.clear()
    password.send_keys('SMMC01')
    submit = driver.find_element_by_xpath('//*[@id="submit"]')
    submit.click()

    time_list_three_hour = []
    wind_three_hour = []
    njd_three_hour = []

    time_list_five_date = []
    time_list_five_day = []
    wind_five_day = []

    time.sleep(2)
    driver.get('http://61.152.145.62/shMMshare/Ybyj/YBHDefault.aspx')
    time.sleep(5)

    for i in range(8):
        tmp_time_hour = driver.find_element_by_xpath('//*[@id="h%d1"]' % (i+1)).text
        time_list_three_hour.append(tmp_time_hour)

        tmp_wind_hour = driver.find_element_by_xpath('//*[@id="h%d5"]' % (i+1)).text
        wind_three_hour.append(tmp_wind_hour)

        tmp_njd_hour = driver.find_element_by_xpath('//*[@id="h%d8"]' % (i+1)).text
        njd_three_hour.append(tmp_njd_hour)


    for i in range(10):
        tmp_date = driver.find_element_by_xpath('//*[@id="d%d1"]' % (i+1)).text
        time_list_five_date.append(tmp_date)

        tmp_day = driver.find_element_by_xpath('//*[@id="d%d2"]' % (i + 1)).text
        time_list_five_day.append(tmp_day)

        tmp_wind = driver.find_element_by_xpath('//*[@id="d%d7"]' % (i + 1)).text
        wind_five_day.append(tmp_wind)


    driver.quit()

    return time_list_three_hour, wind_three_hour, njd_three_hour, \
           time_list_five_date, time_list_five_day, wind_five_day

if __name__ == "__main__":
    url = "http://61.152.145.62/shMMshare/"
    # get_data(url)

    while True:
        print time.strftime('%H:%M:%S')

        three_hour_time, three_hour_wind, three_hour_njd, \
        five_date, five_day, five_wind = \
            get_data(url)

        date = time.strftime("%Y%m%d")
        outPath_hour = "/home/qiu/文档/weather/threeHour_%s.csv" % date
        outPaht_day = "/home/qiu/文档/weather/fiveDay_%s.csv" % date

        hourStr = date + '\n'
        hourStr_time = ''
        hourStr_wind = ''
        hourStr_njd = ''
        for i in range(7):
            tmp_time = three_hour_time[i] + ';'
            hourStr_time = hourStr_time + tmp_time

            tmp_wind = three_hour_wind[i] + ';'
            hourStr_wind = hourStr_wind + tmp_wind

            tmp_njd = three_hour_njd[i] + ';'
            hourStr_njd = hourStr_njd + tmp_njd

        hourStr_time = hourStr_time + three_hour_time[-1] + '\n'
        hourStr_wind = hourStr_wind + three_hour_wind[-1] + '\n'
        hourStr_njd = hourStr_njd + three_hour_njd[-1] + '\n'
        hourStr = hourStr_time + hourStr_wind + hourStr_njd

        file_save(outStr=hourStr, out_path=outPath_hour)

        dayStr = date + '\n'
        dayStr_date = ''
        dayStr_day = ''
        dayStr_wind = ''
        for i in range(7):
            tmp_time = five_date[i] + ';'
            dayStr_date = dayStr_date + tmp_time

            tmp_wind = five_day[i] + ';'
            dayStr_day = dayStr_day + tmp_wind

            tmp_njd = five_wind[i] + ';'
            dayStr_wind = dayStr_wind + tmp_njd

        dayStr_date = dayStr_date + five_date[-1] + '\n'
        dayStr_day = dayStr_day + five_day[-1] + '\n'
        dayStr_wind = dayStr_wind + five_wind[-1] + '\n'
        dayStr = dayStr_date + dayStr_day + dayStr_wind

        file_save(outStr=dayStr, out_path=outPaht_day)


        print "sleeping..."
        time.sleep(3600)





