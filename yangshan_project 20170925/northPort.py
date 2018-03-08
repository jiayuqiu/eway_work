# coding:utf-8

import pandas as pd

import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from selenium import webdriver

class ysCrawler(object):
    """洋山爬虫类"""
    def __init__(self):
        # chromedriver
        self.driver = webdriver.Chrome("/home/qiu/Downloads/chromedriver")

        # 洋山港区URL
        self.ysPortURL = "http://115.231.126.81/Forecast"

    ##########################################################
    #---------------------------------------------------------
    def _clickNorthPort(self):
        """打开北部港区详细天气数据框"""
        northPortBut = self.driver.find_element_by_xpath('//*[@id="map"]/div[1]/div[2]/div[4]/div[3]/div[1]')
        northPortBut.click()

    #---------------------------------------------------------
    def _northTime(self):
        """获取北部港区预报时间"""
        # 获取预报第一列的时间
        startTimeXpath = '//*[@id="t2458472"]/table/thead/tr/td[2]'
        startTimeStr = self.driver.find_element_by_xpath(startTimeXpath).text
        print startTimeStr
        raw_input("============")
        startDate = int(startTimeStr.split('日')[0])
        startClock = int(startTimeStr.split('日')[1].split('时')[0])

        # 根据第一列的时间，推算出后23小时的时间
        northTimeList = []
        northTimeList.append([startDate, startClock])
        for col in range(1, 26):
            clock = startClock + col
            date = startDate
            if clock > 23:
                clock = clock - 24
                date = startDate + 1
            northTimeList.append([date, clock])
        northTimeDF = pd.DataFrame(data=northTimeList, columns=["day", "clock"])
        return northTimeDF


    #---------------------------------------------------------
    def _getWindNjdData(self, timeDF):
        """获取风力、能见度数据"""
        windList = []
        njdList = []

        # 循环每列，找到风力、能见度数据
        for col in range(2, 25):
            # //*[@id="t2458472"]/table/tbody/tr[1]/td[2]/span
            windXpath = '//*[@id="t2458472"]/table/tbody/tr[1]/td[%s]/span' % col
            njdXpath = '//*[@id="t2458472"]/table/tbody/tr[5]/td[%s]/span' % col
            windStr = self.driver.find_element_by_xpath(windXpath).text
            njdStr = self.driver.find_element_by_xpath(njdXpath).text
            if windStr:  # 若找不到风力数据，用0代替
                windNum = int(self.driver.find_element_by_xpath(windXpath).text)
            else:
                windNum = 0
            if njdStr:  # 若找不到能见度数据，用20000代替
                njdNum = int(self.driver.find_element_by_xpath(njdXpath).text)
            else:
                njdNum = 20000

            windList.append(windNum)
            njdList.append(njdNum)
        timeDF["wind"] = windList
        timeDF["njd"] = njdList
        return timeDF

    #---------------------------------------------------------
    def _northWeather(self):
        """获取北部港区天气情况"""

        # 获取时间
        northTimeDF = self._northTime()

        # 获取风力，能见度数据
        weatherDF = self._getWindNjdData(timeDF=northTimeDF)

        return weatherDF

    #---------------------------------------------------------
    def _worstWindNjd(self, weaterDF):
        """获取接下来天气会发生的最恶劣情况"""
        nowDate = time.strftime('%Y%m%d', time.localtime(time.time()))
        nowTime = time.strftime('%H:%M:%S', time.localtime(time.time()))
        nowDay = int(time.strftime('%d', time.localtime(time.time())))
        nowClock = int(time.strftime('%H', time.localtime(time.time())))

        aftWeatherDF = weaterDF[(weaterDF["day"] >= nowDay) &
                                (weaterDF["clock"] >= nowClock)]
        print aftWeatherDF
        raw_input("=========")
        maxWind = max(aftWeatherDF["wind"])
        minNJD = min(aftWeatherDF["njd"])
        return maxWind, minNJD

    #---------------------------------------------------------
    def ysPortCrawler(self):
        """爬取洋山港区内天气"""
        try:
            # 打开北部港区天气预报页面
            self.driver.get(self.ysPortURL)
            time.sleep(5)

            # 打开北部港区天气数据框
            self._clickNorthPort()
            time.sleep(5)

            # 爬取天气数据
            print("开始抓取天气数据")
            weatherDF = self._northWeather()

            # 获取从现在开始，之后24小时最恶劣的风力、能见度
            maxWind, minNjd = self._worstWindNjd(weaterDF=weatherDF)

            # 爬取完成，退出浏览器
            self.driver.quit()

            return maxWind, minNjd
        except Exception, e:
            import traceback
            traceback.print_exc()
            self.driver.quit()

if __name__ == "__main__":
    ysCrawler = ysCrawler()
    ysCrawler.ysPortCrawler()
