# -*- coding:utf-8 -*-

import httplib
import urllib

class sendMsg(object):
    """短信发送类"""
    #---------------------------------------------------------------------
    def __init__(self):
        # 服务器地址
        self.host = "www.jianzhou.sh.cn"

        # 端口号
        self.port = 80

        # 短信接口的URI
        self.send_uri = "/JianzhouSMSWSServer/http/sendBatchMessage"

        # 账号密码
        self.account = "sdk_ysghsj"
        self.psw = "YSGvts7199"

    # ---------------------------------------------------------------------
    def sendFun(self, text, phone):
        """ 
            发送短信程序段
            输入：text -- 短信正文，必须加入"【洋山港海事局】"作为签名，类型：string
                 phone -- 手机号码，用;分割，类型：string
        """

        params = urllib.urlencode({'account': self.account, 'password': self.psw, 'msgText': "【洋山港海事局】" + text,
                                   'destmobile': phone})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = httplib.HTTPConnection(self.host, port=self.port, timeout=30)
        conn.request("POST", self.send_uri, params, headers)
        response = conn.getresponse()
        response_str = response.read()
        conn.close()
        return response_str

# # 服务地址
# host = "www.jianzhou.sh.cn"
#
# # 端口号
# port = 80
#
# # 短信接口的URI
# send_uri = "/JianzhouSMSWSServer/http/sendBatchMessage"
#
# # 账号
# account = "sdk_ysghsj"
#
# # 密码
# psw = "YSGvts7199"
#
#
# def send_sms(text, phone):
#     """
#     能用接口发短信
#     """
#     params = urllib.urlencode({'account': account, 'password': psw, 'msgText': text, 'destmobile': phone})
#     headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
#     conn = httplib.HTTPConnection(host, port=port, timeout=30)
#     conn.request("POST", send_uri, params, headers)
#     response = conn.getresponse()
#     response_str = response.read()
#     conn.close()
#     return response_str
#
#
# if __name__ == '__main__':
#     phone = "15001919669;13524774775"
#     weatherReport = """
#     今日最大风力为9级，今日最小能见度为20000米
# 建议预警：建议启动蓝色大风预警;暂无能见度预警
#     """
#     text = "【洋山港海事局】%s" % weatherReport
#
#     # 调接口发短信
#     print(send_sms(text, phone))
