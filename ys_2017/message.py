# -*- coding:utf-8 -*-

import http.client
import urllib
import time
import pymysql

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
        if "【洋山港海事局】" in text:
            pass
        else:
            text = "【洋山港海事局】" + text
        params = urllib.parse.urlencode({'account': self.account, 'password': self.psw, 'msgText': text,
                                         'destmobile': phone})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = http.client.HTTPConnection(self.host, port=self.port, timeout=30)
        conn.request("POST", self.send_uri, params, headers)
        response = conn.getresponse()
        response_str = response.read()
        conn.close()
        return response_str

    # ----------------------------------------------------------------------
    def _filter_label(self, send_text):
        """
        删除短信内容中的非法标签
        :param send_text: 发送信息，类型：string
        :return: 无非法标签的信息，类型：string
        """
        import re
        pattern = re.compile('\【.*?\】')
        return pattern.sub('', send_text)

    def _send_msg(self):
        """
        从数据库中获取需要发送的短信列表
        :return: 返回需要发送的text和phone
        """
        # 连接数据库
        connection = pymysql.connect(host='192.168.1.63',
                                     user='root',
                                     password='traffic170910@0!7!@#3@1',
                                     db='dbtraffic',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()
        select_sql = """SELECT * FROM t_message"""
        cursor.execute(select_sql)

        # 初始化输出列表
        send_result_list = []
        for row in cursor.fetchall():
            mes_status = row["mes_status"]
            mes_recv_type = row["mes_recv_type"]
            if (mes_status == '1') & (mes_recv_type == '1'):
                if row["mes_entity"]:
                    print(row)
                    send_text = self._filter_label(row["mes_content"])
                    send_status = int(self.sendFun(text=send_text, phone=row["mes_entity"]))
                    send_result_list.append([row["mes_id"], send_status])
        cursor.close()
        connection.close()
        return send_result_list

    def _update_t_message(self, send_result_list):
        """
        更新t_message表格中的发送状态
        :param send_result_list: 短信发送结果，类型：list
        :return:
        """
        # 连接数据库
        connection = pymysql.connect(host='192.168.1.63',
                                     user='root',
                                     password='traffic170910@0!7!@#3@1',
                                     db='dbtraffic',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()
        for result in send_result_list:
            update_sql = """UPDATE t_message SET mes_status=%s WHERE mes_id=%s """ % (result[1], result[0])
            cursor.execute(update_sql)
        connection.commit()
        cursor.close()
        connection.close()

    def ys_send_message(self, interval=60):
        """
        从数据库中获取需要发送的短信列表并发送
        :param interval: 读取数据库中需要发送短信的列表间隔时间，单位：秒，类型：int
        :return: 发送状态，具体数值见“建周短信平台文档”
        """
        # mes_subject -- 信息标题; mes_content -- 信息内容；mes_time -- 信息发送时间;
        # mes_entity -- 信息接受体（手机或者e-mail）; mes_manage_name -- 操作人姓名; mes_manage_id -- 操作人id;
        # mes_recv_type -- 信息接受类型（1-短信，2-邮件）; mes_status -- 1：待发送，2：已发送;
        while True:
            send_result_list = self._send_msg()
            self._update_t_message(send_result_list)
            print("sleeping...")
            time.sleep(interval)


if __name__ == '__main__':
    sendMsg = sendMsg()

    # phone = "15001919669;13761596164;18217758960"
    # # weatherReport = """
    # # 今日最大风力为9级，今日最小能见度为20000米
    # # 建议预警：建议启动蓝色大风预警;暂无能见度预警
    # # """
    # weatherReport = """短信接口测试"""
    # text = "【洋山港海事局】%s" % weatherReport

    # 调接口发短信
    sendMsg.ys_send_message()
