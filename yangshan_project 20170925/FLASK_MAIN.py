# coding:utf-8

import pandas as pd
import time
import os
import sys
import MySQLdb
import numpy as np
import chardet

reload(sys)
sys.setdefaultencoding('utf8')

from flask import Flask,session,redirect,url_for,request,render_template
from ship_control import match_weather, match_ship_port, convert_port_file, \
    match_height, match_stoping_wind, convert_speed2Level

from get_warning_main import get_warning, get_njd_warning
from Send_Email import send_email_n
from send_message import sendMsg

import os
os.urandom(24)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)
UPLOAD_FOLDER = 'static/tempData/'

@app.route("/login")
def index():
    username = session.get('username')
    password = session.get('password')

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    sql = """
          select * from EMPLOYEE where username = '%s'
          """ % username

    cur.execute(sql)
    tmp = cur.fetchall()

    if(len(tmp) == 0):
        checkBool = False
    else:
        if(tmp[0][1] == password):
            checkBool = True
        else:
            checkBool = False

    if (checkBool or (username == "adminren" and password == "renxiong")):
        return render_template('backstage1.html',
                               username=session.get('username'))
    return """
           你的登录信息错误，请重新登录。
           """

@app.route("/",methods=["POST","GET"])
def login():
    if request.method=='POST':
        session['username']=request.form['username']
        session['password']=request.form['password']
        return redirect(url_for('index'))
    return render_template('sign-in.html')

@app.route("/logout")
def logout():
    session.pop('username',None)
    session.pop('password',None)
    return redirect(url_for('login'))

@app.route('/top', methods=['GET', 'POST'])
def top():
    print 'top'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    try:
        chineseName_sql = """
                          select chineseName from EMPLOYEE where username = '%s'
                          """ % session.get('username')

        cur.execute(chineseName_sql)
        tmp_chinese = cur.fetchall()
        chineseName = tmp_chinese[0][0].encode('utf-8')
    except:
        chineseName = None

    loginTime = time.strftime("%Y年%m月%d日%H:%M:%S")

    if(chineseName != None):
        return render_template("top.html",
                               chineseName=chineseName,
                               loginTime=loginTime)
    else:
        return render_template("top.html",
                               chineseName=session.get('username'),
                               loginTime=loginTime)

@app.route('/main', methods=['GET', 'POST'])
def main():
    print 'main'

    return render_template("main.html")

@app.route('/menu', methods=['GET', 'POST'])
def menu():
    print 'menu'

    username = session.get('username')
    if(username == "adminren"):
        return render_template('menu.html')
    elif(username != None):
        return render_template('menu2.html')
    else:
        return "您尚未登录，请登录"

@app.route('/weather_warning', methods=['GET', 'POST'])
def weather_warning():
    print 'weather_warning'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    select_date_str = """
                      select * from weatherData
                      where pubDate = (select max(pubDate) from weatherData)
                      """

    select_report_str = """
        select * from REPORT
        where pubDate = (select max(pubDate) from weatherData)
        """

    select_user_level = """
                        select workLevel from EMPLOYEE where username = '%s'
                        """ % session.get('username')

    cur.execute(select_date_str)
    tod_data = cur.fetchall()
    tod_data = np.array(tod_data)
    cur.execute(select_report_str)
    tod_report = cur.fetchall()
    tod_report = np.array(tod_report)
    cur.execute(select_user_level)
    user_level_sql = cur.fetchall()

    print session.get('username')

    if (session.get('username') == "adminren"):
        user_level = 4
    else:
        if (len(user_level_sql) != 0):
            user_level = user_level_sql[0][0]
        else:
            """
                你的登录已经过期，请重新登录。
            """


    tod_report_sh = tod_report[tod_report[:,4] == "shanghai"]
    tod_report_zs = tod_report[tod_report[:,4] == "zhoushan"]
    newest_report_sh = str(tod_report_sh[-1][0]) + "-" + str(tod_report_sh[-1][1]) + "点" + \
                       '\n' + str(tod_report_sh[-1][2])
    newest_report_zs = str(tod_report_zs[-1][0]) + "-" + str(tod_report_zs[-1][1]) + "点" + \
                      '\n' + str(tod_report_zs[-1][2])


    tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]

    # 对风力数据部分进行判断

    print "sh_avg",tod_data_sh[-1][3]
    print "sh_zf",tod_data_sh[-1][4]

    suggest_warn_sh = get_warning(tmp_list_avg=[tod_data_sh[-1][3]],
                                  tmp_list_zf=[tod_data_sh[-1][4]])
    if_conf_sh = tod_data_sh[-1][7]
    conf_man_sh = tod_data_sh[-1][8]
    conf_level_sh = tod_data_sh[-1][9]
    conf_time_sh = tod_data_sh[-1][10]

    print "zs_avg", tod_data_zs[-1][3]
    print "zs_zf", tod_data_zs[-1][4]
    suggest_warn_zs = get_warning(tmp_list_avg=[tod_data_zs[-1][3]],
                                  tmp_list_zf=[tod_data_zs[-1][4]])
    if_conf_zs = tod_data_zs[-1][7]
    conf_man_zs = tod_data_zs[-1][8]
    conf_level_zs = tod_data_zs[-1][9]
    conf_time_zs = tod_data_zs[-1][10]

    suggest_warning = max(suggest_warn_sh, suggest_warn_zs)
    print "suggest_warn_sh = ", suggest_warn_sh
    print "suggest_warn_zs = ", suggest_warn_zs
    print "suggest_warning = ", suggest_warning

    if((if_conf_sh == 1) and (if_conf_zs == 1)):
        if_conf = 1
        conf_man = conf_man_zs
        conf_level = conf_level_zs
        conf_time = conf_time_zs
    else:
        if_conf = 0
        conf_man = ""
        conf_level = 0
        conf_time = ""

    # 对能见度数据进行判断

    minNJD = tod_data_zs[-1][5]
    suggest_warn_zs_NJD = get_njd_warning(minNJD)
    if_conf_zs_NJD = tod_data_zs[-1][13]
    conf_man_zs_NJD = tod_data_zs[-1][14]
    conf_level_zs_NJD = tod_data_zs[-1][12]
    conf_time_zs_NJD = tod_data_zs[-1][15]

    suggest_warning_NJD = suggest_warn_zs_NJD

    if (if_conf_zs_NJD == 1):
        if_conf_NJD = 1
        conf_man_NJD = conf_man_zs_NJD
        conf_level_NJD = conf_level_zs_NJD
        conf_time_NJD = conf_time_zs_NJD
    else:
        if_conf_NJD = 0
        conf_man_NJD = ""
        conf_level_NJD = 0
        conf_time_NJD = ""


    username = session.get('username')
    password = session.get('password')

    sql = """
              select * from EMPLOYEE where username = '%s'
              """ % username

    cur.execute(sql)
    tmp = cur.fetchall()

    if (len(tmp) == 0):
        checkBool = False
    else:
        if (tmp[0][1] == password):
            checkBool = True
        else:
            checkBool = False

    conn.close()

    if (checkBool or (username == "adminren" and password == "renxiong")):
        return render_template('weather_warning.html',username=session.get('username'),
                               sh_report=newest_report_sh, zs_report=newest_report_zs,
                               warning=suggest_warning, if_conf=if_conf,
                               conf_man=conf_man, conf_time=conf_time, conf_level=conf_level,
                               minNJD=minNJD, njd_warning=suggest_warning_NJD,
                               if_conf_njd=if_conf_NJD, conf_level_njd=conf_level_NJD,
                               userLevel=user_level)
    return """
           你的登录已经过期，请重新登录。
           """

@app.route('/find_history', methods=['GET', 'POST'])
def find_history():
    print 'find_history'
    return render_template('find_history.html')

@app.route('/find_sucess', methods=['GET', 'POST'])
def find_sucess():
    print 'find_sucess'

    find_date = request.form['pub_date']

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    report_sql = """
          select * from REPORT WHERE pubDate = %s
          """ % int(find_date)
    cur.execute(report_sql)
    tmp_report = cur.fetchall()

    data_sql = """
               select * from weatherData where pubDate = '%s'
               """ % int(find_date)
    cur.execute(data_sql)
    tmp_data = cur.fetchall()

    print tmp_data

    chineseName_List = []
    for line in tmp_data:

        find_chineseName = """
                           select chineseName from EMPLOYEE where username='%s'
                           """ % line[8]

        cur.execute(find_chineseName)
        tmp_name = cur.fetchall()
        if(tmp_name):
            chineseName_List.append(tmp_name[0][0].encode('utf-8'))
        else:
            chineseName_List.append(None)

    return render_template('find_sucess.html',
                           history=tmp_report,
                           data = tmp_data,
                           length = len(tmp_report),
                           chineseName_List=chineseName_List)


@app.route('/io_control', methods=['GET', 'POST'])
def io_control():
    print 'io_control'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    sql = """
          select wind, njd from portWeather
          """

    cur.execute(sql)
    tmp = cur.fetchall()
    max_wind = tmp[-1][0]
    minNJD = tmp[-1][1]

    max_wind_level = convert_speed2Level(max_wind)

    return render_template("ship_control.html",
                           max_wind=int(max_wind),
                           minNJD=minNJD)

    # select_date_str = """
    #                   select * from weatherData
    #                   where pubDate = (select max(pubDate) from weatherData)
    #                   """
    #
    # select_report_str = """
    #     select * from REPORT
    #     where pubDate = (select max(pubDate) from weatherData)
    #     """
    #
    # cur.execute(select_date_str)
    # tod_data = cur.fetchall()
    # tod_data = np.array(tod_data)
    # cur.execute(select_report_str)
    # tod_report = cur.fetchall()
    # tod_report = np.array(tod_report)
    #
    # tod_report_sh = tod_report[tod_report[:, 4] == "shanghai"]
    # tod_report_zs = tod_report[tod_report[:, 4] == "zhoushan"]
    #
    # tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    # tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]
    #
    # max_wind = max(tod_data_zs[-1][3], tod_data_zs[-1][4])
    # minNJD = tod_data_zs[-1][5]

@app.route('/bridge_control', methods=['GET', 'POST'])
def bridge_control():
    print 'bridge_control'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    sql = """
          select wind, njd from portWeather
          """

    cur.execute(sql)
    tmp = cur.fetchall()
    max_wind = tmp[-1][0]
    minNJD = tmp[-1][1]

    max_wind_level = convert_speed2Level(max_wind)

    return render_template("bridge_control.html",
                           max_wind = int(max_wind),
                           minNJD = minNJD)

@app.route('/stoping_control', methods=['GET', 'POST'])
def stoping_control():
    print 'stoping_control'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    sql = """
          select wind, njd from portWeather
          """

    cur.execute(sql)
    tmp = cur.fetchall()
    max_wind = tmp[-1][0]
    minNJD = tmp[-1][1]

    max_wind_level = convert_speed2Level(max_wind)

    return render_template("stoping_control.html",
                           max_wind = int(max_wind))

@app.route('/send_email', methods=['GET', 'POST'])
def send_email():
    print 'send_email'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'
    select_user_str = """
                      select emailAddr from sendTo
                      """

    cur.execute(select_user_str)
    tmp = cur.fetchall()
    emailAddr = tmp
    print emailAddr
    emailAddr_str = ''
    for x in emailAddr:
        emailAddr_str = emailAddr_str + str(x[0]) + ';'

    select_date_str = """
                      select * from weatherData
                      where pubDate = (select max(pubDate) from weatherData)
                      """

    select_report_str = """
        select * from REPORT
        where pubDate = (select max(pubDate) from weatherData)
        """

    cur.execute(select_date_str)
    tod_data = cur.fetchall()
    tod_data = np.array(tod_data)
    cur.execute(select_report_str)
    tod_report = cur.fetchall()
    tod_report = np.array(tod_report)

    tod_report_sh = tod_report[tod_report[:, 4] == "shanghai"]
    tod_report_zs = tod_report[tod_report[:, 4] == "zhoushan"]
    newest_report_sh = str(tod_report_sh[-1][0]) + "-" + str(tod_report_sh[-1][1]) + "点" + \
                       '\n' + str(tod_report_sh[-1][2])
    newest_report_zs = str(tod_report_zs[-1][0]) + "-" + str(tod_report_zs[-1][1]) + "点" + \
                       '\n' + str(tod_report_zs[-1][2])

    tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]

    # 对风力数据部分进行判断

    suggest_warn_sh = get_warning(tmp_list_avg=[tod_data_sh[-1][3]],
                                  tmp_list_zf=[tod_data_sh[-1][4]])

    suggest_warn_zs = get_warning(tmp_list_avg=[tod_data_zs[-1][3]],
                                  tmp_list_zf=[tod_data_zs[-1][4]])

    suggest_warning = max(suggest_warn_sh, suggest_warn_zs)

    # 对能见度数据进行判断

    minNJD = tod_data_zs[-1][5]
    suggest_warn_zs_NJD = get_njd_warning(minNJD)
    if_conf_zs_NJD = tod_data_zs[-1][13]
    conf_man_zs_NJD = tod_data_zs[-1][14]
    conf_level_zs_NJD = tod_data_zs[-1][12]
    conf_time_zs_NJD = tod_data_zs[-1][15]

    suggest_warning_NJD = suggest_warn_zs_NJD

    conn.close()

    if(suggest_warning == 1):
        suggest_warning_wind = "建议启动蓝色大风预警"
    elif(suggest_warning == 2):
        suggest_warning_wind = "建议启动黄色大风预警"
    elif (suggest_warning == 3):
        suggest_warning_wind = "建议启动橙色大风预警"
    elif (suggest_warning == 4):
        suggest_warning_wind = "建议启动红色大风预警"
    elif (suggest_warning == 0):
        suggest_warning_wind = "暂无风力预警"


    if (suggest_warning_NJD == 1):
        suggest_warning_njd_str = "建议启动蓝色能见度预警"
    elif (suggest_warning_NJD == 2):
        suggest_warning_njd_str = "建议启动黄色能见度预警"
    elif (suggest_warning_NJD == 3):
        suggest_warning_njd_str = "建议启动橙色能见度风预警"
    elif (suggest_warning_NJD == 4):
        suggest_warning_njd_str = "建议启动红色能见度预警"
    elif (suggest_warning_NJD == 0):
        suggest_warning_njd_str = "暂无能见度预警"

    print suggest_warning, suggest_warning_NJD

    suggest_warning_str = suggest_warning_wind + ';' + suggest_warning_njd_str
    report_str = newest_report_sh + '\n' + newest_report_zs +'\n' + \
        "洋山水域24小时内最低能见为%s米" % minNJD

    return render_template('index.html',
                           suggest_warning=suggest_warning_str,
                           reports=report_str,
                           emailAddr=emailAddr_str)

@app.route('/add_user', methods=['GET', 'POST'])
def add_user():
    print 'add_user'
    return render_template('add_user.html')

@app.route('/change_user', methods=['GET', 'POST'])
def change_user():
    print 'change_user'
    return render_template('change_user.html')

@app.route('/change_user_sucess', methods=['GET', 'POST'])
def change_user_sucess():
    print 'change_user_sucess'

    new_user_name = request.form['new_username'].encode('utf-8')
    new_passwd = request.form['new_passwd'].encode('utf-8')
    new_level = int(request.form['new_level'])
    new_chinesename = request.form['chinesename'].encode('utf-8')
    new_ifMessage = int(request.form['ifMessage'])
    new_tel = request.form['tel'].encode('utf-8')
    new_ifEmail = int(request.form['ifEmail'])
    new_emailAddr = request.form['emailAddr'].encode('utf-8')
    new_ifPop = int(request.form['ifPop'])

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    # sql = """
    #           insert into EMPLOYEE(username, password, chinesename, workLevel,ifMessage,
    #           tel, ifEmail, emailAddr, ifPop)
    #           value ('%s', '%s', '%s', '%d', '%d', '%s', '%d', '%s', '%d')
    #           """ % (new_user_name, new_passwd, new_chinesename, new_level,
    #                  new_ifMessage, new_tel, new_ifEmail, new_emailAddr, new_ifPop)

    sql = """
          update EMPLOYEE set password = '%s', chinesename = '%s', workLevel = '%d',
          ifMessage = '%d', tel = '%s', ifEmail = '%d', emailAddr = '%s', ifPop = '%d'
          where username = '%s'
          """ % (new_passwd, new_chinesename, new_level,new_ifMessage,
                 new_tel, new_ifEmail, new_emailAddr, new_ifPop,new_user_name)

    cur.execute(sql)
    conn.commit()
    conn.close()
    return render_template('change_sucess.html')

@app.route('/delete_user', methods=['GET', 'POST'])
def delete_user():
    print 'delete_user'
    return render_template('delete_user.html')

@app.route('/delete_user_sucess', methods=['GET', 'POST'])
def delete_user_sucess():
    print 'delete_user_sucess'

    delete_username = request.form['delete_username']
    delete_password = request.form['delete_password']

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    sql = """
          select * from EMPLOYEE where username = '%s'
          """ % delete_username

    cur.execute(sql)
    tmp = cur.fetchall()

    if (len(tmp) == 0):
        checkBool = False
    else:
        if (tmp[0][1] == delete_password):
            checkBool = True
        else:
            checkBool = False

    if checkBool:
        delete_sql = """
                     delete from EMPLOYEE where username = '%s' and password = '%s'
                     """ % (delete_username, delete_password)

        cur.execute(delete_sql)
        conn.commit()
        conn.close()

        return render_template('delete_user_sucess.html')
    else:
        return "帐号密码不匹配，删除失败"

@app.route('/pop', methods=['GET', 'POST'])
def pop():
    print 'pop'

    tmp = time.strftime("%H:%M:%S")

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    select_date_str = """
                      select * from weatherData
                      where pubDate = (select max(pubDate) from weatherData)
                      """

    select_report_str = """
        select * from REPORT
        where pubDate = (select max(pubDate) from weatherData)
        """

    cur.execute(select_date_str)
    tod_data = cur.fetchall()
    tod_data = np.array(tod_data)
    cur.execute(select_report_str)
    tod_report = cur.fetchall()
    tod_report = np.array(tod_report)

    tod_report_sh = tod_report[tod_report[:, 4] == "shanghai"]
    tod_report_zs = tod_report[tod_report[:, 4] == "zhoushan"]
    newest_report_sh = str(tod_report_sh[-1][0]) + "-" + str(tod_report_sh[-1][1]) + "点" + \
                       '\n' + str(tod_report_sh[-1][2])
    newest_report_zs = str(tod_report_zs[-1][0]) + "-" + str(tod_report_zs[-1][1]) + "点" + \
                       '\n' + str(tod_report_zs[-1][2])

    tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]

    # 对风力数据部分进行判断

    print "sh_avg", tod_data_sh[-1][3]
    print "sh_zf", tod_data_sh[-1][4]

    suggest_warn_sh = get_warning(tmp_list_avg=[tod_data_sh[-1][3]],
                                  tmp_list_zf=[tod_data_sh[-1][4]])
    if_conf_sh = tod_data_sh[-1][7]
    conf_man_sh = tod_data_sh[-1][8]
    conf_level_sh = tod_data_sh[-1][9]
    conf_time_sh = tod_data_sh[-1][10]

    print "zs_avg", tod_data_zs[-1][3]
    print "zs_zf", tod_data_zs[-1][4]
    suggest_warn_zs = get_warning(tmp_list_avg=[tod_data_zs[-1][3]],
                                  tmp_list_zf=[tod_data_zs[-1][4]])
    if_conf_zs = tod_data_zs[-1][7]
    conf_man_zs = tod_data_zs[-1][8]
    conf_level_zs = tod_data_zs[-1][9]
    conf_time_zs = tod_data_zs[-1][10]

    suggest_warning = max(suggest_warn_sh, suggest_warn_zs)

    if ((if_conf_sh == 1) and (if_conf_zs == 1)):
        if_conf = 1
        conf_man = conf_man_zs
        conf_level = conf_level_zs
        conf_time = conf_time_zs
    else:
        if_conf = 0
        conf_man = ""
        conf_level = 0
        conf_time = ""

    # 对能见度数据进行判断

    minNJD = tod_data_zs[-1][5]
    suggest_warn_zs_NJD = get_njd_warning(minNJD)
    if_conf_zs_NJD = tod_data_zs[-1][13]
    conf_man_zs_NJD = tod_data_zs[-1][14]
    conf_level_zs_NJD = tod_data_zs[-1][12]
    conf_time_zs_NJD = tod_data_zs[-1][15]

    suggest_warning_NJD = suggest_warn_zs_NJD

    if (if_conf_zs_NJD == 1):
        if_conf_NJD = 1
        conf_man_NJD = conf_man_zs_NJD
        conf_level_NJD = conf_level_zs_NJD
        conf_time_NJD = conf_time_zs_NJD
    else:
        if_conf_NJD = 0
        conf_man_NJD = ""
        conf_level_NJD = 0
        conf_time_NJD = ""

    conn.close()

    if(if_conf_NJD == 0 or if_conf == 0):
        if_pop_bool = 1
    else:
        if_pop_bool = 0

    print if_conf, if_conf_NJD
    print if_pop_bool
    return render_template('pop.html', time=tmp, if_pop=if_pop_bool)

@app.route('/change_para', methods=['GET', 'POST'])
def change_para():
    print 'change_para'

    try:
        conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='qiu',
            db='newYangshan',
        )
        cur = conn.cursor()
        # print '启动成功'
    except:
        print "启动失败"

    para_get = """
               SELECT * FROM WARN_PAR
               """
    cur.execute(para_get)
    tmp = cur.fetchall()
    print tmp

    return render_template('show_para.html',
                           para_list=tmp)

@app.route('/shipStatic', methods=['GET', 'POST'])
def shipStatic():
    print 'shipStatic'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    ship_static_sql = """
                      select * from shipStatic
                      """

    cur.execute(ship_static_sql)
    tmp = cur.fetchall()

    length = len(tmp)

    return render_template('shipStatic.html',
                           length=length,
                           plan_suit_df=tmp)

# 确认预警页面
@app.route('/conf_sucess', methods=['GET', 'POST'])
def conf_sucess():
    print 'conf_sucess'

    # 获取正在登录的人员帐号
    username = session.get('username')
    print username

    if_conf = 1
    conf_level = request.form['conf_level']
    conf_time = time.strftime('%Y%m%d-%H:%M')
    conf_man = username

    if_conf_njd = 1
    conf_level_njd = request.form['conf_level_njd']


    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    # 找出最新的上海、舟山气象数据中的日期与时间字段
    select_date_str = """
                      select * from weatherData
                      where pubDate = (select max(pubDate) from weatherData)
                      """

    cur.execute(select_date_str)
    tod_data = cur.fetchall()
    tod_data = np.array(tod_data)

    tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]

    tod_data_zs_date = tod_data_zs[-1][0]
    tod_data_zs_clock = tod_data_zs[-1][1]

    tod_data_sh_date = tod_data_sh[-1][0]
    tod_data_sh_clock = tod_data_sh[-1][1]

    # 根据最新的日期与时间字段进行数据库表更新
    try:

        zs_update_str2 = """
                        UPDATE weatherData SET inConf = '%d', confLevel = '%d',
                        confTime = '%s', confMan = '%s', confNJDLevel = '%d',
                        ifConfNJD = '%d'
                        WHERE pubDate = '%d' and pubClock = '%d' and srcLoc = '%s'
                        """ % (if_conf, int(conf_level), conf_time,conf_man,
                               int(conf_level_njd), int(if_conf_njd),
                               int(tod_data_zs_date), int(tod_data_zs_clock), 'zhoushan')


        sh_update_str2 = """
                        UPDATE weatherData SET inConf = '%d', confLevel = '%d',
                        confTime = '%s', confMan = '%s', confNJDLevel = '%d',
                        ifConfNJD = '%d'
                        WHERE pubDate = '%d' and pubClock = '%d' and srcLoc = '%s'
                        """ % (if_conf, int(conf_level), conf_time,conf_man,
                               int(conf_level_njd), int(if_conf_njd),
                               int(tod_data_sh_date), int(tod_data_sh_clock), 'shanghai')

        cur.execute(zs_update_str2)
        cur.execute(sh_update_str2)
        conn.commit()
        print "数据更新完成"

    except Exception as e:
        print '数据更新出错'
        print e
        conn.rollback()

    if(username):
        return render_template('conf_sucess.html',
                                conf_level=conf_level,
                                conf_time=conf_time,
                                conf_man=conf_man)
    else:
        return "请先登录再确认"

@app.route('/input_sucess', methods=['GET', 'POST'])
def input_sucess():
    print 'input_sucess'

    port = pd.read_csv("./data/port_details.csv")
    parameter = pd.read_csv("./data/parameter.csv")
    ship_dwt = pd.read_csv("./data/ship_dwt.csv")

    plan_file = request.files['plan_file']
    fname = "tempShipControl"

    plan_file.save(os.path.join(UPLOAD_FOLDER, fname))

    file = open("./static/tempData/tempShipControl")
    coding = chardet.detect(file.read())["encoding"]

    print "coding = ", coding

    if (coding != "utf-8"):
        plan = pd.read_csv("./static/tempData/tempShipControl", encoding="gb2312")
        print "文件编码格式为gb2312"
    else:
        plan = pd.read_csv("./static/tempData/tempShipControl", encoding="utf-8")
        print "文件编码格式为utf-8"

    plan.columns = ['io_time','assistance_on_time','io','actual_time','chineseName',
                    'name','imo','country','agent','length','dwt','actual_dwt',
                    'port','assistance_man','type']
    plan['type'] = plan['type'].fillna("集装箱船")
    print plan.head()
    # raw_input("===================main======================")
    # print plan_file

    ####################################################

    # 从码头属性表中找出有用的数据，包括长度与设计吨数

    # 将码头属性表的数据格式转换
    # 将名字变为“洋（一/二/三）（1,2,3）”，文字数字为X期，数字为X号泊位

    port['id'] = convert_port_file(port)
    converted_port = port.loc[:, ['id', 'dwt', 'type']]

    ####################################################

    ### 从计划表中循环每一天数据，判断是否满足靠离泊条件

    converted_plan = plan.loc[:, ['io_time', 'io', 'port', 'length', 'dwt',
                                  'name', 'chineseName', 'type']]

    # 根据船舶静态数据，获得吨级

    plan_num = len(converted_plan['io_time'])

    # 获取目前的天气数据

    max_wind = int(request.form['max_wind'])
    min_njd = int(request.form['min_njd'])

    # 初始化if_suit字段，值为T或者F;wind_reason风力不符理由;njd_reason能见度不符理由
    # port_reason码头属性条件不符理由

    if_suit = []
    wind_reason = []
    njd_reason = []
    port_reason = []
    ships_DWT_list = []

    for index in range(plan_num):
        # 获取当索引为index时的船舶数据

        if(converted_plan.iloc[index,1] == "靠"):

            port_reason_str, static_bool, ship_DWT = \
                match_ship_port(index=index, ship_df=converted_plan, port_df=converted_port,
                                ship_dwt_df=ship_dwt)
            # print "port:", port_reason_str, static_bool, ship_DWT
            port_reason.append("")

            ships_DWT_list.append(ship_DWT)

            if not (converted_plan.iloc[index,3] > 390.0):
                wind_reason_str, njd_reason_str, weather_suit = \
                    match_weather(index=index,
                                  ship_df=converted_plan,
                                  parameter_df=parameter,
                                  max_wind_morning = max_wind,
                                  min_njd_morning = min_njd,
                                  max_wind_afthernoon=max_wind,
                                  min_njd_afthernoon=min_njd)
                # print "weather:", wind_reason_str, njd_reason_str, weather_suit
                njd_reason.append(njd_reason_str)
                wind_reason.append(wind_reason_str)
                if_suit.append(weather_suit)
            else:
                if (max_wind < 7.0):
                    wind_reason.append("")
                    wind_bool = True
                else:
                    wind_reason.append("风力条件不符合")
                    wind_bool = False

                if (min_njd > 2000.0):
                    njd_reason.append("")
                    njd_bool = True
                else:
                    njd_reason.append("能见度条件不符合")
                    njd_bool = False

                if_suit.append(wind_bool and njd_bool)
        else:
            port_reason_str, static_bool, ship_DWT = \
                match_ship_port(index=index, ship_df=converted_plan, port_df=converted_port,
                                ship_dwt_df=ship_dwt)
            # print "port:", port_reason_str, static_bool, ship_DWT
            port_reason.append("")

            ships_DWT_list.append(ship_DWT)

            if not (converted_plan.iloc[index, 3] > 390.0):
                wind_reason_str, njd_reason_str, weather_suit = \
                    match_weather(index=index,
                                  ship_df=converted_plan,
                                  parameter_df=parameter,
                                  max_wind_morning=max_wind,
                                  min_njd_morning=min_njd,
                                  max_wind_afthernoon=max_wind,
                                  min_njd_afthernoon=min_njd)
                # print "weather:", wind_reason_str, njd_reason_str, weather_suit
                njd_reason.append(njd_reason_str)
                wind_reason.append(wind_reason_str)
                if_suit.append(weather_suit)
            else:
                if (max_wind < 7.0):
                    wind_reason.append("")
                    wind_bool = True
                else:
                    wind_reason.append("风力条件不符合")
                    wind_bool = False

                if (min_njd > 2000.0):
                    njd_reason.append("")
                    njd_bool = True
                else:
                    njd_reason.append("能见度条件不符合")
                    njd_bool = False

                if_suit.append(wind_bool and njd_bool)
        # raw_input("==============================")

    plan['if_suit'] = if_suit
    plan['port_reason'] = port_reason
    plan['njd_reason'] = njd_reason
    plan['wind_reason'] = wind_reason
    plan['ship_DWT'] = ships_DWT_list

    plan.to_csv("./data/plan_suit_files/plan_suit_date.csv", index=None)

    plan_suit_out_df = plan.loc[:, ['io_time', 'io', 'chineseName', 'name',
                                    'port', 'length', 'dwt','if_suit',
                                    'port_reason', 'njd_reason', 'wind_reason',
                                    'ship_DWT', 'type']]

    plan_len = len(plan_suit_out_df['io_time'])

    return render_template('input_sucess.html',
                           plan_len=plan_len,
                           plan_suit_df=plan_suit_out_df)

@app.route('/bridge_input', methods=['GET', 'POST'])
def bridge_input():
    print 'bridge_input'

    # 获取船舶对天气条件的抵抗参数
    parameter = pd.read_csv("./data/parameter.csv")

    plan_file = request.files['plan_file']

    fname = "tempBridge"

    plan_file.save(os.path.join(UPLOAD_FOLDER, fname))

    file = open("./static/tempData/tempBridge")
    coding = chardet.detect(file.read())["encoding"]

    if (coding != "utf-8"):
        plan = pd.read_csv("./static/tempData/tempBridge", encoding="gb2312")
        print "文件编码格式为gb2312"
    else:
        plan = pd.read_csv("./static/tempData/tempBridge", encoding="utf-8")
        print "文件编码格式为utf-8"

    converted_plan = plan.loc[:, ['io_time', 'port', 'length', 'dwt',
                                  'name', 'chineseName', 'type', 'height']]

    # 初始化if_suit字段，值为T或者F;wind_reason风力不符理由;njd_reason能见度不符理由
    # port_reason码头属性条件不符理由

    if_suit = []
    wind_reason = []
    njd_reason = []
    height_reason = []
    dwt_reason = []

    # 获取目前的天气数据

    max_wind = int(request.form['max_wind'])
    min_njd = int(request.form['min_njd'])
    max_water_height = int(request.form['max_water_height'])
    max_height = float(request.form['max_height'])
    max_dwt = int(request.form['max_dwt'])

    # print io_plan.head()

    # 根据船舶静态数据，获得吨级

    plan_num = len(converted_plan['io_time'])

    for index in range(plan_num):

        print converted_plan.iloc[index,:]

        if(converted_plan.iloc[index, 3] < max_dwt):
            dwt_str = ""
            dwt_bool = True
        else:
            dwt_str = "载重吨超过限制"
            dwt_bool = False

        # raw_input("========================================")

        wind_reason_str, njd_reason_str, weather_suit = \
            match_weather(index=index,
                          ship_df=converted_plan,
                          parameter_df=parameter,
                          max_wind_morning=max_wind,
                          min_njd_morning=min_njd,
                          max_wind_afthernoon=max_wind,
                          min_njd_afthernoon=min_njd)

        height_reason_str, height_boot = \
            match_height(index=index,
                         ship_df=converted_plan,
                         parameter_df=parameter,
                         max_water_height_morning=max_water_height,
                         max_water_height_afternoon=max_water_height,
                         max_height=max_height)

        height_reason.append(height_reason_str)
        wind_reason.append(wind_reason_str)
        njd_reason.append(njd_reason_str)
        dwt_reason.append(dwt_str)
        if_suit.append(weather_suit and height_boot and dwt_bool)

    plan['if_suit'] = if_suit
    plan['njd_reason'] = njd_reason
    plan['wind_reason'] = wind_reason
    plan['height_reason'] = height_reason
    plan['dwt_reason'] = dwt_reason

    plan.to_csv("./data/plan_suit_files/bridge_suit_date.csv", index=None)

    plan_suit_out_df = plan.loc[:, ['io_time', 'chineseName', 'name',
                                    'port', 'length', 'height', 'if_suit',
                                    'height_reason', 'njd_reason', 'wind_reason',
                                    'dwt', 'dwt_reason']]

    print plan_suit_out_df.head()
    # raw_input("===========================")

    plan_len = len(plan_suit_out_df['io_time'])

    return render_template('bridge_sucess.html',
                           plan_len=plan_len,
                           plan_suit_df=plan_suit_out_df)

@app.route('/stoping_input', methods=['GET', 'POST'])
def stoping_input():
    print 'stoping_input'

    # 获取船舶对天气条件的抵抗参数
    parameter = pd.read_csv("./data/parameter.csv")

    # 获取传入的参数数据

    plan_file = request.files['plan_file']

    fname = "tempStop"

    plan_file.save(os.path.join(UPLOAD_FOLDER, fname))

    file = open("./static/tempData/tempStop")
    coding = chardet.detect(file.read())["encoding"]

    if (coding != "utf-8"):
        plan = pd.read_csv("./static/tempData/tempStop", encoding="gb2312")
        print "文件编码格式为gb2312"
    else:
        plan = pd.read_csv("./static/tempData/tempStop", encoding="utf-8")
        print "文件编码格式为utf-8"

    plan.columns = ['io_time', 'assistance_on_time', 'io', 'actual_time', 'chineseName',
                    'name', 'imo', 'country', 'agent', 'length', 'dwt', 'actual_dwt',
                    'port', 'assistance_man', 'type']
    plan['type'] = plan['type'].fillna("集装箱船")

    stoping_plan = plan.loc[:, ['chineseName','name','imo','country',
                                'agent','length','dwt','port','type']]

    # stoping_plan = pd.read_csv(plan_file)
    print stoping_plan.head()

    max_wind = int(request.form['max_wind_morning'])
    # max_wind_afternoon = int(request.form['max_wind_afternoon'])

    # 开始对比在泊位的船舶是否符合停泊条件

    plan_num = len(stoping_plan['name'])

    if_suit = []
    stop_reason_morning = []
    stop_reason_afternoon = []

    for index in range(plan_num):
        stop_reason_str_morning, stop_reason_str_afternoon, stop_suit = \
            match_stoping_wind(index=index,
                               ship_df=stoping_plan,
                               parameter_df=parameter,
                               max_wind_morning=max_wind,
                               max_wind_afternoon=max_wind)

        if_suit.append(stop_suit)
        stop_reason_morning.append(stop_reason_str_morning)
        stop_reason_afternoon.append(stop_reason_str_afternoon)

    stoping_plan['if_suit'] = if_suit
    stoping_plan['stop_reason_morning'] = stop_reason_morning
    stoping_plan['stop_reason_afternoon'] = stop_reason_afternoon

    stoping_plan.to_csv("./data/plan_suit_files/stop_suit_date.csv", index=None)

    # print stoping_plan.head()

    plan_suit_out_df = stoping_plan.loc[:, ['chineseName', 'name',
                                    'port', 'length', 'if_suit',
                                    'stop_reason_morning', 'stop_reason_afternoon']]

    # print plan_suit_out_df.head()
    # raw_input("===========================")

    plan_len = len(plan_suit_out_df['name'])

    return render_template('stoping_sucess.html',
                           plan_len=plan_len,
                           plan_suit_df=plan_suit_out_df)


@app.route('/change_sucess', methods=['GET', 'POST'])
def change_sucess():
    print 'change_sucess'

    new_para_list = []

    blue_min_avg_wind = request.form['blue_min_avg_wind']
    blue_max_avg_wind = request.form['blue_max_avg_wind']
    blue_min_zf_wind = request.form['blue_min_zf_wind']
    blue_max_zf_wind = request.form['blue_max_zf_wind']
    blue_min_njd = request.form['blue_min_njd']
    blue_max_njd = request.form['blue_max_njd']
    new_para_list.append([1, blue_min_avg_wind, blue_max_avg_wind, blue_min_zf_wind,
                          blue_max_zf_wind, blue_min_njd, blue_max_njd])

    yellow_min_avg_wind = request.form['yellow_min_avg_wind']
    yellow_max_avg_wind = request.form['yellow_max_avg_wind']
    yellow_min_zf_wind = request.form['yellow_min_zf_wind']
    yellow_max_zf_wind = request.form['yellow_max_zf_wind']
    yellow_min_njd = request.form['yellow_min_njd']
    yellow_max_njd = request.form['yellow_max_njd']
    new_para_list.append([2, yellow_min_avg_wind, yellow_max_avg_wind, yellow_min_zf_wind,
                          yellow_max_zf_wind, yellow_min_njd, yellow_max_njd])

    orange_min_avg_wind = request.form['orange_min_avg_wind']
    orange_max_avg_wind = request.form['orange_max_avg_wind']
    orange_min_zf_wind = request.form['orange_min_zf_wind']
    orange_max_zf_wind = request.form['orange_max_zf_wind']
    orange_min_njd = request.form['orange_min_njd']
    orange_max_njd = request.form['orange_max_njd']
    new_para_list.append([3, orange_min_avg_wind, orange_max_avg_wind, orange_min_zf_wind,
                          orange_max_zf_wind, orange_min_njd, orange_max_njd])

    red_min_avg_wind = request.form['red_min_avg_wind']
    red_max_avg_wind = request.form['red_max_avg_wind']
    red_min_zf_wind = request.form['red_min_zf_wind']
    red_max_zf_wind = request.form['red_max_zf_wind']
    red_min_njd = request.form['red_min_njd']
    red_max_njd = request.form['red_max_njd']
    new_para_list.append([4, red_min_avg_wind, red_max_avg_wind, red_min_zf_wind,
                          red_max_zf_wind, red_min_njd, red_max_njd])

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    cur.execute("TRUNCATE TABLE WARN_PAR")

    for line in new_para_list:
        line = [int(x) for x in line]
        print line
        # raw_input("============================")
        para_sql = """
                   insert into WARN_PAR(WARN_LEVEL, MIN_AVG_WIND, MAX_AVG_WIND, MIN_ZF_WIND,
                   MAX_ZF_WIND, MIN_NJD, MAX_NJD) VALUE ('%d','%d','%d','%d','%d','%d','%d')
                   """ % (line[0], line[1], line[2], line[3], line[4], line[5], line[6])

        cur.execute(para_sql)
        conn.commit()

    conn.close()

    return render_template('change_sucess.html')

@app.route('/add_sucess', methods=['GET', 'POST'])
def add_sucess():
    print 'add_sucess'

    new_user_name = request.form['new_username'].encode('utf-8')
    new_passwd = request.form['new_passwd'].encode('utf-8')
    new_level = int(request.form['new_level'])
    new_chinesename = request.form['chinesename'].encode('utf-8')
    new_ifMessage = int(request.form['ifMessage'])
    new_tel = request.form['tel'].encode('utf-8')
    new_ifEmail = int(request.form['ifEmail'])
    new_emailAddr = request.form['emailAddr'].encode('utf-8')
    new_ifPop = int(request.form['ifPop'])

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'

    sql = """
          insert into EMPLOYEE(username, password, chinesename, workLevel,ifMessage,
          tel, ifEmail, emailAddr, ifPop)
          value ('%s', '%s', '%s', '%d', '%d', '%s', '%d', '%s', '%d')
          """ % (new_user_name, new_passwd, new_chinesename, new_level,
                 new_ifMessage, new_tel, new_ifEmail, new_emailAddr, new_ifPop)

    cur.execute(sql)
    conn.commit()

    return render_template('add_sucess.html')

@app.route('/check_users', methods=['GET', 'POST'])
def check_users():
    print 'check_users'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    sql = """
          select * from EMPLOYEE
          """

    cur.execute(sql)
    tmp = cur.fetchall()

    tmp_len = len(tmp)

    print tmp

    return render_template('check_users.html',
                           userNum=tmp_len,
                           user_list=tmp)

@app.route('/emailResult', methods=['GET', 'POST'])
def send_result():
    error_str = '' # 初始化错误信息

    try:
        email_addr = request.form['addrs']
        email_addr_list = email_addr.split(";")
    except Exception as e:
        tmp_error = "error email_addr"
        error_str = error_str + tmp_error
        print e

    try:
        email_subject = request.form['subject']
        print email_subject
    except Exception as e:
        tmp_error = "error email_subject"
        error_str = error_str + tmp_error
        print e

    try:
        email_message = request.form['message']
    except Exception as e:
        tmp_error = "error email_message"
        error_str = error_str + tmp_error
        print e

    # try:
    print email_addr_list, email_subject, email_message
    print "======================================="
    send_email_n(to_addr=email_addr_list, warning_str=email_message, subject_str=email_subject)
    print "发送成功"
    # except Exception as e:
    #     tmp_error = "error through sending"
    #     error_str = error_str + tmp_error
    #     print e
    #     # print "send error"

    print error_str

    if error_str:
        print "发送失败"
        return render_template('index.html', error_message=error_str)
    else:
        return render_template('send_result_ok.html')

@app.route('/check_sendTo', methods=['GET', 'POST'])
def check_sendTo():
    print 'check_sendTo'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    sql = """
          select * from sendTo
          """

    cur.execute(sql)
    tmp = cur.fetchall()

    tmp_len = len(tmp)

    return render_template("check_sendTo.html",
                           sendToNum=len(tmp),
                           sendTo_list=tmp)

@app.route('/add_sendTo', methods=['GET', 'POST'])
def add_sendTo():
    print 'add_sendTo'
    return render_template("addSendTo.html")

@app.route('/delete_sendTo', methods=['GET', 'POST'])
def delete_sendTo():
    print 'delete_sendTo'
    return render_template("delete_sendTo.html")

@app.route('/add_sendTo_sucess', methods=['GET', 'POST'])
def add_sendTo_sucess():
    print 'add_sendTo_sucess'

    chineseName = request.form['chinesename'].encode('utf-8')
    Tel = request.form['tel'].encode('utf-8')
    emailAddr = request.form['emailAddr'].encode('utf-8')

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    add_sendTo_sql = """
                     insert into sendTo(chineseName, emailAddr, tel)
                     value('%s', '%s', '%s')
                     """ % (chineseName, emailAddr, Tel)

    cur.execute(add_sendTo_sql)
    conn.commit()

    conn.close()

    return render_template("add_sucess.html")

@app.route('/delete_sendTo_sucess', methods=['GET', 'POST'])
def delete_sendTo_sucess():
    print 'delete_sendTo'

    delete_chineseName = request.form['delete_chineseName']

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()

    delete_sql = """
        delete from sendTo where chineseName = '%s'
        """ % delete_chineseName.encode("utf-8")

    cur.execute(delete_sql)
    conn.commit()
    conn.close
    return render_template("delete_user_sucess.html")

@app.route('/send_message', methods=['GET', 'POST'])
def send_message():
    print 'send_message'

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='qiu',
        db='newYangshan',
    )
    cur = conn.cursor()
    print '启动成功'
    select_user_str = """
                      select tel from sendTo
                      """

    cur.execute(select_user_str)
    tmp = cur.fetchall()
    tel = tmp
    print tel
    tel_str = ''
    for x in tel:
        tel_str = tel_str + str(x[0]) + ';'

    select_date_str = """
                      select * from weatherData
                      where pubDate = (select max(pubDate) from weatherData)
                      """

    select_report_str = """
        select * from REPORT
        where pubDate = (select max(pubDate) from weatherData)
        """

    cur.execute(select_date_str)
    tod_data = cur.fetchall()
    tod_data = np.array(tod_data)
    cur.execute(select_report_str)
    tod_report = cur.fetchall()
    tod_report = np.array(tod_report)

    tod_report_sh = tod_report[tod_report[:, 4] == "shanghai"]
    tod_report_zs = tod_report[tod_report[:, 4] == "zhoushan"]
    newest_report_sh = str(tod_report_sh[-1][0]) + "-" + str(tod_report_sh[-1][1]) + "点" + \
                       '\n' + str(tod_report_sh[-1][2])
    newest_report_zs = str(tod_report_zs[-1][0]) + "-" + str(tod_report_zs[-1][1]) + "点" + \
                       '\n' + str(tod_report_zs[-1][2])

    tod_data_sh = tod_data[tod_data[:, 2] == "shanghai"]
    tod_data_zs = tod_data[tod_data[:, 2] == "zhoushan"]

    # 对风力数据部分进行判断

    suggest_warn_sh = get_warning(tmp_list_avg=[tod_data_sh[-1][3]],
                                  tmp_list_zf=[tod_data_sh[-1][4]])

    suggest_warn_zs = get_warning(tmp_list_avg=[tod_data_zs[-1][3]],
                                  tmp_list_zf=[tod_data_zs[-1][4]])

    max_wind_today = max(tod_data_sh[-1][3], tod_data_sh[-1][4],
                         tod_data_zs[-1][3], tod_data_zs[-1][4])
    min_njd_today = tod_data_zs[-1][5]

    suggest_warning = max(suggest_warn_sh, suggest_warn_zs)

    # 对能见度数据进行判断

    minNJD = tod_data_zs[-1][5]
    suggest_warn_zs_NJD = get_njd_warning(minNJD)
    if_conf_zs_NJD = tod_data_zs[-1][13]
    conf_man_zs_NJD = tod_data_zs[-1][14]
    conf_level_zs_NJD = tod_data_zs[-1][12]
    conf_time_zs_NJD = tod_data_zs[-1][15]

    suggest_warning_NJD = suggest_warn_zs_NJD

    conn.close()

    if(suggest_warning == 1):
        suggest_warning_wind = "建议启动蓝色大风预警"
    elif(suggest_warning == 2):
        suggest_warning_wind = "建议启动黄色大风预警"
    elif (suggest_warning == 3):
        suggest_warning_wind = "建议启动橙色大风预警"
    elif (suggest_warning == 4):
        suggest_warning_wind = "建议启动红色大风预警"
    elif (suggest_warning == 0):
        suggest_warning_wind = "暂无风力预警"


    if (suggest_warning_NJD == 1):
        suggest_warning_njd_str = "建议启动蓝色能见度预警"
    elif (suggest_warning_NJD == 2):
        suggest_warning_njd_str = "建议启动黄色能见度预警"
    elif (suggest_warning_NJD == 3):
        suggest_warning_njd_str = "建议启动橙色能见度风预警"
    elif (suggest_warning_NJD == 4):
        suggest_warning_njd_str = "建议启动红色能见度预警"
    elif (suggest_warning_NJD == 0):
        suggest_warning_njd_str = "暂无能见度预警"

    print suggest_warning, suggest_warning_NJD

    suggest_warning_str = suggest_warning_wind + ';' + suggest_warning_njd_str
    report_str = newest_report_sh + '\n' + newest_report_zs +'\n' + \
        "洋山水域24小时内最低能见为%s米" % minNJD

    message = "今日最大风力为%s级，今日最小能见度为%s米" % (max_wind_today, min_njd_today) + "\n" + \
        "建议预警：" + suggest_warning_wind + ";" + suggest_warning_njd_str

    return render_template('send_message.html',
                           tel=tel_str,
                           message=message)

@app.route('/sendMsgResult', methods=['GET', 'POST'])
def sendMsg_result():
    print("sendMsg_result")
    phone = request.form['addrs']
    text = request.form['message']
    print phone, text
    sendmessage = sendMsg()
    sendMsgResult = sendmessage.sendFun(text="【洋山港海事局】"+text, phone=phone)
    # except Exception as e:
    #     tmp_error = "error through sending"
    #     error_str = error_str + tmp_error
    #     print e
    #     # print "send error"


    if sendMsgResult < 0:
        print "发送失败"
        return render_template('send_message.html', error_message=sendMsgResult)
    else:
        return render_template('send_result_ok.html')

if __name__ == '__main__':
    app.run(host="0.0.0.0", threaded=True)
    app.run(debug=True)
