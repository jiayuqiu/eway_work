# coding:utf-8

import pymssql
import pandas as pd

if __name__ == "__main__":
    conn = pymssql.connect(host='192.168.1.82', user='sa', password='traffic170910@0!7!@#3@1')
    cur = conn.cursor()
    cur.execute("""select * from KAJH""")
    data = cur.fetchall()
    df = pd.DataFrame(list(data))
    df.to_csv("/home/qiu/Documents/ys_ais/KAJH.csv", index=None)
    if not cur:
        raise (NameError, "数据库连接失败")
    conn.close()