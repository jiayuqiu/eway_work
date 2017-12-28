# sparkmoor
停泊事件 spark

# 判断停泊事件流程图
![image](image/moorFlowChart.jpg)

特殊处理说明：<br>
在判断停泊事件时存在边界问题：<br>
当endIndex为倒数第二条时，需要把暂存的停泊事件按索引输出。
若不进行该操作程序会陷入死循环。

# 判断停泊事件与港口位置关系流程图
![image](image/moorPortFlowChart.jpg)

# 运行spark分布式代码
bin/spark-submit ~/PycharmProjects/eway_work/sparkmoor/moor_main_spark.py /home/qiu/Documents/staticData/Asia_anchores.csv /home/qiu/Documents/staticData/part-00000 /media/qiu/新加卷/2017/01/date_1 thr /home/qiu/Documents/tst_moor

- 参数1：多边形范围数据路径
- 参数2：点港口数据路径
- 参数3：AIS数据路径
- 参数4：AIS数据来源，bm-博懋，cx-船讯网，thr-三阶段
- 参数5：停泊日志输出文件路径
