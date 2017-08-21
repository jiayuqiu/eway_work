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