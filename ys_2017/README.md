# 更新记录

### 20171117 ys_test 1.0

基本完成5个功能

- fix: 东海大桥的判断增加了判断载重吨。
- fix: 气象预警，优化了对今天气象报告数据的兼容度。


### 20171124 ys_test 1.1

优化了交通预警功能


# 主要程序
1. ys_main.py
该程序包含了与AIS相关的服务，东海大桥防撞、应急船舶、交通预警

```bash
nohup python3.5 ys_main.py > ys_main.log 2>&1 &
```

2. port.py
该程序包含了靠离泊预控服务

```bash
nohup python3.5 port.py > port.log 2>&1 &
```

3. weather.py
气象报告爬虫功能

```bash
nohup python3.5 weather.py > weather.log 2>&1 &
```