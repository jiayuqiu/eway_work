# coding:utf-8

import datetime
import platform
import time

def timeFun(sched_timer):
	flag = 0
	while True:
		now = datetime.datetime.now()
		if sched_timer < now < (sched_timer+datetime.timedelta(seconds=1)):
			print("job start, now time is %s" % now)
			flag = 1
			time.sleep(1)
		else:
			if flag == 1:
				sched_timer = sched_timer + datetime.timedelta(minutes=1)
				flag = 0
				
if __name__ == "__main__":
	sched_timer = datetime.datetime(2018, 2, 1, 17, 24, 0)
	print('run task at {0}'.format(sched_timer))
	timeFun(sched_timer)
