# coding:utf-8

import socket
import time


address = ('192.168.18.202', 16197)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(address)

while True:
    data, addr = s.recvfrom(2048)
    create_time = time.time()
    if not data:
        print("client has exist")
        break
    print(data, round(create_time))

s.close()
