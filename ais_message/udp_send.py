import socket

address = ('192.168.18.201', 16197)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

while True:
    msg = input()
    if not msg:
        break
    s.sendto(msg.encode(), address)

s.close()
