import socket

address = ('192.168.18.202', 31500)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(address)

data = s.recv(512)
print('the data received is', data)

s.send(b'hihi')

s.close()
