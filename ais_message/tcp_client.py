import socket

address = ('192.168.1.64', 16197)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(address)

# data = s.recv(512)
# print('the data received is', data)

while True:
    send_string = input("input: ")
    s.send(send_string.encode())

s.close()
