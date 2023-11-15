import socket
import sys
import time
import threading

# Ref: socket programming template: https://realpython.com/python-sockets/
#      multithreaded TCP: https://blog.csdn.net/dezhihuang/article/details/71516103
#      send() in python socket class: https://pythontic.com/modules/socket/send


def main():
    node_name = sys.argv[1]
    print(node_name + " is running.")
    # IP and Port for target centralized server.
    ip = sys.argv[2]
    port = int(sys.argv[3])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((ip, port))
        s.send(("%s %f connected\n" % (node_name, time.time())).encode())
        # for i in range(5): # for testing
        while True:
            try:
              msg = input()
            except:
               break

            timestamp = msg.split()[0]
            event = msg.split()[1]
            info = node_name + " " + timestamp + " " + event # Send infomation includes "node's name, timestamp and event".
            print(info)
            s.send(info.encode())
        
        s.close()

if __name__ == "__main__":
  main()