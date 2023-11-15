import socket
import sys
import time
import threading
import csv

# Ref: socket programming template: https://realpython.com/python-sockets/
#      multithreaded TCP: https://blog.csdn.net/dezhihuang/article/details/71516103
#      send() in python socket class: https://pythontic.com/modules/socket/send
#      csv file read & write: https://docs.python.org/3/library/csv.html

def thread_recv(conn, log_file, log_tab):
    connect = 0 # A flag denotes that this is the first packet accepted.
    b = 0 # The number of bytes received.
    while True:   
        ''' First Log '''
        try:
            size = 128
            buf = conn.recv(size).decode()
            info_l = buf.split() # Received infomation includes "node_name Timestamp Event".
            node_name = info_l[0]
            send_time = float(info_l[1])
            event = info_l[2]
        except:
            # The connection breaks.
            log_file.write(("%f - %s %s" % (time.time(), node_name, "disconnected\n")))
            print(node_name, "disconnected")
            break

        if connect == 0 :
            # Connected.
            recv_time = time.time()
            print(node_name, "connected at %f" % recv_time)   
            log_file.write(("%f - %s %s\n" % (send_time, node_name, event)))
            header_len = (len(info_l[0]) + len(info_l[1]) + len(info_l[2]) + 3) * 8
            del_time = recv_time - send_time
            header_bw = header_len/del_time
            log_tab.writerow([recv_time, send_time, node_name, header_len, del_time, header_bw])

            if len(info_l) > 3:
                print("Longer message\n")
                recv_time = time.time()
                node_name = info_l[3]
                send_time = float(info_l[4])
                event = info_l[5]
                del_time = recv_time - send_time
                
                log_file.write(("%f %s %s\n" % (send_time, node_name, event)))

                msg_len = len(buf) * 8 - header_len # char -> bytes
                msg_bw = msg_len/del_time
                log_tab.writerow([recv_time, send_time, node_name, msg_len, del_time, msg_bw])

            t0 = send_time # Original time.
            connect = 1  
            print(buf) # use for checking
        else:
            # Receiving packets.
            print(buf)
            log_file.write(("%f %s %s\n" % (send_time, node_name, event)))

            recv_time = time.time()
            del_time = recv_time - send_time

            # b += len(buf)
            # bw = b / (recv_time - t0)

            msg_len = len(buf) * 8 # char -> bytes
            msg_bw = msg_len/del_time
            log_tab.writerow([recv_time, send_time, node_name, msg_len, del_time, msg_bw])
            #log_tab.writerow(("%s, %f, %f\n") % (node_name, del_time, bw))

    conn.close()



def main():
  print("Logger is running.")
  port = int(sys.argv[1])

  log_file = open("./log_file.txt", 'w')
  # with open('log_tab.csv', 'w', newline = '') as csvfile:
  log_tab = csv.writer(open('log_tab.csv', 'w'))
  log_tab.writerow(['Log Timestamp','Event Timestamp','Node Name','Msg Length','Delay','Bandwidth'])

  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      s.bind(("127.0.0.1", port))
      s.listen(8) # for large scenario: up to 8 nodes

      while True:
        conn, addr = s.accept()
        t = threading.Thread(target = thread_recv, args = (conn, log_file, log_tab))
        t.start()

      s.close()

  log_file.close()
  # log_tab.close()

if __name__ == "__main__":
  main()




