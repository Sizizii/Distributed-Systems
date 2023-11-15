import socket
import sys
import time
import threading
import pickle
import time

# Reference:
# socket.connect_ex(): https://www.cnblogs.com/lucio1128/p/12464302.html
# socket & pickle: https://pythonprogramming.net/pickle-objects-sockets-tutorial-python-3/


HEADERSIZE = 5
LOCALHOST = "192.168.56.1"
# ---------------------------Class Node---------------------------
class Node:
    # Node on localhost
    timestamp = 0
    accounts = {} # Account record
    seen = [] # messgae observed
    events = [] # Event queue implementing total ordering
    event_reply = {} # Stores the replies for each message
    
    num_conn = 0
    alive_connections = {} # All alive connections with other nodes. Key: Nodename -> Value: send socket
    #recv_conns = []

    accountLock = threading.Lock()
    connectionLock = threading.Lock()
    timestampLock = threading.Lock()
    seenLock = threading.Lock()
    eventLock = threading.Lock()
    replyLock = threading.Lock()

    plotLock = threading.Lock()

    def __init__(self, nodename, port) -> None:
        self.nodename = nodename
        self.nodenum = nodename.removeprefix("node")
        self.port = port
        return

    # -------- Process Messages ----- #   
    def receive(self, msg):
        #Upon receive any message, judge if seen before
        # print("Receive message: %s, type: %d" % (msg.content, msg.type))
        if msg.type == 0: # proposed message
            self.seenLock.acquire()
            self.seen.append(msg.mID)
            self.seenLock.release()

            if msg.sender == self.nodename:
                self.replyLock.acquire()
                self.event_reply[msg.mID] = []
                self.replyLock.release()

                self.connectionLock.acquire()
                if len(self.alive_connections.keys()) == 0:
                    msg.deliverable = 1
                self.connectionLock.release()

                self.insert_msg(msg)
                self.multicast(msg)

                if msg.deliverable == 1:
                    self.deliver(msg)
            
            else:
                prev_prio = msg.prio_level
                self.timestampLock.acquire()
                self.timestamp += 1
                msg.prio_level = float(str(self.timestamp) + "." + str(self.nodenum))
                self.timestampLock.release()
                initiator = msg.sender
                msg.sender = self.nodename
                msg.type = 1
                self.insert_msg(msg)
                self.unicast(initiator, msg)
                # msg.prio_level = max(prev_prio, msg.prio_level)

            

        elif msg.type == 1: # reply message
            new_prio = self.update_prio(msg)

            if new_prio == -1:
                self.eventLock.acquire()
                # print("Wrong reply!! Check!")
                print('----------- no msg in prio', msg.mID, file=sys.stderr)
                for event in self.events:
                    print(event.content, event.deliverable, event.mID, end='| ', file=sys.stderr)
                print(file=sys.stderr)
                self.eventLock.release()

            self.connectionLock.acquire()
            check_list = self.alive_connections.keys()
            self.connectionLock.release()

            self.replyLock.acquire()
            if msg.mID in self.event_reply.keys():
                self.event_reply[msg.mID].append(msg.sender)
                reply_list = self.event_reply[msg.mID]
                self.replyLock.release()
                if set(check_list).issubset(set(reply_list)):
                    msg.type = 2
                    msg.deliverable = 1
                    msg.prio_level = new_prio
                    
                    self.eventLock.acquire()
                    for i in range(len(self.events)):
                        if self.events[i].mID == msg.mID:
                            self.events[i].deliverable = 1
                    self.eventLock.release()
                    self.multicast(msg)

                    # all reply received
                    self.replyLock.acquire()
                    del self.event_reply[msg.mID]
                    self.replyLock.release()
                    time.sleep(0.01) #mark
                    self.deliver(msg)

            else: # just in case, won't happen
                # print("Receive faulty unicast")
                self.replyLock.release()

        elif msg.type == 2: # agreed message
            agreed_before = 1

            if str(msg.mID).split(".")[1] != self.nodenum:
                self.eventLock.acquire()
                for i in range(len(self.events)):
                    if self.events[i].mID == msg.mID:
                        if self.events[i].deliverable != 1:
                            agreed_before = 0
                            if self.events[i].prio_level < msg.prio_level:
                                self.events.pop(i)
                                if len(self.events) == 0 or len(self.events) == i:
                                    self.events.append(msg)
                                else:
                                    for j in range(i, len(self.events)):
                                        if self.events[j].prio_level > msg.prio_level:
                                            self.events.insert(j, msg)
                                            break
                                        elif j == (len(self.events) - 1):
                                            self.events.append(msg)

                            else: # already reordered
                                self.events[i].deliverable = 1
                        break #?
                    
                self.eventLock.release()

                if agreed_before == 0:
                    self.multicast(msg)

                self.deliver(msg)           

        return
    
    def insert_msg(self, msg):
        self.eventLock.acquire()
        if len(self.events) == 0:
            self.events.append(msg)
            # print("Append message %s at front" % msg.mID)
        else:
            for i in range(len(self.events)):
                if self.events[i].prio_level > msg.prio_level:
                    self.events.insert(i, msg)
                    # print("Append message %s at index %d" % (msg.mID, i))
                    break
                elif i == (len(self.events) - 1):
                    self.events.append(msg)
                    # print("Append message %s at back" % msg.mID)
        # print('----------- After insert_msg', file=sys.stderr)
        # for event in self.events:
        #     print(event.content, event.deliverable, event.mID, end='| ', file=sys.stderr)
        # print(file=sys.stderr)
        self.eventLock.release()
        return
    
    def unicast(self, receiver, msg):
        buf = pickle.dumps(msg)
        buf = bytes(f"{len(buf):<{HEADERSIZE}}", 'utf-8') + buf
        buf += bytes(f"{0:<{256-len(buf)}}", 'utf-8')

        self.connectionLock.acquire()
        if receiver in self.alive_connections.keys():
            s = self.alive_connections[receiver]
        else:
            s = -1
        self.connectionLock.release()

        if s!= -1:
            try:
                # print("Unicast Reply to %s" % receiver)
                s.send(buf)
                # # Plot
                local_node.plotLock.acquire()
                global total_bytes
                global start
                total_bytes += len(str(buf))
                plot_file.write("2 %d %f\n" % (total_bytes, time.time() - start))
                local_node.plotLock.release()
            except:
                # print("Connection to %s breaks!" % receiver)
                s.close()
                # self.connectionLock.acquire()
                # if receiver in self.alive_connections.keys():
                #     del self.alive_connections[receiver]
                # self.connectionLock.release()
                # time.sleep(2)   
                # self.delete_node_msg(receiver)

        return
    
    def multicast(self, msg):
        self.connectionLock.acquire()
        send_list = self.alive_connections.keys()
        self.connectionLock.release()
        msg.sender = self.nodename
        buf = pickle.dumps(msg)
        buf = bytes(f"{len(buf):<{HEADERSIZE}}", 'utf-8') + buf
        buf += bytes(f"{0:<{256-len(buf)}}", 'utf-8')

        for node in send_list:
            self.connectionLock.acquire()
            if node in self.alive_connections.keys():
                s = self.alive_connections[node]
            else:
                s = -1
            self.connectionLock.release()

            if s!= -1:
                try:
                    # print("Multicast to %s" % node)
                    s.send(buf)
                    # Plot
                    local_node.plotLock.acquire()
                    global total_bytes
                    global start
                    total_bytes += len(str(buf))
                    plot_file.write("2 %d %f\n" % (total_bytes, time.time() - start))
                    local_node.plotLock.release()
                except:
                    # print("Connection to %s breaks!" % node)
                    s.close()
                    # self.connectionLock.acquire()
                    # if node in self.alive_connections.keys():
                    #     del self.alive_connections[node]
                    # self.connectionLock.release()
                    # time.sleep(2)   
                    # self.delete_node_msg(node)   
        return
    
    # -------- Updates ----- #
    # def update_prio(self, msg):
    #     flag = 0
    #     msg_idx = -1
    #     self.eventLock.acquire()
    #     for i in range(len(self.events)):
    #         if self.events[i].mID == msg.mID:
    #             if self.events[i].prio_level < msg.prio_level:
    #                 msg_idx = i
    #                 for j in range(i + 1, len(self.events)):
    #                     if self.events[j].prio_level > msg.prio_level:
    #                         self.events.insert(j, msg)
    #                         break
    #                     elif j == len(self.events) - 1:
    #                         self.events.append(msg)
    #                         break
    #     if msg_idx != -1:
    #         self.events.pop(msg_idx)
    #     self.eventLock.release()
    #     return


    def update_prio(self, msg):
        prio = -1
        self.eventLock.acquire()
        for i in range(len(self.events)):
            if self.events[i].mID == msg.mID:
                if self.events[i].prio_level < msg.prio_level:
                    self.events.pop(i)
                    prio = msg.prio_level
                    if len(self.events) == 0 or len(self.events) == i:
                        self.events.append(msg)
                    else:
                        for j in range(i, len(self.events)):
                            if self.events[j].prio_level > msg.prio_level:
                                self.events.insert(j, msg)
                                break
                            elif j == (len(self.events) - 1):
                                self.events.append(msg)
                else:
                    prio = self.events[i].prio_level

                break
        # print('----------- After update_prio', file=sys.stderr)
        # for event in self.events:
        #     print(event.content, event.deliverable, event.mID, end='| ', file=sys.stderr)
        # print(file=sys.stderr)
        self.eventLock.release()
        return prio

    def update_reply(self, msg):
        self.replyLock.acquire()
        # check if received first
        if msg.mID in self.event_reply.keys():
            self.event_reply[msg.mID].append(msg.sender)
        else:
            # if not from input, add reply
            if msg.sender == self.nodename:
                self.event_reply[msg.mID] = [msg.sender]
            else:
                self.event_reply[msg.mID] = [msg.sender, self.nodename]

        self.replyLock.release()
        return
    
    def update_deliverability(self, del_node):
        self.connectionLock.acquire()
        check_list = self.alive_connections.keys()
        self.connectionLock.release()

        deliver_mID = []
        
        self.replyLock.acquire()
        for mID in self.event_reply.keys():
            reply_list = set(self.event_reply[mID])
            if set(check_list).issubset(set(reply_list)):
                deliver_mID.append(mID)
                # del self.event_reply[mID]
        self.replyLock.release()
            
        msg_to_send = []

        self.eventLock.acquire()
        for i in range(len(self.events)):
            if self.events[i].mID in deliver_mID:
                self.events[i].type = 2
                self.events[i].deliverable = 1
                self.events[i].sender = self.nodename
                msg_to_send.append(self.events[i])
        self.eventLock.release()

        for msg in msg_to_send:
            self.multicast(msg)

        self.replyLock.acquire()
        for _ in deliver_mID:
          del self.event_reply[_]
        self.replyLock.release()

        self.deliver(0)
        
    
    def update_connection(self, nodename, s):
        self.connectionLock.acquire()
        self.alive_connections[nodename] = s
        print("%s is connected" % nodename)
        self.connectionLock.release()
        return
    
    def delete_connection(self, nodename):
        self.connectionLock.acquire()
        if nodename in self.alive_connections.keys():
          del self.alive_connections[nodename]
        self.connectionLock.release()
        return
    
    def update_timestamp(self):
        self.timestampLock.acquire()
        self.timestamp += 1
        timestamp_copy = self.timestamp
        self.timestampLock.release()
        return timestamp_copy
    
    def delete_node_msg(self, nodename):
        print("In delete node")
        nodenum = nodename.removeprefix("node")
        self.eventLock.acquire()
        max_len = len(self.events)
        i = 0
        while i < max_len:
            if (str(self.events[i].mID).split(".")[1] == nodenum) and (self.events[i].deliverable == 0):
                del self.events[i] #不加1, 继续查下一个
                max_len -= 1
            else:
                i += 1
        self.eventLock.release()

        self.deliver(-1)

    # ------- Deliver ------------- #   
    def deliver(self, msg):
        # events从头开始看，判断每个event是否收到所有reply，
        # 是则update_account,继续处理下一个event直到处理到msg为止， 
        # 否则break
        pop = -1
        trans = []
        self.eventLock.acquire()
        for i in range(len(self.events)):
            event = self.events[i]

            if event.deliverable == 1:

                pop = i
                trans.append(event.content)
                print("Deliver %s: %s, prio: %f" % (event.mID, event.content, event.prio_level))
                
                # Plot
                local_node.plotLock.acquire()
                global plot_file
                plot_file.write("1 %f %f\n" % (event.mID, time.time()))
                local_node.plotLock.release()
            else:
                break

        if pop != -1:
            for i in range(pop + 1):
                self.events.pop(0)
        # print('----------- After deliver', file=sys.stderr)
        # for event in self.events:
        #     print(event.content, event.deliverable, event.mID, end='| ', file=sys.stderr)
        # print(file=sys.stderr)
        self.eventLock.release()

        for tran in trans:
            self.update_account(tran)
            
        return
        
    
    def balance(self):
        #print(self.accounts)
        return

    def update_account(self,content):
        trans = content.split()
        if trans[0] == "DEPOSIT":
            self.accountLock.acquire()
            if trans[1] not in self.accounts.keys():
                self.accounts[trans[1]] = 0
                # print("√: Create account %s" % trans[1])
            self.accounts[trans[1]] += int(trans[2])
            # print("√: Account %s deposits %s" % (trans[1], trans[2]))
            self.accountLock.release()
        elif trans[0] == "TRANSFER":
            self.accountLock.acquire()
            if trans[1] not in self.accounts.keys():
                print("Nonexist source account.")
            elif self.accounts[trans[1]] < int(trans[4]):
                print("Invalid transfer transaction.")
            else:
                self.accounts[trans[1]] -= int(trans[4])
                if trans[3] not in self.accounts.keys():
                    self.accounts[trans[3]] = 0
                    # print("√: Create account %s" % trans[3])
                self.accounts[trans[3]] += int(trans[4])
                # print("√: Account %s transfer %s to account %s" % (trans[1], trans[4], trans[3]))
                self.balance()
            self.accountLock.release()
        else:
            print("Invalid operation.\n")
        return 

# ---------------------------Class Node---------------------------

# ---------------------------Class Message---------------------------
class Message:
    def __init__(self, mID, content, prio_level, sender, type) -> None:
        self.mID = mID  # initiate_timestamp.initiator
        self.content = content
        self.prio_level = prio_level
        self.sender = sender
        self.type = type # 0 for propose, 1 for reply, 2 for agreed
        self.deliverable = 0
        return

# ---------------------------Class Message---------------------------

# # ---------------------------Class Connection---------------------------
# class Connection:
#     # Connection to other node
#     ip = "127.0.0.1"
#     #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # socket
#     def __init__(self, nodename, port, tcp_socket):
#         self.nodename = nodename
#         self.port = port
#         self.s = tcp_socket
#         # s.connect()
#         return
    
#     def __del__(self):
#         self.socket.close()
#         return
# # ---------------------------Class Connection---------------------------

def extern_event(num):
    # Process other events from external input.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as skt:
        skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        skt.bind((LOCALHOST, local_node.port))
        skt.listen(num) # for large scenario: up to 8 nodes
        ts = []

        for i in range(num):
            conn, addr = skt.accept()
            conn.settimeout(5)
            t = threading.Thread(target = thread_recv, args = (conn,))
            t.daemon = True
            ts.append(t)
            t.start()
        for th in ts:
            th.join()
        
    return

def thread_recv(conn_skt):
    sender = -1
    while True:   
        try:
            size = 256
            buf = conn_skt.recv(size)

            # Plot
            local_node.plotLock.acquire()
            global total_bytes
            global start
            total_bytes += len(str(buf))
            plot_file.write("2 %d %f\n" % (total_bytes, time.time() - start))
            local_node.plotLock.release()

            # parse
            msg_len = int(buf[:HEADERSIZE])
            msg_l = buf[HEADERSIZE: -(256-(msg_len + HEADERSIZE))]
            msg = pickle.loads(msg_l)
            # process
            sender = msg.sender
           
            local_node.receive(msg)
            # local_node.deliver(msg)


        except socket.timeout:
                print("Socket timeout")
                break
        except:
            print("Connection break")
            # The connection breaks.
            if sender != -1:
                local_node.delete_connection(sender)
            conn_skt.close()

            local_node.update_deliverability(sender)

            time.sleep(2)   
            local_node.delete_node_msg(sender)
            break
    return


def main():
    node_name = sys.argv[1]
    print(node_name + " is running.")
    # IP and Port for target centralized server.
    port = int(sys.argv[2])
    global local_node # Global variable. Node on localhost.
    local_node = Node(node_name, port) 
    file_path = "./" + sys.argv[3]

     # Process local events from internal input.
    config_file = open(file_path, "r")
    local_node.num_conn = int(config_file.readline()) # Number of connection.


    # Plot
    global plot_file
    global total_bytes
    plot_file = open(("Plot/%s_data.txt" % local_node.nodename), "w+")
    total_bytes = 0

    global start
    start = time.time()

    # Start a thread to recv event from other nodes.
    t_recv = threading.Thread(target=extern_event, args=(local_node.num_conn,))
    t_recv.daemon = True
    t_recv.start()
 
    


    # establish all required tcp connections -> used for multicast
    for i in range(local_node.num_conn):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_config = config_file.readline().split()
        while True:
            try:
                con_err = s.connect((LOCALHOST, int(node_config[2])))
                local_node.update_connection(node_config[0], s)
                break
            except:
                continue
                
    config_file.close()

    time.sleep(2) # Wait for other nodes to establish the connection.

    while True:
        try:
            buf = input() # Reac event from input.
            # print("New event: %s" % buf)
            timestamp = local_node.update_timestamp()
            mID = float(str(timestamp) + "." + str(local_node.nodenum))
            prio_level = mID
            msg = Message(mID, buf, prio_level, node_name, 0)

            # Plot
            local_node.plotLock.acquire()
            plot_file.write("0 %f %f\n" % (msg.mID, time.time()))
            local_node.plotLock.release()

            local_node.receive(msg)
            # local_node.deliver(msg)
            
        except EOFError:
            print('End of file, prepare to exit')
            break

    t_recv.join()
    # Plot
    print(local_node.accounts)
    plot_file.write("2 %d %f\n" % (total_bytes, time.time() - start))
    plot_file.close()
    print("EXIT")
    return


if __name__ == "__main__":
  main()
