import matplotlib.pyplot as plt
import numpy as np

NODE_NUM = 8

def main():
    event = {}
    bandwidth = [0 for i in range(NODE_NUM)]
    for i in range(1, NODE_NUM+1):
        f = open("Plot/node%d_data.txt" % i, "r")
        while True:
            line = f.readline()
            if line != '':
                parts = line.split()
                if parts[0] == '0':
                    if parts[1] in event:
                        event[parts[1]] = (float(parts[2]), float((event[parts[1]])[1]))
                    else:
                        event[parts[1]] = (float(parts[2]), 0)
                elif parts[0] == '1':
                    if parts[1] in event:
                        event[parts[1]] = (event[parts[1]][0], max(float(parts[2]), (event[parts[1]])[1]))
                    else:
                        event[parts[1]] = (0, float(parts[2]))
                elif parts[0] == '2':
                    bandwidth[i-1] = int(parts[1]) / float(parts[2])
            else:
                break
        f.close()
    print(event)
    print(bandwidth)
    
    del_event = []
    for j in event:
        if event[j][0] == 0 or event[j][1] == 0:
            del_event.append(j)

    for _ in del_event:
        del event[_]

    plt.figure()
    bw_x = [i for i in range(1,NODE_NUM+1)]
    bw_y = bandwidth
    plt.plot(bw_x, bw_y, label = 'Bandwidth')
    plt.legend()
    plt.xlabel("Node")
    plt.ylabel("BW(B/s)")
    plt.xticks(bw_x)

    plt.figure()
    t_x = [i for i in range(1, len(event.keys())+1)]
    t_y = []
    for e in event.keys():
        del_t = event[e][1] - event[e][0]
        t_y.append(del_t)
    plt.plot(t_x, t_y, 'r', label = 'time')
    plt.legend()
    plt.xlabel("Event")
    plt.ylabel("Time(s)")
    plt.xticks([int(_) for _ in np.linspace(0, len(t_x), 5)])
    plt.show()
    

                
          
if __name__ == "__main__":
  main()