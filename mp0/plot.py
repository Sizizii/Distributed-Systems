import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

delay_min = []
delay_max = []
delay_avg = []
bw_min = []
bw_max = []
bw_avg = []
for i in range(1,11):
    df = pd.read_csv('log_tab%i.csv' % i, usecols = ['Delay', 'Bandwidth'])
    delay_np = np.array(df.loc[:,"Delay"])
    delay_min.append(np.min(delay_np))
    delay_max.append(np.max(delay_np))
    delay_avg.append(np.mean(delay_np))
    
    bw_np = np.array(df.loc[:,"Bandwidth"])
    bw_min.append(np.min(bw_np))
    bw_max.append(np.max(bw_np))
    bw_avg.append(np.mean(bw_np))
print(delay_min)
print(delay_max)
print(delay_avg)
print(bw_min)
print(bw_max)
print(bw_avg)

fig, ax = plt.subplots()
x = np.array([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
ax.plot(x, delay_min, label='Min') 
ax.plot(x, delay_max, label='Max')
ax.plot(x, delay_avg, label='Mean') 
ax.set_xlabel('Rate')
ax.set_ylabel('Delay (s)') 
ax.set_title('Delay v.s. Rate') 
ax.set_xticks(x)
ax.legend() 
plt.show()
fig.savefig("Delay.jpg")

fig, ax = plt.subplots()
ax.plot(x, bw_min, label='Min') 
ax.plot(x, bw_max, label='Max')
ax.plot(x, bw_avg, label='Mean') 
ax.set_xlabel('Rate')
ax.set_ylabel('Bandwidth (bytes/s)') 
ax.set_title('Bandwidth v.s. Rate') 
ax.set_xticks(x)
ax.legend() 
plt.show()
fig.savefig("Bandwidth.jpg")

