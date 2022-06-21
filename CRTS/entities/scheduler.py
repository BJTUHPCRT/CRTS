import pandas as pd
import random

# machine_cpu = [1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10]
# machine_mips = [5,10,5,10,5,10,5,10,5,10,5,10,5,10,5,10,5,10,10]
#machine_cpu = [9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7 ,9.8, 9.9, 10]

machine_cpu = []
for i in range(150):
    if i < 50 :
        machine_cpu.append(0.25)
    if i > 50 and i< 120:
        machine_cpu.append(0.5)
    else:
        machine_cpu.append(1)


machine_mips = []
for i in range(150):
    if i < 20 :
        machine_mips.append(50)
    if i > 20 and i<= 50:
        machine_mips.append(80)
    if i > 50 and i <= 70:
        machine_mips.append(120)
    if i > 70 and i <=90:
        machine_mips.append(200)
    if i > 90 and i <= 120:
        machine_mips.append(300)
    if i > 120 and i <= 140:
        machine_mips.append(400)
    else:
        machine_cpu.append(500)

machine_idlepowers = [40,80,40,80,40,80,40,80,40,80] # 可以用list定义增大异构性

machine_activepowers = [100,200,100,200,100,200,100,200,100,200]
machine_utilization = []
machine_utilization1 = []
machine_utilization2_1 = []
machine_utilization2_2 = []
machine_utilization3_1 = []
machine_utilization3_2 = []
p_idle = 0.7

print("----------------全部任务-----------------")
df  = pd.read_csv('googleClusterTrace.csv')
# df = df.iloc[:20]
h_max = max(machine_cpu)
pro = []
k_list = []
i_list = []
pro1_list = []
pro2_list = []
pro_list = []

for i in range(len(df['sum'])):
    machine_utilization.append(0)
    machine_utilization1.append(0)
    machine_utilization2_1.append(0)
    machine_utilization2_2.append(0)
    machine_utilization3_1.append(0)
    machine_utilization3_2.append(0)

def dataint(down,up,k):
    """
    产生多维随机整数
    :param down:
    :param up:
    :param k:
    :return:
    """
    data=[]
    for i in range(k):
        temp = random.uniform(down, up)
        data.append(temp)
    return data
file1_size = dataint(5, 100,len(df['sum']))
file2_size = dataint(10, 100,len(df['sum']))


for k, load in enumerate(df['GM0']):
    for i in range(len(machine_cpu)):
        for j in range(len(machine_mips)):
            if df.iloc[k,2] < machine_cpu[i] and df.iloc[k,0]<machine_cpu[i] and df.iloc[k,1]<machine_cpu[i]:
                pro1 = (h_max-machine_cpu[i]+df.iloc[k,0]) / h_max
                pro2 = (h_max-machine_cpu[i]+df.iloc[k,1]) / h_max
                k_list.append(k+1)
                i_list.append(i)
                pro1_list.append(pro1)
                pro2_list.append(pro2)
                pro_list.append(pro1*pro2)
                pro.append([k+1,i,pro1,pro2,pro1*pro2])
# ans = pd.DataFrame({"第k组任务": k_list, "第i台服务器":i_list, "第1个任务放到第i个服务器上的概率":pro1_list,"第2个任务放到第i个服务器上的概率":pro1_list, "第k组任务放到第i台服务器的联合概率":pro_list})
# ans.to_csv("../load/task_scedule_highthan9.csv",index=False)
# print(pro)

power = 0
response_time = 0
execute_time = 0
waiting_time = 0
total_execute_time = 0

for i,load in enumerate(df['sum']):
    machine_utilization[i] = (load) / machine_cpu[4]
    power += machine_idlepowers[i%2] + machine_utilization[i]*machine_activepowers[i%2]
             #* (load/10)
    execute_time =  (file1_size[i]+file2_size[i])/15
    #response_time =  (file1_size[i]+file2_size[i])/20
    total_execute_time += execute_time
    if execute_time>5:

        waiting_time += 5
        for j in range(len(machine_cpu)):
            if j in (4,5):
                continue
            power += machine_idlepowers[j]
response_time = total_execute_time + waiting_time
# for i in enumerate(df['sum']):
#     response_time = (file1_size[i]+file2_size[i])/20
#     if response_time>10:
#         total_time += response_time
#         total_time += 10

# for i in range(len(machine_cpu)):
#     if i in (4,5):
#         continue
#     power += machine_idlepowers[i]*len(df)
print("power-----{}".format(power))
print("response_time---{}".format(response_time))
print("total_time: {}".format(total_execute_time))

# for i,size in enumerate(file_size):
#     if i > 1:


# 轮询算法
task1_list = []
task2_list = []
powers = 0
response_time_1 = 0
execute_time_1 = 0
waiting_time_1 = 0
total_execute_time1 = 0
for i in range(len(df['sum'])):
    #print([i,machine_cpu[i%10]])
    machine_utilization1[i] = (load) / machine_cpu[i%9]
    # powers += p_idle*machine_idlepowers[i%10] +(df.iloc[i,0]/machine_cpu[i%10])/machine_cpu[i%10]*machine_activepowers[i%10]*(1-p_idle)
    # powers += p_idle*machine_idlepowers[i%10] +(df.iloc[i,1]/machine_cpu[i%10])/machine_cpu[i%10]*machine_activepowers[i%10]*(1-p_idle)
    powers += machine_idlepowers[i % 9] + (df.iloc[i, 0] / machine_cpu[i % 9]) / machine_cpu[i % 9] * \
              machine_activepowers[i % 9] * machine_utilization1[i]
    powers += machine_idlepowers[i % 9] + (df.iloc[i, 1] / machine_cpu[i % 9]) / machine_cpu[i % 9] * \
              machine_activepowers[i % 9] * machine_utilization1[i]
    execute_time_1 = (file1_size[i]/machine_mips[i%9])+(file2_size[i]/machine_mips[(i+1)%9])
    #response_time_1 = (file1_size[i]/machine_mips[i%9])+(file2_size[i]/machine_mips[(i+1)%9])
    #print(response_time_1)
    total_execute_time1 += execute_time_1
    if execute_time_1>5:
        waiting_time_1 += 5
    for j in range(len(machine_cpu)):
        if j == machine_cpu[i%9]:
            continue
        powers += machine_idlepowers[j]
response_time_1 = total_execute_time1 + waiting_time_1
# for i in enumerate(df['sum']):
#     response_time = (file1_size[i]+file2_size[i])/machine_mips[i%2]
#     if response_time>10:
#         total_time1 += response_time
#         total_time1 += 10
print("能耗------{}".format(powers))
print("response_time---{}".format(response_time_1))
print("total_time： {}".format(total_execute_time1))


# 随机算法
k1 = random.randint(0,9)
k2 = random.randint(0,9)
powerss = 0
response_time_2 = 0
waiting_time_2 = 0
execute_time_2 = 0
total_execute_time2 = 0
#index1 = random.sample(range(0, 10), 1)


for i in range(len(df['sum'])):

    index1 = random.randint(1, 9)
    index1 = index1
    index2 = random.randint(1, 9)
    index2 = index2
    machine_utilization2_1[i] = df.iloc[i, 0] / machine_cpu[index1]
    machine_utilization2_2[i] = df.iloc[i, 1] / machine_cpu[index2]
    #print([i,machine_cpu[i%10]])

    # powerss += p_idle*machine_idlepowers[k1] +(df.iloc[i,0]/machine_cpu[k1])/machine_cpu[k1]*machine_activepowers[k1]*(1-p_idle)
    # powerss += p_idle*machine_idlepowers[k2] +(df.iloc[i,1]/machine_cpu[k2])/machine_cpu[k2]*machine_activepowers[k2]*(1-p_idle)
    powerss +=  machine_idlepowers[index1] + (df.iloc[i, 0] / machine_cpu[index1]) / machine_cpu[index1] * \
               machine_activepowers[index1] * machine_utilization2_1[i]
   # index2 = random.sample(range(0, 10), 1) - 1
    powerss +=  machine_idlepowers[index2] + (df.iloc[i, 1] / machine_cpu[index2]) / machine_cpu[index2] * \
               machine_activepowers[index2] * machine_utilization2_2[i]
    execute_time_2 = (file1_size[i]/machine_mips[index1])+(file2_size[i]/machine_mips[index2])
    total_execute_time2 += execute_time_2
    if execute_time_2>5:

        waiting_time_2 += 5
    for j in range(len(machine_cpu)):
        if j in (index1, index2):
            continue
        powerss += machine_idlepowers[j]
response_time_2 = total_execute_time2 + waiting_time_2
# for i in enumerate(df['sum']):
#     response_time = (file1_size[i]+file2_size[i])/machine_mips[i%2]
#     if response_time>10:
#         total_time2 += response_time
#         total_time2 += 10
print("能耗----{}".format(powerss))
print("response_time---{}".format(response_time_2))
print("total_time: {}".format(total_execute_time2))
# 贪婪算法
# k11 = random.randrange(1,9,2)
# k22 = random.randrange(1,9,2)
k11 = 9
k22 = 8
# print("k11:")
# print(k11)
powersss = 0
response_time_3 = 0
execute_time_3 = 0
waiting_time_3 = 0
total_execute_time3 = 0
for i in range(len(df['sum'])):
    #print([i,machine_cpu[i%10]])
    machine_utilization3_1[i] = df.iloc[i,0] / machine_cpu[k11]
    machine_utilization3_2[i] = df.iloc[i,1] / machine_cpu[k22]
    powersss += machine_idlepowers[k11] +(df.iloc[i,0]/machine_cpu[k11])/machine_cpu[k11]*machine_activepowers[k11]*machine_utilization3_1[i]
    powersss += machine_idlepowers[k22] +(df.iloc[i,1]/machine_cpu[k22])/machine_cpu[k22]*machine_activepowers[k22]*machine_utilization3_2[i]
    execute_time_3 = (file1_size[i]/machine_mips[k11])+(file2_size[i]/machine_mips[k22])
    total_execute_time3 += execute_time_3
    if execute_time_3>5:

        waiting_time_3 += 5
    for j in range(len(machine_cpu)):
        if j in (k11, k22):
            continue
        powersss += machine_idlepowers[j]
response_time_3 = total_execute_time3 + waiting_time_3
# for i in enumerate(df['sum']):
#     response_time = (file1_size[i]+file2_size[i])/machine_mips[i%2]
#     if response_time>10:
#         total_time3 += response_time
#         total_time3 += 10
print("能耗-----{}".format(powersss))
print("response_time---{}".format(response_time_3))
print("total_time:{}".format(total_execute_time3))