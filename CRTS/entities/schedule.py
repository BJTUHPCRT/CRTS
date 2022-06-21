import pandas as pd


# machine_cpu = [1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10]
# machine_mips = [5,10,5,10,5,10,5,10,5,10,5,10,5,10,5,10,5,10,10]
machine_cpu = [8,8,9,9,10,10,11,11,12,12]
machine_mips = [5,10]

df  = pd.read_csv('../load/GM0_GM1_sum_10s.csv')
df1 = pd.read_csv('../load/GM1_top10min_load_all.csv')
df2 = pd.read_csv('../load/GM0_top10min_load_all.csv')

a1 = df1.iloc[0,1]
a2 = df2.iloc[0,1]
count = 1000000 * 60 # 1分钟
b1 = a1 + count
b2 = a2 + count


load1_cpu = []
load2_cpu = []
load1_size = []
load2_size = []
count1 = 0
count2 = 0
s1 = count1
s2 = count2
for i,t in enumerate(df1['timestamp']):
    if t >= a1 and t<=b1:
        count1 += 1

    else:
        print(s1, i, count1)
        load1_cpu.append(sum(df1.iloc[s1:i+1,2]))
        load1_size.append(sum(df1.iloc[s1:i+1,4]))
        a1 = b1
        b1= a1 + count
        s1 = count1

for i,t in enumerate(df2['timestamp']):
    if t >= a2 and t<=b2:
        count2 += 1

    else:
        print(s2, i, count2)
        load2_cpu.append(sum(df2.iloc[s2:i+1,2]))
        load2_size.append(sum(df2.iloc[s2:i+1,4]))
        a2 = b2
        b2= a2 + count
        s2 = count2
print(load1_cpu)
print(load2_cpu)
print(load1_size)
print(load2_size)