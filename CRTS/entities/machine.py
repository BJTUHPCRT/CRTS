"""
this class is from GoogleCluster dataset's job event table,aims at help generating tasks  workload.
"""

import random

class Machine:
    def __init__(self,  machine_event):
        #self.timestamp = machine_event[0]
        self.machineID = machine_event[1]
        #self.eventType = machine_event[2]           # the state of machine:ADD(available) REMOVE(was removed) UPDATE(available with some resources updated)
        #self.platformID = machine_event[3]           # reflect the machine microarchitecture
        self.CPUCapacity = machine_event[4]   #machine中cpu的个数，和含有cpu最大的machine进行正则化后
        # temp = random.uniform(1, 2)
        # self.CPUCapacity = round(temp, 1)
        #self.MemoryCapacity = machine_event[5]
        self.availableCPU = machine_event[4]

        self.service_rate = 0

        self.running_task = []                      #running_task队列中的task是并行的
        self.waiting_task = []
        self.utilization = 0

        self.execute_taskNumber = 0
        self.idlePower = 70
        self.activePower = 30
        self.time_horizon = 0     #时间线

        self.can_stop = False
        self.total_task_waitingTime = 0
        self.total_task_responseTime = 0

        self.real_execute_time = 0
        self.clockSpeed = 0  # 自己定义的，体现machine的异构性
        self.taskNumber = 0

    def allocate_task(self, task):
        #self.check_if_have_task_finished(internal_time)
        #暂时假设等待队列无限大
        task.arrival_Time = self.time_horizon
        # print('machineID:',self.machineID,'taskID:',task.timestamp,'task arrival time',task.arrival_Time)
        task.executeTime = task.size / self.service_rate
        if len(self.waiting_task) == 0 and self.availableCPU >= task.CPURequest:
            #task实际执行时间短于需要的时间
            task.execute_startTime = self.time_horizon
            #task.startTime = 0
            self.running_task.append(task)
            self.availableCPU -= task.CPURequest
        else:
            self.waiting_task.append(task)
            # print(len(self.waiting_task))

        # self.utilization = (self.CPUCapacity - self.availableCPU) / self.CPUCapacity
        self.taskNumber += 1

    def check_if_have_task_finished(self):
        #print(" i am in machine" + self.machineID + "running job")
        self.time_horizon += 5               #时间前进20秒，判断这20内有没有任务执行完毕
        self.machine_running()

    def executed_all_tasks(self):
        #flag = True                         #标志while结束
        while len(self.waiting_task) or len(self.running_task):
           if(self.machine_running()):
               break

    def machine_running(self):
        # 真正执行的过程
        for i in range(len(self.running_task)):
            if self.running_task[i] is not None and (self.time_horizon - self.running_task[i].execute_startTime) >= self.running_task[i].executeTime:
                #求解task的时间
                real_execute_Time = self.time_horizon - self.running_task[i].execute_startTime
                waitingTime = self.running_task[i].execute_startTime - self.running_task[i].arrival_Time
                responseTime = waitingTime + real_execute_Time
                # print('taskID:', self.running_task[i].timestamp, 'waitingTime', waitingTime)
                # print('machineID:', self.machineID, 'taskID:', self.running_task[i].timestamp, 'task arrival time', self.running_task[i].arrival_Time)
                # print('task:',self.running_task[i].timestamp,waitingTime,responseTime)
                # print(self.running_task[i].executeTime, responseTime)
                # print(responseTime)
                #task的时间只能在machine上获得
                self.total_task_waitingTime += waitingTime
                self.total_task_responseTime += responseTime

                self.availableCPU += self.running_task[i].CPURequest
                # print("$$$$$$$$$$$$$$$$$$$running_task  task have executed:$$$$$$$$$$$$$$$$$$$", self.running_task[i].jobID,self.running_task[i].taskIndex)
                self.running_task[i] = None
                #num_remove_running_task.append(i)
                #self.running_task.remove(self.running_task[i])
                # 检查是否能放新task
                # 这里可以加等待队列那边的策略，这里是先到先得
                for next in range(len(self.waiting_task)):
                    if self.waiting_task[next] is not None and self.availableCPU > self.waiting_task[next].CPURequest:

                        self.running_task.append(self.waiting_task[next])
                        self.waiting_task[next].execute_startTime = self.time_horizon  # 任务执行开始的时间
                        self.availableCPU -= self.waiting_task[next].CPURequest
                        # print('machineID:', self.machineID, 'taskID:', self.waiting_task[next].timestamp,
                        #       'task arrival time', self.waiting_task[next].arrival_Time,'execute_startTime:',self.waiting_task[next].execute_startTime)
                        # print("$$$$$$$$$$$$$$$$$$waiting_task  task have entered the running task queue:$$$$$$$$$$$$$$$", self.waiting_task[next].jobID,self.waiting_task[next].taskIndex)
                        self.waiting_task[next] = None
                        #num_remove_waiting_task.append(next)
                        #self.waiting_task.remove(self.waiting_task[next])
                    else:
                        break #保证顺序进入running_task队列

                self.execute_taskNumber += 1
            else:
                continue #检查所有running_task中的task

        if (not self.running_task) and self.waiting_task:
            print('CPU', self.availableCPU, self.waiting_task[0].CPURequest)

        #统一移除，避免错误
        for j in range(len(self.running_task)):
            if None in self.running_task:
                self.running_task.remove(None)
                self.taskNumber -= 1
        for k in range(len(self.waiting_task)):
            if None in self.waiting_task:
                self.waiting_task.remove(None)

        flag = True              #负责跳while循环
        # self.utilization = (self.CPUCapacity - self.availableCPU) / self.CPUCapacity
        return flag

    def stop_machine(self):
        #len(self.running_task) == True，则列表为空
        if len(self.running_task) and len(self.waiting_task) :
            return False
        else:
            return True

