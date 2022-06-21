"""
   environment is  simulating the datacenter.
"""
from loadDataset import smalltest
from entities import machine
from entities import task
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import math
import time
import logging
import pandas
import copy

class environment:
    def __init__(self, end='no_new_task'):
        #job and machine are lists
        # self.job = job
        # self.machine = machine
        #self.schedulingPolicy = schedulingPolicy
        self.tasks = []
        self.task_reset = []
        self.current_task = None
        self.task_number = 0
        self.machines = []
        self.machines_reset = []
        self.action_len = 0
        self.state_len = 0
        self.total_power = []
        self.energy = 0
        self.total_job_latency = []
        self.average_job_waitingTime = 0
        self.action_job_waitingTime = []
        self.execute_taskNumber = 0
        # self.reward = []
        self.theta = 0.3 #trade-off of power and job latncy
        #self.task_allocated
        self.task_allocate_counter = 0
        self.done = False
        self.start = True
        #控制变化的参数
        self.unchange_task_number = 0 #因为第一个task没有算到平均中
        self.detection_list = []
        self.delaytime_sum = 0
        self.detection_number = 0
        self.stage_total_task_cpu = 0
        self.average_task_cpu = 0
        #self.machine_available

        # self.action_space = []

        # self.DQN_rewards = []
        # self.onestep_reward = []
        # self.sum_onestep_reward = 0
        self.onestep_counter = []
        self.onestep_fixedstep = 5
        self.onestep_currentstep = 0
        self.onestep_machines = []

        # self.fluction_index = [8, 1, 1, 1, 5, 2, 0, 1, 1, 1, 1, 0, 3, 2, 0]
        self.fluction_index = [6, 1, 5, 4, 2, 5, 6, 5, 4, 0, 4, 0, 3, 5]
        self.fluction_counter = 0

        #initialize system
        import sys
        path = sys.path[0] + '\cpus\\'
        task_events, machine_events = smalltest.test_workload()
        task_cpu = pandas.read_csv(path + 'gauss_sim_cpu_w2.csv',sep=',', encoding='utf-8',engine='python')
        cpu_list = task_cpu.values
        task_cpu = pandas.read_csv(path + 'sim_mi_random.csv', sep=',', encoding='utf-8', engine='python')
        mi_list = task_cpu.values

        tt = task_events[0, :]
        for i in range(3000):
            if not math.isnan(task_events[i, 9]):
                task_events[i, 0] = i #定义时间戳
                task_events[i, 9] = cpu_list[i, 0]
                task_events[i, 13] = mi_list[i, 0]
                self.tasks.append(task.Task(task_events[i, :]))
            else:
                continue

        self.task_number = len(self.tasks)
        self.tasks.sort(key=lambda x: x.timestamp, reverse=False)  #workload
        self.task_reset = self.tasks

        machine_CPUCapcity = [10, 10, 15, 10, 10, 10, 10, 10, 15, 10,
                              15, 10, 10, 10, 15, 10, 10, 10, 15, 10,
                              20, 10, 10, 10, 10, 20, 10, 10, 10, 10]

        machine_mips = [100, 20, 200, 60, 100, 100, 20, 200, 40, 100,
                        20, 400, 200, 80, 200, 100, 800, 100, 400, 40,
                        100, 300, 200, 50, 800, 400, 300, 100, 800, 50]  # 没有按照与machine的CPU成比例，增大异构性

        machine_idlepowers = [100, 60, 200, 80, 100, 100, 60, 150, 40, 60,
                          10, 400, 200, 80, 200, 100, 800, 100, 400, 40,
                          100, 300, 200, 50, 800, 400, 300, 100, 800, 50]

        machine_activepowers = [ 200, 180, 100, 180, 200, 200, 180, 100, 80, 200,
                          70, 120, 100, 180, 100, 200, 300, 200, 120, 160,
                          120, 220, 100, 170, 300, 160, 220, 100, 300, 170]

        for i in range(30):
            machine_temp = machine.Machine(machine_events[i, :])
            machine_temp.machineID = i
            machine_temp.CPUCapacity = machine_CPUCapcity[i]
            machine_temp.availableCPU = machine_CPUCapcity[i]
            machine_temp.service_rate = machine_mips[i]
            machine_temp.idlePower = machine_idlepowers[i]
            machine_temp.activePower = machine_activepowers[i]
            self.machines.append(machine_temp)
            self.machines_reset.append(machine_temp)
            self.action_space.append(machine_temp.machineID)
        #self.machines_reset = self.machines
        self.action_len = len(self.action_space)

    def observe(self):
        # 每次都现取要分配的task
        arriving_task = self.get_new_task()  # 只需要改变task_allocate_counter就可以实现取800个task

        # observation 应该是task队列中未分配组成的队列 和 可用machine队列,应是整个环境的观测值
        # 暂时简化只用state当观测值
        # current task is a part of observation,and DQN take an acti  on of current from the observation
        self.current_task = arriving_task
        observation = np.zeros((1, self.action_len + 1))  # dtype=tf.float32
        if arriving_task is None:
            observation[0, 0] = 0.0
        else:
            observation[0, 0] = arriving_task.CPURequest
            # print("task cpu request:", task_new.CPURequest)
        # print(observation[0,0])
        count = len(self.machines)
        Index = 1
        for i in range(count):
            observation[0, Index] = round(
                (self.machines[i].CPUCapacity - self.machines[i].availableCPU) / self.machines[i].CPUCapacity, 2)
            Index += 1
        self.state_len = len(observation)
        return observation

    def DQNobserve(self,tmp_task):
        # 每次都现取要分配的task
        arriving_task = tmp_task  # 只需要改变task_allocate_counter就可以实现取800个task

        # observation 应该是task队列中未分配组成的队列 和 可用machine队列,应是整个环境的观测值
        # 暂时简化只用state当观测值
        # current task is a part of observation,and DQN take an acti  on of current from the observation
        self.current_task = arriving_task
        observation = np.zeros((1, self.action_len + 1))  # dtype=tf.float32
        if arriving_task is None:
            observation[0, 0] = 0.0
        else:
            observation[0, 0] = arriving_task.CPURequest
            # print("task cpu request:", task_new.CPURequest)
        # print(observation[0,0])
        count = len(self.machines)
        Index = 1
        for i in range(count):
            observation[0, Index] = round(
                (self.machines[i].CPUCapacity - self.machines[i].availableCPU) / self.machines[i].CPUCapacity, 2)
            Index += 1
        self.state_len = len(observation)
        return observation

    def step(self, observation, action):
        #进行action执行阶段，真正进行任务分配的阶段
        done = False
        workload_changed = False
        fluction_index = -1
        reward = 0
        observation_ = copy.deepcopy(observation)


        #当前action分配的任务 is current task' action,not next task
        current_task = self.current_task
        # print('current task', self.task_allocate_counter)
        if current_task != None:
            self.onestep_machines = copy.deepcopy(self.machines)
            # time.sleep(0.01) #保证每个task的arrival time都不一样
            # action = 1
            self.machines[action].allocate_task(current_task)
            current_task.machineID = action
            #生成观测值
            Index = 1 + action
            observation_[0, Index] = math.fabs(round((self.machines[action].CPUCapacity - self.machines[action].availableCPU) / self.machines[action].CPUCapacity, 2))

            reward = self.get_reward(current_task, self.machines[action], action)  #将machine当前状态传进去，避免线程执行状态被破坏

        self.reward.append(reward)

        if self.task_allocate_counter == self.task_number:
            done = True

        if self.task_allocate_counter != 0 and (self.task_allocate_counter + 10) % 200 == 0 and self.task_allocate_counter != 2990:
            workload_changed = True
            fluction_index = self.fluction_index[self.fluction_counter]
            if self.fluction_counter < len(self.fluction_index):
                self.fluction_counter += 1
                # print(self.fluction_counter)
            else:
                self.fluction_counter = 0
        return observation_, reward, done, workload_changed, fluction_index  #第一个和最后一个task的记录应不应该存储？？

    def temp_step(self,action,observation):
        # 进行action执行阶段，真正进行任务分配的阶段
        power = 0
        job_latency = 0

        # 当前action分配的任务 is current task' action,not next task
        current_task = self.current_task
        # we should judge if the task queue is empty
        if current_task != None:
            self.machines[action].allocate_task(current_task)
            current_task.machineID = action

            reward = self.get_reward(current_task, self.machines[action],action)
            # # 生成one step rewards
            # tmp_rewards = []
            # for i in range(len(self.onestep_machines)):
            #     rrward = self.get_onestep_reward(current_task, self.onestep_machines[i], i)
            #     tmp_rewards.append(rrward)
            #
            # min_reward = min(tmp_rewards)
            # self.onestep_reward.append(min_reward)
            # # self.sum_onestep_reward += max_reward
            # # onestep_action = tmp_rewards.index(max_reward)
            # self.onestep_currentstep += 1
            # if self.onestep_currentstep == self.onestep_fixedstep:
            #     sum_1 = np.sum(self.onestep_reward)
            #     sum_2 = np.sum(self.DQN_rewards)
            #     # print(sum_1, sum_2)
            #     improvement = sum_1 - sum_2
            #
            #     reward -= improvement
            #
            #     self.onestep_currentstep = 0
            #     self.onestep_reward = []
            #     self.DQN_rewards = []
        self.reward.append(reward)

        observation_ = copy.deepcopy(observation)
        #生成观测值
        Index = 1 + action
        observation_[0, Index] = math.fabs(round((self.machines[action].CPUCapacity - self.machines[action].availableCPU) / self.machines[action].CPUCapacity, 2))

        return observation_, reward  # 第一个和最后一个task的记录应不应该存储？？

    # def standerd_rewards(self, reward):
    #     average_r = 0
    #     for i in range(len(reward)):
    #         average_r += reward[i]
    #     average_r = average_r / len(reward)
    #
    #     return average_r

    def get_new_task(self):
        task = None
        if self.task_allocate_counter < self.task_number:
            task = self.tasks[self.task_allocate_counter]
            self.task_allocate_counter += 1
        else:
            self.task_allocate_counter = 0
        if task != None:
            self.detection_list.append(task.CPURequest) #为异常检测做准备
        return task

    # def get_reward(self, current_task, machine, action):
    #     total_power = 0
    #     waiting_delay = 0
    #     for i in range(len(self.machines)):
    #         self.machines[i].utilization = (self.machines[i].CPUCapacity - self.machines[i].availableCPU) / self.machines[i].CPUCapacity
    #         total_power += self.machines[i].idlePower + self.machines[i].activePower * self.machines[i].utilization
    #     current_job_response_time = 0
    #     time_remaining = 0
    #     #反应此任务的分配结果好坏
    #     #reward一定要反应当前job分配产生的反馈
    #     #这种反馈目标是负载均衡
    #     machine_id = current_task.machineID
    #
    #     if current_task in machine.running_task:
    #         current_job_response_time = current_task.size / machine.service_rate
    #
    #     #这里不考虑running中的task时间
    #     #这里显示的是队列长短，machine执行的越快，队列越短，能一定程度上体现异构
    #     if current_task in machine.waiting_task:
    #         for k in range(len(machine.waiting_task)):
    #             try:
    #                 if machine.waiting_task[k] is not None:
    #                     time_remaining = machine.waiting_task[k].executeTime  #已经加了当前task的执行时间了
    #                 else:
    #                     time_remaining = 0
    #                 current_job_response_time += time_remaining
    #                 # current_job_response_time += waiting_delay
    #             except: pass
    #     # 生成one step rewards
    #     tmp_rewards = []
    #     for i in range(len(self.onestep_machines)):
    #         rrward = self.get_onestep_reward(current_task, self.onestep_machines[i], i)
    #         tmp_rewards.append(rrward)
    #
    #     min_reward = min(tmp_rewards)
    #     reward = current_job_response_time - min_reward
    #     # self.DQN_rewards.append(reward) #只考虑响应时间
    #
    #     self.action_job_waitingTime.append(current_job_response_time) #每个action对应的响应时间,不按照时间，而是队列情况，与实际waitingTime不同
    #
    #     return reward

    # def get_onestep_reward(self, current_task,machine,action):
    #     total_power = 0
    #     waiting_delay = 0
    #     current_job_response_time = 0
    #
    #     excute_time = current_task.size / machine.service_rate
    #
    #     if machine.waiting_task:
    #         for k in range(len(machine.waiting_task)):
    #             if machine.waiting_task[k] is not None:
    #                 time_remaining = machine.waiting_task[k].executeTime  #在这里没有加当前task的执行时间
    #             else:
    #                 time_remaining = 0
    #             current_job_response_time += time_remaining
    #
    #     reward = excute_time + current_job_response_time
    #
    #     return reward

    def compute_power(self):
        power = 0
        if self.done == False:
            for i in range(len(self.machines)):
                self.machines[i].utilization = (self.machines[i].CPUCapacity - self.machines[i].availableCPU) / self.machines[i].CPUCapacity
                power += self.machines[i].idlePower + self.machines[i].activePower * self.machines[i].utilization
            self.total_power.append(power)
            self.energy += 2 * power

        # print('power:', power)
    def compute_latency(self):
        job_latency = 0
        if self.done == False:
            for i in range(len(self.machines)):
                for j in range(len(self.machines[i].running_task)):
                    job_latency += 1

                for k in range(len(self.machines[i].waiting_task)):
                    job_latency += 1
            self.total_job_latency.append(job_latency)

    def compute_average_response_time(self):
        total_response_time = 0
        taskNumber = 0
        self.execute_taskNumber = 0
        for i in range(len(self.machines)):
            total_response_time += self.machines[i].total_task_responseTime
            taskNumber += self.machines[i].taskNumber
            self.execute_taskNumber += self.machines[i].execute_taskNumber

        self.average_job_waitingTime = total_response_time / self.task_number

    def compute_executed_taskNumber(self):
        self.execute_taskNumber = 0
        for i in range(len(self.machines)):
            self.execute_taskNumber += self.machines[i].execute_taskNumber
        # print(self.execute_taskNumber)
        if self.execute_taskNumber == self.task_number:
            environment.done = True
            print(self.execute_taskNumber)
            return False
        else:
            return True

    def reset(self):
        self.task_allocate_counter = 0
        self.average_job_waitingTime = 0
        self.done = False
        self.start = True
        self.tasks = self.task_reset
        print('reset:',self.tasks[0].waitingTime)
        self.machines = self.machines_reset

        self.reward = []
        self.total_power = []
        self.total_job_latency = []
        self.action_job_waitingTime = []
        self.energy = 0

        self.unchange_task_number = 0  # 因为第一个task没有算到平均中
        self.delaytime_sum = 0
        self.detection_number = 0
        self.stage_total_task_cpu = 0
        self.average_task_cpu = 0
        self.fluction_counter = 0

        for i in range(len(self.machines)):
            self.machines[i].running_task = []
            self.machines[i].waiting_task = []
            self.machines[i].utilization = 0.0
            self.machines[i].availableCPU = self.machines[i].CPUCapacity
