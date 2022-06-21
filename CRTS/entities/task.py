
"""
this class is from GoogleCluster datasets' task event table,the real workload, which is the smallest scheduling unit.
"""

#import loadDataset.loadGoogleDataset
import random
import math
class Task:
    def __init__(self,task_events):
        self.timestamp = task_events[0]           # task created time
        self.missingInfo = task_events[1]
        self.jobID = task_events[2]
        self.taskIndex = task_events[3]           #jobID + index as the taskID
        self.machineID = task_events[4]
        self.eventType = task_events[5]           # the task state: be submitted or get scheduled and become runnable
        self.userName = task_events[6]
        self.schedulingClass = task_events[7]     # represents how latency-sensitive it is,3 representing a more latency-sensitive task
        self.priority = task_events[8] # free：lowest priority production: highest priority
        if math.isnan(task_events[9]):    #和含有cpu个数最大的machine的cpu数量进行的正则化
            self.CPURequest = 0.0
        else:
            self.CPURequest = task_events[9]
        self.RAMRequest = task_events[10]
        self.DiskRequest = task_events[11]
        self.machineConstraint = task_events[12]
        self.size = task_events[13]

        self.waitingTime = 0
        self.responseTime = 0
        self.executeTime = 0   #应改成size，执行时间是计算出来的

        #self.executeTime = 1
        self.arrival_Time = 0
        self.execute_startTime = 0#每个task应该有自己的开始时间，时间戳只提供task间隔
        self.real_execute_Time = 0
