import copy
from OptimalSolutionSearch import GreedySearch


class ResourceScheduler:
    def __init__(self, taskType, caseID):
        filePath = "./input/task" + str(taskType) + "_case" + str(caseID) + ".txt"
        self.taskType = taskType
        with open(filePath, "r") as f:

            data = f.readline().splitlines()[0].split(' ')
            self.numJob = int(data[0])
            self.numHost = int(data[1])
            self.alpha = float(data[2])

            if taskType == 2:
                self.St = float(data[3])

            self.hostCore = []
            data = f.readline().splitlines()[0].split(' ')
            for d in data:
                self.hostCore.append(int(d))

            self.jobBlock = []
            data = f.readline().splitlines()[0].split(' ')
            for d in data:
                self.jobBlock.append(int(d))

            self.Sc = []
            data = f.readline().splitlines()[0].split(' ')
            for d in data:
                self.Sc.append(int(d))

            self.dataSize = []
            for i in range(self.numJob):
                data = f.readline().splitlines()[0].split(' ')
                data = [int(i) for i in data]
                self.dataSize.append(data)

            self.location = []
            for i in range(self.numJob):
                data = f.readline().splitlines()[0].split(' ')
                data = [int(i) for i in data]
                self.location.append(data)

            self.jobFinishTime = [0 for i in range(self.numJob + 1)]
            self.jobCore = [0 for i in range(self.numJob)]

            self.runLoc = [[(-1, -1, -1)] * self.jobBlock[i] for i in range(self.numJob)]

            self.hostCoreTask = [[[] for i in range(self.hostCore[i])] for i in range(self.numHost)]
            for i in range(self.numHost):
                for j in range(self.hostCore[i]):
                    self.hostCoreTask[i][j].append((-1, -1, 0, 0))

            self.hostCoreFinishTime = [[0] * self.hostCore[i] for i in range(self.numHost)]

            self.sortedJobIdx = []

    def schedule(self):
        # 1.确定job的分配顺序：按照job数据总量从大到小排序
        jobTotalConsumption = []
        for i in range(self.numJob):
            jobTotalConsumption.append(sum(self.dataSize[i]))
        jobTotalConsumption = [totalSize / self.Sc[idx] for idx, totalSize in enumerate(jobTotalConsumption)]
        self.sortedJobIdx = sorted(range(len(jobTotalConsumption)), key=lambda k: jobTotalConsumption[k], reverse=True)

        # 2.进行贪心搜索
        optimalRs = GreedySearch(self)

        # 3.更新参数hostCoreFinishTime
        for hostID, host in enumerate(optimalRs.hostCoreTask):
            for coreID, core in enumerate(host):
                optimalRs.hostCoreFinishTime[hostID][coreID] = core[-1][3]

        return optimalRs

    def outputSolutionFromBlock(self):
        print("\n----------Task (type:%d) Solution (Block Perspective):----------" % self.taskType)
        for i in range(self.numJob):
            jobSpeed = self.g(self.jobCore[i]) * self.Sc[i]
            print("\tJob %d obtains %d cores (speed=%.2f MB/s) and finishes at time %.2f" % (i, self.jobCore[i], jobSpeed, self.jobFinishTime[i]))
            for j in range(self.jobBlock[i]):
                print("\t\tBlock%d: H%d, C%d, R%d (time=%.2fs)" % (j, self.runLoc[i][j][0], self.runLoc[i][j][1], self.runLoc[i][j][2], self.dataSize[i][j] / jobSpeed))
        print("\n")

        print("The maximum finish time: %f" % max(self.jobFinishTime))
        print("The total response time: %f" % sum(self.jobFinishTime))

    def outputSolutionFromCore(self):
        print("\n----------Task (type:%d) Solution (Core Perspective):----------" % self.taskType)
        maxHostTime = 0
        totalRunningTime = 0
        for i in range(self.numHost):
            hostTime = max(self.hostCoreFinishTime[i])
            maxHostTime = max(hostTime, maxHostTime)
            totalRunningTime += sum(self.hostCoreFinishTime[i])
            print("Host %d finishes at time %f" % (i, hostTime))
            for j in range(self.hostCore[i]):
                print("\tCore %d has %d tasks and finishes at time %f" % (j, len(self.hostCoreTask[i][j]), self.hostCoreFinishTime[i][j]))
                for k in range(len(self.hostCoreTask[i][j])):
                    print("\t\tJ%d, B%d, runtime %f to %f" % (self.hostCoreTask[i][j][k][0], self.hostCoreTask[i][j][k][1], self.hostCoreTask[i][j][k][2], self.hostCoreTask[i][j][k][3]))
                print("\n")

        print("\n")

        print("The maximum finish time of hosts: %f" % maxHostTime)
        print("The total efficacious running time: %f" % totalRunningTime)
        print("Utilization rate: %f" % (totalRunningTime / sum(self.hostCore) / maxHostTime))

    def visualization(self):
        pass

    def g(self, numCore):
        if numCore > 10:
            numCore = 10
        return 1 - self.alpha * (numCore - 1)




