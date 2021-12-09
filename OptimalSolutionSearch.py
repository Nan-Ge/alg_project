import copy


def findNextJobReverse(rs):
    for i in range(len(rs.sortedJobIdx) - 1, -1, -1):
        if rs.jobCore[rs.sortedJobIdx[i]] <= 0:
            return rs.sortedJobIdx[i]
    return -1


def withdrawJobAssignment(rs, jobID):
    rs.jobFinishTime[jobID] = 0
    rs.jobCore[jobID] = 0

    jobHostCore = []
    for idx, block in enumerate(rs.runLoc[jobID]):
        hostID, coreID, rank = block
        if (hostID, coreID) not in jobHostCore:
            jobHostCore.append((hostID, coreID))
        rs.runLoc[jobID][idx] = (-1, -1, -1)

    for (hostID, coreID) in jobHostCore:
        while 1:
            if rs.hostCoreTask[hostID][coreID][-1][0] == jobID:
                rs.hostCoreTask[hostID][coreID].pop()
            else:
                break


def fillVacancy(rs, lastJob):
    limitFinisTime = max(rs.jobFinishTime)
    while 1:
        lastFinishJobs, availableCores = findAvailableCores(rs)
        findAvailableJob = 0
        for jobIdx in rs.sortedJobIdx:
            if rs.jobCore[jobIdx] <= 0 and jobIdx != lastJob:
                if findAvailableJob != 0:
                    uShapeAssign(rs, jobIdx, lastFinishJobs[1], availableCores[1])
                    if max(rs.jobFinishTime) > limitFinisTime:
                        withdrawJobAssignment(rs, jobIdx)
                    else:
                        uShapeAssign(rs, jobIdx, lastFinishJobs[1], availableCores[1])
                        findAvailableJob = 1

        if findAvailableJob == 0:
            break


def calcIdleRation(rs):
    totalTime = sum(rs.hostCore) * max(rs.jobFinishTime)
    totalIdleTime = 0
    for host in rs.hostCoreTask:
        for core in host:
            for index, task in enumerate(core):
                currentJob = task[0]

                # currentJob在当前核上，后面没有job
                if index == len(core) - 1:
                    totalIdleTime += max(rs.jobFinishTime) - task[3]
                # currentJob在当前核上，后面还有job
                elif currentJob != core[index + 1][0]:  # 表示找到currentJob的最后一个task
                    totalIdleTime += rs.jobFinishTime[currentJob] - task[3]

    return totalIdleTime / totalTime


def findAvailableCores(rs):
    FinishJob = []  # 当前hostCoreTask，最后完成的若干job
    for host in rs.hostCoreTask:
        for core in host:
            if core[-1][0] not in FinishJob:
                FinishJob.append(core[-1][0])

    FinishJobTime = [rs.jobFinishTime[job] for job in FinishJob]
    sortedJobIdx = sorted(range(len(FinishJobTime)), key=lambda k: FinishJobTime[k], reverse=True)  # 按照运行时间从大到小排序

    '''注意：[[] * 10] 与 [[] for i in range(10)] 的区别'''
    availableCores = [[] for i in range(len(FinishJobTime))]

    for idx, jobIdx in enumerate(sortedJobIdx):
        fTime = rs.jobFinishTime[FinishJob[jobIdx]]
        for hostID, host in enumerate(rs.hostCoreTask):
            for coreID, core in enumerate(host):
                if core[-1][3] <= fTime:
                    availableCores[idx].append((hostID, coreID))

    lastFinishJobs = [FinishJob[idx] for idx in sortedJobIdx]

    if len(FinishJob) == 1:
        lastFinishJobs.append(lastFinishJobs[0])
        availableCores.append(availableCores[0])

    return lastFinishJobs, availableCores


def uShapeAssign(rs, jobID, finishJob, availableCore):
    jobIDFinishTime = -1
    numCore = len(availableCore)

    if numCore <= rs.jobBlock[jobID]:
        rs.jobCore[jobID] = numCore
    else:
        rs.jobCore[jobID] = rs.jobBlock[jobID]

    jobSpeed = rs.Sc[jobID] * rs.g(rs.jobCore[jobID])
    jobBlockTimeConsumption = [i / jobSpeed for i in rs.dataSize[jobID]]  # 计算jobID的每个Block的运行时间
    sortedBlockIdx = sorted(range(len(jobBlockTimeConsumption)),
                            key=lambda k: jobBlockTimeConsumption[k], reverse=True)  # 按照运行时间从大到小排序

    # U形分配
    jobStartTime = rs.jobFinishTime[finishJob]
    u_ptr = 0
    u_ptr_move = 1
    for blockIdx in sortedBlockIdx:
        hostID = availableCore[u_ptr][0]  # 目标host
        coreID = availableCore[u_ptr][1]  # 目标core

        if rs.hostCoreTask[hostID][coreID][-1][0] != jobID:  # 当前核当前job的第1个block
            startTime = jobStartTime
        else:  # 当前核当前job的第x(x>1)个block
            startTime = rs.hostCoreTask[hostID][coreID][-1][3]

        if rs.taskType == 1:
            endTime = startTime + jobBlockTimeConsumption[blockIdx]
        elif rs.taskType == 2:
            if hostID != rs.location[jobID][blockIdx]:
                endTime = startTime + jobBlockTimeConsumption[blockIdx] + rs.dataSize[jobID][blockIdx] / rs.St
            else:  # 待分配的Block不需要传输
                endTime = startTime + jobBlockTimeConsumption[blockIdx]

        if endTime > jobIDFinishTime:  # 记录当前分配job的完成时间
            jobIDFinishTime = endTime

        rs.hostCoreTask[hostID][coreID].append((jobID, blockIdx, startTime, endTime))
        rs.runLoc[jobID][blockIdx] = (hostID, coreID, len(rs.hostCoreTask[hostID][coreID]) - 1)

        u_ptr += u_ptr_move

        if u_ptr == numCore:
            u_ptr_move = -1
            u_ptr += u_ptr_move
        elif u_ptr == -1:
            u_ptr_move = 1
            u_ptr += u_ptr_move

    # 更新系统参数
    rs.jobFinishTime[jobID] = jobIDFinishTime


def findOptimalJobAssignment(rs, jobID):  # 进行 “1+x+1”的job分配
    initLastFinishJobs, initAvailableCores = findAvailableCores(rs)
    idleRatio = []
    candidateRs = []

    for numCore in range(1, len(initAvailableCores[1]) + 1):
        new_rs = copy.deepcopy(rs)

        # 1.U形分配job "1"
        uShapeAssign(new_rs, jobID, initLastFinishJobs[1], initAvailableCores[1][0: numCore])

        # 2.循环分配job "x"
        while 1:
            lastFinishJobs, availableCores = findAvailableCores(new_rs)  # 寻找可分配的核
            nextJob = findNextJobReverse(new_rs)
            if nextJob == -1:
                break
            uShapeAssign(new_rs, nextJob, lastFinishJobs[-1], availableCores[-1])
            if new_rs.jobFinishTime[nextJob] > new_rs.jobFinishTime[jobID]:
                beforeReassignFinishTime = max(new_rs.jobFinishTime)
                break

        # 3.两种情况的判定，分配最后1个job
        if nextJob != -1:
            withdrawJobAssignment(new_rs, nextJob)  # 撤销最后一个job的分配
            uShapeAssign(new_rs, nextJob, lastFinishJobs[0], availableCores[0])  # 重新分配最后一个job
            afterReassignFinishTime = max(new_rs.jobFinishTime)
            withdrawJobAssignment(new_rs, nextJob)  # 撤销最后一个job的分配

            if beforeReassignFinishTime <= afterReassignFinishTime:
                uShapeAssign(new_rs, nextJob, lastFinishJobs[-1], availableCores[-1])  # 维持原分配方案
            else:
                fillVacancy(new_rs, nextJob)  # 填充空白区
                uShapeAssign(new_rs, nextJob, lastFinishJobs[0], availableCores[0])  # 重新分配最后一个job

        idleRatio.append(calcIdleRation(new_rs))
        candidateRs.append(new_rs)

    optimalRsIdx = idleRatio.index(min(idleRatio))
    return candidateRs[optimalRsIdx]


def GreedySearch(rs):
    for idx in rs.sortedJobIdx:
        if rs.jobCore[idx] <= 0:
            rs = findOptimalJobAssignment(rs, idx)
    return rs
