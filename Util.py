import random
import time


def generator(rs, taskType):
    random.seed(int(time.time()))
    rs.numJob = 5
    rs.numHost = 1
    rs.alpha = 0.08

    if taskType == 2:
        rs.St = 500

    minCore, maxCore = 3, 10
    minBlock, maxBlock = 5, 10
    minSize, maxSize = 50, 200
    minSpeed, maxSpeed = 20, 100

    rs.hostCore = [0] * rs.numHost
    rs.jobBlock = [0] * rs.numJob
    rs.Sc = [0] * rs.numJob
    rs.dataSize = [0] * rs.numJob
    rs.location = [0] * rs.numJob

    print("-----------Generator starts--------------")

    print("numJob=%d, numHost=%d, alpha=%d" % (rs.numJob, rs.numHost, rs.alpha))

    if taskType == 2:
        print("St=%d" % rs.St)

    print("\nhostCore:")
    for i in range(rs.numHost):
        rs.hostCore[i] = random.randint(minCore, maxCore)
    print(rs.hostCore)

    print("\njobBlock:")
    for i in range(rs.numJob):
        rs.jobBlock[i] = random.randint(minBlock, maxBlock)
    print(rs.jobBlock)

    print("\njobCalculatingSpeed:")
    for i in range(rs.numJob):
        rs.Sc[i] = random.uniform(minSpeed, maxSpeed)
    print(rs.Sc)

    print("\nblockDataSize:")
    for i in range(rs.numJob):
        rs.dataSize[i] = [0] * rs.jobBlock[i]
        for j in range(rs.jobBlock[i]):
            rs.dataSize[i][j] = random.randint(minSize, maxSize)
        print(rs.dataSize[i])

    print("\njobBlockInitialLocation:")
    for i in range(rs.numJob):
        rs.location[i] = [0] * rs.jobBlock[i]
        for j in range(rs.jobBlock[i]):
            rs.location[i][j] = random.randint(0, rs.numHost-1)
        print(rs.location[i])

    print("-----------Generator ends--------------")

    rs.jobFinishTime = [0 for i in range(rs.numJob)]
    rs.jobCore = [0 for i in range(rs.numJob)]

    rs.runLoc = [[(-1, -1, -1)] * rs.jobBlock[i] for i in range(rs.numJob)]

    rs.hostCoreTask = [[[] for i in range(rs.hostCore[i])] for i in range(rs.numHost)]
    for i in range(rs.numHost):
        for j in range(rs.hostCore[i]):
            rs.hostCoreTask[i][j].append((-1, -1, -1, -1))

    rs.sortedJobIdx = []

    rs.hostCoreFinishTime = [[0] * rs.hostCore[i] for i in range(rs.numHost)]

    rs.sortedJobIdx = []
    rs.optimalRs = 0








