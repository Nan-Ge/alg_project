from ResourceScheduler import ResourceScheduler
from Util import generator

taskType = 2
caseID = 1
rs = ResourceScheduler(taskType=taskType, caseID=caseID)
# generator(rs, taskType)
optRs = rs.schedule()
optRs.outputSolutionFromBlock()
optRs.outputSolutionFromCore()

