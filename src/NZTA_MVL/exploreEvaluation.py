'''
Created on 3/09/2014

@author: achim
'''

import sys
import os.path
import tables

def iterJourneys(transitionFile, agentTypeName):
    theFile=tables.open_file(transitionFile, "r")
    transitionTable=theFile.get_node("/transitionLogs", agentTypeName)

    journeys={} # maps agent id to journey started
    # assuming from and to enums are the same
    statesEnum=transitionTable.coldescrs["fromState"].enum

    colIndexes=dict((v,k) for k,v in enumerate(transitionTable.description._v_names))
    startId=statesEnum["start"]
    stopId=statesEnum["end"]
    
    for transition in transitionTable.iterrows():
        
        agentId=transition["agentId"]
        if agentId in journeys:
            # add a copy of this row (maybe more efficient with fetch_all_fields)
            journeys[agentId].append(transition.fetch_all_fields())
        
            if transition["toState"]==stopId:
                completedJourney=journeys[agentId]
                del journeys[agentId]
                journeyStages=[(statesEnum(step[colIndexes["fromState"]]),
                                step[colIndexes["timeStamp"]],
                                step[colIndexes["effort"]]) for step in completedJourney]
                yield (agentId, completedJourney[0][colIndexes["timeStamp"]], journeyStages) # do some mapping of names
        
        if transition["fromState"]==startId:
            j=journeys.get(agentId, [])
            j.append(transition.fetch_all_fields())
            journeys[agentId]=j

    transitionTable.close()
    theFile.close()
    
def generateTransitionCounts(transitionFile):
    theFile=tables.open_file(transitionFile, "r")
    
    transitionDict={}
    transitionGroup=theFile.get_node("/transitionLogs")
    for tt in transitionGroup:
        if isinstance(tt, tables.Table):
            agentTypeName=tt.name
        
            # read table
            transitionTable=theFile.get_node("/transitionLogs", agentTypeName)
            statesEnum=transitionTable.coldescrs["fromState"].enum # hope, from and to enums are the same
            
            for transition in transitionTable.iterrows():
                fromState=statesEnum(transition["fromState"])
                toState=statesEnum(transition["toState"])
                trans=(agentTypeName, fromState, toState)
                transitionDict[trans]=transitionDict.get(trans, 0)+1
                trans=("all", fromState, toState)
                transitionDict[trans]=transitionDict.get(trans, 0)+1

    theFile.close()

    # change to string labels
    return transitionDict

def generateAvgDwellTimes(transitionFile):
    
    theFile=tables.open_file(transitionFile, "r")
    
    transitionGroup=theFile.get_node("/transitionLogs")
    sumDwellTimes={}
    nDwellTimes={}
    for tt in transitionGroup:
        if isinstance(tt, tables.Table):
            agentTypeName=tt.name
    
            transitionTable=theFile.get_node("/transitionLogs", agentTypeName)
            # trawl through enitre group
            
            statesEnum=transitionTable.coldescrs["fromState"].enum # hope, from and to enums are the same
        
            for transition in transitionTable.iterrows():
                fromState=statesEnum(transition["fromState"])
                dt=transition["dwellTime"]
                state=(agentTypeName, fromState)
                if state not in nDwellTimes:
                    sumDwellTimes[state]=dt
                    nDwellTimes[state]=1
                else:
                    sumDwellTimes[state]+=dt
                    nDwellTimes[state]+=1

                state=("all", fromState)
                if state not in nDwellTimes:
                    sumDwellTimes[state]=dt
                    nDwellTimes[state]=1
                else:
                    sumDwellTimes[state]+=dt
                    nDwellTimes[state]+=1

    theFile.close()

    # convert into avg and change names        
    return dict([(s, sdt/nDwellTimes[s]) for s,sdt in sumDwellTimes.items()])

def generateSQL(theFile):
    runID=os.path.splitext(os.path.basename(theFile))[0]
    
    tableDef="""CREATE TABLE IF NOT EXISTS `postproc_transitions` (
  `experimentID` CHAR(32) NOT NULL,
  `startState` VARCHAR(45) NULL,
  `endState` VARCHAR(45) NULL,
  `persona` VARCHAR(45) NULL,
  `count` INT NULL
  )"""
    print(tableDef,';')

    for (a, s1, s2), n in generateTransitionCounts(theFile).items():
        print("INSERT INTO `postproc_transitions` "
              "(`experimentID`, `startState`, `endState`, `persona`, `count`) VALUES "
              "('{0:s}',        '{1:s}',      '{2:s}',    '{3:s}',    {4:d})".format(runID, s1, s2, a, n),";")


    tableDef="""CREATE TABLE IF NOT EXISTS `postproc_avgdwelltimes` (
  `experimentID` CHAR(32) NOT NULL,
  `state` VARCHAR(45) NULL,
  `persona` VARCHAR(45) NULL,
  `avgdwelltime` INT NULL
  )"""
    print(tableDef,";")

    for (a, s), dt in generateAvgDwellTimes(theFile).items():
        print("INSERT INTO `postproc_avgdwelltimes` "
              "(`experimentID`, `state`, `persona`, `avgdwelltime`) VALUES "
              "('{0:s}', '{1:s}',      '{2:s}',     '{3:f}')".format(runID, s, a, dt),";")

if __name__ == '__main__':
    generateSQL(sys.argv[1])

#     #print(tt)
#     tt=iterJourneys(theFile, "maryOwner")
#     for i,t, journey in tt:
#         print(i)
#     del tt
    