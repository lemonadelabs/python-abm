'''
Created on 2/09/2014

@author: achim
'''
import tables
import resource #@UnresolvedImport
import os
import sys
import time
import threading
from multiprocessing import Process, Pipe # @UnresolvedImport

import types
import numpy

from ABM import fsmAgent

class progressMonitor(threading.Thread):
    
    # the schedule contains tuples (a,b)
    # meaning: till a seconds log in an interval of b seconds
    
    schedule=[(200,100), (20,10), (2,1), (0.2, 0.1)]
    minInterval=0.05

    def __init__(self, theWorld, logOutput):
        self.theWorld=theWorld
        self.logAction=logOutput
        self.quitFlag=threading.Event()
        self.quitFlag.clear()
        threading.Thread.__init__(self)

    def run(self):
        self.startTime=time.time()
        
        while not self.quitFlag.is_set():
            try:
                self.logAction(self.theWorld)
            except Exception as e:
                print("Error in progress logging: {}".format(e), file=sys.stderr)
                self.quitFlag.set()
                return
            # determine next tick
            theRunTime=time.time()-self.startTime
            timeInterval=0
            for runTime, timeInterval in self.schedule:
                if theRunTime>runTime:
                    break
            self.quitFlag.wait(max(timeInterval, self.minInterval))

        try:
            self.logAction(self.theWorld)
        except Exception:
            self.quitFlag.set()

class offloadedHdfLogger:
    
    class parameterTableFormat(tables.IsDescription):
        # inherits the limits of the ABMsimulations.parameters table
        varName=tables.StringCol(64) #@UndefinedVariable
        varType=tables.EnumCol(tables.Enum(["INT", "FLOAT", "BOOL", "STR", "RUN"]), "STR", "uint8") #@UndefinedVariable
        varValue=tables.StringCol(128) #@UndefinedVariable
    
    class hdfProgressTable(tables.IsDescription):
        timeStep = tables.Float64Col(pos=1) # @UndefinedVariable
        machineTime = tables.Float64Col(pos=2) # @UndefinedVariable
        agentNo = tables.Int64Col(pos=3) # @UndefinedVariable
        eventNo = tables.Int64Col(pos=4) # @UndefinedVariable
        memSize = tables.Int64Col(pos=5) # @UndefinedVariable
    
    @staticmethod
    def offloadedProcess(logFileName, messagepipe):
        theFile=tables.open_file(logFileName, "w") #, driver="H5FD_CORE")
        theFile.create_group("/", "transitionLogs")

        speciesTables={}

        # do a loop!        
        while True:
            try:
                msg=messagepipe.recv()
            except EOFError:
                break
            if msg[0]=="parameters":
                # expect two dictionaries
                parameters, runParameters=msg[1], msg[2]
                if "/parameters" in theFile:
                    parameterTable=theFile.root.parameters
                else:
                    parameterTable=theFile.create_table("/", "parameters", offloadedHdfLogger.parameterTableFormat)
                parameterRow=parameterTable.row
                
                varTypeEnum=parameterTable.coldescrs["varType"].enum
                varTypeDict={int:   varTypeEnum["INT"],
                             str:   varTypeEnum["STR"],
                             float: varTypeEnum["FLOAT"],
                             bool:  varTypeEnum["BOOL"]}
                runType=varTypeEnum["RUN"]
                
                for k,v in parameters.items():
                    varType=varTypeDict[type(v)]
                    parameterRow["varName"]=str(k)
                    parameterRow["varType"]=varType
                    parameterRow["varValue"]=str(v)
                    parameterRow.append()
                    
                for k,v in runParameters.items():
                    parameterRow["varName"]=str(k)
                    parameterRow["varType"]=runType
                    parameterRow["varValue"]=str(v)
                    parameterRow.append()
                
                del parameterRow
                parameterTable.close()

            # need a table def
            # and a transition log
            elif msg[0]=="registerTransitionType":
                # change lists to enumerations!
                theColumns={}
                for name, col in msg[2].items():
                    if type(col) in [list,tuple]:
                        col=tables.EnumCol(tables.Enum(col), "start", "uint16") # @UndefinedVariable
                    elif type(col) is str:
                        col=eval(col) # ToDo: remove eval
                    theColumns[name]=col
                        
                # gets species name and table format as dict
                transitions=type("transitions", (tables.IsDescription,), theColumns) 
                theTable=theFile.create_table("/transitionLogs", msg[1], transitions)
                speciesTables[msg[1]]=theTable

            elif msg[0]=="logTransition":
                # gets species name and values in order as defined by the table format
                # todo: check the format!
                table=speciesTables[msg[1]]
                row=table.row
                agentId, t1, t2, fromState, toState, effort=msg[2]
                row["agentId"]=agentId
                row["timeStamp"]=t2
                row["fromState"]=table.coldescrs["fromState"].enum[fromState if fromState else "start"]
                row["toState"]=table.coldescrs["toState"].enum[toState if toState else "start"]
                row["dwellTime"]=t2-t1
                row["effort"]=effort

                if len(msg)>2:
                    for name, value in msg[3].items():
                        row[name]=value                
                row.append()
            
            # also a progress table
            elif msg[0]=="progress":
                # if not there, create new table
                if "/progress" not in theFile:
                    theFile.create_table('/', 'progress', offloadedHdfLogger.hdfProgressTable)
                # add values as they are...
                theFile.root.progress.append([msg[1]])   
            elif msg[0]=="end":
                break
            
            else:
                print("unknown type {}".format(msg[0]))

        # done
        del speciesTables
        # and end
        theFile.close()
        
    def __init__(self, filename):
        self.speciesTables={}
        self.msgPipe, self.recvPipe= Pipe()
        self.loggingProcess=Process(target=self.offloadedProcess, args=(filename, self.recvPipe))
        self.loggingProcess.start()
        
    def __del__(self):
        if hasattr(self, "msgPipe"):
            self.msgPipe.send(["end"])
            self.recvPipe.close()
            self.msgPipe.close()
        if hasattr(self, "loggingProcess"):
            self.loggingProcess.join()

    def registerTransitionTable(self, agent, extraParameterTypes={}):
        # see whether this is already registered
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))
        
        agentType=type(agent)
        # initialize the enum list
        stateNames=set(["start", "end"]) # default, must be there
        for attr in dir(agent):
            nameSplit=attr.split("_")
            if len(nameSplit)>=2 and nameSplit[0] in ["enter", "leave", "activity"]:
                attr_object=getattr(agent, attr)
                attr_name="_".join(nameSplit[1:])
                if type(attr_object) is types.MethodType:
                    stateNames.add(attr_name)

        stateNames=list(stateNames)
        stateNames.sort()
        tableDef={ "timeStamp": tables.Float64Col(), # @UndefinedVariable
                  "fromState": stateNames,
                  "toState": stateNames,
                  "dwellTime":tables.Float32Col(), # @UndefinedVariable
                  "effort": tables.Float32Col(),   # @UndefinedVariable
                  "agentId": tables.UInt64Col()}  # @UndefinedVariable
        
        tableDef.update(extraParameterTypes)
        # and then handle the extra parameters!
        # how to get the extra parameters?
        
        self.msgPipe.send(["registerTransitionType", str(agentType.__name__), tableDef])
        self.speciesTables[agentType]=tableDef

    def reportTransition(self, agent, s1, s2, t1, t2, **extraParams):

        # see whether this is already registered
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType=type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            self.registerTransitionTable(agent)
        
        # that's the order expected for msg[2]: agentId, t1, t2, fromState, toState, effort
        self.msgPipe.send(["logTransition", str(agentType.__name__), [agent.agentId, t1, t2, s1, s2, agent.effort], extraParams])

    def logMessage(self, message):
        print("logMessage not implemented")
        pass
        
    def writeParameters(self, parameters, runParameters={}):
        self.msgPipe.send(["parameters", parameters, runParameters])
       
    def logProgress(self, theWorld=None):
        if not hasattr(self, "startTime"):
            self.startTime=time.time()
                    
        stats=open("/proc/{:d}/statm".format(os.getpid())).read().split(" ")
        memSize=int(stats[5])*resource.getpagesize()
        if theWorld is not None:
            theData=(float(theWorld.wallClock),
                     time.time()-self.startTime,
                     sum([len(a) for a in theWorld.theAgents.values()]),
                     len(theWorld.theScheduler.schedule_heap),
                     memSize)
        else:
            theData=(0.0, time.time()-self.startTime, 0, 0, memSize)
        self.msgPipe.send(["progress", theData])

class hdfLogger:

    class parameterTableFormat(tables.IsDescription):
        # inherits the limits of the ABMsimulations.parameters table
        varName=tables.StringCol(64) #@UndefinedVariable
        varType=tables.EnumCol(tables.Enum(["INT", "FLOAT", "BOOL", "STR", "RUN"]), "STR", "uint8") #@UndefinedVariable
        varValue=tables.StringCol(128) #@UndefinedVariable
    
    def __init__(self, logFileName):
        
        self.speciesTables={}
        self.speciesRows={}
        self.speciesEnum={}
        
        # needing another place to flush the tables and close the file
        self.theFile=tables.open_file(logFileName, "w") #, driver="H5FD_CORE")
        self.theFile.create_group("/", "transitionLogs")
    
        # create a table for messages
        # doesn't look smart in hdfview, but works out of the box
        #self.theLog=self.theFile.create_vlarray("/", "log", tables.VLStringAtom())
        self.theLog=self.theFile.create_earray(where=self.theFile.root,
                                               name="log",
                                               atom=tables.StringAtom(itemsize=120),
                                               shape=(0,),
                                               title="log messages",
                                               filters=tables.Filters(complevel=9, complib='zlib'))

    def writeParameters(self, parameters, runParameters={}):
        # create a table for it?!
        # pretty much like a parameters table?!
        # or pickle an object?!
        parameterTable=self.theFile.create_table("/", "parameters", hdfLogger.parameterTableFormat)
        parameterRow=parameterTable.row
        
        varTypeEnum=parameterTable.coldescrs["varType"].enum
        varTypeDict={int:   varTypeEnum["INT"],
                     str:   varTypeEnum["STR"],
                     float: varTypeEnum["FLOAT"],
                     bool:  varTypeEnum["BOOL"]}
        runType=varTypeEnum["RUN"]
        
        for k,v in parameters.items():
            varType=varTypeDict[type(v)]
            parameterRow["varName"]=str(k)
            parameterRow["varType"]=varType
            parameterRow["varValue"]=str(v)
            parameterRow.append()
            
        for k,v in runParameters.items():
            parameterRow["varName"]=str(k)
            parameterRow["varType"]=runType
            parameterRow["varValue"]=str(v)
            parameterRow.append()
        
        del parameterRow
        parameterTable.close()
        
    def __del__(self):
        if hasattr(self, "speciesRows"):
            del self.speciesRows
        if hasattr(self, "speciesTables"):
            for v in self.speciesTables.values():
                v.close()
            del self.speciesTables
        if hasattr(self, "theLog"):
            self.theLog.close()
            del self.theLog
        if hasattr(self, "theFile"):
            self.theFile.close()
            del self.theFile

    def reportTransition(self, agent, s1, s2, t1, t2):
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType=type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            # initialize the enum list
            stateNames=set(["start", "end"]) # default, must be there
            for attr in dir(agent):
                nameSplit=attr.split("_")
                if len(nameSplit)>=2 and nameSplit[0] in ["enter", "leave", "activity"]:
                    attr_object=getattr(agent, attr)
                    attr_name="_".join(nameSplit[1:])
                    if type(attr_object) is types.MethodType:
                        stateNames.add(attr_name)

            stateNames=list(stateNames)
            stateNames.sort()
            self.speciesEnum[agentType]=stateEnum=tables.Enum(stateNames)
            transitions=type("transitions", (tables.IsDescription,), {"timeStamp": tables.Float64Col(), # @UndefinedVariable
                                                                      "fromState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "toState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "dwellTime":tables.Float32Col(), # @UndefinedVariable
                                                                      "effort": tables.Float32Col(),   # @UndefinedVariable
                                                                      "agentId": tables.UInt64Col()})  # @UndefinedVariable

            theTable=self.theFile.create_table("/transitionLogs", agentType.__name__, transitions)
            self.speciesTables[agentType]=theTable
            theTransition=self.speciesRows[agentType]=theTable.row
            self.logMessage("allocated transition table for {:s}".format(agentType.__name__))
        else:
            stateEnum=self.speciesEnum[agentType]
            theTransition=self.speciesRows[agentType]
            
        theTransition["agentId"]=agent.agentId
        theTransition["timeStamp"]=t2
        theTransition["fromState"]=stateEnum[s1 if s1 else "start"]
        theTransition["toState"]=stateEnum[s2 if s2 else "start"]
        theTransition["dwellTime"]=t2-t1
        theTransition["effort"]=agent.effort
        # fill in the values
        theTransition.append()
        del theTransition
    
    def logMessage(self, message):
        #for l in logtext.splitlines():
        #        dump_file.root.log.append()
        self.theLog.append(numpy.array([message], dtype="S120"))
