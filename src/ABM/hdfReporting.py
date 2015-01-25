'''
Created on 2/09/2014

@author: achim
'''
import tables
import sys
import time
import threading
from multiprocessing import Process, Pipe, Queue, JoinableQueue # @UnusedImport
import types
import numpy

from ABM.reporting import offloadedReporting

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
            timeInterval=0.0
            for runTime, timeInterval in self.schedule:
                if theRunTime>runTime:
                    break
            self.quitFlag.wait(max(timeInterval, self.minInterval))

        try:
            self.logAction(self.theWorld)
        except Exception:
            self.quitFlag.set()


class OffloadHDF(Process):

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

    def __init__(self, hdfFileName, transitionsPipe):
        self.hdfFileName=hdfFileName
        self.transitionsPipe=transitionsPipe
        super().__init__()

    def run(self):
 
        theFile=tables.open_file(self.hdfFileName, "w") #, driver="H5FD_CORE")
        theFile.create_group("/", "transitionLogs")
        theLog=theFile.create_earray(where=theFile.root,
                                     name="log",
                                     atom=tables.StringAtom(itemsize=120),
                                     shape=(0,),
                                     title="log messages",
                                     filters=tables.Filters(complevel=9, complib='zlib'))
        speciesTables={}
    
        try:
            # do a loop!
            while True:
                try:
                    msg=self.transitionsPipe.recv()
                    #msg=messagequeue.get()
                except EOFError:
                    break
                if msg[0]=="parameters":
                    # expect two dictionaries
                    parameters, runParameters=msg[1], msg[2]
                    if "/parameters" in theFile:
                        parameterTable=theFile.root.parameters
                    else:
                        parameterTable=theFile.create_table("/", "parameters", OffloadHDF.parameterTableFormat)
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
                    
                    parameterTable.close()
                    del parameterRow, parameterTable
        
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
                    speciesTables[msg[1]]=theFile.create_table("/transitionLogs", msg[1], transitions, filters=tables.Filters(complevel=9, complib="lzo", least_significant_digit=3))
        
                elif msg[0]=="changeFile":
                    # close tables and file
                    for t in speciesTables.values():
                        t.close()
                        del t
                    del speciesTables
                    theLog.close()
                    del theLog
                    theFile.close()
                    del theFile

                    # set new file name
                    self.hdfFileName=msg[1]
                    # open new one
                    theFile=tables.open_file(self.hdfFileName, "w") #, driver="H5FD_CORE")
                    theFile.create_group("/", "transitionLogs")
                    theLog=theFile.create_earray(where=theFile.root,
                                                 name="log",
                                                 atom=tables.StringAtom(itemsize=120),
                                                 shape=(0,),
                                                 title="log messages",
                                                 filters=tables.Filters(complevel=9, complib='zlib'))
                    speciesTables={}
                    # expecting replay of species tables
        
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
                    del table, row
    
                # also a progress table
                elif msg[0]=="progress":
                    # if not there, create new table
                    if "/progress" not in theFile:
                        theFile.create_table('/', 'progress', OffloadHDF.hdfProgressTable)
                    # add values as they are...
                    theFile.root.progress.append([msg[1]])
                    
                elif msg[0]=="message":
                    theLog.append(numpy.array([str(msg[1])], dtype="S120"))
        
                elif msg[0]=="end":
                    break
                
                else:
                    print("unknown type {}".format(msg[0]))
        except:
            raise
        finally:
            #messagequeue.close()
            self.transitionsPipe.close()
            #print("finished ", messagepipe)
            # done, be pedantic about closing all resources
            for t in speciesTables.values():
                t.close()
                del t
            del speciesTables
            theLog.close()
            del theLog
            theFile.close()
            del theFile


class offloadedHdfLogger(offloadedReporting):
    
    def __init__(self, filename):
        super().__init__()
        recvPipe, msgPipe = Pipe(duplex=False)
        self.outputPipes.append(msgPipe)
        self.loggingProcess=OffloadHDF(filename, recvPipe)
        # queue is even slower!
        #self.msgQueue=JoinableQueue()
        #self.loggingProcess=Process(target=offloadedHdfLogger.offloadedProcess, args=(filename, self.msgQueue))
        #print("started with", self.recvPipe)
        self.loggingProcess.start()
        recvPipe.close() # is open on the other side!

    def changeLoggingFile(self, filename):
        self.send(["changeFile", filename])
        # replay all the table definitions
        for agentType, tableDef in self.speciesTables.items():
            self.send(["registerTransitionType", str(agentType.__name__), tableDef])

class hdfLogger:
    # no offloading here

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
