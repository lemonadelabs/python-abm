'''
Created on 2/09/2014

@author: achim
'''
import tables
import types
from multiprocessing import Process, Pipe, Queue, JoinableQueue # @UnusedImport

import numpy

from ABM.reporting import offloadedReporting
from ABM import fsmAgent
from ABM.scenario import scenario

class HDFLoggingProcess(Process):

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

                    if type(parameters) is dict:                        
                        if "/parameters" in theFile:
                            parameterTable=theFile.root.parameters
                        else:
                            parameterTable=theFile.create_table("/", "parameters", HDFLoggingProcess.parameterTableFormat)
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
                    elif type(parameters) is scenario:
                        print("writing scenarios")
                        parameters.writeToHDF(theFile.root, 'scenario')
                    else:
                        print("unsupported type: {}".format(type(parameters)))
        
                # need a table def
                # and a transition log
                elif msg[0]=="registerTransitionType":
                    # change lists to enumerations!
                    theColumns={}
                    for name, col in msg[2].items():
                        if type(col) is dict:
                            col=tables.EnumCol(tables.Enum(col), "start", "uint16") # @UndefinedVariable
                            #col=tables.EnumCol(tables.Enum(col), "start", "uint16") # @UndefinedVariable
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
                    row["fromState"]=fromState
                    row["toState"]=toState
                    row["dwellTime"]=t2-t1
                    row["effort"]=effort
        
                    if len(msg)>2:
                        for name, value in msg[3].items():
                            if type(value) is str:
                                row[name]=numpy.array(value.encode(), dtype="S")
                            else:
                                row[name]=value
                    row.append()
                    del table, row
    
                # also a progress table
                elif msg[0]=="progress":
                    # if not there, create new table
                    if "/progress" not in theFile:
                        theFile.create_table('/', 'progress', HDFLoggingProcess.hdfProgressTable)
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
            del self.transitionsPipe
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
        self.loggingProcess=HDFLoggingProcess(filename, recvPipe)
        self.loggingProcess.start()
        recvPipe.close() # is open on the other side!

    def changeLoggingFile(self, filename):
        self.send(["changeFile", filename])
        # replay all the table definitions
        for agentType, tableDef in self.speciesTables.items():
            self.send(["registerTransitionType", str(agentType.__name__), tableDef])

class hdfLogger:
    # no offloading here

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
        if type(parameters) is dict:
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
        elif type(parameters) is scenario:
            print("writing scenarios")
            parameters.writeToHDF(self.theFile.root, 'scenario')
        else:
            print("unsupported type: {}".format(type(parameters)))

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
    
    def registerTransitionTable(self, agent, extraColumns={}, stateNames=None):
        agentType=type(agent)
        if agentType not in self.speciesTables:
            # create agent table on the fly
            # initialize the enum list
            if stateNames is None:
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
            
            descriptionDict={"timeStamp": tables.Float64Col(), # @UndefinedVariable
                              "fromState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                              "toState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                              "dwellTime":tables.Float32Col(), # @UndefinedVariable
                              "effort": tables.Float32Col(),   # @UndefinedVariable
                              "agentId": tables.UInt64Col()}   # @UndefinedVariable

            for k,v in extraColumns.items():
                if type(v) is str:
                    v=eval(v)
                descriptionDict[k]=v
            
            transitions=type("transitions", (tables.IsDescription,), descriptionDict)  

            theTable=self.theFile.create_table("/transitionLogs", agentType.__name__, transitions)
            self.speciesTables[agentType]=theTable
            self.speciesRows[agentType]=theTable.row
            self.logMessage("allocated transition table for {:s}".format(agentType.__name__))
        else:
            # re-registering
            pass
        
    def reportTransition(self, agent, s1, s2, t1, t2, **other):
        if not isinstance(agent, fsmAgent):
            raise TypeError("{:s} is not an fsmAgent".format(str(agent)))

        agentType=type(agent)
        if agentType not in self.speciesTables:
            self.registerTransitionTable(agent)
            
        stateEnum=self.speciesEnum[agentType]
        theTransition=self.speciesRows[agentType]
            
        theTransition["agentId"]=agent.agentId
        theTransition["timeStamp"]=t2
        theTransition["fromState"]=stateEnum[s1 if s1 else "start"]
        theTransition["toState"]=stateEnum[s2 if s2 else "start"]
        theTransition["dwellTime"]=t2-t1
        theTransition["effort"]=agent.effort
        # add the others
        for name, value in other.items():
            if type(value) is str:
                value=numpy.array(value.encode(), dtype="S")
            theTransition[name]=value
        
        # fill in the values
        theTransition.append()
        del theTransition
    
    def logMessage(self, message):
        #for l in logtext.splitlines():
        #        dump_file.root.log.append()
        self.theLog.append(numpy.array([message], dtype="S120"))
