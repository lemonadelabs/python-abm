'''
Created on 2/09/2014

@author: achim
'''
import tables
import types

from ABM import fsmAgent

class hdfLogger:
    
    def __init__(self, logFileName):
        
        self.speciesTables={}
        self.speciesRows={}
        
        # needing another place to flush the tables and close the file
        self.theFile=tables.open_file(logFileName, "w", driver="H5FD_CORE")
        self.theFile.create_group("/", "transitionLogs")
    
        # create a table for messages
        # doesn't look smart in hdfview, but works out of the box
        self.theLog=self.theFile.create_vlarray("/", "log", tables.VLStringAtom())

    class parameterTable(tables.IsDescription):
        # inherits the limits of the ABMsimulations.parameters table
        varname=tables.StringCol(64) #@UndefinedVariable
        varType=tables.EnumCol(tables.Enum(["INT", "FLOAT", "BOOL", "STR", "RUN"])) #@UndefinedVariable
        varValue=tables.StringCol(128) #@UndefinedVariable

    def writeParameters(self, parameters, runParameters={}):
        # create a table for it?!
        # pretty much like a parameters table?!
        # or pickle an object?!
        parametersTable=self.theFile.createTable("/", "parameters", hdfLogger.parameterTable)
        parametersRow=parametersTable.row()
        
        varTypeEnum=hdfLogger.parameterTable.varType
        varTypeDict={varTypeEnum["INT"]:int,
                    varTypeEnum["STR"]:str,
                    varTypeEnum["FLOAT"]:float,
                    varTypeEnum["BOOL"]:bool}
        runType=varTypeEnum["RUN"]
        
        for k,v in parameters.items():
            varType=varTypeDict[type(v)]
            parametersRow["varName"]=str(k)
            parametersRow["varType"]=varType
            parametersRow["varValue"]=str(v)
            parametersRow.append()
            
        for k,v in runParameters.items():
            parametersRow["varName"]=str(k)
            parametersRow["varType"]=runType
            parametersRow["varValue"]=str(v)
            parametersRow.append()
        
        del parametersRow
        parametersTable.close()
        
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

            type(self).stateEnum=stateEnum=tables.Enum(list(stateNames))
            transitions=type("transitions", (tables.IsDescription,), {"timeStamp": tables.Float64Col(), # @UndefinedVariable
                                                                      "fromState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "toState": tables.EnumCol(stateEnum, "start", "uint16"), # @UndefinedVariable
                                                                      "dwellTime":tables.Float32Col(), # @UndefinedVariable
                                                                      "effort": tables.Float32Col(),   # @UndefinedVariable
                                                                      "agentId": tables.UInt64Col()})  # @UndefinedVariable

            # needing another place to flush the tables and close the file
            
            theTable=self.theFile.createTable("/transitionLogs", agentType.__name__, transitions)
            self.speciesTables[agentType]=theTable
            self.speciesRows[agentType]=theTable.row
            self.logMessage("allocated transition table for {:s}".format(agentType.__name__))

        theTransition=self.speciesRows[agentType]
        theTransition["agentId"]=agent.agentId
        theTransition["timeStamp"]=t2
        theTransition["fromState"]=self.stateEnum[s1 if s1 else "start"]
        theTransition["toState"]=self.stateEnum[s2 if s2 else "start"]
        theTransition["dwellTime"]=t2-t1
        theTransition["effort"]=agent.effort
        # fill in the values
        theTransition.append()
        del theTransition
    
    def logMessage(self, message):
        self.theLog.append(str(message))
