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
        
    def __del__(self):
        if hasattr(self, "speciesTables"):
            for table in self.speciesTables.values():
                table.flush()
            if hasattr(self, "speciesRows"):
                del self.speciesRows
            del self.speciesTables
        
        if hasattr(self, "theFile"):
            self.theFile.close()

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
    
    def logMessage(self, message):
        self.theLog.append(str(message))
